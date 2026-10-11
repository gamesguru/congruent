use std::{
	any::Any,
	future::Future,
	panic::AssertUnwindSafe,
	pin::Pin,
	sync::{Arc, OnceLock},
	task::{Context, Poll},
};

use async_executor::Executor;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};

static EXECUTOR: OnceLock<Arc<Executor<'static>>> = OnceLock::new();

fn executor() -> Arc<Executor<'static>> {
	Arc::clone(EXECUTOR.get_or_init(|| {
		let executor = Arc::new(Executor::new());
		let workers = std::thread::available_parallelism().map_or(1, usize::from);
		for index in 0..workers {
			let executor = Arc::clone(&executor);
			std::thread::Builder::new()
				.name(format!("conduwuit-smol-{index}"))
				.spawn(move || smol::block_on(executor.run(std::future::pending::<()>())))
				.expect("failed to start conduwuit executor worker");
		}
		executor
	}))
}

#[derive(Clone, Debug)]
pub struct RuntimeHandle {
	executor: Arc<Executor<'static>>,
}

#[derive(Clone, Copy, Debug)]
pub struct TimeoutError;

impl std::fmt::Display for TimeoutError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("future timed out")
	}
}

impl std::error::Error for TimeoutError {}

pub async fn timeout<F>(
	duration: std::time::Duration,
	future: F,
) -> Result<F::Output, TimeoutError>
where
	F: Future,
{
	use futures::future::{Either, select};

	match select(Box::pin(future), Box::pin(smol::Timer::after(duration))).await {
		| Either::Left((output, _)) => Ok(output),
		| Either::Right((..)) => Err(TimeoutError),
	}
}

pub async fn timeout_at<F>(
	deadline: std::time::Instant,
	future: F,
) -> Result<F::Output, TimeoutError>
where
	F: Future,
{
	timeout(deadline.saturating_duration_since(std::time::Instant::now()), future).await
}

impl Default for RuntimeHandle {
	fn default() -> Self { Self::new() }
}

impl RuntimeHandle {
	#[must_use]
	pub fn new() -> Self { Self { executor: executor() } }

	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		let future = async move {
			match AssertUnwindSafe(future).catch_unwind().await {
				| Ok(value) => Ok(value),
				| Err(panic) => {
					log::error!("smol task panicked: {}", crate::debug::panic_str(&panic));
					Err(JoinError::Panic(panic))
				},
			}
		};

		JoinHandle { task: Some(self.executor.spawn(future)) }
	}

	pub fn spawn_blocking<F, T>(&self, function: F) -> JoinHandle<T>
	where
		F: FnOnce() -> T + Send + 'static,
		T: Send + 'static,
	{
		self.spawn(blocking::unblock(function))
	}
}

/// A collection of tasks running on the conduwuit executor.
pub struct JoinSet<T> {
	set: FuturesUnordered<JoinHandle<T>>,
}

impl<T> Default for JoinSet<T> {
	fn default() -> Self { Self::new() }
}

impl<T> JoinSet<T> {
	#[must_use]
	pub fn new() -> Self { Self { set: FuturesUnordered::new() } }

	pub fn spawn_on<F>(&mut self, future: F, runtime: &RuntimeHandle)
	where
		F: Future<Output = T> + Send + 'static,
		T: Send + 'static,
	{
		self.set.push(runtime.spawn(future));
	}

	pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> { self.set.next().await }

	pub fn abort_all(&mut self) {
		for task in &mut self.set {
			task.abort();
		}
	}

	#[must_use]
	pub fn len(&self) -> usize { self.set.len() }

	#[must_use]
	pub fn is_empty(&self) -> bool { self.set.is_empty() }
}

#[derive(Debug)]
pub enum JoinError {
	Cancelled,
	Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
	#[must_use]
	pub const fn is_cancelled(&self) -> bool { matches!(self, Self::Cancelled) }

	#[must_use]
	pub const fn is_panic(&self) -> bool { matches!(self, Self::Panic(..)) }

	#[must_use]
	pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
		match self {
			| Self::Panic(panic) => panic,
			| Self::Cancelled => panic!("attempted to extract panic from cancelled task"),
		}
	}
}

impl std::fmt::Display for JoinError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			| Self::Cancelled => f.write_str("task was cancelled"),
			| Self::Panic(..) => f.write_str("task panicked"),
		}
	}
}

impl std::error::Error for JoinError {}

#[must_use]
pub struct JoinHandle<T> {
	task: Option<smol::Task<Result<T, JoinError>>>,
}

impl<T> JoinHandle<T> {
	pub fn abort(&mut self) { self.task = None; }
}

impl<T> Future for JoinHandle<T> {
	type Output = Result<T, JoinError>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		let Some(task) = this.task.as_mut() else {
			return Poll::Ready(Err(JoinError::Cancelled));
		};

		match Pin::new(task).poll(cx) {
			| Poll::Pending => Poll::Pending,
			| Poll::Ready(result) => {
				this.task = None;
				Poll::Ready(result)
			},
		}
	}
}

impl<T> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.detach();
		}
	}
}
