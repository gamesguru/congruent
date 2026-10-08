use std::{
	future::Future,
	pin::Pin,
	sync::{Arc, OnceLock},
	task::{Context, Poll},
};

use async_executor::Executor;

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

impl Default for RuntimeHandle {
	fn default() -> Self { Self::new() }
}

impl RuntimeHandle {
	#[must_use]
	pub fn new() -> Self { Self { executor: executor() } }

	#[must_use]
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		JoinHandle { task: Some(self.executor.spawn(future)) }
	}
}

#[must_use]
pub struct JoinHandle<T> {
	task: Option<smol::Task<T>>,
}

impl<T: Send + 'static> JoinHandle<T> {
	pub fn abort(&mut self) {
		if let Some(task) = self.task.take() {
			smol::spawn(async move {
				let _ = task.cancel().await;
			})
			.detach();
		}
	}
}

impl<T> Future for JoinHandle<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		Pin::new(this.task.as_mut().expect("polled completed task")).poll(cx)
	}
}

impl<T> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.detach();
		}
	}
}
