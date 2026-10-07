use std::sync::Arc;

#[derive(Default)]
pub struct State;

impl State {
	#[must_use]
	pub fn new() -> Self { Self }
}

pub struct Capture;

impl Capture {
	#[must_use]
	pub fn new(_state: &Arc<State>) -> Arc<Self> { Arc::new(Self) }

	pub fn start(self: &Arc<Self>) {}
}
