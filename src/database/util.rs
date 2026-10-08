#[derive(Clone, Copy, Debug)]
pub(crate) enum Direction {
	Forward,
	Reverse,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IteratorMode<'a> {
	Start,
	End,
	From(&'a [u8], Direction),
}

macro_rules! unhandled {
	($msg:literal) => {
		unimplemented!($msg)
	};
}

pub(crate) use unhandled;
