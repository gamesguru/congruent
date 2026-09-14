use std::{fmt::Display, str::FromStr};

use super::PduCount;
use crate::{Error, err};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopoToken {
	pub depth: u64,
	pub pdu_count: PduCount,
}

impl TopoToken {
	#[must_use]
	pub fn is_legacy(&self) -> bool { self.depth == 0 && self.pdu_count != PduCount::min() }

	#[must_use]
	pub fn max() -> Self {
		Self {
			depth: u64::MAX,
			pdu_count: PduCount::max(),
		}
	}

	#[must_use]
	pub fn min() -> Self { Self { depth: 0, pdu_count: PduCount::min() } }
}

impl Display for TopoToken {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "t{}_{}", self.depth, self.pdu_count)
	}
}

impl FromStr for TopoToken {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Some(rest) = s.strip_prefix('t') {
			if let Some((depth_str, count_str)) = rest.split_once('_') {
				let depth = depth_str
					.parse()
					.map_err(|_| err!(Request(InvalidParam("Invalid topo depth"))))?;
				let pdu_count = count_str.parse()?;
				return Ok(Self { depth, pdu_count });
			}
		}
		// Fallback for legacy PduCount tokens
		let pdu_count: PduCount = s.parse()?;
		Ok(Self { depth: 0, pdu_count })
	}
}
