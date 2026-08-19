use std::{
	fmt::{Display, Formatter},
	str::FromStr,
};

use conduwuit::{Error, Result};
use ruma::{UInt, api::client::error::ErrorKind};

#[derive(Debug, Eq, PartialEq)]
pub struct PaginationToken {
	/// Number of rooms already returned across previous pages, used as an
	/// offset to resume a depth-first walk of the space tree.
	pub offset: u64,
	pub limit: UInt,
	pub max_depth: UInt,
	pub suggested_only: bool,
}

impl FromStr for PaginationToken {
	type Err = Error;

	fn from_str(value: &str) -> Result<Self> {
		let mut values = value.split('_');
		let mut pag_tok = || {
			let offset = u64::from_str(values.next()?).ok()?;

			let limit = UInt::from_str(values.next()?).ok()?;
			let max_depth = UInt::from_str(values.next()?).ok()?;
			let slice = values.next()?;
			let suggested_only = if values.next().is_none() {
				if slice == "true" {
					true
				} else if slice == "false" {
					false
				} else {
					None?
				}
			} else {
				None?
			};

			Some(Self { offset, limit, max_depth, suggested_only })
		};

		if let Some(token) = pag_tok() {
			Ok(token)
		} else {
			Err(Error::BadRequest(ErrorKind::InvalidParam, "invalid token"))
		}
	}
}

impl Display for PaginationToken {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}_{}_{}_{}", self.offset, self.limit, self.max_depth, self.suggested_only)
	}
}
