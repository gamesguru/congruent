use std::{net::IpAddr, str::FromStr};

use crate::{Error, Result, err};

/// A CIDR block with canonicalized network bits and a precomputed mask.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IpCidr {
	V4 {
		network: u32,
		mask: u32,
	},
	V6 {
		network: u128,
		mask: u128,
	},
}

impl FromStr for IpCidr {
	type Err = Error;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		let (ip, prefix) = value
			.split_once('/')
			.map_or((value, None), |(ip, prefix)| (ip, Some(prefix)));
		let ip = ip
			.parse::<IpAddr>()
			.map_err(|_| err!(Config("ip_range_denylist", "Invalid IP address in {value}")))?;

		match ip {
			| IpAddr::V4(ip) => {
				let prefix = prefix.map_or(Ok(32), |prefix| {
					prefix.parse::<u32>().map_err(|_| {
						err!(Config("ip_range_denylist", "Invalid IPv4 prefix in {value}"))
					})
				})?;
				if prefix > 32 {
					return Err(err!(Config("ip_range_denylist", "IPv4 prefix must be <= 32")));
				}
				let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
				Ok(Self::V4 { network: u32::from(ip) & mask, mask })
			},
			| IpAddr::V6(ip) => {
				let prefix = prefix.map_or(Ok(128), |prefix| {
					prefix.parse::<u32>().map_err(|_| {
						err!(Config("ip_range_denylist", "Invalid IPv6 prefix in {value}"))
					})
				})?;
				if prefix > 128 {
					return Err(err!(Config("ip_range_denylist", "IPv6 prefix must be <= 128")));
				}
				let mask = u128::MAX.checked_shl(128 - prefix).unwrap_or(0);
				Ok(Self::V6 { network: u128::from(ip) & mask, mask })
			},
		}
	}
}

impl<'de> serde::Deserialize<'de> for IpCidr {
	fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let value = <String as serde::Deserialize>::deserialize(deserializer)?;
		value.parse().map_err(serde::de::Error::custom)
	}
}

impl IpCidr {
	#[must_use]
	pub fn contains(&self, target: &IpAddr) -> bool {
		let target = match target {
			| IpAddr::V6(target) => target
				.to_ipv4_mapped()
				.map_or(IpAddr::V6(*target), IpAddr::V4),
			| target => *target,
		};

		match (self, target) {
			| (Self::V4 { network, mask }, IpAddr::V4(target)) =>
				u32::from(target) & mask == *network,
			| (Self::V6 { network, mask }, IpAddr::V6(target)) =>
				u128::from(target) & mask == *network,
			| _ => false,
		}
	}
}
