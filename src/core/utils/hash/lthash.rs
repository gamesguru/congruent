use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rezzy::LtHash;

/// Converts an LtHash into a little-endian byte vector.
#[must_use]
#[inline]
pub fn lthash_to_bytes(lthash: &LtHash) -> Vec<u8> {
	let mut bytes = vec![0_u8; lthash.0.len().saturating_mul(2)];
	for (i, val) in lthash.0.iter().enumerate() {
		let le = val.to_le_bytes();
		let idx = i.saturating_mul(2);
		bytes[idx] = le[0];
		bytes[idx.saturating_add(1)] = le[1];
	}
	bytes
}

/// Restores an LtHash from a little-endian byte slice.
#[must_use]
#[inline]
pub fn lthash_from_bytes(bytes: &[u8]) -> Option<LtHash> {
	if bytes.len() != 2048 {
		return None;
	}
	let mut arr = [0_u16; 1024];
	for (i, chunk) in bytes.chunks_exact(2).enumerate() {
		arr[i] = u16::from_le_bytes([chunk[0], chunk[1]]);
	}
	Some(LtHash(arr))
}

/// Serializes an LtHash into a `(lattice, digest)` pair.
#[must_use]
pub fn serialize_lthash(lthash: &LtHash) -> (String, String) {
	let bytes = lthash_to_bytes(lthash);
	let lattice = URL_SAFE_NO_PAD.encode(&bytes);

	let mut digest = String::with_capacity(64);
	for b in lthash.digest() {
		use std::fmt::Write;
		let _ = write!(&mut digest, "{b:02x}");
	}

	(lattice, digest)
}

#[cfg(test)]
mod tests {
	use super::*;

	const EXPECTED_LATTICE: &str = concat!(
		"AAABAAIAAwAEAAUABgAHAAgACQAKAAsADAANAA4ADwAQABEAEgATABQAFQAWABcAGAAZABoAGwAcAB0AHgAfACAAIQAiACMA",
		"JAAlACYAJwAoACkAKgArACwALQAuAC8AMAAxADIAMwA0ADUANgA3ADgAOQA6ADsAPAA9AD4APwBAAEEAQgBDAEQARQBGAEcA",
		"SABJAEoASwBMAE0ATgBPAFAAUQBSAFMAVABVAFYAVwBYAFkAWgBbAFwAXQBeAF8AYABhAGIAYwBkAGUAZgBnAGgAaQBqAGsA",
		"bABtAG4AbwBwAHEAcgBzAHQAdQB2AHcAeAB5AHoAewB8AH0AfgB_AIAAgQCCAIMAhACFAIYAhwCIAIkAigCLAIwAjQCOAI8A",
		"kACRAJIAkwCUAJUAlgCXAJgAmQCaAJsAnACdAJ4AnwCgAKEAogCjAKQApQCmAKcAqACpAKoAqwCsAK0ArgCvALAAsQCyALMA",
		"tAC1ALYAtwC4ALkAugC7ALwAvQC-AL8AwADBAMIAwwDEAMUAxgDHAMgAyQDKAMsAzADNAM4AzwDQANEA0gDTANQA1QDWANcA",
		"2ADZANoA2wDcAN0A3gDfAOAA4QDiAOMA5ADlAOYA5wDoAOkA6gDrAOwA7QDuAO8A8ADxAPIA8wD0APUA9gD3APgA-QD6APsA",
		"_AD9AP4A_wAAAQEBAgEDAQQBBQEGAQcBCAEJAQoBCwEMAQ0BDgEPARABEQESARMBFAEVARYBFwEYARkBGgEbARwBHQEeAR8B",
		"IAEhASIBIwEkASUBJgEnASgBKQEqASsBLAEtAS4BLwEwATEBMgEzATQBNQE2ATcBOAE5AToBOwE8AT0BPgE_AUABQQFCAUMB",
		"RAFFAUYBRwFIAUkBSgFLAUwBTQFOAU8BUAFRAVIBUwFUAVUBVgFXAVgBWQFaAVsBXAFdAV4BXwFgAWEBYgFjAWQBZQFmAWcB",
		"aAFpAWoBawFsAW0BbgFvAXABcQFyAXMBdAF1AXYBdwF4AXkBegF7AXwBfQF-AX8BgAGBAYIBgwGEAYUBhgGHAYgBiQGKAYsB",
		"jAGNAY4BjwGQAZEBkgGTAZQBlQGWAZcBmAGZAZoBmwGcAZ0BngGfAaABoQGiAaMBpAGlAaYBpwGoAakBqgGrAawBrQGuAa8B",
		"sAGxAbIBswG0AbUBtgG3AbgBuQG6AbsBvAG9Ab4BvwHAAcEBwgHDAcQBxQHGAccByAHJAcoBywHMAc0BzgHPAdAB0QHSAdMB",
		"1AHVAdYB1wHYAdkB2gHbAdwB3QHeAd8B4AHhAeIB4wHkAeUB5gHnAegB6QHqAesB7AHtAe4B7wHwAfEB8gHzAfQB9QH2AfcB",
		"-AH5AfoB-wH8Af0B_gH_AQACAQICAgMCBAIFAgYCBwIIAgkCCgILAgwCDQIOAg8CEAIRAhICEwIUAhUCFgIXAhgCGQIaAhsC",
		"HAIdAh4CHwIgAiECIgIjAiQCJQImAicCKAIpAioCKwIsAi0CLgIvAjACMQIyAjMCNAI1AjYCNwI4AjkCOgI7AjwCPQI-Aj8C",
		"QAJBAkICQwJEAkUCRgJHAkgCSQJKAksCTAJNAk4CTwJQAlECUgJTAlQCVQJWAlcCWAJZAloCWwJcAl0CXgJfAmACYQJiAmMC",
		"ZAJlAmYCZwJoAmkCagJrAmwCbQJuAm8CcAJxAnICcwJ0AnUCdgJ3AngCeQJ6AnsCfAJ9An4CfwKAAoECggKDAoQChQKGAocC",
		"iAKJAooCiwKMAo0CjgKPApACkQKSApMClAKVApYClwKYApkCmgKbApwCnQKeAp8CoAKhAqICowKkAqUCpgKnAqgCqQKqAqsC",
		"rAKtAq4CrwKwArECsgKzArQCtQK2ArcCuAK5AroCuwK8Ar0CvgK_AsACwQLCAsMCxALFAsYCxwLIAskCygLLAswCzQLOAs8C",
		"0ALRAtIC0wLUAtUC1gLXAtgC2QLaAtsC3ALdAt4C3wLgAuEC4gLjAuQC5QLmAucC6ALpAuoC6wLsAu0C7gLvAvAC8QLyAvMC",
		"9AL1AvYC9wL4AvkC-gL7AvwC_QL-Av8CAAMBAwIDAwMEAwUDBgMHAwgDCQMKAwsDDAMNAw4DDwMQAxEDEgMTAxQDFQMWAxcD",
		"GAMZAxoDGwMcAx0DHgMfAyADIQMiAyMDJAMlAyYDJwMoAykDKgMrAywDLQMuAy8DMAMxAzIDMwM0AzUDNgM3AzgDOQM6AzsD",
		"PAM9Az4DPwNAA0EDQgNDA0QDRQNGA0cDSANJA0oDSwNMA00DTgNPA1ADUQNSA1MDVANVA1YDVwNYA1kDWgNbA1wDXQNeA18D",
		"YANhA2IDYwNkA2UDZgNnA2gDaQNqA2sDbANtA24DbwNwA3EDcgNzA3QDdQN2A3cDeAN5A3oDewN8A30DfgN_A4ADgQOCA4MD",
		"hAOFA4YDhwOIA4kDigOLA4wDjQOOA48DkAORA5IDkwOUA5UDlgOXA5gDmQOaA5sDnAOdA54DnwOgA6EDogOjA6QDpQOmA6cD",
		"qAOpA6oDqwOsA60DrgOvA7ADsQOyA7MDtAO1A7YDtwO4A7kDugO7A7wDvQO-",
		"A78DwAPBA8IDwwPEA8UDxgPHA8gDyQPKA8sDzAPNA84DzwPQA9ED0gPTA9QD1QPWA9cD2APZA9oD2wPcA90D3gPfA-",
		"AD4QPiA-MD5APlA-YD5wPoA-kD6gPrA-wD7QPuA-8D8APxA_ID8wP0A_UD9gP3A_gD-QP6A_sD_AP9A_4D_wM",
	);
	const EXPECTED_DIGEST: &str =
		"5ef0ce69ffde6f004921d360a19bcde51a94c359645de5fac4d66690fa51eabd";

	fn golden_lthash() -> LtHash {
		LtHash(core::array::from_fn(|i| u16::try_from(i).expect("lattice index fits in u16")))
	}

	#[test]
	fn lthash_round_trip_and_serialize_golden_vector() {
		let lthash = golden_lthash();
		let bytes = lthash_to_bytes(&lthash);
		let expected_bytes = URL_SAFE_NO_PAD
			.decode(EXPECTED_LATTICE)
			.expect("golden lattice decodes");

		assert_eq!(bytes, expected_bytes);
		assert_eq!(lthash_from_bytes(&bytes), Some(lthash));

		let (lattice, digest) = serialize_lthash(&lthash);
		assert_eq!(lattice, EXPECTED_LATTICE);
		assert_eq!(digest, EXPECTED_DIGEST);
	}

	#[test]
	fn lthash_from_bytes_rejects_invalid_lengths() {
		for len in [0, 1, 2047, 2049, 4096] {
			assert!(lthash_from_bytes(&vec![0_u8; len]).is_none(), "len={len}");
		}
	}

	#[test]
	fn serialize_lthash_matches_blake2b_256() {
		use blake2::{Blake2b, Digest, digest::consts::U32};

		let lthash = golden_lthash();
		let bytes = lthash_to_bytes(&lthash);

		let mut hasher = Blake2b::<U32>::new();
		Digest::update(&mut hasher, &bytes);
		let expected = format!("{:x}", hasher.finalize());

		assert_eq!(expected, EXPECTED_DIGEST);
		assert_eq!(serialize_lthash(&lthash).1, expected);
	}
}
