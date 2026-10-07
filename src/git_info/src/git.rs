use std::process::Command;

pub(crate) fn run(args: &[&str]) -> Option<String> {
	Command::new("git")
		.args(args)
		.output()
		.ok()
		.filter(|o| o.status.success())
		.and_then(|o| String::from_utf8(o.stdout).ok())
		.map(|s| s.trim().to_owned())
		.filter(|s| !s.is_empty())
}

pub(crate) fn description() -> Option<String> {
	// --always fallback handles shallow clones (no tags) by returning the short
	// hash
	let s = run(&["describe", "--tags", "--match", "v*", "--always", "--dirty"])?;
	Some(format(&s))
}

fn format(s: &str) -> String {
	let s = s.trim().trim_start_matches('v').to_owned();
	if let Some((prefix, suffix)) = s.rsplit_once("-g") {
		if let Some((ver, count)) = prefix.rsplit_once('-') {
			if count.chars().all(char::is_numeric) {
				return format!("{ver}+{count}~{suffix}");
			}
		}
	}

	// Trim out unintuitive git suffixes (g being a hexadecimal value)
	match s.rsplit_once("-g") {
		| Some((prefix, suffix)) => format!("{prefix}~{suffix}"),
		| None => s,
	}
}

#[cfg(test)]
mod tests {
	use super::format;

	#[test]
	fn test_format() {
		assert_eq!(format("v0.5.5-26-g23701cf0-dirty"), "0.5.5+26~23701cf0-dirty");
		assert_eq!(format("v0.5.5-26-g23701cf0"), "0.5.5+26~23701cf0");
		assert_eq!(format("0.5.5-26-g23701cf0"), "0.5.5+26~23701cf0");
		// Shallow clone / no tags / just grafted hash (e.g. git describe --always)
		assert_eq!(format("abc1234"), "abc1234");
		assert_eq!(format("abc1234-dirty"), "abc1234-dirty");
		assert_eq!(format("v0.5.5-beta-g23701cf0"), "0.5.5-beta~23701cf0");
		// Tag names containing "-g" should not be corrupted
		assert_eq!(format("v1.0.0-gamma-g1234abc"), "1.0.0-gamma~1234abc");
	}

	#[test]
	fn test_format_exact_tag() {
		// Exact tag match (HEAD is the tagged commit, no commits after)
		assert_eq!(format("v0.5.6"), "0.5.6");
		assert_eq!(format("0.5.6"), "0.5.6");
		assert_eq!(format("v1.0.0"), "1.0.0");
	}

	#[test]
	fn test_format_single_commit_after_tag() {
		// Only 1 commit after tag
		assert_eq!(format("v0.5.6-1-gabc1234"), "0.5.6+1~abc1234");
	}

	#[test]
	fn test_format_exact_tag_dirty() {
		// Exact tag but dirty working tree
		assert_eq!(format("v0.5.6-dirty"), "0.5.6-dirty");
	}

	#[test]
	fn test_format_empty_and_whitespace() {
		assert_eq!(format(""), "");
		assert_eq!(format("  "), "");
		assert_eq!(format("  v0.5.5-26-g23701cf0  "), "0.5.5+26~23701cf0");
	}

	#[test]
	fn test_format_long_hash() {
		// Full 40-char SHA-1 hashes (git describe --long --always)
		assert_eq!(
			format("abc1234def5678abc1234def5678abc1234def5678"),
			"abc1234def5678abc1234def5678abc1234def5678"
		);
		// Full describe with 40-char hash
		assert_eq!(
			format("v0.5.5-1-gabc1234def5678abc1234def5678abc1234def5678"),
			"0.5.5+1~abc1234def5678abc1234def5678abc1234def5678"
		);
	}

	#[test]
	fn test_format_prerelease_variants() {
		// Pre-release tags with commits after
		assert_eq!(format("v1.0.0-rc1-5-gabcdef0"), "1.0.0-rc1+5~abcdef0");
		assert_eq!(format("v1.0.0-alpha.1-10-g1234567"), "1.0.0-alpha.1+10~1234567");
		// Pre-release tag with dirty and commits
		assert_eq!(format("v1.0.0-rc1-5-gabcdef0-dirty"), "1.0.0-rc1+5~abcdef0-dirty");
	}

	#[test]
	fn test_branch_tag() {
		use super::branch_tag;

		assert_eq!(branch_tag("main"), Some("b=main".into()));
		assert_eq!(branch_tag("develop"), Some("b=develop".into()));
		assert_eq!(branch_tag("feature/foo"), Some("b=feature/foo".into()));
		assert_eq!(branch_tag("HEAD"), Some("b=HEAD".into()));
		assert_eq!(branch_tag("master"), Some("b=master".into()));
	}
}
