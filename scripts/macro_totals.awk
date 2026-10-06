# Passes `-Zmacro-stats` output through and appends totals.
#
# Nested expansions are counted at every level (a `vec!` inside `endpoint_request!` shows
# in both rows), so the totals are an upper bound on distinct generated code. Use them to
# compare runs, not as an absolute size.

function num(s) { gsub(/_/, "", s); return s + 0 }

{ print }

/^macro-stats =+$/ { rules++ }

/^macro-stats / && !/^macro-stats =/ && !/Macro Name/ && !/^macro-stats -/ && !/MACRO EXPANSION/ {
	n = split($0, f, /[ \t]+/)
	if (n < 6 || f[n] !~ /^[0-9_.]+$/ || f[n - 3] !~ /^[0-9_]+$/) next
	uses = num(f[n - 4]); bytes = num(f[n - 1]); lines = num(f[n - 3])
	name = $0
	sub(/^macro-stats +/, "", name)
	sub(/ +[0-9_]+ +[0-9_]+ +[0-9_.]+ +[0-9_]+ +[0-9_.]+ *$/, "", name)
	if (name ~ /^#\[derive/) kind = "derives"
	else if (name ~ /^(::|alloc::|core::|std::)/ || name ~ /^(vec|stringify|matches|write|assert|assert_eq|debug_assert|debug_assert_eq|panic|cfg|format)!$/) kind = "std/external"
	else kind = "crate macros"
	u[kind] += uses; l[kind] += lines; b[kind] += bytes
	tu += uses; tl += lines; tb += bytes
}

/^macro-stats =+$/ && rules == 2 {
	printf "macro-stats %-34s %6s %10s %12s\n", "TOTALS", "Uses", "Lines", "Bytes"
	split("crate macros,derives,std/external", order, ",")
	for (i = 1; i <= 3; i++) {
		k = order[i]
		printf "macro-stats %-34s %6d %10d %12d\n", k, u[k], l[k], b[k]
	}
	printf "macro-stats %-34s %6d %10d %12d\n", "ALL (nested counted twice)", tu, tl, tb
}
