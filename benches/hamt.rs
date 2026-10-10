use std::{collections::HashMap, hint::black_box, sync::Arc, time::Instant};

use conduwuit_service::rooms::state_hamt::room_structural_key;

type Node = rezzy::hamt::HamtNode<u64, u64>;
type NodeMap = HashMap<rezzy::hamt::StructuralHash, Arc<Node>>;

fn collect_nodes(node: &Arc<Node>, map: &mut NodeMap) {
	map.insert(node.structural_hash, Arc::clone(node));
	for child in &node.children {
		if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
			collect_nodes(child_node, map);
		}
	}
}

/// Iterations for a case over `elements` inputs, scaled from a fixed work
/// budget so a full sweep stays in the low seconds at every size.
fn iterations(work_budget: u64, elements: u64) -> u64 {
	work_budget
		.checked_div(elements.max(1))
		.unwrap_or(0)
		.clamp(3, 2_000)
}

fn measure(group: &str, case: &str, elements: u64, iters: u64, mut f: impl FnMut()) {
	// one untimed iteration to fault in the tree/page cache before timing
	f();

	let start = Instant::now();
	for _ in 0..iters {
		f();
	}

	let elapsed = start.elapsed();
	let per_op = elapsed
		.as_nanos()
		.checked_div(u128::from(iters))
		.unwrap_or(0);
	let per_element = per_op.checked_div(u128::from(elements.max(1))).unwrap_or(0);

	println!("{group:<22} {case:<22} {elements:>9} {iters:>7} {per_op:>14} {per_element:>16}");
}

fn bench_hamt_construction(measure_iters: bool) {
	let sizes: [u64; 5] = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7_u8; 32];
	let room_id =
		slipstream::OwnedRoomId::parse("!bench_room:test.local").expect("valid room ID");
	let structural_key = room_structural_key(&server_secret, &room_id);
	let lattice = rezzy::incremental::LtHash::default();

	for &size in &sizes {
		let iters = if measure_iters { iterations(200_000, size) } else { 1 };

		measure("hamt_construction", "build_root_handle", size, iters, || {
			// Feed a fresh input iterator each iteration (regenerating lazily is
			// cheaper than the O(n) Vec clone the timer previously paid) so the
			// timed work is construction proper, and black_box the result so the
			// compiler cannot dead-code-eliminate the whole tree build.
			let _ = black_box(rezzy::hamt::build_hamt_root_handle(
				&structural_key,
				&lattice,
				(0..size).map(|i| (i, i.saturating_mul(1_000).saturating_add(7))),
			));
		});
	}
}

fn bench_hamt_point_lookups(measure_iters: bool) {
	let sizes: [u64; 5] = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7_u8; 32];
	let room_id =
		slipstream::OwnedRoomId::parse("!bench_room:test.local").expect("valid room ID");
	let structural_key = room_structural_key(&server_secret, &room_id);

	for &size in &sizes {
		let entries: Vec<(u64, u64)> = (0..size)
			.map(|i| (i, i.saturating_mul(1_000).saturating_add(7)))
			.collect();
		let lattice = rezzy::incremental::LtHash::default();

		let (_root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
				.expect("failed to build benchmark HAMT tree");

		let mut node_map = NodeMap::new();
		collect_nodes(&root_node, &mut node_map);

		let target_keys = [0_u64, size / 2, size.saturating_sub(1)];
		let iters = if measure_iters { iterations(2_000_000, size) } else { 1 };

		measure("hamt_point_lookups", "point_lookup_search", size, iters, || {
			let mut resolver = |hash: &rezzy::hamt::StructuralHash| -> Result<
				Arc<rezzy::hamt::HamtNode<u64, u64>>,
				std::convert::Infallible,
			> {
				Ok(node_map
					.get(hash)
					.cloned()
					.expect("node must exist in memory map"))
			};

			for &key in &target_keys {
				let res = root_node.search(&structural_key, &key, &mut resolver);
				let _ = black_box(res);
			}
		});
	}
}

fn bench_hamt_delta_isolation(measure_iters: bool) {
	let base_size: u64 = 50_000;
	let delta_sizes: [u64; 4] = [1, 10, 100, 1_000];
	let server_secret = [7_u8; 32];
	let room_id =
		slipstream::OwnedRoomId::parse("!bench_room:test.local").expect("valid room ID");
	let structural_key = room_structural_key(&server_secret, &room_id);

	let base_entries: Vec<(u64, u64)> = (0..base_size)
		.map(|i| (i, i.saturating_mul(1_000).saturating_add(7)))
		.collect();
	let base_lattice = rezzy::incremental::LtHash::default();

	let (_base_root_handle, base_root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, base_entries.clone())
			.expect("failed to build base HAMT tree");

	for &delta_count in &delta_sizes {
		let mut new_entries = base_entries.clone();
		for (i, entry) in new_entries
			.iter_mut()
			.enumerate()
			.take(usize::try_from(delta_count).expect("delta count fits in usize"))
		{
			*entry = (u64::try_from(i).expect("benchmark index fits in u64"), 999_999);
		}

		let (_new_root_handle, new_root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, new_entries)
				.expect("failed to build new HAMT tree");

		let mut combined_nodes = NodeMap::new();
		collect_nodes(&base_root_node, &mut combined_nodes);
		collect_nodes(&new_root_node, &mut combined_nodes);

		let iters = if measure_iters {
			iterations(200_000, delta_count)
		} else {
			1
		};

		measure("hamt_delta_isolation", "isolate_delta", delta_count, iters, || {
			let mut resolver = |hash: &rezzy::hamt::StructuralHash| -> Result<
				Arc<rezzy::hamt::HamtNode<u64, u64>>,
				std::convert::Infallible,
			> {
				Ok(combined_nodes
					.get(hash)
					.cloned()
					.expect("node must exist in combined map"))
			};

			let lattice = rezzy::incremental::LtHash::default();
			let res = rezzy::hamt::delta::isolate_delta::<u64, u64, _, std::convert::Infallible>(
				&base_root_node,
				&lattice,
				&new_root_node,
				&lattice,
				&mut resolver,
			);
			let _ = black_box(res);
		});
	}
}

fn bench_lthash(measure_iters: bool) {
	let element_counts: [u64; 4] = [100, 1_000, 10_000, 50_000];

	for &count in &element_counts {
		let iters = if measure_iters { iterations(100_000, count) } else { 1 };

		measure("lthash_state_hashing", "lthash_checksum", count, iters, || {
			let event_id = "$bench_event:test.local";
			let mut hash = rezzy::incremental::LtHash::ZERO;
			for i in 0..count {
				let key_str = i.to_string();
				hash.insert("m.room.member", &key_str, &event_id);
			}
			let _ = black_box(hash.digest());
		});
	}
}

fn main() {
	// `cargo bench` passes `--bench`, `cargo test --all-targets` runs this binary
	// with no arguments at all; the latter only smoke-runs every case once.
	let measure_iters = std::env::args().any(|arg| arg == "--bench");

	println!(
		"{:<22} {:<22} {:>9} {:>7} {:>14} {:>16}",
		"group", "case", "elements", "iters", "ns/op", "ns/element",
	);

	bench_hamt_construction(measure_iters);
	bench_hamt_point_lookups(measure_iters);
	bench_hamt_delta_isolation(measure_iters);
	bench_lthash(measure_iters);
}
