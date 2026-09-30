use std::{collections::HashMap, hint::black_box, sync::Arc};

use conduwuit_service::rooms::state_hamt::room_structural_key;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ruma::owned_room_id;

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

fn bench_hamt_construction(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_construction");

	let sizes: [u64; 5] = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7_u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");

	for &size in &sizes {
		group.throughput(Throughput::Elements(size));

		let structural_key = room_structural_key(&server_secret, &room_id);
		let lattice = rezzy::state::LtHash::default();

		group.bench_with_input(BenchmarkId::new("build_root_handle", size), &size, |b, _| {
			b.iter(|| {
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
		});
	}

	group.finish();
}

fn bench_hamt_point_lookups(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_point_lookups");

	let sizes: [u64; 5] = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7_u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");
	let structural_key = room_structural_key(&server_secret, &room_id);

	for &size in &sizes {
		let entries: Vec<(u64, u64)> = (0..size)
			.map(|i| (i, i.saturating_mul(1_000).saturating_add(7)))
			.collect();
		let lattice = rezzy::state::LtHash::default();

		let (_root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
				.expect("failed to build benchmark HAMT tree");

		let mut node_map = NodeMap::new();
		collect_nodes(&root_node, &mut node_map);

		let target_keys = [0_u64, size / 2, size.saturating_sub(1)];

		group.bench_with_input(BenchmarkId::new("point_lookup_search", size), &size, |b, _| {
			b.iter(|| {
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
		});
	}

	group.finish();
}

fn bench_hamt_delta_isolation(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_delta_isolation");

	let base_size: u64 = 50_000;
	let delta_sizes = [1, 10, 100, 1_000];
	let server_secret = [7_u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");
	let structural_key = room_structural_key(&server_secret, &room_id);

	let base_entries: Vec<(u64, u64)> = (0..base_size)
		.map(|i| (i, i.saturating_mul(1_000).saturating_add(7)))
		.collect();
	let base_lattice = rezzy::state::LtHash::default();

	let (_base_root_handle, base_root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, base_entries.clone())
			.expect("failed to build base HAMT tree");

	for &delta_count in &delta_sizes {
		let mut new_entries = base_entries.clone();
		for (i, entry) in new_entries.iter_mut().enumerate().take(delta_count) {
			*entry = (u64::try_from(i).expect("benchmark index fits in u64"), 999_999);
		}

		let (_new_root_handle, new_root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, new_entries)
				.expect("failed to build new HAMT tree");

		let mut combined_nodes = NodeMap::new();
		collect_nodes(&base_root_node, &mut combined_nodes);
		collect_nodes(&new_root_node, &mut combined_nodes);

		group.bench_with_input(
			BenchmarkId::new("isolate_delta", delta_count),
			&delta_count,
			|b, _| {
				b.iter(|| {
					let mut resolver = |hash: &rezzy::hamt::StructuralHash| -> Result<
						Arc<rezzy::hamt::HamtNode<u64, u64>>,
						std::convert::Infallible,
					> {
						Ok(combined_nodes
							.get(hash)
							.cloned()
							.expect("node must exist in combined map"))
					};

					let lattice = rezzy::state::LtHash::default();
					let res = rezzy::hamt::delta::isolate_delta::<
						u64,
						u64,
						_,
						std::convert::Infallible,
					>(
						&base_root_node,
						&lattice,
						&new_root_node,
						&lattice,
						&mut resolver,
					);
					let _ = black_box(res);
				});
			},
		);
	}

	group.finish();
}

fn bench_lthash(c: &mut Criterion) {
	let mut group = c.benchmark_group("lthash_state_hashing");

	let element_counts: [u64; 4] = [100, 1_000, 10_000, 50_000];

	for &count in &element_counts {
		group.throughput(Throughput::Elements(count));

		group.bench_with_input(BenchmarkId::new("lthash_checksum", count), &count, |b, _| {
			let event_id = ruma::owned_event_id!("$bench_event:test.local");
			b.iter(|| {
				let mut hash = rezzy::LtHash::ZERO;
				for i in 0..count {
					let key_str = i.to_string();
					hash.insert("m.room.member", &key_str, &event_id);
				}
				hash.digest()
			});
		});
	}

	group.finish();
}

criterion_group!(
	benches,
	bench_hamt_construction,
	bench_hamt_point_lookups,
	bench_hamt_delta_isolation,
	bench_lthash
);
criterion_main!(benches);
