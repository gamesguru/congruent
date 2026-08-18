use std::{collections::HashMap, sync::Arc};

use conduwuit_service::rooms::state_hamt::room_structural_key;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ruma::owned_room_id;

fn bench_hamt_construction(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_construction");

	let sizes = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");

	for &size in &sizes {
		group.throughput(Throughput::Elements(size as u64));

		let structural_key = room_structural_key(&server_secret, &room_id);
		let lattice = rezzy::state::LtHash::default();

		group.bench_with_input(BenchmarkId::new("build_root_handle", size), &size, |b, _| {
			b.iter(|| {
				// Regenerate the entries rather than cloning the full Vec so the timed
				// work measures tree construction alone, not an O(n) copy.
				let _res = rezzy::hamt::build_hamt_root_handle(
					&structural_key,
					&lattice,
					(0..size as u64).map(|i| (i, i * 1000 + 7)),
				);
			});
		});
	}

	group.finish();
}

fn bench_hamt_point_lookups(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_point_lookups");

	let sizes = [10, 100, 1_000, 10_000, 50_000];
	let server_secret = [7u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");
	let structural_key = room_structural_key(&server_secret, &room_id);

	for &size in &sizes {
		let entries: Vec<(u64, u64)> = (0..size as u64).map(|i| (i, i * 1000 + 7)).collect();
		let lattice = rezzy::state::LtHash::default();

		let (_root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
				.expect("failed to build benchmark HAMT tree");

		let mut node_map: HashMap<
			rezzy::hamt::StructuralHash,
			Arc<rezzy::hamt::HamtNode<u64, u64>>,
		> = HashMap::new();

		fn collect_nodes(
			node: Arc<rezzy::hamt::HamtNode<u64, u64>>,
			map: &mut HashMap<rezzy::hamt::StructuralHash, Arc<rezzy::hamt::HamtNode<u64, u64>>>,
		) {
			map.insert(node.structural_hash, node.clone());
			for child in &node.children {
				if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
					collect_nodes(child_node.clone(), map);
				}
			}
		}
		collect_nodes(root_node.clone(), &mut node_map);

		let target_keys = [0u64, (size / 2) as u64, (size - 1) as u64];

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
					let _ = std::hint::black_box(res);
				}
			});
		});
	}

	group.finish();
}

fn bench_hamt_delta_isolation(c: &mut Criterion) {
	let mut group = c.benchmark_group("hamt_delta_isolation");

	let base_size = 50_000;
	let delta_sizes = [1, 10, 100, 1_000];
	let server_secret = [7u8; 32];
	let room_id = owned_room_id!("!bench_room:test.local");
	let structural_key = room_structural_key(&server_secret, &room_id);

	let base_entries: Vec<(u64, u64)> =
		(0..base_size as u64).map(|i| (i, i * 1000 + 7)).collect();
	let base_lattice = rezzy::state::LtHash::default();

	let (_base_root_handle, base_root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, base_entries.clone())
			.expect("failed to build base HAMT tree");

	for &delta_count in &delta_sizes {
		let mut new_entries = base_entries.clone();
		for i in 0..delta_count {
			new_entries[i] = (i as u64, 999_999);
		}

		let (_new_root_handle, new_root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &base_lattice, new_entries)
				.expect("failed to build new HAMT tree");

		let mut combined_nodes: HashMap<
			rezzy::hamt::StructuralHash,
			Arc<rezzy::hamt::HamtNode<u64, u64>>,
		> = HashMap::new();

		fn collect_nodes(
			node: Arc<rezzy::hamt::HamtNode<u64, u64>>,
			map: &mut HashMap<rezzy::hamt::StructuralHash, Arc<rezzy::hamt::HamtNode<u64, u64>>>,
		) {
			map.insert(node.structural_hash, node.clone());
			for child in &node.children {
				if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
					collect_nodes(child_node.clone(), map);
				}
			}
		}
		collect_nodes(base_root_node.clone(), &mut combined_nodes);
		collect_nodes(new_root_node.clone(), &mut combined_nodes);

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
					let _ = std::hint::black_box(res);
				});
			},
		);
	}

	group.finish();
}

fn bench_lthash(c: &mut Criterion) {
	let mut group = c.benchmark_group("lthash_state_hashing");

	let element_counts = [100, 1_000, 10_000, 50_000];

	for &count in &element_counts {
		group.throughput(Throughput::Elements(count as u64));

		group.bench_with_input(BenchmarkId::new("lthash_checksum", count), &count, |b, _| {
			let event_id = ruma::owned_event_id!("$bench_event:test.local");
			b.iter(|| {
				let mut hash = rezzy::LtHash::ZERO;
				for i in 0..count {
					let key_str = i.to_string();
					hash.insert("m.room.member", &key_str, &event_id);
				}
				hash.checksum()
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
