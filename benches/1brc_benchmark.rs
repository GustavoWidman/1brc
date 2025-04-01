use criterion::{Criterion, criterion_group, criterion_main};
use one_billion_row_challenge::{perform_calculations_only, perform_full_challenge};

fn benchmark_implementations(c: &mut Criterion) {
    let mut group = c.benchmark_group("1brc_calculations");

    // Single-threaded benchmark
    group.bench_function("perform_calculations_only", |b| {
        b.iter(|| perform_calculations_only().unwrap())
    });

    group.finish();

    let mut group = c.benchmark_group("1brc_full");

    // Single-threaded benchmark
    group.bench_function("perform_full_challenge", |b| {
        b.iter(|| perform_full_challenge().unwrap())
    });

    group.finish();
}

criterion_group!(benches, benchmark_implementations);
criterion_main!(benches);
