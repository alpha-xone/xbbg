//! Criterion comparison for synthetic BQL JSON parser throughput.
//!
//! This evaluation harness compares parse-only serde_json and simd-json costs.
//! Production parsing remains unchanged.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use serde_json::Value;
use std::hint::black_box;

fn bench_bql_json_parsers(c: &mut Criterion) {
    let cases: [(&'static str, usize, &'static [&'static str]); 3] = [
        ("json_simple_1x1", 1, &["px_last"]),
        (
            "json_wide_1x5",
            1,
            &["px_last", "px_open", "px_high", "px_low", "px_volume"],
        ),
        ("json_rows_1000x2", 1_000, &["px_last", "px_volume"]),
    ];

    let mut group = c.benchmark_group("bql_json_parsers");
    for (scenario, rows, fields) in cases {
        let json = bql_json_fixture(rows, fields);
        let bytes = json.len() as u64;
        group.throughput(Throughput::Bytes(bytes));

        group.bench_with_input(
            BenchmarkId::new("serde_json_from_str", scenario),
            &json,
            |b, json| {
                b.iter(|| {
                    let parsed: Value = serde_json::from_str(black_box(json.as_str()))
                        .expect("synthetic BQL fixture should parse with serde_json");
                    black_box(parsed);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("simd_json_from_slice", scenario),
            &json,
            |b, json| {
                b.iter_batched(
                    || json.as_bytes().to_vec(),
                    |mut bytes| {
                        let parsed: Value =
                            simd_json::serde::from_slice(black_box(bytes.as_mut_slice()))
                                .expect("synthetic BQL fixture should parse with simd-json");
                        black_box(parsed);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// Copied from xbbg_benchmark_suite::bql_json_fixture so parser comparisons use
// the same synthetic BQL-shaped payloads as the suite's json_* scenarios.
fn bql_json_fixture(rows: usize, fields: &[&str]) -> String {
    let ids = (0..rows)
        .map(|i| format!("\"TICKER{i} US Equity\""))
        .collect::<Vec<_>>()
        .join(",");
    let dates = (0..rows)
        .map(|i| format!("\"2026-04-{:02}\"", (i % 28) + 1))
        .collect::<Vec<_>>()
        .join(",");
    let currencies = (0..rows).map(|_| "\"USD\"").collect::<Vec<_>>().join(",");

    let field_json = fields
        .iter()
        .enumerate()
        .map(|(field_idx, field)| {
            let values = (0..rows)
                .map(|i| format!("{}", 100.0 + field_idx as f64 + i as f64 / 100.0))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                r#""{field}":{{"idColumn":{{"name":"ID","type":"STRING","values":[{ids}]}} ,"valuesColumn":{{"name":"VALUE","type":"DOUBLE","values":[{values}]}} ,"secondaryColumns":[{{"name":"DATE","type":"DATE","values":[{dates}]}},{{"name":"CURRENCY","type":"STRING","values":[{currencies}]}}],"responseExceptions":[],"partialErrorMap":{{"errorIterator":null}}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        r#"{{"clientContext":{{"clientRequestId":"offline-bql-benchmark"}},"responseExceptions":null,"results":{{{field_json}}}}}"#
    )
}

criterion_group!(benches, bench_bql_json_parsers);
criterion_main!(benches);
