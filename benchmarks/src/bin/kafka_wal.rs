// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Mutation, OpType, Row, Rows, SemanticType, Value, WalEntry,
};
use clap::{Parser, ValueEnum};
use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use common_wal::config::kafka::DatanodeKafkaConfig as KafkaConfig;
use common_wal::options::{KafkaWalOptions, WalOptions};
use futures::StreamExt;
use log_store::kafka::log_store::KafkaLogStore;
use mito2::wal::Wal;
use rand::distributions::{Alphanumeric, DistString, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rskafka::client::ClientBuilder;
use store_api::storage::RegionId;
use tokio::sync::Barrier;

/// Each data source acts like a region.
struct DataSource {
    next_sequence: AtomicU64,
    next_timestamp: AtomicI64,
    topic: String,
    schema: Vec<ColumnSchema>,
    rng: Mutex<Option<SmallRng>>,
    rows_factor: usize,
    total_produced_bytes: AtomicUsize,
    total_restored_bytes: AtomicUsize,
}

impl DataSource {
    fn new(topic: String, schema: Vec<ColumnSchema>, rows_factor: usize, rng_seed: u64) -> Self {
        Self {
            next_sequence: AtomicU64::new(1),
            next_timestamp: AtomicI64::new(1655276557000),
            topic,
            schema,
            rng: Mutex::new(Some(SmallRng::seed_from_u64(rng_seed))),
            rows_factor,
            total_produced_bytes: AtomicUsize::new(0),
            total_restored_bytes: AtomicUsize::new(0),
        }
    }

    fn scrape(&self) -> WalEntry {
        let num_rows = self.rows_factor * 5;
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence: self
                .next_sequence
                .fetch_add(num_rows as u64, Ordering::Relaxed),
            rows: Some(self.build_rows(num_rows)),
        };

        let entry = WalEntry {
            mutations: vec![mutation],
        };
        self.total_produced_bytes
            .fetch_add(Self::entry_estimated_size(&entry), Ordering::Relaxed);

        entry
    }

    // TODO(niebayes): maybe compute once and cache the result.
    // TODO(niebayes): ensure the calculation is correct.
    fn entry_estimated_size(entry: &WalEntry) -> usize {
        let wrapper_bytes =
            size_of::<WalEntry>() + entry.mutations.capacity() * size_of::<Mutation>();

        let rows = entry.mutations[0].rows.as_ref().unwrap();
        let schema_bytes = rows.schema.capacity() * size_of::<ColumnSchema>();
        let values_bytes = (rows.rows.capacity() * size_of::<Row>())
            + rows
                .rows
                .iter()
                .map(|r| r.values.capacity() * size_of::<Value>())
                .sum::<usize>();

        wrapper_bytes + schema_bytes + values_bytes
    }

    fn build_rows(&self, num_rows: usize) -> Rows {
        let cols = self
            .schema
            .clone()
            .iter()
            .map(|col_schema| {
                let col_data_type = ColumnDataType::try_from(col_schema.datatype).unwrap();
                self.build_col(&col_data_type, num_rows)
            })
            .collect::<Vec<_>>();

        let rows = (0..num_rows)
            .map(|i| {
                let values = cols.iter().map(|col| col[i].clone()).collect();
                Row { values }
            })
            .collect();

        Rows {
            schema: self.schema.clone(),
            rows,
        }
    }

    fn build_col(&self, col_data_type: &ColumnDataType, num_rows: usize) -> Vec<Value> {
        let mut rng_guard = self.rng.lock().unwrap();
        let rng = rng_guard.as_mut().unwrap();
        match col_data_type {
            ColumnDataType::TimestampMillisecond => (0..num_rows)
                .map(|_| {
                    let ts = self.next_timestamp.fetch_add(1000, Ordering::Relaxed);
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(ts)),
                    }
                })
                .collect(),
            ColumnDataType::Int32 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0, 10_000));
                    Value {
                        value_data: Some(ValueData::I32Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::Float32 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0.0, 5000.0));
                    Value {
                        value_data: Some(ValueData::F32Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::String => (0..num_rows)
                .map(|_| {
                    let v = Alphanumeric.sample_string(rng, 10);
                    Value {
                        value_data: Some(ValueData::StringValue(v)),
                    }
                })
                .collect(),
            _ => unreachable!(),
        }
    }
}

/// Write workload.
#[derive(Clone, ValueEnum, Default)]
enum Workload {
    /// small height, small width
    #[default]
    Normal,
    /// small height, large width
    Fat,
    /// large height, small width
    Thin,
}

/// Benchmarker config.
struct Config {
    bootstrap_brokers: Vec<String>,
    num_topics: u32,
    num_data_sources: u32,
    num_scrapes: u32,
    max_batch_size: u64,
    linger: u64,
    rng_seed: u64,
    workload: Workload,
    skip_read: bool,
}

#[derive(Parser)]
#[command(name = "Kafka wal benchmarker")]
struct Args {
    #[clap(long, short = 'b', default_value = "localhost:9092")]
    bootstrap_brokers: String,

    #[clap(long, default_value_t = 16)]
    num_topics: u32,

    #[clap(long, default_value_t = 2000)]
    num_data_sources: u32,

    #[clap(long, default_value_t = 1000)]
    num_scrapes: u32,

    #[clap(long, default_value_t = 1)]
    max_batch_size: u64,

    #[clap(long, default_value_t = 20)]
    linger: u64,

    #[clap(long, default_value_t = 42)]
    rng_seed: u64,

    #[clap(long, short = 'w', value_enum, default_value_t = Workload::default())]
    workload: Workload,

    #[clap(long, default_value_t = false)]
    skip_read: bool,
}

struct Benchmarker {
    cfg: Config,
    wal: Arc<Wal<KafkaLogStore>>,
    data_sources: Arc<HashMap<u64, DataSource>>,
    entry_id: Arc<AtomicU64>,
}

impl Benchmarker {
    async fn new(cfg: Config) -> Self {
        // Creates topics.
        let client = ClientBuilder::new(cfg.bootstrap_brokers.clone())
            .build()
            .await
            .unwrap();
        let ctrl_client = client.controller_client().unwrap();
        let (topics, tasks): (Vec<_>, Vec<_>) = (0..cfg.num_topics)
            .map(|i| {
                let topic = format!("greptime_wal_topic_{}", i);
                let task = ctrl_client.create_topic(
                    topic.clone(),
                    1,
                    cfg.bootstrap_brokers.len() as i16,
                    2000,
                );
                (topic, task)
            })
            .unzip();
        let _ = futures::future::try_join_all(tasks).await;

        // Creates kafka log store.
        let kafka_cfg = KafkaConfig {
            broker_endpoints: cfg.bootstrap_brokers.clone(),
            max_batch_size: ReadableSize::mb(cfg.max_batch_size),
            linger: Duration::from_millis(cfg.linger),
            ..Default::default()
        };
        let store = Arc::new(KafkaLogStore::try_new(&kafka_cfg).await.unwrap());

        let (rows_factor, cols_factor) = match cfg.workload {
            Workload::Normal => (1, 1),
            Workload::Fat => (1, 5),
            Workload::Thin => (4, 1),
        };
        assert!(rows_factor > 0 && cols_factor > 0);

        // Creates data sources.
        let mut rng = SmallRng::seed_from_u64(cfg.rng_seed);
        let data_sources = (0..cfg.num_data_sources)
            .map(|i| {
                let ts_col = ColumnSchema {
                    column_name: "ts".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Tag as i32,
                    datatype_extension: None,
                };
                let mut schema = vec![ts_col];

                for _ in 0..cols_factor {
                    let i32_col = ColumnSchema {
                        column_name: "i32_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
                        datatype: ColumnDataType::Int32 as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                    };
                    let f32_col = ColumnSchema {
                        column_name: "f32_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
                        datatype: ColumnDataType::Float32 as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                    };
                    let str_col = ColumnSchema {
                        column_name: "str_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension: None,
                    };

                    schema.append(&mut vec![i32_col, f32_col, str_col]);
                }

                let data_source = DataSource::new(
                    // Assgins topics to data sources in a round-robin manner.
                    topics[i as usize % topics.len()].clone(),
                    schema,
                    rows_factor,
                    cfg.rng_seed,
                );
                (i as u64, data_source)
            })
            .collect();

        Self {
            cfg,
            wal: Arc::new(Wal::new(store)),
            data_sources: Arc::new(data_sources),
            entry_id: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn run(&self) {
        info!("Kafka wal benchmarker starts");
        let start = tokio::time::Instant::now();

        let scrapes = Arc::new(AtomicU32::new(1));
        let max_scrapes = self.cfg.num_scrapes;

        let num_workers = num_cpus::get();
        let barrier = Arc::new(Barrier::new(num_workers));
        let write_start = Instant::now();
        let writers = (0..num_workers)
            .map(|i| {
                let barrier = barrier.clone();
                let scrapes = scrapes.clone();
                let data_sources = self.data_sources.clone();
                let wal = self.wal.clone();
                let entry_id = self.entry_id.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    info!("Writer {i} starts");

                    loop {
                        let s = scrapes.fetch_add(1, Ordering::Relaxed);
                        if s >= max_scrapes {
                            break;
                        }
                        info!("Writer {i} performs scrape {s}");

                        let mut wal_writer = wal.writer();
                        for (id, source) in data_sources.iter() {
                            let entry = source.scrape();
                            wal_writer
                                .add_entry(
                                    RegionId::from_u64(*id),
                                    entry_id.fetch_add(1, Ordering::Relaxed),
                                    &entry,
                                    &WalOptions::Kafka(KafkaWalOptions {
                                        topic: source.topic.clone(),
                                    }),
                                )
                                .unwrap();
                        }
                        wal_writer.write_to_wal().await.unwrap();
                    }

                    info!("Writer {i} terminates");
                })
            })
            .collect::<Vec<_>>();
        futures::future::join_all(writers).await;

        let write_elapsed = write_start.elapsed().as_millis();
        assert!(write_elapsed > 0);

        let mut read_elapsed = 0;
        if !self.cfg.skip_read {
            let next_replay = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(num_workers));
            let read_start = Instant::now();
            let readers = (0..num_workers)
                .map(|i| {
                    let barrier = barrier.clone();
                    let next_replay = next_replay.clone();
                    let data_sources = self.data_sources.clone();
                    let wal = self.wal.clone();

                    tokio::spawn(async move {
                        barrier.wait().await;
                        info!("Reader {i} starts");

                        loop {
                            let id = next_replay.fetch_add(1, Ordering::Relaxed);
                            if id >= data_sources.len() as u64 {
                                break;
                            }
                            info!("Readers {i} replays data source {id}");

                            let source = &data_sources[&id];
                            let wal_options = WalOptions::Kafka(KafkaWalOptions {
                                topic: source.topic.clone(),
                            });

                            // Consumes the wal stream to emulate replay.
                            let mut wal_stream =
                                wal.scan(RegionId::from_u64(id), 0, &wal_options).unwrap();
                            while let Some(res) = wal_stream.next().await {
                                let (_, entry) = res.unwrap();
                                source.total_restored_bytes.fetch_add(
                                    DataSource::entry_estimated_size(&entry),
                                    Ordering::Relaxed,
                                );
                            }
                        }

                        info!("Reader {i} terminates");
                    })
                })
                .collect::<Vec<_>>();
            futures::future::join_all(readers).await;
            read_elapsed = read_start.elapsed().as_millis();
        }

        let total_elapsed = start.elapsed().as_millis();
        let cost_report = format!(
            "write costs: {} ms, read costs: {} ms, in total: {} ms",
            write_elapsed, read_elapsed, total_elapsed
        );

        let total_written_bytes = self
            .data_sources
            .values()
            .map(|source| source.total_produced_bytes.load(Ordering::Relaxed))
            .sum::<usize>();
        let write_throughput = total_written_bytes as f64 / write_elapsed as f64 * 1000.0;

        let total_read_bytes = self
            .data_sources
            .values()
            .map(|source| source.total_restored_bytes.load(Ordering::Relaxed))
            .sum::<usize>();
        // This is the effective read throughput from which the read amplification is removed.
        // TODO(niebayes): add metrics inside kafka log store to measure the read amplification and the actual read throughput.
        let read_throughput = if read_elapsed > 0 {
            total_read_bytes as f64 / read_elapsed as f64 * 1000.0
        } else {
            0.0
        };

        // FIXME(niebayes): figure out why total_written_bytes < total_read_bytes.
        let throughput_report = format!(
            "total written bytes: {} bytes, total read bytes: {} bytes, write throuput: {} bytes/s ({} mb/s), read throughput: {} bytes/s ({} mb/s)",
            total_written_bytes,
            total_read_bytes,
            write_throughput.floor() as u128,
            (write_throughput / (1 << 20) as f64).floor() as u128,
            read_throughput.floor() as u128,
            (read_throughput / (1 << 20) as f64).floor() as u128,
        );

        info!("Kafka wal benchmarker terminates. \n{cost_report}\n{throughput_report}",);
    }
}

// TODO(niebayes): add timer metrics to wal to measure write, read latency, i.e. duration.

fn main() {
    common_telemetry::init_default_ut_logging();

    let args = Args::parse();
    let cfg = Config {
        bootstrap_brokers: args
            .bootstrap_brokers
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        num_topics: args.num_topics,
        num_data_sources: args.num_data_sources,
        num_scrapes: args.num_scrapes,
        max_batch_size: args.max_batch_size,
        linger: args.linger,
        rng_seed: args.rng_seed,
        workload: args.workload,
        skip_read: args.skip_read,
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            Benchmarker::new(cfg).await.run().await;
        });
}
