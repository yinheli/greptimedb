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

use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::{ColumnDataType, ColumnSchema, SemanticType};
use benchmarks::metrics;
use benchmarks::wal::region::Region;
use benchmarks::wal::region_worker::RegionWorker;
use clap::{Parser, ValueEnum};
use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use common_wal::config::kafka::DatanodeKafkaConfig as KafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use common_wal::options::{KafkaWalOptions, WalOptions};
use itertools::Itertools;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use mito2::wal::Wal;
use prometheus::{Encoder, TextEncoder};
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rskafka::client::ClientBuilder;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::Barrier;

// TODO(niebayes): try to embed kafka to the benchmarker.

/// Wal provider.
#[derive(Clone, ValueEnum, Default, Debug)]
enum WalProvider {
    #[default]
    Kafka,
    RaftEngine,
}

/// Write workload.
#[derive(Clone, ValueEnum, Default, Debug)]
enum Workload {
    /// small height, small width
    #[default]
    Normal,
    /// small height, large width
    Fat,
    /// large height, small width
    Thin,
}

#[derive(Parser)]
#[command(name = "Wal benchmarker")]
struct Args {
    #[clap(long, value_enum, default_value_t = WalProvider::default())]
    wal_provider: WalProvider,

    #[clap(long, short = 'b', default_value = "localhost:9092")]
    bootstrap_brokers: String,

    #[clap(long, default_value_t = 10)]
    num_workers: u32,

    #[clap(long, default_value_t = 16)]
    num_topics: u32,

    #[clap(long, default_value_t = 1000)]
    num_regions: u32,

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

    #[clap(long, default_value_t = false)]
    skip_write: bool,
}

/// Benchmarker config.
#[derive(Debug, Clone)]
struct Config {
    wal_provider: WalProvider,
    bootstrap_brokers: Vec<String>,
    num_workers: u32,
    num_topics: u32,
    num_regions: u32,
    num_scrapes: u32,
    max_batch_size: u64,
    linger: u64,
    rng_seed: u64,
    workload: Workload,
    skip_read: bool,
    skip_write: bool,
}

struct Benchmarker {
    cfg: Config,
    region_workers: Vec<Arc<RegionWorker>>,
}

impl Benchmarker {
    async fn new(cfg: Config, topics: &[String]) -> Self {
        let (rows_factor, cols_factor) = match cfg.workload {
            Workload::Normal => (1, 1),
            Workload::Fat => (1, 5),
            Workload::Thin => (4, 1),
        };
        assert!(rows_factor > 0 && cols_factor > 0);

        let mut rng = SmallRng::seed_from_u64(cfg.rng_seed);
        let chunk_size = (cfg.num_regions as f32 / cfg.num_workers as f32).ceil() as usize;
        let region_workers = (0..cfg.num_regions)
            .map(|id| {
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

                let wal_options = match cfg.wal_provider {
                    WalProvider::Kafka => {
                        assert!(!topics.is_empty());
                        WalOptions::Kafka(KafkaWalOptions {
                            topic: topics.get(id as usize % topics.len()).cloned().unwrap(),
                        })
                    }
                    WalProvider::RaftEngine => WalOptions::RaftEngine,
                };
                Region::new(
                    RegionId::from_u64(id as u64),
                    schema,
                    wal_options,
                    rows_factor,
                    cfg.rng_seed,
                )
            })
            .chunks(chunk_size)
            .into_iter()
            .map(|chunk| {
                let regions = chunk.collect::<Vec<_>>();
                Arc::new(RegionWorker::new(regions))
            })
            .collect::<Vec<_>>();

        Self {
            cfg,
            region_workers,
        }
    }

    async fn run<S: LogStore>(&self, wal: Arc<Wal<S>>) {
        info!("Wal benchmarker starts");
        let start = tokio::time::Instant::now();

        let mut write_elapsed = 0;
        if !self.cfg.skip_write {
            info!("Benchmarking write ...");

            let barrier = Arc::new(Barrier::new(self.region_workers.len()));
            let write_start = Instant::now();
            let num_scrapes = self.cfg.num_scrapes;

            let writers = self
                .region_workers
                .iter()
                .enumerate()
                .map(|(worker_id, worker)| {
                    let barrier = barrier.clone();
                    let wal = wal.clone();
                    let worker = worker.clone();
                    tokio::spawn(async move {
                        barrier.wait().await;
                        // info!("Writer {worker_id} starts");
                        for scrape_idx in 0..num_scrapes {
                            // info!("Writer {worker_id} is executing the {scrape_idx} scrape");
                            worker.write_all(&wal).await;
                        }
                        // info!("Writer {worker_id} terminates");
                    })
                })
                .collect::<Vec<_>>();
            futures::future::join_all(writers).await;

            write_elapsed = write_start.elapsed().as_millis();
            assert!(write_elapsed > 0);
        }

        let mut read_elapsed = 0;
        if !self.cfg.skip_read {
            info!("Benchmarking read ...");

            let barrier = Arc::new(Barrier::new(self.region_workers.len()));
            let read_start = Instant::now();

            let readers = self
                .region_workers
                .iter()
                .enumerate()
                .map(|(worker_id, worker)| {
                    let barrier = barrier.clone();
                    let wal = wal.clone();
                    let worker = worker.clone();
                    tokio::spawn(async move {
                        barrier.wait().await;
                        // info!("Reader {worker_id} starts");
                        worker.open_all(&wal).await;
                        // info!("Reader {worker_id} terminates");
                    })
                })
                .collect::<Vec<_>>();
            futures::future::join_all(readers).await;

            read_elapsed = read_start.elapsed().as_millis();
            assert!(read_elapsed > 0);
        }

        let total_elapsed = start.elapsed().as_millis();
        let cost_report = format!(
            "write costs: {} ms, read costs: {} ms, in total: {} ms",
            write_elapsed, read_elapsed, total_elapsed
        );

        let total_written_bytes = metrics::METRIC_WAL_WRITE_BYTES_TOTAL.get();
        let write_throughput = if write_elapsed > 0 {
            total_written_bytes as f64 / write_elapsed as f64 * 1000.0
        } else {
            0.0
        };
        // This is the effective read throughput from which the read amplification is removed.
        let total_read_bytes = metrics::METRIC_WAL_READ_BYTES_TOTAL.get();
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

        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metrics = prometheus::gather();
        encoder.encode(&metrics, &mut buffer).unwrap();
        let metrics_report = String::from_utf8(buffer).unwrap();

        info!("Wal benchmarker terminates. \n\n{cost_report}\n\n{throughput_report}\n\n{metrics_report}");
    }
}

fn main() {
    common_telemetry::init_default_ut_logging();

    let args = Args::parse();
    let cfg = Config {
        wal_provider: args.wal_provider,
        bootstrap_brokers: args
            .bootstrap_brokers
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        num_workers: args.num_workers.min(num_cpus::get() as u32),
        num_topics: args.num_topics,
        num_regions: args.num_regions,
        num_scrapes: args.num_scrapes,
        max_batch_size: args.max_batch_size,
        linger: args.linger,
        rng_seed: args.rng_seed,
        workload: args.workload,
        skip_read: args.skip_read,
        skip_write: args.skip_write,
    };
    let cfg_clone = cfg.clone();

    // TODO(niebayes): add validation for args.

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match cfg.wal_provider {
                WalProvider::Kafka => {
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

                    let wal = Arc::new(Wal::new(store));
                    Benchmarker::new(cfg, &topics).await.run(wal).await;
                }
                WalProvider::RaftEngine => {
                    let store = RaftEngineLogStore::try_new(
                        "/tmp/greptimedb/raft-engine-wal".to_string(),
                        RaftEngineConfig::default(),
                    )
                    .await
                    .map(Arc::new)
                    .unwrap();
                    let wal = Arc::new(Wal::new(store));
                    Benchmarker::new(cfg, &[]).await.run(wal).await;
                }
            }
            info!("Benchmark config: \n{:?}", cfg_clone);
        });
}
