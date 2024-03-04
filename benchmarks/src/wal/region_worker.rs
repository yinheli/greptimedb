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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::StreamExt;
use mito2::wal::Wal;
use store_api::logstore::LogStore;

use crate::metrics;
use crate::wal::region::Region;

/// A region worker handles writting and openning for a set of regions.
pub struct RegionWorker {
    regions: Vec<Region>,
}

impl RegionWorker {
    /// Constructs a new region worker bound to the given regions.
    /// Each region is handled by only one region worker.
    pub fn new(regions: Vec<Region>) -> Self {
        Self { regions }
    }

    /// Scrapes each region and aggregates their wal entries to a batch, then writes the entry batch to wal.
    pub async fn write_all<S: LogStore>(&self, wal: &Arc<Wal<S>>) {
        let mut wal_writer = wal.writer();
        for region in self.regions.iter() {
            let entry = region.scrape();
            metrics::METRIC_WAL_WRITE_BYTES_TOTAL
                .inc_by(Region::entry_estimated_size(&entry) as u64);

            wal_writer
                .add_entry(
                    region.id,
                    region.next_entry_id.fetch_add(1, Ordering::Relaxed),
                    &entry,
                    &region.wal_options,
                )
                .unwrap();
        }
        wal_writer.write_to_wal().await.unwrap();
    }

    /// Opens all regions by replays wal entries from wal.
    pub async fn open_all<S: LogStore>(&self, wal: &Arc<Wal<S>>) {
        for region in self.regions.iter() {
            let mut wal_stream = wal.scan(region.id, 0, &region.wal_options).unwrap();
            while let Some(res) = wal_stream.next().await {
                let (_, entry) = res.unwrap();
                metrics::METRIC_WAL_READ_BYTES_TOTAL
                    .inc_by(Region::entry_estimated_size(&entry) as u64);
            }
        }
    }
}
