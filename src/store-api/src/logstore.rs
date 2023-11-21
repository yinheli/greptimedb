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

//! LogStore APIs.

use common_error::ext::ErrorExt;

use crate::logstore::entry::{Entry, Id as EntryId};
use crate::logstore::entry_stream::SendableEntryStream;
use crate::logstore::namespace::Namespace;
use crate::storage::RegionId;

pub mod entry;
pub mod entry_stream;
pub mod namespace;

// TODO(niebayes): Move `Topic` def to common_wal or common_config.
type Topic = String;
pub type KafkaOffset = i64;

/// LogRoute contains necessary information for routing wal operations.
#[derive(Clone, Debug, Default)]
pub struct LogRoute {
    pub region_id: RegionId,
    pub topic: Option<Topic>,
}

impl LogRoute {
    fn with_region_id(region_id: RegionId) -> Self {
        Self {
            region_id,
            ..Default::default()
        }
    }
}

// FIXME(niebayes): is it reasonable to implement this `From`?
impl From<RegionId> for LogRoute {
    fn from(region_id: RegionId) -> Self {
        Self::with_region_id(region_id)
    }
}

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + 'static + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync + 'static;
    type Namespace: Namespace;
    type Entry: Entry;

    /// Stop components of logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Append an `Entry` to WAL with given namespace and return a response.
    async fn append(&self, e: Self::Entry) -> Result<AppendResponse, Self::Error>;

    /// Append a batch of entries and return a response.
    async fn append_batch(&self, e: Vec<Self::Entry>) -> Result<AppendResponse, Self::Error>;

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error>;

    /// Create a new `Namespace`.
    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(&self, data: D, entry_id: EntryId, ns: Self::Namespace)
        -> Self::Entry;

    /// Create a namespace of the associate Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    // TODO(niebayes): rewrite by taking as input region id and log options where log options is stored as a hashmap in the region options.
    fn namespace(&self, log_route: LogRoute) -> Self::Namespace;

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, ns: Self::Namespace, entry_id: EntryId) -> Result<(), Self::Error>;
}

// TODO(niebayes): Define an `AppendBatchResponse` to store a set of offsets where each offset corresponds to a topic.
#[derive(Debug, Default)]
pub struct AppendResponse {
    /// The logical entry id of the first log entry appended.
    pub entry_id: EntryId,
    /// The physical offset in the log store of the first log entry appended.
    pub offset: Option<i64>,
}
