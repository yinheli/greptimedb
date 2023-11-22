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
use std::sync::Arc;

use common_config::wal::kafka::KafkaOptions;
use common_meta::wal::kafka::KafkaTopic as Topic;
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::error::Error as RsKafkaError;
use rskafka::record::{Record, RecordAndOffset};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::{AppendResponse, LogRoute, LogStore};
use store_api::storage::RegionId;

use crate::error::{
    ConvertEntryIdToOffsetSnafu, EmptyKafkaOffsetsSnafu, EmptyLogEntriesSnafu, Error,
    GetKafkaTopicClientSnafu, ReadRecordFromKafkaSnafu, Result, WriteEntriesToKafkaSnafu,
};
use crate::kafka::topic_client_manager::{TopicClientManager, TopicClientManagerRef};
use crate::kafka::{EntryImpl, NamespaceImpl};

#[derive(Debug)]
pub struct KafkaLogStore {
    opts: KafkaOptions,
    topic_client_manager: TopicClientManagerRef,
}

impl KafkaLogStore {
    pub async fn try_new(kafka_opts: &KafkaOptions) -> Result<Self> {
        Ok(Self {
            opts: kafka_opts.clone(),
            topic_client_manager: Arc::new(TopicClientManager::try_new(kafka_opts).await?),
        })
    }

    /// Publishes a sequence of log entries to a given Kafka topic, return the start offset of the first log entry.
    async fn publish_entries_to_topic(
        &self,
        entries: Vec<EntryImpl>,
        topic: Topic,
    ) -> Result<AppendResponse> {
        // Safety: the caller ensures the input entries is not empty.
        assert!(!entries.is_empty());

        let first_entry = entries.first().unwrap();
        let first_entry_id = first_entry.id;
        let region_id = *first_entry.ns.region_id();

        let topic_client = self
            .topic_client_manager
            .get_or_insert(&topic)
            .await
            .map_err(|_| {
                GetKafkaTopicClientSnafu {
                    topic: topic.clone(),
                    region_id,
                }
                .build()
            })?;
        let producer = &topic_client.producer;

        let produce_tasks = try_into_records(entries)?
            .into_iter()
            .map(|record| producer.produce(record))
            .collect::<Vec<_>>();

        let offsets = futures::future::try_join_all(produce_tasks)
            .await
            .context(WriteEntriesToKafkaSnafu { topic, region_id })?;
        let start_offset = offsets.first().context(EmptyKafkaOffsetsSnafu)?;

        Ok(AppendResponse {
            entry_id: first_entry_id,
            offset: Some(*start_offset),
        })
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;
    type Entry = EntryImpl;
    type Namespace = NamespaceImpl;

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        EntryImpl::new(data.as_ref().to_vec(), entry_id, ns)
    }

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse> {
        let topic = entry.ns.topic().clone();
        self.publish_entries_to_topic(vec![entry], topic).await
    }

    /// Append a batch of entries.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendResponse> {
        let first_entry_id = entries.first().context(EmptyLogEntriesSnafu)?.id;

        let mut topic_entries: HashMap<String, Vec<_>> = HashMap::new();
        for entry in entries {
            topic_entries
                .entry(entry.ns.topic().clone())
                .or_default()
                .push(entry)
        }

        let publish_tasks = topic_entries
            .into_iter()
            .map(|(topic, entries)| self.publish_entries_to_topic(entries, topic))
            .collect::<Vec<_>>();
        let responses = futures::future::try_join_all(publish_tasks).await?;

        // The returned responses are reordered by topic and hence it's necessary to find the minimum offset manually.
        let start_offset = responses
            .into_iter()
            .map(|response|
                // Safety: the offset must be Some in Kafka log store.
                response.offset.unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .min();
        // Safety: there must be at least one response since the input entries is guaranteed not being empty.
        assert!(start_offset.is_some());

        Ok(AppendResponse {
            entry_id: first_entry_id,
            offset: start_offset,
        })
    }

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`. The generated entries will be filtered by the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        let topic = ns.topic().clone();
        let region_id = *ns.region_id();
        let offset = try_into_offset(entry_id)?;

        let raw_client = self
            .topic_client_manager
            .get_or_insert(&topic)
            .await?
            .raw_client
            .clone();
        let mut stream_consumer = StreamConsumerBuilder::new(raw_client, StartOffset::At(offset))
            .with_max_batch_size(self.opts.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.opts.max_wait_time.as_millis() as i32)
            .build();

        // TODO(niebayes): we may also need a buffering mechanism on fetching entries since the mapping between
        // entries and records is one-one currently.
        let stream = async_stream::stream!({
            while let Some(fetch_result) = stream_consumer.next().await {
                yield handle_fetch_result(fetch_result, &topic, &region_id, offset);
            }
        });
        Ok(Box::pin(stream))
    }

    /// Create a namespace of the associate Namespace type
    fn namespace(&self, log_route: LogRoute) -> Self::Namespace {
        // Safety: the caller ensures the topic exists.
        NamespaceImpl::new(log_route.region_id, log_route.topic.unwrap())
    }

    /// Create a new `Namespace`.
    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, _ns: Self::Namespace, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stop components of logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

// The mapping between entries and records is one-one currently.
// TODO(niebayes): Shall limit a record to be within the max batch size and group entries into several records if necessary.
fn try_into_records(entries: Vec<EntryImpl>) -> Result<Vec<Record>> {
    entries
        .into_iter()
        .map(|entry| entry.try_into())
        .collect::<Result<Vec<_>>>()
}

/// Try to convert the given entry id into an offset used by Kafka.
fn try_into_offset(entry_id: EntryId) -> Result<i64> {
    entry_id
        .try_into()
        .map_err(|_| ConvertEntryIdToOffsetSnafu { entry_id }.build())
}

fn try_from_record(record: Record) -> Result<Vec<EntryImpl>> {
    let entry = record.try_into()?;
    Ok(vec![entry])
}

fn handle_fetch_result(
    fetch_result: std::result::Result<(RecordAndOffset, i64), RsKafkaError>,
    topic: &Topic,
    region_id: &RegionId,
    start_offset: i64,
) -> Result<Vec<EntryImpl>> {
    match fetch_result {
        Ok((record_and_offset, _)) => {
            let entries = try_from_record(record_and_offset.record)?
                .into_iter()
                .filter(|entry| entry.ns.region_id() == region_id)
                .collect();
            Ok(entries)
        }
        Err(e) => Err(e).context(ReadRecordFromKafkaSnafu {
            start_offset,
            topic,
            region_id: *region_id,
        }),
    }
}

// TODO(niebayes): add unit tests for KafkaLogStore.
