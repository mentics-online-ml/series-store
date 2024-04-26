mod util;

use std::array::IntoIter;
use std::collections::HashMap;
use std::env;
use std::str::from_utf8;
use std::time::Duration;

use anyhow::{bail, Context};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, ToBytes};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use shared_types::{EventId, Features, Raw};
use util::*;

pub struct SeriesReader {
    consumer: BaseConsumer,
    topics: Topics<String>,
}

impl SeriesReader {
    pub fn new() -> anyhow::Result<Self> {
        let topics = get_topics();
        let consumer = util::create_consumer();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topics.raw, 0, Offset::OffsetTail(1))?;
        tpl.add_partition_offset(&topics.event, 0, Offset::OffsetTail(1))?;
        tpl.add_partition_offset(&topics.label, 0, Offset::OffsetTail(1))?;
        consumer.assign(&tpl)?;
        // consumer.seek_partitions(tpl, Duration::from_millis(1000))?;

        Ok(Self { consumer, topics })
    }

    pub fn try_most_recent_event_ids(&self) -> anyhow::Result<Topics<u64>> {
        // let list = self.consumer.committed(Duration::from_millis(2000))?;
        // println!("*************");
        // println!("committed: {:?}", list);
        // println!("position: {:?}", self.consumer.position()?);
        // println!("*************");
        // let elms = list.elements();
        // let hm: HashMap<_, _> = elms.iter().map(|tpl| (tpl.topic(), to_event_id(tpl.offset()))).collect();
        // Ok(Topics {
        //     raw: hm[self.topics.raw.as_str()],
        //     event: hm[&self.topics.event.as_str()],
        //     label: hm[&self.topics.label.as_str()]
        // })

        // let marks: Topics<u64> =
         self.topics_iter().map(|topic| {
            self.consumer.fetch_watermarks(&topic, 0, Duration::from_millis(2000)).map(|w| w.1 as u64)
        }).collect::<Result<Topics<u64>,KafkaError>>().with_context(|| "Could not get watermarks")
        // let raw = self.consumer.fetch_watermarks(&self.topics.raw, 0, Duration::from_millis(2000))?;
        // let event = self.consumer.fetch_watermarks(&self.topics.event, 0, Duration::from_millis(2000))?;
        // let label = self.consumer.fetch_watermarks(&self.topics.label, 0, Duration::from_millis(2000))?;
        // println!("watermarks: {:?}", marks);
        // marks
    }

    fn topics_iter(&self) -> IntoIter<&String, 3> { //std::slice::Iter<'_, &String> {
        [&self.topics.raw, &self.topics.event, &self.topics.label].into_iter()
    }

    // pub fn try_most_recent_raw(&self) -> anyhow::Result<Raw> {
    //     let msg = self.try_most_recent(&self.topics.raw)?;
    //     let event_id = try_event_id(&msg)?;
    //     let bytes = msg.payload().with_context(|| format!("Invalid payload in message {event_id}"))?;
    //     let str = from_utf8(bytes)?;
    //     Ok(Raw {id: event_id, raw: str.to_string() })
    // }

    // pub fn try_most_recent_features(&self) -> anyhow::Result<Features> {
    //     let msg = self.try_most_recent(&self.topics.raw)?;
    //     let event_id = try_event_id(&msg)?;
    //     let bytes = msg.payload().with_context(|| format!("Invalid payload in message {event_id}"))?;
    //     Ok(Features { id: event_id, x: deserialize_features(bytes).x })
    // }

    pub fn try_most_recent(&self, topic: &str) -> anyhow::Result<BorrowedMessage> {
        // let c = self.consumer.committed(Duration::from_millis(2000));
        // println!("Committed 1:\n{:?}", c);

        // // let hm = HashMap::from([((topic.to_string(), 0), Offset::OffsetTail(1))]);
        // // let tpl = TopicPartitionList::from_topic_map(&hm).unwrap();
        // let mut tpl = TopicPartitionList::new();
        // let tp = tpl.add_partition(topic, 0);

        // let pos = self.consumer.position()?;
        // println!("Position 1:\n{:?}", pos);

        // self.consumer.assign(&tpl)?;

        // let pos = self.consumer.position()?;
        // println!("Position 2:\n{:?}", pos);

        // let c = self.consumer.committed(Duration::from_millis(2000));
        // println!("Committed 2:\n{:?}", c);

        // let pos = self.consumer.position()?;
        // println!("TPL:\n{:?}", pos);
        // panic!("stop");

        match self.consumer.poll(Duration::from_millis(2000)) {
            Some(res) => res.with_context(|| format!("Error polling {topic}")),
            None => bail!("No message found for topic {}", topic),
        }
    }
}

pub struct SeriesWriter {
    producer: FutureProducer,
    topics: Topics<String>,
}

impl SeriesWriter {
    pub fn new() -> Self {
        let producer = util::create_producer();
        Self { producer, topics: get_topics() }
    }

    pub fn write_raw<'a, K: ToBytes + ?Sized>(
        &'a self,
        key: &'a K,
        event_id: EventId,
        timestamp: i64,
        raw: &'a str,
    ) -> Result<DeliveryFuture, KafkaError> {
        self.write(event_id, &self.topics.raw, key, timestamp, raw)
            .map_err(|e| e.0)
    }

    pub fn write_features<'a, K: ToBytes + ?Sized>(
        &'a self,
        key: &'a K,
        event_id: EventId,
        timestamp: i64,
        features: &'a Features,
    ) -> Result<DeliveryFuture, KafkaError> {
        self.write(
            event_id,
            &self.topics.event,
            key,
            timestamp,
            &serialize_features(&features),
        )
        .map_err(|e| e.0)
    }

    fn write<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized>(
        &self,
        event_id: EventId,
        topic: &'a str,
        key: &'a K,
        timestamp: i64,
        payload: &'a P,
    ) -> Result<DeliveryFuture, (KafkaError, FutureRecord<'a, K, P>)> {
        let meta = make_meta(EVENT_ID_FIELD, &serialize_event_id(event_id));
        let rec = FutureRecord::to(topic)
            .key(key)
            .timestamp(timestamp)
            .headers(meta)
            .payload(payload);
        self.producer.send_result(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // let result = add(2, 2);
        // assert_eq!(result, 4);
    }
}
