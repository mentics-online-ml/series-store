mod util;

use std::array::IntoIter;
use std::time::Duration;

use anyhow::{bail, Context};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, ToBytes};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use shared_types::{Event, EventId, Logger, Raw};
use util::*;

pub struct SeriesReader {
    consumer: BaseConsumer,
    pub topics: Topics<String>,
    logger: Box<dyn Logger>,
}

impl SeriesReader {
    pub fn new(logger: Box<dyn Logger>) -> anyhow::Result<Self> {
        let topics = get_topics();
        let consumer = util::create_consumer();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topics.raw, 0, Offset::OffsetTail(1))?;
        tpl.add_partition_offset(&topics.event, 0, Offset::OffsetTail(1))?;
        tpl.add_partition_offset(&topics.label, 0, Offset::OffsetTail(1))?;
        consumer.assign(&tpl)?;

        Ok(Self { consumer, topics, logger })
    }

    pub fn try_most_recent_event_ids(&self) -> anyhow::Result<Topics<u64>> {
         self.topics_iter().map(|topic| {
            self.consumer.fetch_watermarks(&topic, 0, Duration::from_millis(2000)).map(|w| w.1 as u64)
        }).collect::<Result<Topics<u64>,KafkaError>>().with_context(|| "Could not get watermarks")
    }

    fn topics_iter(&self) -> IntoIter<&String, 3> { //std::slice::Iter<'_, &String> {
        [&self.topics.raw, &self.topics.event, &self.topics.label].into_iter()
    }

    pub fn try_most_recent(&self, topic: &str) -> anyhow::Result<BorrowedMessage> {
        match self.consumer.poll(Duration::from_millis(2000)) {
            Some(res) => res.with_context(|| format!("Error polling {topic}")),
            None => bail!("No message found for topic {}", topic),
        }
    }

    // pub fn foreach_event<F: Fn(Event) -> ()>(&self, func: F) {
    //     for maybe_msg in self.consumer.iter() {
    //         match maybe_msg {
    //             Ok(msg) => {
    //                 match msg.payload() {
    //                     Some(payload) => {
    //                         func(deserialize_event(payload));
    //                     }
    //                     None => self.logger.log(format!("Could not get payload for message {:?}", msg))
    //                 }
    //             },
    //             Err(e) => {
    //                 self.logger.log(format!("Error reading series-store {:?}", e));
    //             },
    //         }
    //     }
    // }

    pub async fn foreach_event<F, Fut>(&self, func: F)
    where
        Fut: std::future::Future<Output = ()>,
        F: Fn(Event) -> Fut,
    {
        for maybe_msg in self.consumer.iter() {
            match maybe_msg {
                Ok(msg) => {
                    match msg.payload() {
                        Some(payload) => {
                            let des = deserialize_event(payload);
                            func(des).await;
                        }
                        None => self.logger.log(format!("Could not get payload for message {:?}", msg))
                    }
                },
                Err(e) => {
                    self.logger.log(format!("Error reading series-store {:?}", e));
                },
            }
        }
    }

    pub fn seek(&self, topic: &str, offset: i64) -> anyhow::Result<()> {
        self.consumer.seek(&topic, 0, Offset::OffsetTail(-offset), Duration::from_millis(2000))?;
        Ok(())
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

    pub fn write_event<'a, K: ToBytes + ?Sized>(
        &'a self,
        key: &'a K,
        event_id: EventId,
        timestamp: i64,
        event: &'a Event,
    ) -> Result<DeliveryFuture, KafkaError> {
        self.write(
            event_id,
            &self.topics.event,
            key,
            timestamp,
            &serialize_event(&event),
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
