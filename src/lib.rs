#![feature(trait_alias)]

mod util;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use anyhow::bail;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::ToBytes;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::TopicPartitionList;
use serde::de::DeserializeOwned;

use shared_types::*;
use util::*;


// Reexports
pub use rdkafka::Offset;
pub use rdkafka::message::{BorrowedMessage,Message};

const TIMEOUT: Duration = Duration::from_millis(2000);
const PARTITION: i32 = 0;

pub trait EventType = SeriesEvent + DeserializeOwned;

#[derive(Clone,PartialEq,Eq,Hash)]
pub struct Topic {
    pub name: String,
    pub object_type: String,
    pub symbol: String,
    pub event_type: String
}

impl Topic {
    pub fn new(object_type: &str, symbol: &str, event_type: &str) -> Self {
        let name = Self::topic_name(object_type, symbol, event_type);
        Self { name, object_type: object_type.to_owned(), symbol: symbol.to_owned(), event_type: event_type.to_owned() }
    }

    fn topic_name(object_type: &str, symbol: &str, event_type: &str) -> String {
        [object_type, symbol, event_type].join("-")
    }
}
impl Display for Topic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.name)
    }
}

pub const SYMBOLS: [&str; 2] = ["SPY", "SPX"];
pub const EVENT_TYPES: [&str; 3] = ["quote", "trade", "timesale"];

pub fn expected_topics(object_type: &str) -> Vec<Topic> {
    SYMBOLS.iter().flat_map(|symbol| {
        EVENT_TYPES.iter().map(move |event_type| {
            Topic::new(object_type, symbol, event_type)
        })
    }).collect()
}

pub struct SeriesReader {
    consumer: BaseConsumer,
    topics: Vec<Topic>,
    subscription: TopicPartitionList,
    logger: Box<dyn Logger>,
}

impl SeriesReader {
    pub fn new(logger: Box<dyn Logger>) -> anyhow::Result<Self> {
        let consumer = util::create_consumer();
        Ok(Self { consumer, topics: Vec::new(), subscription: TopicPartitionList::new(), logger })
    }

    pub fn subscribe(&mut self, topic: &Topic, offset: Offset) -> anyhow::Result<()> {
        // let offset = Offset::End;
        self.topics.push(topic.to_owned());

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic.into(), PARTITION, offset)?;
        self.consumer.incremental_assign(&tpl)?;

        // let topics: Vec<&str> = self.topics.iter().map(|t| t.name.as_str()).collect();
        // self.consumer.subscribe(&topics)?;

        // let metadata = self.consumer.fetch_metadata(None, TIMEOUT)?;
        // println!("metadata: {:?}", metadata.topics().len());

        self.subscription = self.consumer.subscription()?;
        // println!("subscription: {:?}", self.subscription);
        // println!("assignment: {:?}", self.consumer.assignment());

        // let partition = self.subscription.elements_for_topic(&topic.name).first().map_or(0, |p| p.partition());
        // println!("Calling seek with: {:?}, {}, {:?}", topic.name, PARTITION, offset);
        // self.consumer.seek(&topic.name, PARTITION, offset, TIMEOUT)?;
        Ok(())
    }

    pub fn get_max_event_id(&self, topic: &Topic) -> anyhow::Result<EventId> {
        self.seek(topic, Offset::OffsetTail(1))?;
        let msg = self.read()?;
        try_event_id(&msg)
    }

    pub fn calc_next_event_ids(&mut self) -> anyhow::Result<HashMap<Topic, EventId>> {
        // let mut mapping = HashMap::new();
        expected_topics("raw").iter().map(|topic| {
            self.subscribe(topic, Offset::End)?;
            let next_id = if let Ok(id) = self.get_max_event_id(topic) {
                println!("For topic {}, found max id {}", topic.name, id);
                id + 1
            } else {
                1
            };
            Ok((topic.to_owned(), next_id))
            // mapping.insert(topic.to_owned(), next_id);
        }).collect()
    }

    // pub fn try_most_recent_event_ids(&self, topic: &Topic) -> anyhow::Result<u64> {
    //     self.consumer.fetch_watermarks(&topic.name, PARTITION, TIMEOUT)
    //         .map(|w| w.1 as u64).with_context(|| format!("Could not get watermarks for topic {}", topic.name))
    //     //  self.topics_iter().map(|topic| {
    //     //     self.consumer.fetch_watermarks(topic, 0, TIMEOUT).map(|w| w.1 as u64)
    //     // }).collect::<Result<Topics<u64>,KafkaError>>().with_context(|| "Could not get watermarks")
    // }

    // fn topics_iter(&self) -> IntoIter<&String, 3> { //std::slice::Iter<'_, &String> {
    //     [&self.topics.raw, &self.topics.event, &self.topics.label].into_iter()
    // }

    // pub fn try_most_recent(&self, topic: &str) -> anyhow::Result<BorrowedMessage> {
    //     match self.consumer.poll(TIMEOUT) {
    //         Some(res) => res.with_context(|| format!("Error polling {topic}")),
    //         None => bail!("No message found for topic {}", topic),
    //     }
    // }

    pub fn seek(&self, topic: &Topic, offset: Offset) -> anyhow::Result<()> {
        Ok(self.consumer.seek(&topic.name, PARTITION, offset, TIMEOUT)?)
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

    // pub async fn foreach_event<F, Fut>(&self, func: F)
    // where
    //     Fut: std::future::Future<Output = ()>,
    //     F: Fn(Event) -> Fut,
    // {
    //     for maybe_msg in self.consumer.iter() {
    //         match maybe_msg {
    //             Ok(msg) => {
    //                 match msg.payload() {
    //                     Some(payload) => {
    //                         let des = deserialize_event(payload);
    //                         func(des).await;
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

    // pub async fn for_each_msg<T, F, Fut>(&self, func: F)
    pub async fn for_each_msg<A: EventType, T: EventHandler<A>>(&self, handler: &mut T)
    where
        // T: DeserializeOwned,
        // Fut: std::future::Future<Output = bool>,
        // F: Fn(T) -> Fut,
    {
        for maybe_msg in self.consumer.iter() {
            let do_continue = match maybe_msg {
                Ok(msg) => {
                    // println!("Offset: {:?}", msg.offset());
                    self.proc_msg(&msg, handler).await.unwrap_or_else(|e| {
                        self.logger.log(format!("Error {:?} processing message {:?}", e, msg));
                        false
                    })
                },
                Err(e) => {
                    self.logger.log(format!("Error reading series-store {:?}", e));
                    false
                },
            };
            if !do_continue {
                break;
            }
        }
    }

    async fn proc_msg<'a, T: EventType, H: EventHandler<T>>(&self, msg: &BorrowedMessage<'a>, handler: &mut H) -> anyhow::Result<bool> {
        let event: T = msg_to(msg)?;
        Ok(handler.handle(event))
    }

    pub fn read(&self) -> anyhow::Result<BorrowedMessage> {
         match self.consumer.poll(Duration::from_millis(2000)) {
            Some(x) => Ok(x?),
            None => bail!("Timed out reading from series-store")
         }
    }

    pub fn read_into<T: EventType>(&self) -> anyhow::Result<T> {
        msg_to(&self.read()?)
    }

    // pub fn skip_if<F>(&self, predicate: F) -> anyhow::Result<BorrowedMessage>
    // where F: Fn(&BorrowedMessage) -> anyhow::Result<bool> {
    //     loop {
    //         let msg = self.read()?;
    //         if predicate(&msg)? {
    //             return Ok(msg);
    //         }
    //     }
    // }

    // pub fn skip_count_if<F>(&self, count:usize, predicate: F) -> anyhow::Result<Option<BorrowedMessage>>
    // where F: Fn(&BorrowedMessage) -> anyhow::Result<bool> {
    //     for _ in 0..(count - 1) {
    //         let msg = self.read()?;
    //         if !predicate(&msg)? {
    //             return Ok(None);
    //         }
    //      }
    //      Wrong: Ok(Some(self.read()?))
    // }

    // pub fn skip_event_while<F, T: EventType>(&self, predicate: F) -> anyhow::Result<T>
    // where F: Fn(&T) -> anyhow::Result<bool> {
    //     loop {
    //         let msg = self.read()?;
    //         let ev: T = msg_to(&msg)?;
    //         if !predicate(&ev)? {
    //             return Ok(ev);
    //         }
    //     }
    // }

    // pub fn skip_event_while_count<F, T: EventType>(&self, count: usize, predicate: F) -> anyhow::Result<T>
    // where F: Fn(&T) -> anyhow::Result<bool> {
    //     for _ in 0..(count-1) {
    //         let msg = self.read()?;
    //         let ev: T = msg_to(&msg)?;
    //         if !predicate(&ev)? {
    //             return Ok(ev);
    //         }
    //     }
    // }

    pub fn read_count(&self, count: usize) -> anyhow::Result<Vec<BorrowedMessage>> {
        (0..count).map(|_| {
            self.read()
        }).collect()

        // println!("Reading {} messages", count);
        // let assignment = self.consumer.assignment()?;
        // println!("  assignment: {:?}", assignment);
        // let watermarks = self.consumer.fetch_watermarks(assignment.elements()[0].topic(), 0, TIMEOUT)?;
        // println!("  watermarks: {:?}", watermarks);
        // let pos = self.consumer.position()?;
        // println!("  pos: {:?}", pos);

        // let watermarks = self.consumer.fetch_watermarks(self.topics, PARTITION, TIMEOUT)?;
        // if watermarks.1 - watermarks.0 < count as i64 {
        //     bail!("Not enough messages found {} < {}", watermarks.1 - watermarks.0, count);
        // }
        // let mut v = Vec::with_capacity(count);
        // for _ in 0..count {
        //     v.push(self.read()?);
        //     // match self.consumer.poll(Duration::from_millis(2000)) {
        //     //     Some(Ok(msg)) => {
        //     //         v.push(msg);
        //     //     },
        //     //     Some(Err(e)) => return Err(anyhow!(e)), //Err(anyhow::Error::new(e)),
        //     //     None => bail!("Insufficient messages found {}", i)
        //     // }
        // }
        // Ok(v)
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

    // pub fn write_raw<'a, K: ToBytes + ?Sized>(&'a self,
    //     key: &'a K,
    //     event_type: &'a str,
    //     symbol: &'a str,
    //     event_id: EventId,
    //     timestamp: i64,
    //     raw: &'a str,
    // ) -> Result<DeliveryFuture, KafkaError> {
    //     let topic = topic_name(&self.topics.raw, symbol, event_type);
    //     self.write(event_id, &topic, key, timestamp, raw)
    // }

    // pub fn write_event<'a, K: ToBytes + ?Sized>(&'a self,
    //     key: &'a K,
    //     event_id: EventId,
    //     timestamp: i64,
    //     event: &'a Event,
    // ) -> Result<DeliveryFuture, KafkaError> {
    //     self.write(event_id, &self.topics.event, key, timestamp, &serialize_event(event))
    // }

    pub fn write<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized>(&self,
        event_id: EventId,
        topic: &Topic,
        key: &'a K,
        timestamp: i64,
        payload: &'a P,
    ) -> Result<DeliveryFuture, KafkaError> {
        let meta = make_meta(EVENT_ID_FIELD, &serialize_event_id(event_id));
        let rec = FutureRecord::to(topic.into())
            .key(key)
            .timestamp(timestamp)
            .headers(meta)
            .payload(payload);
        self.producer.send_result(rec).map_err(|e| e.0)
    }
}

// impl From<Topic> for String {
//     fn from(topic: Topic) -> Self { topic.name }
// }

impl<'a> From<&'a Topic> for &'a str {
    fn from(topic: &'a Topic) -> Self { &topic.name }
}

pub fn msg_to<T: EventType>(msg: &BorrowedMessage) -> anyhow::Result<T> {
    let raw = msg.payload();
    if let Some(bytes) = raw {
        // let payload = std::str::from_utf8(bytes)?;
        let mut event: T = serde_json::from_reader(bytes)?;
        let event_id = try_event_id(msg)?;
        assert!(event_id != 0);
        event.set_event_id(event_id);
        Ok(event)
    } else {
        bail!("No payload")
    }
    // let bytes = msg.payload().ok_or(anyhow::anyhow!("No payload"))?;
    // let payload = std::str::from_utf8(bytes)?;
    // Ok(serde_json::from_str(payload)?)
    // let quote: Quote = serde_json::from_str(payload)?;
}

// fn is_in_trading_time<T: EventType>(msg: &BorrowedMessage) -> anyhow::Result<bool> {
//     let x: T = msg_to(msg)?;
//     x.
//     Ok(is_in_trading_time(ts))
// }

pub trait EventHandler<T: DeserializeOwned> {
    fn handle(&mut self, event: T) -> bool;
}
