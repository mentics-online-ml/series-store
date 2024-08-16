mod util;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use anyhow::{bail, anyhow, Context};
use itertools::join;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;
use serde::de::DeserializeOwned;

use series_proc::EventHandler;
use shared_types::*;
use series::*;
use util::*;

// Reexports
pub use rdkafka::Offset;
pub use rdkafka::message::{BorrowedMessage,Message};

const TIMEOUT: Duration = Duration::from_millis(2000);
pub const PARTITION: i32 = 0;

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

    fn as_str(&self) -> &str {
        &self.name
    }
}
impl Display for Topic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.name)
    }
}

impl std::fmt::Debug for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Topic").field("name", &self.name).finish()
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
}

impl SeriesReader {
    pub fn new(group_id: &str) -> anyhow::Result<Self> {
        let consumer = util::create_consumer(group_id);
        Ok(Self { consumer, topics: Vec::new(), subscription: TopicPartitionList::new() })
    }

    pub fn new_topic(group_id: &str, topic: &Topic) -> anyhow::Result<Self> {
        Self::new_topic2(group_id, topic, false)
    }

    pub fn new_topic2(group_id: &str, topic: &Topic, reset: bool) -> anyhow::Result<Self> {
        let mut res = Self::new(group_id)?;
        let offset = if reset { Offset::Beginning } else { Offset::Stored };
        // res.subscribe(topic, offset)?;
        Self::subscribe(&mut res, topic, offset)?;
        Ok(res)
    }

    pub fn print_status(&self) -> anyhow::Result<()> {
        println!("Status for series on topics: {}", join(self.topics.iter(), ", "));
        // let s: String = self.consumer.group_metadata().unwrap().into();
        // println!("  group_metadata: {:?}", self.consumer.);
        println!("  committed_offsets: {:?}", self.consumer.committed(TIMEOUT)?);
        println!("  position (last read offset): {:?}", self.consumer.position()?);
        println!("  assignments: {:?}", self.consumer.assignment()?);

        for topic in self.topics.iter() {
            println!("  Topic: {topic}:");
            let watermarks = self.consumer.fetch_watermarks(topic.as_str(), PARTITION, TIMEOUT)?;
            println!("    watermarks: {:?}", watermarks);
        }
        Ok(())
    }

    // pub fn valid_offset_ids(&self, offset1: OffsetId, offset2: OffsetId) -> anyhow::Result<bool> {
    //     // TODO: this assumes only one topic which might not be correct?
    //     let (low, high) = self.consumer.fetch_watermarks(self.topics[0].as_str(), PARTITION, TIMEOUT)?;
    //     Ok(offset1 >= low && offset1 <= high && offset2 >= low && offset2 <= high)
    // }

    // pub fn fetch_watermarks(&self) -> anyhow::Result<(OffsetId, OffsetId)> {
    //     self.consumer.fetch_watermarks(self.topics[0].as_str(), PARTITION, TIMEOUT).with_context(|| "Error fetching watermarks")
    // }

    pub fn subscribe(&mut self, topic: &Topic, offset: Offset) -> anyhow::Result<()> {
        println!("Subscribing to topic: {}, offset: {:?}", topic, offset);
        if self.topics.contains(topic) {
            // If we allowed this through, it would result in error: "Subscription error: _CONFLICT"
            bail!("Attempted to subscribe to already subscribed topic: {}", topic);
        }
        self.topics.push(topic.to_owned());
        // let mut tpl = TopicPartitionList::new();
        self.subscription.add_partition_offset(topic.into(), PARTITION, offset)?;
        // self.consumer.incremental_assign(&tpl)?;
        self.consumer.assign(&self.subscription)?;
        // self.subscription = self.consumer.subscription()?;
        self.print_status()?;
        Ok(())
    }

    pub fn get_max_event_id(&self, topic: &Topic) -> anyhow::Result<EventId> {
        // self.seek_for(topic, PARTITION, Offset::OffsetTail(1))?;
        // let msg = self.read()?;
        // try_event_id(&msg)
        let (_, high) = self.consumer.fetch_watermarks(&topic.name, PARTITION, TIMEOUT).with_context(|| "Failed calling fetch_watermarks")?;
        Ok(high)
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

    pub fn commit(&self) -> anyhow::Result<()> {
        self.consumer.commit_consumer_state(CommitMode::Async).with_context(|| "Error committing consumer state")
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

    pub fn seek_for(&self, topic: &Topic, partition: i32, offset: Offset) -> anyhow::Result<()> {
        Ok(self.consumer.seek(&topic.name, partition, offset, TIMEOUT)?)
    }

    pub fn seek(&self, offset: OffsetId) -> anyhow::Result<()> {
        let off = if offset == 0 {
            Offset::Beginning
        } else {
            Offset::Offset(offset)
        };
        Ok(self.consumer.seek(&self.topics[0].name, PARTITION, off, TIMEOUT)?)
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
                        println!("Error {:?} processing message {:?}", e, msg);
                        false
                    })
                },
                Err(e) => {
                    println!("Error reading series-store {:?}", e);
                    false
                },
            };
            if !do_continue {
                break;
            }
        }
    }

    async fn proc_msg<'a, T: EventType, H: EventHandler<T>>(&self, msg: &BorrowedMessage<'a>, handler: &mut H) -> anyhow::Result<bool> {
        let event: T = msg_to_event(msg)?;
        Ok(handler.handle(event))
    }

    pub fn read(&self) -> anyhow::Result<BorrowedMessage> {
        self.try_read(None).transpose().unwrap()
    }

    pub fn try_read<T: Into<Timeout>>(&self, timeout: T) -> anyhow::Result<Option<BorrowedMessage>> {
        Ok(self.consumer.poll(timeout).transpose()?)
        // let res = self.consumer.poll(timeout).transpose()?;
        // res.with_context(|| "Series returned nothing")

        // match self.consumer.poll(Duration::from_millis(2000)) {
        //     Some(x) => Ok(x?),
        //     None => {
        //         match self.consumer.poll(Duration::from_millis(2000)) {
        //             Some(x) => Ok(x?),
        //             None => bail!("Timed out reading from series-store")
        //         }
        //     }
        // }
    }

    pub fn read_into_event<T: EventType>(&self) -> anyhow::Result<T> {
        msg_to_event(&self.read()?)
    }

    pub fn read_into<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        msg_to(&self.read()?)
    }

    pub fn collect_while<F,T: EventType>(&self, proc: F) -> anyhow::Result<Vec<T>>
    where F: Fn(&T) -> bool {
        let mut v = Vec::new();
        loop {
            let ev = self.read_into_event()?;
            if !proc(&ev) {
                break;
            }
            v.push(ev);
        }
        Ok(v)
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

    pub fn read_count_into<T: EventType>(&self, count: usize) -> anyhow::Result<Vec<T>> {
        (0..count).map(|_| {
            self.read_into_event()
        }).collect()
    }

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

    pub fn offset_from_oldest(&self, relative_offset: OffsetId) -> anyhow::Result<OffsetId> {
        if self.topics.len() > 1 {
            panic!("offset_from_oldest called for ambiguous topic")
        }
        let (low, _) = self.consumer.fetch_watermarks(&self.topics[0].name, PARTITION, TIMEOUT)?;
        // let (low, _) = self.consumer.fetch_watermarks(topic, PARTITION, TIMEOUT)?;
        Ok(low + relative_offset)
    }

    pub fn reset_offset(&self) -> anyhow::Result<()> {
        // println!("Called store_offset with {}", self.topics[0].name);
        self.consumer.seek(&self.topics[0].name, PARTITION, Offset::Beginning, TIMEOUT)?;
        self.consumer.poll(TIMEOUT);
        // self.consumer.commit(&self.subscription, CommitMode::Sync);
        self.consumer.commit_consumer_state(CommitMode::Sync)?;
        // self.consumer.store_offset(&self.topics[0].name, PARTITION, 0)?;
        // self.consumer.poll(Duration::from_millis(0));
        // self.consumer.commit(&self.subscription, CommitMode::Sync)?;
        Ok(())
    }
}

pub struct SeriesWriter {
    topic: Topic,
    producer: FutureProducer,
}

impl SeriesWriter {
    pub fn new(topic: Topic) -> Self {
        let producer = util::create_producer();
        Self { topic, producer }
    }

    pub async fn write_topic<'a, P: ToBytes + ?Sized>(
        &self, event_id: EventId, timestamp: i64, payload: &'a P
    ) -> anyhow::Result<(i32, i64)> {
        let meta = make_meta(EVENT_ID_FIELD, &serialize_event_id(event_id));
        let rec = FutureRecord::to((&self.topic).into())
            .key("key")
            .timestamp(timestamp)
            .headers(meta)
            .payload(payload);
        self.producer.send(rec, TIMEOUT).await.map_err(|(err, _)| {
                anyhow!("Failed to write to {}: {}", &self.topic, err)
            })
    }

    pub async fn write<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized>(&self,
        event_id: EventId,
        topic: &Topic,
        key: &'a K,
        timestamp: i64,
        payload: &'a P,
    ) -> anyhow::Result<(i32, i64)> { // OwnedDeliveryResult {
        let meta = make_meta(EVENT_ID_FIELD, &serialize_event_id(event_id));
        let rec = FutureRecord::to(topic.into())
            .key(key)
            .timestamp(timestamp)
            .headers(meta)
            .payload(payload);
        self.producer.send(rec, TIMEOUT).await.map_err(|(err, _)| {
                anyhow!("Failed to write to {}: {}", topic, err)
            })
    }
}

impl Default for SeriesWriter {
    fn default() -> Self {
        Self::new(Topic::new("", "", ""))
    }
}

impl<'a> From<&'a Topic> for &'a str {
    fn from(topic: &'a Topic) -> Self { &topic.name }
}

pub fn msg_to_event<T: EventType>(msg: &BorrowedMessage) -> anyhow::Result<T> {
    let raw = msg.payload();
    if let Some(bytes) = raw {
        let mut event: T = serde_json::from_reader(bytes)?;
        let event_id = try_event_id(msg)?;
        assert!(event_id != 0);
        event.set_ids(event_id, msg.offset());
        Ok(event)
    } else {
        bail!("No payload")
    }
}

pub fn msg_to<T: DeserializeOwned>(msg: &BorrowedMessage) -> anyhow::Result<T> {
    let raw = msg.payload();
    if let Some(bytes) = raw {
        Ok(serde_json::from_reader(bytes)
            .with_context(|| format!("Failed to deserialize {}: {}", msg.offset(), String::from_utf8(bytes.to_vec()).unwrap()  ))?
        )
    } else {
        bail!("No payload")
    }
}

// fn is_in_trading_time<T: EventType>(msg: &BorrowedMessage) -> anyhow::Result<bool> {
//     let x: T = msg_to(msg)?;
//     x.
//     Ok(is_in_trading_time(ts))
// }
