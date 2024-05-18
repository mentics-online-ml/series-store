use std::env;
use std::str::from_utf8;
use anyhow::Context;
use rdkafka::consumer::BaseConsumer;
use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureProducer;
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use shared_types::{EventId, Event};

pub const EVENT_ID_FIELD: &str = "event_id";

#[derive(Debug)]
pub struct Topics<T> {
    pub raw: T,
    pub event: T,
    pub label: T,
}

impl<T: Default> FromIterator<T> for Topics<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut ii = iter.into_iter();
        let raw = ii.next().unwrap_or_default();
        let event = ii.next().unwrap_or_default();
        let label = ii.next().unwrap_or_default();
        Topics { raw, event, label }
    }
}

fn get_brokers() -> String { env::var("REDPANDA_ENDPOINT").expect("Required environment variable REDPANDA_ENDPOINT not set") }

pub(crate) fn get_topics() -> Topics<String> {
    let topic_raw = env::var("TOPIC_RAW").unwrap();
    let topic_event = env::var("TOPIC_EVENT").unwrap();
    let topic_label = env::var("TOPIC_LABEL").unwrap();
    Topics {
        raw: topic_raw,
        event: topic_event,
        label: topic_label,
    }
}

pub(crate) fn create_producer() -> FutureProducer {
    let brokers = get_brokers();
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}

pub(crate) fn create_consumer() -> BaseConsumer {
    let brokers = get_brokers();
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "default_group")
        .create()
        .expect("Consumer creation error")
}

pub(crate) fn make_meta<B: ToBytes + ?Sized>(key: &str, value: &B) -> OwnedHeaders {
    OwnedHeaders::new().insert(Header {
        key,
        value: Some(value),
    })
}

pub(crate) fn try_header_value<'a>(msg: &'a BorrowedMessage, key:&str) -> Option<&'a [u8]> {
    msg.headers().map(|headers| headers.iter().find(|h| h.key == key).map(|h| h.value)).flatten().flatten()
}

pub(crate) fn try_event_id<'a>(msg: &'a BorrowedMessage) -> anyhow::Result<EventId> {
    let bytes = try_header_value(msg, EVENT_ID_FIELD).with_context(|| "Event id not found in message")?;
    deserialize_event_id(bytes)
}

 // TODO: Want to use binary someday, but during dev, text is easier to troubleshoot.

pub(crate) fn serialize_event_id(event_id: EventId) -> String {
    event_id.to_string()
}

pub(crate) fn deserialize_event_id(bytes: &[u8]) -> anyhow::Result<EventId> {
    let str = from_utf8(bytes)?;
    Ok(str.parse::<u64>()?)
}

pub(crate) fn serialize_event(event: &Event) -> Vec<u8> {
    // TODO
    format!("{}: placeholder data", event.id).into_bytes()
}

pub(crate) fn deserialize_event(bytes: &[u8]) -> Event {
    // TODO
    Event::default()
}

// pub(crate) fn to_event_id(off: Offset) -> u64 {
//     match off {
//         Offset::Offset(n) => n as u64,
//         _ => 0
//     }
// }
