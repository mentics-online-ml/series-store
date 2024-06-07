use std::env;
use std::str::from_utf8;
use anyhow::Context;
use rdkafka::consumer::BaseConsumer;
use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureProducer;
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use shared_types::EventId;

pub const EVENT_ID_FIELD: &str = "event_id";

fn get_brokers() -> String { env::var("REDPANDA_ENDPOINT").expect("Required environment variable REDPANDA_ENDPOINT not set") }

pub(crate) fn create_producer() -> FutureProducer {
    let brokers = get_brokers();
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}

pub(crate) fn create_consumer(group_id: &str) -> BaseConsumer {
    let brokers = get_brokers();
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "beginning")
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
    // msg.headers().map(|headers| headers.iter().find(|h| h.key == key).map(|h| h.value)).flatten().flatten()
    msg.headers().and_then(|headers| headers.iter().find(|h| h.key == key).map(|h| h.value)).flatten()
}

pub(crate) fn try_event_id(msg: &BorrowedMessage) -> anyhow::Result<EventId> {
    let bytes = try_header_value(msg, EVENT_ID_FIELD).with_context(|| "Event id not found in message")?;
    deserialize_event_id(bytes)
}

 // TODO: Want to use binary someday, but during dev, text is easier to troubleshoot.

pub(crate) fn serialize_event_id(event_id: EventId) -> String {
    event_id.to_string()
}

pub(crate) fn deserialize_event_id(bytes: &[u8]) -> anyhow::Result<EventId> {
    let str = from_utf8(bytes)?;
    Ok(str.parse::<EventId>()?)
}
