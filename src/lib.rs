#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate chrono;

use std::result;
use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};
use chrono::{DateTime, UTC, TimeZone};

#[derive(Debug)]
pub enum OplogError {
    MissingField(bson::ValueAccessError),
    Database(mongodb::Error),
    UnknownOperation(String),
}

impl From<bson::ValueAccessError> for OplogError {
    fn from(original: bson::ValueAccessError) -> OplogError {
        OplogError::MissingField(original)
    }
}

impl From<mongodb::Error> for OplogError {
    fn from(original: mongodb::Error) -> OplogError {
        OplogError::Database(original)
    }
}

type Result<T> = result::Result<T, OplogError>;

pub struct Oplog {
    cursor: Cursor,
}

#[derive(PartialEq, Debug)]
pub struct Operation<'a> {
    id: i64,
    timestamp: DateTime<UTC>,
    document: &'a bson::Document,
    kind: Kind<'a>
}

#[derive(PartialEq, Debug)]
pub enum Kind<'a> {
    Insert { namespace: &'a str },
    Update,
    Delete,
    Command,
    Database,
    Noop,
}

impl<'a> Operation<'a> {
    pub fn new(document: &'a bson::Document) -> Result<Operation<'a>> {
        let op = try!(document.get_str("op"));

        match op {
            "n" => document_to_noop(document),
            "i" => document_to_insert(document),
            _ => Err(OplogError::UnknownOperation(op.to_owned())),
        }
    }

    fn new_with_kind<'f>(document: &'f bson::Document, kind: Kind<'f>) -> Result<Operation<'f>> {
        let h = try!(document.get_i64("h"));
        let ts = try!(document.get_time_stamp("ts"));
        let o = try!(document.get_document("o"));

        Ok(Operation {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            document: o,
            kind: kind
        })
    }
}

fn document_to_noop(document: &bson::Document) -> Result<Operation> {
    Operation::new_with_kind(document, Kind::Noop)
}

fn document_to_insert(document: &bson::Document) -> Result<Operation> {
    let kind = Kind::Insert {
        namespace: try!(document.get_str("ns"))
    };

    Operation::new_with_kind(document, kind)
}

fn timestamp_to_datetime(timestamp: i64) -> DateTime<UTC> {
    let seconds = timestamp >> 32;
    let nanoseconds = ((timestamp & 0xFFFFFFFF) * 1000000) as u32;

    UTC.timestamp(seconds, nanoseconds)
}

impl Iterator for Oplog {
    type Item = bson::Document;

    fn next(&mut self) -> Option<bson::Document> {
        loop {
            if let Some(Ok(op)) = self.cursor.next() {
                return Some(op);
            }
        }
    }
}

impl Oplog {
    pub fn new(client: Client) -> Result<Oplog> {
        let coll = client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        Ok(Oplog { cursor: try!(coll.find(None, Some(opts))) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use bson::oid::ObjectId;
    use chrono::{UTC, TimeZone};

    macro_rules! assert_eq_pretty {
        ($left:expr, $right:expr) => {
            let (left, right) = (&$left, &$right);
            assert!(left == right,
                "assertion failed: `(left == right)`\n\
                left: {:#?}\n\
                right: {:#?}",
                left, right
            );

            assert!(right == left,
                "assertion failed: `(right == left)`\n\
                left: {:#?}\n\
                right: {:#?}",
                left, right
            );
        }
    }

    #[test]
    fn operation_converts_noops() {
        let ref doc = doc! {
            "ts" => (Bson::TimeStamp(1479419535 << 32)),
            "h" => (-2135725856567446411i64),
            "v" => 2,
            "op" => "n",
            "ns" => "",
            "o" => {
                "msg" => "initiating set"
            }
        };

        let operation = Operation::new(&doc).unwrap();
        assert_eq_pretty!(
            operation,
            Operation {
                id: -2135725856567446411i64,
                timestamp: UTC.timestamp(1479419535, 0),
                document: &doc! { "msg" => "initiating set" },
                kind: Kind::Noop,
            }
        );
    }

    #[test]
    fn operation_converts_inserts() {
        let oid = ObjectId::with_string("583050b26813716e505a5bf2").unwrap();
        let ref doc = doc! {
            "ts" => (Bson::TimeStamp(1479561394 << 32)),
            "h" => (-1742072865587022793i64),
            "v" => 2,
            "op" => "i",
            "ns" => "foo.bar",
            "o" => {
                "_id" => (Bson::ObjectId(oid.clone())),
                "foo" => "bar"
            }
        };
        let operation = Operation::new(doc).unwrap();

        assert_eq_pretty!(
            operation,
            Operation {
                id: -1742072865587022793i64,
                timestamp: UTC.timestamp(1479561394, 0),
                document: &doc! {
                    "_id" => (Bson::ObjectId(oid)),
                    "foo" => "bar"
                },
                kind: Kind::Insert { namespace: "foo.bar" }
            }
        );

        // Compare these
        // assert_eq!(
        //     operation,
        //     Operation::Database {
        //         id: 2013,
        //         namespace: "hello",
        //     }
        // );

        // assert_eq_pretty!(
        //     operation,
        //     Operation::Database {
        //         id: 2013,
        //         namespace: "hello",
        //     }
        // );
    }
}
