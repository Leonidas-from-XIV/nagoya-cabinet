#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;
use std::io::{IoResult, IoError, IoUnavailable, SeekStyle, MemWriter};
use serialize::ebml::{reader,writer};
use serialize::{Encodable, Decodable};
mod buffer;

#[deriving(Encodable, Decodable)]
enum SqlType {
	Char(uint),
	Varchar(uint),
	Integer,
}

#[deriving(Encodable, Decodable)]
enum SqlAttribute {
	Null,
	NotNull,
}

#[deriving(Encodable, Decodable)]
struct Column {
	name: ~str,
	datatype: SqlType,
	attributes: Vec<SqlAttribute>,
}

#[deriving(Encodable, Decodable)]
struct Relation {
	name: ~str,
	columns: Vec<Column>,
}

impl Relation {
	pub fn new(name: ~str) -> Relation {
		Relation {name: name, columns: Vec::new()}
	}

	pub fn add_column(&mut self, column: Column) {
		self.columns.push(column);
	}
}

#[deriving(Encodable, Decodable)]
struct Schema {
	relations: Vec<Relation>,
}

struct SchemaWriter{
	buffer_manager: buffer::BufferManager,
	location: u64,
}

impl Writer for SchemaWriter {
	fn write(&mut self, buf: &[u8]) -> IoResult<()> {
		Err(IoError {kind: IoUnavailable, desc: "Unavailable", detail: None})
	}
}

impl Seek for SchemaWriter {
	fn tell(&self) -> IoResult<u64> {
		Ok(self.location)
	}

	fn seek(&mut self, pos: i64, style: SeekStyle) -> IoResult<()> {
		Err(IoError {kind: IoUnavailable, desc: "Unavailable", detail: None})
	}
}

impl SchemaWriter {
	pub fn new(bufman: buffer::BufferManager) -> SchemaWriter {
		SchemaWriter {buffer_manager: bufman, location: 0}
	}
}

impl Schema {
	pub fn new() -> Schema {
		Schema {relations: Vec::new()}
	}

	pub fn new_from_disk(bufmanager: buffer::BufferManager) -> Schema {
		// TODO
		Schema {relations: Vec::new()}
	}

	pub fn add_relation(&mut self, relation: Relation) {
		self.relations.push(relation);
	}

	pub fn save_to_disk(&self) {
		let mut wr = MemWriter::new();
		let v: u64 = 42;
		{
			let mut ebml_w = writer::Encoder(&mut wr);
			let _ = self.encode(&mut ebml_w);
		}
		let ebml_doc = reader::Doc(wr.get_ref());
		let mut deser = reader::Decoder(ebml_doc);
		let v1: Schema = Decodable::decode(&mut deser).unwrap();
		println!("v1 == {:?}", v1);

		for rel in self.relations.iter() {
			//rel.save_to_disk()
		}
	}
}

struct Record {
	// TODO: do we even need this?
	len: uint,
	data: Vec<u8>,
}

impl Record {
	/* move semantics, no copy */
	pub fn new(len: uint, data: Vec<u8>) -> Record {
		Record {len: len, data: data}
	}
	
	pub fn get_data<'a>(&'a self) -> &'a [u8] {
		self.data.as_slice()
	}
}

struct SPSegment;

/* our newtype struct: create a TID type as an alias to another type */
struct TID(u8);

impl SPSegment {
	pub fn insert(r: Record) -> TID {
		TID(0)
	}

	pub fn remove(tid: TID) -> bool {
		false
	}

	pub fn lookup(tid: TID) -> Record {
		// TODO
		Record {len: 1, data: vec!(1)}
	}

	pub fn update(tid: TID, r: Record) -> bool {
		false
	}
}


#[test]
fn main() {
}

#[test]
fn create_schema() {
	let name = Column {name: ~"name", datatype: Varchar(128), attributes: vec!(NotNull)};
	let age = Column {name: ~"age", datatype: Integer, attributes: vec!(NotNull)};
	let mut relation = Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);
	let mut schema = Schema::new();
	schema.add_relation(relation);
	schema.save_to_disk();
	assert!(false);
}
