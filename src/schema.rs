#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;
use std::io::MemWriter;
use serialize::ebml::writer;
use serialize::ebml::reader;
use serialize::{Encodable, Decodable};
mod buffer;

enum SqlType {
	Char(uint),
	Varchar(uint),
	Integer,
}

enum SqlAttribute {
	Null,
	NotNull,
}

struct Column {
	name: ~str,
	datatype: SqlType,
	attributes: Vec<SqlAttribute>,
}

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

struct Schema {
	relations: Vec<Relation>,
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
			let _ = v.encode(&mut ebml_w);
		}
		let ebml_doc = reader::Doc(wr.get_ref());
		let mut deser = reader::Decoder(ebml_doc);
		let v1: u64 = Decodable::decode(&mut deser).unwrap();
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

/* our newtype struct: create a TID type as an alias to u64 */
struct TID(u64);

impl SPSegment {
	pub fn insert(r: Record) -> TID {
		TID(0_u64)
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
