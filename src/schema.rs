#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;
use std::cmp::min;
use std::io::{IoResult, IoError, InvalidInput, SeekStyle, MemWriter};
use std::io::{SeekSet, SeekEnd, SeekCur};
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
		let pageno = self.location / buffer::PAGE_SIZE as u64;
		let start_from = (self.location % buffer::PAGE_SIZE as u64) as uint;
		//let remaining_bytes = buffer::PAGE_SIZE - start_from;

		let pagelock = self.buffer_manager.fix_page(pageno).unwrap_or_else(|| fail!("Fix page failed"));
		let mut copied = 0;
		{
			let mut page = pagelock.write();
			let content = page.get_mut_data();

			for i in range(0, min(buf.len(), buffer::PAGE_SIZE)) {
				content[start_from+i] = buf[i];
				copied += 1;
				self.location += 1;
			}
		}
		//TODO remaining bytes from buf
		println!("copied {}/{}, location: {}", copied, buf.len(), self.location);
		//assert!(copied, buf.len());
		if copied == buf.len() {
			Ok(())
		} else {
			Err(IoError {
				kind: InvalidInput,
				desc: "Did not copy all data",
				detail: None,
			})
		}
	}
}

impl Seek for SchemaWriter {
	fn tell(&self) -> IoResult<u64> {
		Ok(self.location)
	}

	fn seek(&mut self, pos: i64, style: SeekStyle) -> IoResult<()> {
		println!("Seeking {}", pos);
		match style {
			SeekSet => {
				self.location = pos as u64;
				Ok(())
			}
			SeekEnd => {
				Err(IoError {
					kind: InvalidInput,
					desc: "seek to end not supported",
					detail: None,
				})
			}
			SeekCur => {
				self.location = (self.location as i64 + pos) as u64;
				Ok(())
			}
		}
	}
}

impl SchemaWriter {
	pub fn new(bufman: buffer::BufferManager) -> SchemaWriter {
		SchemaWriter {buffer_manager: bufman, location: 0}
	}

	pub fn get_data(&mut self) -> Vec<u8> {
		let mut data: Vec<u8> = Vec::new();

		println!("location: {}", self.location);
		for i in range(0, self.location / buffer::PAGE_SIZE as u64 + 1) {
			println!("Reading page {}", i);
			let pagelock = self.buffer_manager.fix_page(i).unwrap_or_else(
				|| fail!("Failed fixing page {}", i));
			let page = pagelock.read();
			data.push_all(page.get_data());
		}
		data
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

	pub fn save_to_disk(&self, bufmanager: buffer::BufferManager) {
		let mut wr1 = MemWriter::new();
		let mut wr2 = SchemaWriter::new(bufmanager);
		let v: u64 = 42;
		{
			let mut ebml_w1 = writer::Encoder(&mut wr1);
			let _ = self.encode(&mut ebml_w1);
			let mut ebml_w2 = writer::Encoder(&mut wr2);
			let _ = self.encode(&mut ebml_w2);
		}

		let dta = wr2.get_data();
		println!("wr1 len: {}", wr1.get_ref().len());
		println!("wr2 len: {}", dta.len());
		let ebml_doc1 = reader::Doc(wr1.get_ref());
		let ebml_doc2 = reader::Doc(dta.slice_to(290));
		let mut deser1 = reader::Decoder(ebml_doc1);
		let mut deser2 = reader::Decoder(ebml_doc2);
		let v1: Schema = match Decodable::decode(&mut deser1) {
			Ok(v) => v,
			Err(e) => fail!("Error decoding: {}", e),
		};
		println!("v1 == {:?}", v1);
		let v2: Schema = match Decodable::decode(&mut deser2) {
			Ok(v) => v,
			Err(e) => fail!("Error decoding: {}", e),
		};
		println!("v2 == {:?}", v2);

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
	let mut manager = buffer::BufferManager::new(1024, Path::new("."));
	schema.save_to_disk(manager);
	assert!(false);
}
