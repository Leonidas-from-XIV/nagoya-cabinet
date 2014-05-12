#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;
use std::cmp::min;
use std::io::{IoResult, IoError, InvalidInput, SeekStyle, BufWriter, BufReader};
use std::io::{SeekSet, SeekEnd, SeekCur};
use std::mem::size_of;
use sync::{Arc, RWLock};
use serialize::ebml::{reader,writer};
use serialize::{Encodable, Decodable};
mod buffer;

pub static SLOT_BITS: uint = 16;

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

enum LookupResult {
	Direct(Record),
	Indirect(TID),
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

struct SchemaWriter<'a> {
	buffer_manager: &'a mut buffer::BufferManager,
	location: u64,
	maximum: u64,
}

impl<'a> Writer for SchemaWriter<'a> {
	fn write(&mut self, buf: &[u8]) -> IoResult<()> {
		let pageno = self.location / buffer::PAGE_SIZE as u64 + 1;
		let start_from = (self.location % buffer::PAGE_SIZE as u64) as uint;

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
		self.buffer_manager.unfix_page(pagelock, true);
		if self.location > self.maximum {
			self.maximum = self.location;
			let pagelock = self.buffer_manager.fix_page(0).unwrap_or_else(
				|| fail!("Fixing zero page failed"));
			{
				let mut page = pagelock.write();
				let content = page.get_mut_data();
				let mut writer = BufWriter::new(content);
				match writer.write_le_u64(self.maximum) {
					Ok(_) => (),
					Err(e) => fail!("Failed writing lenght to page: {}", e)
				};
			}
			self.buffer_manager.unfix_page(pagelock, true);
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

impl<'a> Seek for SchemaWriter<'a> {
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

impl<'a> SchemaWriter<'a> {
	pub fn new<'b>(bufman: &'b mut buffer::BufferManager) -> SchemaWriter<'b> {
		SchemaWriter {buffer_manager: bufman, location: 0,
			maximum: 0}
	}

	pub fn get_data(&mut self) -> Vec<u8> {
		let pagelock = self.buffer_manager.fix_page(0).unwrap_or_else(
			|| fail!("Failed fixing 0 page for schema length"));
		let mut size;
		{
			let page = pagelock.read();
			let mut reader = BufReader::new(page.get_data());
			size = reader.read_le_u64().unwrap();
		}
		self.buffer_manager.unfix_page(pagelock, false);
		println!("Size: {}", size);

		let mut data: Vec<u8> = Vec::with_capacity(size as uint);
		let mut read = 0;

		println!("location: {}", self.location);
		for i in range(1, self.location / buffer::PAGE_SIZE as u64 + 2) {
			println!("Reading page {}", i);
			let pagelock = self.buffer_manager.fix_page(i).unwrap_or_else(
				|| fail!("Failed fixing page {}", i));
			{
				let page = pagelock.read();
				let content = page.get_data();
				let mut j = 0;
				while read < size {
					data.push(content[j]);
					j += 1;
					read += 1;
				}
			}
			self.buffer_manager.unfix_page(pagelock, false);
		}
		data
	}
}

impl Schema {
	pub fn new() -> Schema {
		Schema {relations: Vec::new()}
	}

	pub fn new_from_disk(bufmanager: &mut buffer::BufferManager) -> Schema {
		let mut wr = SchemaWriter::new(bufmanager);
		let data = wr.get_data();
		let ebml_doc = reader::Doc(data.as_slice());
		let mut deser = reader::Decoder(ebml_doc);
		let value: Schema = match Decodable::decode(&mut deser) {
			Ok(v) => v,
			Err(e) => fail!("Error decoding: {}", e),
		};
		value
	}

	pub fn add_relation(&mut self, relation: Relation) {
		self.relations.push(relation);
	}

	pub fn save_to_disk(&self, bufmanager: &mut buffer::BufferManager) {
		let mut wr = SchemaWriter::new(bufmanager);
		{
			let mut ebml_w = writer::Encoder(&mut wr);
			let _ = self.encode(&mut ebml_w);
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

struct SPSegment<'a> {
	id: u64,
	manager: &'a mut buffer::BufferManager,
}

struct SlottedPageHeader {
	slot_count: uint,
	free_slot: uint,
	data_start: uint,
	free_space: uint,
}

struct Slot(u64);

impl Slot {
	fn new(data: u64) -> Slot {
		assert!(data < 1<<48);
		Slot(data)
	}

	fn empty() -> Slot {
		Slot(0)
	}

	fn new_from_offset_len(offset: uint, len: uint) -> Slot {
		assert!(offset < 1<<24);
		assert!(len < 1<<24);
		Slot(offset as u64 << 24 | len as u64)
	}

	fn new_from_tid(tid: TID) -> Slot {
		let TID(n) = tid;
		let tid_marker = (0b11111111_11111111 << 48);
		Slot(tid_marker | n)
	}

	fn offset(&self) -> uint {
		let &Slot(n) = self;
		(n >> 24) as uint
	}

	fn len(&self) -> uint {
		let &Slot(n) = self;
		let mask = (1 << 24) - 1;
		(n & mask) as uint
	}

	fn is_tid(&self) -> bool {
		let &Slot(n) = self;
		// check if topmost 16 bit are all 1
		(n >> 48) == 0b11111111_11111111
	}

	fn as_tid(&self) -> TID {
		assert!(self.is_tid());
		let &Slot(n) = self;
		// delete topmost 16 bit by shifting left and back
		let cleared = (n << 16) >> 16;
		TID::new_from_u64(cleared)
	}

	fn as_u64(&self) -> u64 {
		let &Slot(n) = self;
		n
	}
}

struct SlottedPage {
	header: SlottedPageHeader,
	frame: Arc<RWLock<buffer::BufferFrame>>,
}

impl SlottedPage {
	pub fn new(frame: Arc<RWLock<buffer::BufferFrame>>) -> SlottedPage {
		let header = {
			let mut frame = frame.write();
			let mut br = BufReader::new(frame.get_data());
			let slot_count = br.read_le_uint().unwrap();
			let free_slot = br.read_le_uint().unwrap();
			let data_start = br.read_le_uint().unwrap();
			let free_space = br.read_le_uint().unwrap();

			if slot_count == 0 && free_space == 0 {
				// blank frame, construct header
				let slot_count = 0;
				let free_slot = 0;
				let data_start = buffer::PAGE_SIZE;
				let free_space = buffer::PAGE_SIZE - size_of::<SlottedPageHeader>();
				SlottedPageHeader {slot_count: slot_count,
					free_slot: free_slot, data_start: data_start,
					free_space: free_space}
			} else {
				SlottedPageHeader {slot_count: slot_count,
					free_slot: free_slot, data_start: data_start,
					free_space: free_space}
			}

		};
		println!("SlottedPageHeader: {:?}", header);
		SlottedPage {frame: frame, header: header}
	}

	fn write_header(&mut self) {
		let mut frame = self.frame.write();
		let mut bw = BufWriter::new(frame.get_mut_data());
		bw.write_le_uint(self.header.slot_count);
		bw.write_le_uint(self.header.free_slot);
		bw.write_le_uint(self.header.data_start);
		bw.write_le_uint(self.header.free_space);
	}

	fn try_insert(&mut self, r: &Record) -> (bool, uint) {
		println!("s.h.free_space {}", self.header.free_space);
		if self.header.free_space < r.len + size_of::<Slot>() {
			return (false, 0)
		}
		// adjust the new start of data to be more to the frone
		self.header.data_start -= r.len;
		// we added the data plus one slot, reduce free space
		self.header.free_space -= r.len + size_of::<Slot>();
		let slot = Slot::new_from_offset_len(self.header.data_start, r.len);
		{
			let mut frame = self.frame.write();
			let mut bw = BufWriter::new(frame.get_mut_data());
			// seek to place where we can store data
			bw.seek(self.header.data_start as i64, SeekSet);
			// copy it over from record
			bw.write(r.get_data());
			// seek to beginning of slot storage
			bw.seek((size_of::<SlottedPageHeader>() +
				self.header.slot_count * size_of::<Slot>()) as i64,
				SeekSet);
			// write out slot
			bw.write_le_u64(slot.as_u64());
		}
		let res = (true, self.header.slot_count);
		self.header.free_slot += 1;
		self.header.slot_count += 1;

		self.write_header();
		res
	}

	fn lookup(&self, slot_id: uint) -> (bool, LookupResult) {
		let frame = self.frame.read();
		let mut br = BufReader::new(frame.get_data());
		// move to slot position
		br.seek((size_of::<SlottedPageHeader>() + slot_id * size_of::<Slot>()) as i64,
			SeekSet);
		// read offset and length of slot_id
		let slot_data = br.read_le_u64().unwrap();
		let slot = Slot::new(slot_data);

		if slot.is_tid() {
			// the slot contains a TID, not an (offset, len)
			return (false, Indirect(slot.as_tid()))
		}

		// jump to that offset
		br.seek(slot.offset() as i64, SeekSet);
		// read length of data from there
		println!("Reading {} from offset {}", slot.len(), slot.offset());
		let content = match br.read_exact(slot.len()) {
			Ok(c) => c,
			Err(e) => fail!("Failed reading from segmented page, {}", e),
		};
		// construct and return a record from that data
		let v = Vec::from_slice(content);
		(false, Direct(Record {len: slot.len(), data: v}))
	}

	fn update(&self, slot_id: uint, r: &Record) -> (bool, bool) {
		// TODO
		(false, false)
	}

	fn remove(&self, slot_id: uint) -> (bool, bool) {
		let mut frame = self.frame.write();
		// TODO: check for moved slot first
		let mut bw = BufWriter::new(frame.get_mut_data());
		bw.seek((size_of::<SlottedPageHeader>() + slot_id * size_of::<Slot>()) as i64,
			SeekSet);
		// zero out the slot
		bw.write_le_u64(Slot::empty().as_u64());
		// done
		(true, true)
	}
}

struct TID(u64);

impl TID {
	fn new(page_id: u64, slot_id: uint) -> TID {
		assert!(page_id < 1<<32);
		assert!(slot_id < 1<<16);
		let res = page_id << 16 | slot_id as u64;
		assert!(res < 1<<48);
		TID(res)
	}

	fn new_from_u64(num: u64) -> TID {
		assert!(num < 1<<48);
		TID(num)
	}

	fn page_id(&self) -> u64 {
		let &TID(n) = self;
		n >> 16
	}

	fn slot_id(&self) -> uint {
		let &TID(n) = self;
		// slot id is 16 bit max
		(n as u16) as uint
	}
}

fn join_segment(segment: u64, page: u64) -> u64{
	(segment << buffer::PAGE_BITS) | page
}

impl<'a> SPSegment<'a> {
	pub fn insert(&mut self, r: &Record) -> TID {
		println!("inserting")
		for i in range(0, 1<<buffer::PAGE_BITS) {
			println!("Testing page {} for insertion", i);
			let pagelock = match self.manager.fix_page(join_segment(self.id, i as u64)) {
				Some(p) => p,
				None => fail!("Failed aquiring page {}", i),
			};
			let mut sp = SlottedPage::new(pagelock.clone());
			let (inserted, slot) = sp.try_insert(r);
			println!("try_insert: {}", inserted);
			self.manager.unfix_page(pagelock, inserted);
			if inserted {
				return TID::new(i as u64, slot);
			}
			break;
		}
		TID::new(0, 0)
	}

	pub fn remove(&mut self, tid: TID) -> bool {
		let slot_id = tid.slot_id();
		self.with_slotted_page(tid, |sp| sp.remove(slot_id))
	}

	/*
	 * fix a page, create slotted page, call the closure with that slotted
	 * page and unfix that page
	 */
	fn with_slotted_page<T>(&mut self, tid: TID, f: |SlottedPage| -> (bool, T)) -> T {
		let page_id = tid.page_id();
		let full_page_id = join_segment(self.id, page_id);
		let pagelock = match self.manager.fix_page(full_page_id) {
			Some(p) => p,
			None => fail!("Failed looking up page {}", page_id),
		};
		let (wrote, result) = {
			let sp = SlottedPage::new(pagelock.clone());
			f(sp)
		};
		self.manager.unfix_page(pagelock, wrote);
		result
	}

	pub fn lookup(&mut self, tid: TID) -> Record {
		let slot_id = tid.slot_id();
		match self.with_slotted_page(tid, |sp| sp.lookup(slot_id)) {
			Direct(record) => record,
			Indirect(tid) => {
				let slot_id = tid.slot_id();
				match self.with_slotted_page(tid, |sp| sp.lookup(slot_id)) {
					Indirect(_) => fail!("Multi-level indirections not supported"),
					Direct(record) => {
						// TODO: split of 8 bytes containing
						// original TID
						record
					}
				}
			}
		}
	}

	pub fn update(&mut self, tid: TID, r: &Record) -> bool {
		let slot_id = tid.slot_id();
		self.with_slotted_page(tid, |sp| sp.update(slot_id, r))
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
	schema.save_to_disk(&mut manager);
	let new_schema = Schema::new_from_disk(&mut manager);
	println!("new_schema == {:?}", new_schema);
	assert!(false);
}

#[test]
fn slotted_page_create() {
	let mut manager = buffer::BufferManager::new(1024, Path::new("."));
	let mut seg = SPSegment {id: 1, manager: &mut manager};
	let rec = Record {len: 1, data: vec!(42)};
	let tid = seg.insert(&rec);
	println!("TID: {:?}", tid);
	let slot = Slot::new_from_tid(tid);
	println!("Slot: {:?}, was TID? {}", slot, slot.is_tid());
	let reconstructed_tid = slot.as_tid();
	println!("reconstructed TID: {:?}", reconstructed_tid);
	let rec2 = seg.lookup(tid);
	println!("Record: {}", rec2.data);
	let rec3 = Record {len: 2, data: vec!(42, 42)};
	seg.update(tid, &rec3);
	seg.remove(tid);
	assert!(false);
}
