use std::cmp::min;
use std::io::{IoResult, IoError, InvalidInput, SeekStyle, BufWriter, BufReader, TempDir};
use std::io::{SeekSet, SeekEnd, SeekCur};
use std::mem::size_of;
use std::fmt::{Formatter, Result, Show};
use sync::{Arc, RWLock};
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

/*
 * a lookup might either return a record directly or return a TID which has to be
 * followed to the proper place
 */
enum LookupResult {
	Direct(Record),
	Indirect(TID),
}

/*
 * A remove operation might either succeed directly or succeed but return another
 * entry that also has to be deleted because it pointed to it.
 */
enum DeleteResult {
	DeleteDone,
	DeleteCascade(TID),
}

enum UpdateResult {
	UpdateDone,
	DeleteOld(TID),
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
					Err(e) => fail!("Failed writing length to page: {}", e)
				};
			}
			self.buffer_manager.unfix_page(pagelock, true);
		}
		//TODO remaining bytes from buf
		info!("copied {}/{}, location: {}", copied, buf.len(), self.location);
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
		debug!("Seeking {}", pos);
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
		debug!("Size: {}", size);

		let mut data: Vec<u8> = Vec::with_capacity(size as uint);
		let mut read = 0;

		debug!("location: {}", self.location);
		for i in range(1, self.location / buffer::PAGE_SIZE as u64 + 2) {
			debug!("Reading page {}", i);
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

#[deriving(Eq)]
struct Record {
	data: Vec<u8>,
}

impl Show for Record {
	fn fmt(&self, f: &mut Formatter) -> Result {
		write!(f.buf, "Record({})", self.data)
	}
}

impl Record {
	/* move semantics, no copy */
	pub fn new(data: Vec<u8>) -> Record {
		Record {data: data}
	}

	fn len(&self) -> uint {
		self.data.len()
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

impl Show for Slot {
	fn fmt(&self, f: &mut Formatter) -> Result {
		write!(f.buf, "Slot(offset={}, len={})", self.offset(), self.len())
	}
}

impl Slot {
	fn new(data: u64) -> Slot {
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
		let tid_marker = 0b11111111_11111111 << 48;
		Slot(tid_marker | n)
	}

	fn offset(&self) -> uint {
		let &Slot(n) = self;
		// mask out the high bits
		let mask = (1 << 24) - 1;
		((n >> 24) & mask) as uint
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
		//println!("SlottedPageHeader: {:?}", header);
		SlottedPage {frame: frame, header: header}
	}

	fn write_header(&mut self) {
		let mut frame = self.frame.write();
		let mut bw = BufWriter::new(frame.get_mut_data());
		match
		bw.write_le_uint(self.header.slot_count).and_then(|_|
		bw.write_le_uint(self.header.free_slot).and_then(|_|
		bw.write_le_uint(self.header.data_start).and_then(|_|
		bw.write_le_uint(self.header.free_space)))) {
			Ok(()) => (),
			Err(e) => fail!("Writing header failed, {}", e),
		}
	}

	fn try_insert(&mut self, r: &Record) -> (bool, uint) {
		info!("s.h.free_space {}", self.header.free_space);
		let record_len = r.len();
		if self.header.free_space < record_len + size_of::<Slot>() {
			return (false, 0)
		}
		// adjust the new start of data to be more to the frone
		self.header.data_start -= record_len;
		// we added the data plus one slot, reduce free space
		self.header.free_space -= record_len + size_of::<Slot>();
		let slot = Slot::new_from_offset_len(self.header.data_start, record_len);
		self.write_slot(self.header.free_slot, slot);
		{
			let mut frame = self.frame.write();
			let mut bw = BufWriter::new(frame.get_mut_data());
			// seek to place where we can store data
			match bw.seek(self.header.data_start as i64, SeekSet) {
				Ok(()) => (),
				Err(e) => fail!("Failed to seek to {} while trying to insert, {}",
					self.header.data_start, e),
			}
			// copy it over from record
			match bw.write(r.get_data()) {
				Ok(()) => (),
				Err(e) => fail!("Failed to write payload while trying to insert, {}",
					e),
			}
		}
		let res = (true, self.header.free_slot);
		self.header.free_slot += 1;
		self.header.slot_count += 1;

		self.write_header();
		res
	}

	fn lookup(&self, slot_id: uint) -> (bool, LookupResult) {
		let slot = self.read_slot(slot_id);
		let frame = self.frame.read();
		let mut br = BufReader::new(frame.get_data());

		if slot.is_tid() {
			// the slot contains a TID, not an (offset, len)
			return (false, Indirect(slot.as_tid()))
		}

		// jump to that offset
		match br.seek(slot.offset() as i64, SeekSet) {
			Ok(()) => (),
			Err(e) => fail!("Failed to seek to {} for record lookup, {}",
				slot.offset(), e),
		}
		// read length of data from there
		info!("Reading {} from offset {}", slot.len(), slot.offset());
		let content = match br.read_exact(slot.len()) {
			Ok(c) => c,
			Err(e) => fail!("Failed reading from segmented page, {}", e),
		};
		// construct and return a record from that data
		let v = Vec::from_slice(content);
		(false, Direct(Record::new(v)))
	}

	fn update(&self, tid_to_update: TID, new_tid: TID) -> (bool, UpdateResult) {
		let slot_id = tid_to_update.slot_id();
		let slot = self.read_slot(slot_id);
		let new_slot = Slot::new_from_tid(new_tid);

		self.write_slot(slot_id, new_slot);

		if slot.is_tid() {
			// the old slot contained a TID which is not referenced
			// anymore, so we have to delete it.
			(true, DeleteOld(slot.as_tid()))
		} else {
			// if it used to contain an (offset, len) we are done directly
			(true, UpdateDone)
		}
	}

	fn slot_offset(slot_id: uint) -> i64 {
		(size_of::<SlottedPageHeader>() + slot_id * size_of::<Slot>()) as i64
	}

	fn write_slot(&self, slot_id: uint, slot: Slot) {
		let slot_offset = SlottedPage::slot_offset(slot_id);
		let mut frame = self.frame.write();
		let mut bw = BufWriter::new(frame.get_mut_data());

		match bw.seek(slot_offset, SeekSet) {
			Ok(()) => (),
			Err(e) => fail!("Failed seeking to {} while writing slot, {}",
				slot_offset, e),
		}
		match bw.write_le_u64(slot.as_u64()) {
			Ok(()) => (),
			Err(e) => fail!("Failed writing slot data to {}, {}",
				slot_offset, e),
		}
	}

	fn read_slot(&self, slot_id: uint) -> Slot {
		let slot_offset = SlottedPage::slot_offset(slot_id);
		let frame = self.frame.read();
		let mut br = BufReader::new(frame.get_data());

		match br.seek(slot_offset, SeekSet) {
			Ok(()) => Slot::new(br.read_le_u64().unwrap()),
			Err(e) => fail!("Failed seeking to {} while reading slot, {}",
				slot_offset, e),
		}
	}

	fn remove(&mut self, slot_id: uint) -> (bool, DeleteResult) {
		let slot = self.read_slot(slot_id);
		info!("Removing slot_id {}, {:?}, is_tid? {}", slot_id, slot, slot.is_tid());
		// zero out the slot
		self.write_slot(slot_id, Slot::empty());

		self.header.slot_count -= 1;
		self.write_header();

		if slot.is_tid() {
			// this entry linked to another TID, tell the called to remove
			// it as well
			(true, DeleteCascade(slot.as_tid()))
		} else {
			// this was a leaf node, we're done deleting
			(true, DeleteDone)
		}
	}
}

#[deriving(Eq)]
pub struct TID(u64);

impl Show for TID {
	fn fmt(&self, f: &mut Formatter) -> Result {
		write!(f.buf, "TID(page_id={}, slot_id={})",
			self.page_id(), self.slot_id())
	}
}

impl TID {
	pub fn new(page_id: u64, slot_id: uint) -> TID {
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
	pub fn insert(&mut self, r: &Record) -> Option<TID> {
		for i in range(0, 1<<buffer::PAGE_BITS) {
			info!("Testing page {} for insertion", i);
			let pagelock = match self.manager.fix_page(join_segment(self.id, i as u64)) {
				Some(p) => p,
				None => fail!("Failed aquiring page {}", i),
			};
			let mut sp = SlottedPage::new(pagelock.clone());
			let (inserted, slot) = sp.try_insert(r);
			info!("try_insert: {}", inserted);
			self.manager.unfix_page(pagelock, inserted);
			if inserted {
				return Some(TID::new(i as u64, slot));
			}
		}
		// checked all the pages and didn't find any storage? whoa!
		None
	}

	pub fn remove(&mut self, tid: TID) -> bool {
		let slot_id = tid.slot_id();
		match self.with_slotted_page(tid, |mut sp| sp.remove(slot_id)) {
			DeleteDone => true,
			DeleteCascade(tid) => self.remove(tid),
		}
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
		// TODO: prepend old tid to record
		let new_tid = self.insert(r).unwrap();
		match self.with_slotted_page(tid, |sp| sp.update(tid, new_tid)) {
			UpdateDone => true,
			DeleteOld(obsolete_tid) => self.remove(obsolete_tid)
		}
	}
}

#[test]
fn create_schema() {
	let dir = match TempDir::new("slottedpage") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	let p = dir.path();
	//let p = Path::new(".");

	let name = Column {name: ~"name", datatype: Varchar(128), attributes: vec!(NotNull)};
	let age = Column {name: ~"age", datatype: Integer, attributes: vec!(NotNull)};
	let mut relation = Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);
	let mut schema = Schema::new();
	schema.add_relation(relation);

	let mut manager = buffer::BufferManager::new(1024, p.clone());
	schema.save_to_disk(&mut manager);
	let new_schema = Schema::new_from_disk(&mut manager);
	println!("new_schema == {:?}", new_schema);
}

#[test]
fn slotted_page_create() {
	let dir = match TempDir::new("slottedpage") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	let p = dir.path();
	//let p = Path::new(".");

	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let mut seg = SPSegment {id: 1, manager: &mut manager};

	let rec = Record::new(vec!(42));
	let tid = seg.insert(&rec).unwrap();
	println!("TID: {}", tid);

	let slot = Slot::new_from_tid(tid);
	println!("Slot: {}, was TID? {}", slot, slot.is_tid());
	let reconstructed_tid = slot.as_tid();
	println!("reconstructed TID: {} correct? {}", reconstructed_tid,
		reconstructed_tid == tid);
	assert_eq!(tid, reconstructed_tid);

	let rec2 = seg.lookup(tid);
	println!("Record (rec): {}", rec);
	println!("Record (rec2): {}", rec2);
	assert_eq!(rec, rec2);

	let rec3 = Record::new(vec!(23, 42));
	seg.update(tid, &rec3);
	let rec4 = seg.lookup(tid);
	println!("Record (rec4): {}", rec4);
	assert_eq!(rec3, rec4);

	let rec5 = Record::new(vec!(1, 2, 3));
	seg.update(tid, &rec5);
	let rec6 = seg.lookup(tid);
	println!("Record (rec5): {}", rec5);
	println!("Record (rec6): {}", rec6);
	assert_eq!(rec5, rec6);

	seg.remove(tid);
}
