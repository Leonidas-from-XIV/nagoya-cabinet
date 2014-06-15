use std::io::{TempDir, MemWriter};
use std::str::from_utf8;
use sync::{Arc, Mutex, RWLock};
use schema;
use buffer;

#[deriving(Show, Eq, Hash)]
struct Register {
	record: schema::Record,
	datatype: schema::SqlType,
}

impl Register {
	fn new(rec: schema::Record, typ: schema::SqlType) -> Register {
		Register {
			record: rec,
			datatype: typ,
		}
	}

	fn set_int(&mut self, val: int) {
		// TODO
	}

	fn get_int(&self) -> int {
		self.record.to_int()
	}

	fn set_str(&mut self) {
		// TODO
	}

	fn get_str<'a>(&'a self) -> &'a str {
		let d = self.record.get_data();
		from_utf8(d).unwrap()
	}
}

impl Ord for Register {
	fn lt(&self, other: &Register) -> bool {
		// not the most robust implementation
		self.get_int() < other.get_int()
	}
}

trait Operatorish<T>: Iterator<T> {
	//fn open();
	//fn close(&self);
}

struct TableScan {
	relation: schema::Relation,
	segment: Arc<Mutex<schema::SPSegment>>,
	current: uint,
}

impl TableScan {
	fn new(rel: schema::Relation, seg: Arc<Mutex<schema::SPSegment>>) -> TableScan {
		TableScan {
			relation: rel,
			segment: seg,
			current: 0,
		}
	}
}

impl Operatorish<Vec<Register>> for TableScan {
}

impl Iterator<Vec<Register>> for TableScan {
	fn next(&mut self) -> Option<Vec<Register>> {
		if (self.current as u64) < self.relation.inserted {
			let mut seg = self.segment.lock();
			let tup = self.relation.get(seg.deref_mut(), self.current);
			let mut res = tup.move_iter().map(|(v, t)| Register::new(v, t)).
				collect::<Vec<Register>>();
			debug!("TS: {}", res);
			self.current += 1;
			Some(res)
		} else {
			None
		}
	}
}

struct Print<'a, T, V> {
	input: T,
	output: &'a mut V,
}

impl<'a, T: Operatorish<Vec<Register>>, V: Writer> Print<'a, T, V> {
	fn new(input: T, output: &'a mut V) -> Print<'a, T, V> {
		Print {
			input: input,
			output: output
		}
	}
}

impl<'a, T: Operatorish<Vec<Register>>, V: Writer> Iterator<Vec<Register>> for Print<'a, T, V> {
	fn next(&mut self) -> Option<Vec<Register>> {
		let cur = self.input.next();
		match cur {
			Some(reg) => {
				for item in reg.iter() {
					match item.datatype {
						schema::Varchar(_) => self.output.write(item.get_str().as_bytes()),
						schema::Integer => self.output.write(
							format!("{}", item.get_int()).as_bytes())
					};
					self.output.write(", ".as_bytes());
				}
				self.output.write("\n".as_bytes());
				Some(reg)
			},
			None => None,
		}
	}
}

struct Project<T> {
	input: T,
	registerids: Vec<uint>,
}

impl<T: Operatorish<Vec<Register>>> Project<T> {
	fn new(input: T, regs: Vec<uint>) -> Project<T> {
		Project {
			input: input,
			registerids: regs,
		}
	}
}

impl<T: Operatorish<Vec<Register>>> Iterator<Vec<Register>> for Project<T> {
	fn next(&mut self) -> Option<Vec<Register>> {
		let cur = self.input.next();
		match cur {
			None => None,
			Some(mut reg) => {
				let mut res = Vec::with_capacity(self.registerids.len());
				let mut regids = self.registerids.clone();
				regids.reverse();
				for index in regids.move_iter() {
					let value = reg.remove(index);
					let v = match value {
						None => fail!("Projection doesn't have vield {}", index),
						Some(v) => v,
					};
					res.push(v);
				}
				Some(res)
			}
		}
	}
}

impl<T: Operatorish<Vec<Register>>> Operatorish<Vec<Register>> for Project<T> {
}

#[deriving(Show)]
enum Selectable {
	Varchar(~str),
	Integer(int),
}

struct Select<T> {
	input: T,
	index: uint,
	value: Selectable,
}

impl<T: Operatorish<Vec<Register>>> Select<T> {
	fn new(input: T, index: uint, value: Selectable) -> Select<T> {
		Select {
			input: input,
			index: index,
			value: value,
		}
	}
}

impl<T: Operatorish<Vec<Register>>> Iterator<Vec<Register>> for Select<T> {
	fn next(&mut self) -> Option<Vec<Register>> {
		// initial value
		let mut cur = self.input.next();
		while cur.is_some() {
			// we made sure cur is Some(…), so we can unwrap safely
			let reg = cur.unwrap();

			// check what we got to select for and retrieve values
			// accordingly
			match self.value {
				// comparing to a string
				Varchar(ref v) => {
					println!("Comparing {} with {}",
						reg.get(self.index).get_str(),
						v);
					// getting the register value as string
					if reg.get(self.index).get_str() == *v {
						return Some(reg)
					} else {
						// not equal, skip to next entry
						cur = self.input.next();
						continue
					}
				},
				// comparing to a number
				Integer(v) => {
					// compare against reg as integer
					if reg.get(self.index).get_int() == v {
						return Some(reg)
					} else {
						// not equal, skip to next entry
						cur = self.input.next();
						continue
					}
				}
			}
		}
		// no more entries, signal iterator exhaustion
		None
	}
}

impl<T: Operatorish<Vec<Register>>> Operatorish<Vec<Register>> for Select<T> {
}

struct HashJoin<T> {
	left: T,
	right: T,
	on: (uint, uint),
}

impl<T: Operatorish<Vec<Register>>> HashJoin<T> {
	fn new(left: T, right: T, on: (uint, uint)) -> HashJoin<T> {
		// TODO create hashmap of 'left'
		HashJoin {
			left: left,
			right: right,
			on: on,
		}
	}
}

impl<T: Operatorish<Vec<Register>>> Iterator<Vec<Register>> for HashJoin<T> {
	fn next(&mut self) -> Option<Vec<Register>> {
		None
	}
}

impl<T: Operatorish<Vec<Register>>> Operatorish<Vec<Register>> for HashJoin<T> {
}

#[test]
fn simple_tablescan() {
	let dir = match TempDir::new("tablescan") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	//let p = dir.path();
	let p = Path::new(".");

	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let mut seg = schema::SPSegment::new(1, Arc::new(RWLock::new(manager)));

	let name = schema::Column::new(~"name", schema::Varchar(128), vec!(schema::NotNull));
	let age = schema::Column::new(~"age", schema::Integer, vec!(schema::NotNull));
	let mut relation = schema::Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);
	relation.insert(&mut seg, vec!(schema::Record::from_str(~"Alice"), schema::Record::from_int(20)));
	relation.insert(&mut seg, vec!(schema::Record::from_str(~"Bob"), schema::Record::from_int(40)));
	let segmut = Arc::new(Mutex::new(seg));

	{
		let mut ts = TableScan::new(relation.clone(), segmut.clone());
		for tuple in ts {
			println!("Got entry {}", tuple);
		}
	}

	{
		let mut mw = MemWriter::new();
		{
			let mut ts = TableScan::new(relation.clone(), segmut.clone());
			let mut pr = Print::new(ts, &mut mw);
			// force write by iterating, strange API
			for _ in pr {}
		}
		println!("Saved: {}", from_utf8(mw.unwrap()).unwrap());
	}

	{
		let mut mw = MemWriter::new();
		{
			let mut ts = TableScan::new(relation.clone(), segmut.clone());
			let mut pr = Project::new(ts, vec!(0));
			let mut pr = Print::new(pr, &mut mw);
			for _ in pr {}
		}
		println!("Saved: {}", from_utf8(mw.unwrap()).unwrap());
	}

	{
		let mut mw = MemWriter::new();
		{
			let mut ts = TableScan::new(relation.clone(), segmut.clone());
			let mut se = Select::new(ts, 1, Integer(20));
			let mut pr = Print::new(se, &mut mw);
			for _ in pr {}
		}
		println!("Saved: {}", from_utf8(mw.unwrap()).unwrap());
	}

	{
		let mut mw = MemWriter::new();
		{
			let mut ts = TableScan::new(relation.clone(), segmut.clone());
			let mut se = Select::new(ts, 0, Varchar(~"Bob"));
			let mut pr = Print::new(se, &mut mw);
			for _ in pr {}
		}
		println!("Saved: {}", from_utf8(mw.unwrap()).unwrap());
	}

	assert!(false);
}

#[test]
fn simple_hashjoin() {
	let dir = match TempDir::new("hashjoin") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	//let p = dir.path();
	let p = Path::new(".");

	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let mut seg = schema::SPSegment::new(1, Arc::new(RWLock::new(manager)));

	/* first relation */
	let mut people = schema::Relation::new(~"Person");
	let id = schema::Column::new(~"id", schema::Integer, vec!(schema::NotNull));
	let name = schema::Column::new(~"name", schema::Varchar(128), vec!(schema::NotNull));
	people.add_column(id);
	people.add_column(name);
	people.insert(&mut seg, vec!(schema::Record::from_int(0), schema::Record::from_str(~"Alice")));
	people.insert(&mut seg, vec!(schema::Record::from_int(1), schema::Record::from_str(~"Bob")));
	people.insert(&mut seg, vec!(schema::Record::from_int(2), schema::Record::from_str(~"Eve")));
	people.insert(&mut seg, vec!(schema::Record::from_int(3), schema::Record::from_str(~"Mallory")));

	/* second relation */
	let mut oses = schema::Relation::new(~"OSes");
	let ident = schema::Column::new(~"ident", schema::Integer, vec!(schema::NotNull));
	let os = schema::Column::new(~"OS", schema::Varchar(128), vec!(schema::NotNull));
	oses.add_column(ident);
	oses.add_column(os);
	oses.insert(&mut seg, vec!(schema::Record::from_int(0), schema::Record::from_str(~"Plan 9")));
	oses.insert(&mut seg, vec!(schema::Record::from_int(1), schema::Record::from_str(~"NetBSD")));
	oses.insert(&mut seg, vec!(schema::Record::from_int(3), schema::Record::from_str(~"GNU/Linux")));


	let mut mw = MemWriter::new();
	let segmut = Arc::new(Mutex::new(seg));
	let mut ts_left = TableScan::new(oses, segmut.clone());
	let mut ts_right = TableScan::new(people, segmut.clone());
	let mut hj = HashJoin::new(ts_left, ts_right, (0,0));
	{
		let mut pr = Print::new(hj, &mut mw);
		for _ in pr {}
	}
	println!("Saved: {}", from_utf8(mw.unwrap()).unwrap());

	assert!(false);
}
