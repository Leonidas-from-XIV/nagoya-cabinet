use std::hash::Hash;
use std::io::TempDir;
use schema;
use buffer;

#[deriving(Show)]
struct Register {
	record: schema::Record,
}

impl Register {
	fn new(rec: schema::Record) -> Register {
		Register {
			record: rec,
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

	fn get_str(&self) -> ~str {
		// TODO
		~"TODO"
	}
}

// TODO: Iterator<T> + Ord + Hash
trait Operatorish<T>: Iterator<T> {
	//fn open();
	//fn close(&self);
}

struct TableScan<'a, 'b> {
	relation: schema::Relation,
	segment: &'a mut schema::SPSegment<'b>,
	current: uint,
}

impl<'a, 'b> TableScan<'a, 'b> {
	fn new(rel: schema::Relation, seg: &'a mut schema::SPSegment<'b>) -> TableScan<'a, 'b> {
		TableScan {
			relation: rel,
			segment: seg,
			current: 0,
		}
	}
}

impl<'a, 'b> Operatorish<Vec<Register>> for TableScan<'a, 'b> {
}

impl<'a, 'b> Iterator<Vec<Register>> for TableScan<'a, 'b> {
	fn next(&mut self) -> Option<Vec<Register>> {
		if (self.current as u64) < self.relation.inserted {
			let tup = self.relation.get(self.segment, self.current);
			let mut res = tup.move_iter().map(|e| Register::new(e)).
				collect::<Vec<Register>>();
			println!("res: {}", res);
			self.current += 1;
			Some(res)
		} else {
			None
		}
	}
}

/*
impl Ord for TableScan {
	fn lt(&self, other: &TableScan) -> bool {
		// TODO
		true
	}
}
*/

#[test]
fn simple_tablescan() {
	let dir = match TempDir::new("tablescan") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	//let p = dir.path();
	let p = Path::new(".");

	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let mut seg = schema::SPSegment {id: 1, manager: &mut manager};

	let name = schema::Column::new(~"name", schema::Varchar(128), vec!(schema::NotNull));
	let age = schema::Column::new(~"age", schema::Integer, vec!(schema::NotNull));
	let mut relation = schema::Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);
	relation.insert(&mut seg, vec!(schema::Record::from_str(~"Alice"), schema::Record::from_int(20)));
	relation.insert(&mut seg, vec!(schema::Record::from_str(~"Bob"), schema::Record::from_int(40)));

	let mut ts = TableScan::new(relation, &mut seg);
	for tuple in ts {
		println!("Got entry {:?}", tuple);
	}

	assert!(false);
}
