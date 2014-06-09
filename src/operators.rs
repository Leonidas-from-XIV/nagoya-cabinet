use std::hash::Hash;
use std::io::TempDir;
use schema;
use buffer;

struct Register<'a> {
	record: &'a schema::Record,
}

impl<'a> Register<'a> {
	fn setInteger(val: int) {
	}

	fn getInteger() -> int {
		0
	}

	fn setString() {
	}

	fn getString() -> ~str {
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
	segment: &'a schema::SPSegment<'b>,
}

impl<'a, 'b> TableScan<'a, 'b> {
	fn new(rel: schema::Relation, seg: &'a schema::SPSegment<'b>) -> TableScan<'a, 'b> {
		TableScan {
			relation: rel,
			segment: seg,
		}
	}
}

impl<'a, 'b, 'c> Operatorish<Vec<Register<'c>>> for TableScan<'a, 'b> {
}

impl<'a, 'b, 'c> Iterator<Vec<Register<'c>>> for TableScan<'a, 'b> {
	fn next(&mut self) -> Option<Vec<Register<'c>>> {
		//let tup = self.relation.get(0);
		None
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
