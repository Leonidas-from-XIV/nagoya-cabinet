use std::hash::Hash;
use std::io::TempDir;
mod schema;

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

struct TableScan {
	relation: schema::Relation,
}

impl TableScan {
	fn new(rel: schema::Relation) -> TableScan {
		TableScan {relation: rel}
	}
}

impl<'a> Operatorish<Vec<Register<'a>>> for TableScan {
}

impl<'a> Iterator<Vec<Register<'a>>> for TableScan {
	fn next(&mut self) -> Option<Vec<Register<'a>>> {
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

	//let mut manager = schema::buffer::BufferManager::new(1024, p.clone());
	//let mut seg = schema::SPSegment {id: 1, manager: &mut manager};

	let name = schema::Column {name: ~"name", datatype: schema::Varchar(128), attributes: vec!(schema::NotNull)};
	let age = schema::Column {name: ~"age", datatype: schema::Integer, attributes: vec!(schema::NotNull)};
	let mut relation = schema::Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);
	//relation.insert(seg, vec!(schema::Record::from_str(~"Alice"), schema::Record::from_int(20)));
	//relation.insert(seg, vec!(schema::Record::from_str(~"Bob"), schema::Record::from_int(40)));

	let mut ts = TableScan::new(relation);
	for tuple in ts {
		println!("Got entry {:?}", tuple);
	}

	assert!(false);
}
