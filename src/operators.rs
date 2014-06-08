mod schema;

struct Register;

impl Register {
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

trait Operatorish {
	fn open();
	fn next(&self) -> Option<Vec<&Register>>;
	fn close(&self);
}

struct TableScan {
	relation: schema::Relation,
}

impl TableScan {
	fn new(rel: schema::Relation) -> TableScan {
		TableScan {relation: rel}
	}
}

impl Iterator<Vec<Register>> for TableScan {
	fn next(&mut self) -> Option<Vec<Register>> {
		None
	}
}


#[test]
fn simple_tablescan() {
	let name = schema::Column {name: ~"name", datatype: schema::Varchar(128), attributes: vec!(schema::NotNull)};
	let age = schema::Column {name: ~"age", datatype: schema::Integer, attributes: vec!(schema::NotNull)};
	let mut relation = schema::Relation::new(~"Person");
	relation.add_column(name);
	relation.add_column(age);

	let mut ts = TableScan::new(relation);
	for tuple in ts {
		println!("Got entry {:?}", tuple);
	}

	assert!(false);
}
