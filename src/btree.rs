mod buffer;
mod schema;

struct BTree<K> {
	dummy: K
}

impl<K: TotalOrd> BTree<K> {
	fn new(segment_id: u64, dummy: K) -> BTree<K> {
		BTree { dummy: dummy }
	}

	fn insert(&mut self, key: K, value: schema::TID) {
	}

	fn erase(&mut self, key: K) {
	}

	fn lookup(self, key: K) -> Option<schema::TID> {
		None
	}
}

#[test]
fn simple_insert() {
	let bt = BTree::new(23, 42);
}
