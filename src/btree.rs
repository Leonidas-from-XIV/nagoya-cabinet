mod buffer;

struct BTree<K> {
	dummy: K
}

impl<K: TotalOrd> BTree<K> {
	fn new(segment_id: u64, dummy: K) -> BTree<K> {
		BTree { dummy: dummy }
	}
}

#[test]
fn simple_insert() {
	let bt = BTree::new(23, 42);
}
