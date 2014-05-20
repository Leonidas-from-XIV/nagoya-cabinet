use std::cast;
use std::mem::size_of;
use std::raw::Slice;

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

/*
enum Node<K> {
	Inner(BranchPage<K>),
	Leaf(LeafPage<K>),
}
*/

struct LeafEntry<K> {
	key: K,
	tid: schema::TID,
}

struct BranchEntry<K> {
	key: K,
	page_id: u64,
}

struct LeafPage<'a, K> {
	entries: &'a [LeafEntry<K>],
}

impl<'a, K> LeafPage<'a, K> {
	fn new(page: &[u8]) -> LeafPage<'a, K> {
		let entry_size = size_of::<LeafEntry<K>>();
		//let mut r: &[LeafEntry<K>] = unsafe { cast::transmute(page) };
		//unsafe { r.set_len(buffer::PAGE_SIZE / entry_size) };
		let r = unsafe {
			cast::transmute(
				Slice::<LeafEntry<K>> {
					data: page.as_ptr() as *() as *LeafEntry<K>,
					len: page.len() / entry_size,
				}
			)
		};
		LeafPage { entries: r }
	}
}

struct BranchPage<'a, K> {
	entries: &'a [BranchEntry<K>],
}

#[test]
fn simple_insert() {
	let bt = BTree::new(23, 42);
}

#[test]
fn create_leaf_page() {
	let p = Path::new(".");
	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let pagelock = manager.fix_page(0).unwrap();
	let page = pagelock.read();
	let lp: LeafPage<u64> = LeafPage::new(page.get_data());
	println!("lp len: {}", lp.entries.len());
	assert!(false);
}
