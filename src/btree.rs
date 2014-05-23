use std::cast;
use std::mem::size_of;
use std::raw::Slice;
use std::num::Zero;

mod buffer;
mod schema;

struct BTree<'a, K> {
	segment: u64,
	manager: &'a mut buffer::BufferManager,
	tree: LazyBranchNode,
}

impl<'a, K: TotalOrd + Zero> BTree<'a, K> {
	fn new<'b>(segment_id: u64, dummy: K, manager: &'b mut buffer::BufferManager) -> BTree<'b, K> {
		BTree {
			segment: segment_id,
			manager: manager,
			tree: LazyBranchNode::new(1),
		}
	}

	//fn locate_page

	fn insert(&mut self, key: K, value: schema::TID) {
		let node: BranchPage<K> = self.tree.load(self.manager);

		//for i in range(0, node.entries.len()) {
		//	println!("i {:?}", node.entries[i]);
		//}
		node.insert_value(key, value);
	}

	fn erase(&mut self, key: K) {
	}

	fn lookup(self, key: K) -> Option<schema::TID> {
		None
	}
}

/* a placeholder for the actual page */
struct LazyBranchNode {
	page_id: u64,
}

// TODO implement Drop to unfix page
impl LazyBranchNode {
	fn new(page_id: u64) -> LazyBranchNode {
		LazyBranchNode {page_id: page_id}
	}

	fn load<K>(&self, manager: &mut buffer::BufferManager) -> BranchPage<K> {
		let pagelock = manager.fix_page(self.page_id).unwrap();
		let page = pagelock.read();
		let bp: BranchPage<K> = BranchPage::new(page.get_data());
		bp
	}
}

enum Node<'a, K> {
	Inner(BranchPage<'a, K>),
	Leaf(LeafPage<'a, K>),
}

struct LeafEntry<K> {
	key: K,
	tid: schema::TID,
}

struct BranchEntry<K> {
	key: K,
	page_id: u64,
}

struct LeafPage<'a, K> {
	capacity: uint,
	entries: &'a [LeafEntry<K>],
}

fn load_node<'a, K: Zero>(page: &[u8]) -> Node<'a, K> {
	if page[0] == 255 {
		Leaf(LeafPage::new(page))
	} else {
		Inner(BranchPage::new(page))
	}
}

impl<'a, K: Zero> LeafPage<'a, K> {
	fn new(page: &[u8]) -> LeafPage<'a, K> {
		// first byte is a Leaf/Branch marker
		let entry_size = size_of::<LeafEntry<K>>();
		let entry_num = (page.len() - size_of::<u8>()) / entry_size;

		//let mut r: &[LeafEntry<K>] = unsafe { cast::transmute(page) };
		//unsafe { r.set_len(buffer::PAGE_SIZE / entry_size) };
		let r: &[LeafEntry<K>] = unsafe {
			cast::transmute(
				Slice::<LeafEntry<K>> {
					data: page.as_ptr() as *() as *LeafEntry<K>,
					len: entry_num,
				}
			)
		};

		/* find slots that are used */
		let mut capacity = r.len();
		for i in range(0, r.len()) {
			if !r[i].key.is_zero() && !r[i].tid.is_invalid() {
				capacity -= 1;
			}
		}

		LeafPage {
			entries: r,
			capacity: capacity
		}
	}

	fn insert(&mut self, key: K, tid: schema::TID) -> Option<()> {
		if self.capacity == 0 {
			return None
		}

		// find place to insert
		for i in range(0, self.entries.len()) {
			//let e = self.entries[i];
			println!("Entry {:?}", self.entries[i]);
		}
		Some(())
	}
}

struct BranchPage<'a, K> {
	capacity: uint,
	entries: &'a [BranchEntry<K>],
}

impl<'a, K> BranchPage<'a, K> {
	fn new(page: &[u8]) -> BranchPage<'a, K> {
		let entry_size = size_of::<BranchEntry<K>>();
		let entry_num = (page.len() - size_of::<u8>()) / entry_size;
		let r: &[BranchEntry<K>] = unsafe {
			cast::transmute(
				Slice::<BranchEntry<K>> {
					data: page.as_ptr() as *() as *BranchEntry<K>,
					len: entry_num,
				}
			)
		};

		/* find slots that are used */
		let mut capacity = r.len();
		for i in range(0, r.len()) {
			if r[i].page_id != 0 {
				capacity -= 1;
			}
		}

		println!("BranchPage computed capacity: {}", capacity);

		BranchPage {
			entries: r,
			capacity: capacity
		}
	}

	/* might return a new branch node if this one was split */
	fn insert_value(&self, key: K, value: schema::TID) -> Option<LazyBranchNode> {
		let mut place = 0;
		// locate the place where to insert
		for i in range(0, self.entries.len()) {
			if self.entries[i].page_id == 0 {
				continue
			}
			println!("Entry i {:?}, key {:?}", self.entries[i], self.entries[i].key);
			// TODO find place
		}
		let go_to_page = self.entries[place].page_id;
		if go_to_page == 0 {
			// TODO: create page
		}

		// currently not caring about splitting branch pages
		None
	}

	//fn insert_page
}

#[test]
fn simple_insert() {
	let p = Path::new(".");
	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let mut bt = BTree::new(23, 42, &mut manager);
	bt.insert(42, schema::TID::new(0, 0));
	assert!(false);
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
