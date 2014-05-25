use std::cast;
use std::mem::size_of;
use std::raw::Slice;
use std::num::Zero;
use std::rc::Rc;
use sync::Mutex;

mod buffer;
mod schema;

static LEAF_MARKER: u8 = 0b111111111;
static BRANCH_MARKER: u8 = 0b0;

/* simple type alias to simplify signatures */
type ConcurrentManager = Rc<Mutex<buffer::BufferManager>>;

struct BTree<'a, K> {
	segment: u64,
	manager: ConcurrentManager,
	tree: LazyNode,
	next_free_page: u64,
}

impl<'a, K: TotalOrd + Zero> BTree<'a, K> {
	fn new<'b>(segment_id: u64, manager: ConcurrentManager) -> BTree<'b, K> {
		let tree_base = buffer::join_segment(segment_id, 1);
		// TODO read tree and next free page from page 0
		BTree {
			segment: segment_id,
			manager: manager,
			tree: LazyNode::new(tree_base),
			next_free_page: 2,
		}
	}

	fn insert(&mut self, key: K, value: schema::TID) {
		let node = self.tree.load(self.manager.clone());
		// try insertion and see if the root was split
		let candidate = match node {
			Branch(mut n) => n.insert_value(self, key, value),
			Leaf(mut n) => n.insert_value(key, value),
		};
		// set new tree root if it was split
		match candidate {
			Some(new_node) => self.tree = new_node,
			None => (),
		}
	}

	fn next_page(&mut self) -> u64 {
		let n = self.next_free_page;
		self.next_free_page += 1;
		n
	}

	fn create_branch_page(&mut self) -> LazyNode {
		let next = self.next_page();
		let page_path = buffer::join_segment(self.segment, next);
		let mut manager = self.manager.lock();
		let pagelock = manager.fix_page(page_path).unwrap();
		let mut page = pagelock.write();
		let data = page.get_mut_data();
		data[0] = BRANCH_MARKER;
		LazyNode::new(page_path)
	}

	fn create_leaf_page(&mut self) -> LazyNode {
		let next = self.next_page();
		let page_path = buffer::join_segment(self.segment, next);
		let mut manager = self.manager.lock();
		let pagelock = manager.fix_page(page_path).unwrap();
		let mut page = pagelock.write();
		let data = page.get_mut_data();
		// marker to be a leaf page
		data[0] = LEAF_MARKER;
		LazyNode::new(page_path)

	}

	fn erase(&mut self, key: K) {
	}

	fn lookup(self, key: &K) -> Option<schema::TID> {
		let node = self.tree.load(self.manager.clone());
		match node {
			Branch(n) => n.lookup(self.manager.clone(), key),
			Leaf(n) => n.lookup(key),
		}
	}
}

/* a placeholder for the actual page */
struct LazyNode {
	page_id: u64,
}

impl LazyNode {
	fn new(page_id: u64) -> LazyNode {
		LazyNode {page_id: page_id}
	}

	/*
	 * As this is just a placeholder, return the actual node that his is
	 * representing
	 */
	fn load<'a, K: TotalOrd + Zero>(&self, manager: ConcurrentManager) -> Node<'a, K> {
		let pagelock = {
			let mut managerlock = manager.lock();
			managerlock.fix_page(self.page_id).unwrap()
		};

		let mut is_leaf = false;
		{
			let page = pagelock.read();
			let page_data = page.get_data();
			if page_data[0] == LEAF_MARKER {
				is_leaf = true;
			} else if page_data[0] == BRANCH_MARKER {
				is_leaf = false;
			} else {
				fail!("Invalid page type");
			}
		}
		if is_leaf {
			Leaf(LeafNode::new(manager, pagelock))
		} else {
			Branch(BranchNode::new(manager, pagelock))
		}
	}

}

/* a node might either be an inner node (branch node) or a leaf node (LeafNode) */
enum Node<'a, K> {
	Branch(BranchNode<'a, K>),
	Leaf(LeafNode<'a, K>),
}

/* leaf pages consist mainly of leaf entries which are (K, TID) pairs */
struct LeafEntry<K> {
	key: K,
	tid: schema::TID,
}

/* branch pages consist mainly of branch entries which are (K, page_id) pairs */
struct BranchEntry<K> {
	key: K,
	page_id: u64,
}


struct LeafNode<'a, K> {
	capacity: uint,
	entries: &'a mut [LeafEntry<K>],
	manager: ConcurrentManager,
	frame: buffer::ConcurrentFrame,
}

impl<'a, K: TotalOrd + Zero> LeafNode<'a, K> {
	fn new(manager: ConcurrentManager, frame: buffer::ConcurrentFrame) -> LeafNode<'a, K> {
		let mut r = {
			let framelock = frame.read();
			let page = framelock.get_data();
			// first byte is a Leaf/Branch marker
			let entry_size = size_of::<LeafEntry<K>>();
			let entry_num = (page.len() - size_of::<u8>()) / entry_size;
			let start_from = entry_size;

			let mut entries: &mut[LeafEntry<K>] = unsafe {
				cast::transmute(
					Slice::<LeafEntry<K>> {
						data: page.slice_from(start_from).as_ptr() as *() as *LeafEntry<K>,
						len: entry_num,
					}
				)
			};
			entries
		};

		/* find slots that are used */
		let mut capacity = r.len();
		for i in range(0, r.len()) {
			if !r[i].key.is_zero() && !r[i].tid.is_invalid() {
				capacity -= 1;
			}
		}

		println!("Creating leaf node");
		LeafNode {
			entries: r,
			capacity: capacity,
			manager: manager,
			frame: frame,
		}
	}

	fn insert_value(&mut self, key: K, tid: schema::TID) -> Option<LazyNode> {
		if self.capacity == 0 {
			// TODO split
			fail!("Not implemented");
			return None
		}

		// find place to insert
		let location = self.find_slot(&key);
		println!("Location found: {}", location);
		// free that spot
		self.shift_from(location);
		// and put it in
		self.entries[location].key = key;
		self.entries[location].tid = tid;

		// insertion went fine, done
		None
	}

	/* finds the location at which a key should be inserted */
	fn find_slot(&self, key: &K) -> uint {
		let mut found = 0;
		for i in range(0, self.entries.len()) {
			println!("Entry {:?}", self.entries[i]);
			if &self.entries[i].key > key {

				found = i - 1;
				break;
			}
		}
		found
	}

	/* moves all items from `index` one number back */
	fn shift_from(&mut self, index: uint) {
		// actually, this rotates right by 1 starting from index
		let last_elem = self.entries.len() - 1;
		for i in range(index, last_elem) {
			self.entries.swap(i, last_elem);
		}
	}

	fn lookup(self, key: &K) -> Option<schema::TID> {
		for i in range(0, self.entries.len()) {
			if &self.entries[i].key == key {
				return Some(self.entries[i].tid)
			}
		}
		None
	}
}

#[unsafe_destructor]
impl<'a, K> Drop for LeafNode<'a, K> {
	fn drop(&mut self) {
		let mut manager = self.manager.lock();
		println!("Ohai, unfixing page");
		// TODO: figure out if page was modified since loading
		manager.unfix_page(self.frame.clone(), true);
	}
}

struct BranchNode<'a, K> {
	capacity: uint,
	entries: &'a mut [BranchEntry<K>],
	manager: ConcurrentManager,
	frame: buffer::ConcurrentFrame,
}

impl<'a, K: TotalOrd + Zero> BranchNode<'a, K> {
	fn new(manager: ConcurrentManager, frame: buffer::ConcurrentFrame) -> BranchNode<'a, K> {
		let mut r = {
			let framelock = frame.read();
			let page = framelock.get_data();

			let entry_size = size_of::<BranchEntry<K>>();
			let entry_num = (page.len() - size_of::<u8>()) / entry_size;
			let start_from = entry_size;
			let mut r: &mut [BranchEntry<K>] = unsafe {
				cast::transmute(
					Slice::<BranchEntry<K>> {
						data: page.slice_from(start_from).as_ptr() as *() as *BranchEntry<K>,
						len: entry_num,
					}
				)
			};
			r
		};

		/* find slots that are used */
		let mut capacity = r.len();
		for i in range(0, r.len()) {
			if r[i].page_id != 0 {
				capacity -= 1;
			}
		}

		println!("BranchNode computed capacity: {}", capacity);

		BranchNode {
			entries: r,
			capacity: capacity,
			manager: manager,
			frame: frame,
		}
	}

	/* might return a new branch node if this one was split */
	fn insert_value(&mut self, tree: &mut BTree<K>, key: K, value: schema::TID) -> Option<LazyNode> {
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
			let lazy_node = tree.create_leaf_page();
			let new_node = lazy_node.load(tree.manager.clone());
			let split_candidate = match new_node {
				Leaf(mut n) => n.insert_value(key, value),
				Branch(_) => fail!("Did not create a leaf page"),
			};
			self.entries[place].page_id = lazy_node.page_id;
			return split_candidate
		}

		// currently not caring about splitting branch pages
		None
	}

	fn lookup(self, manager: ConcurrentManager, key: &K) -> Option<schema::TID> {
		println!("Doing lookup in branch node");
		let mut next_page = None;
		for i in range(0, self.entries.len()) {
			// skip all empty fields
			if self.entries[i].key.is_zero() && self.entries[i].page_id == 0 {
				continue
			}
			println!("Entry i {:?}, key {:?}", self.entries[i], self.entries[i].key);
			if &self.entries[i].key <= key {
				// found the branch into which to descend
				next_page = Some(self.entries[i].page_id);
				break;
			}
		}
		match next_page {
			None => None,
			Some(page_id) => {
				let ln = LazyNode::new(page_id);
				let node = ln.load(manager);
				match node {
					Branch(n) => n.lookup(self.manager.clone(), key),
					Leaf(n) => n.lookup(key),
				}
			}
		}
	}
}

#[unsafe_destructor]
impl<'a, K> Drop for BranchNode<'a, K> {
	fn drop(&mut self) {
		let mut manager = self.manager.lock();
		println!("Ohai, unfixing page");
		// TODO: figure out if page was modified since loading
		manager.unfix_page(self.frame.clone(), true);
	}
}

#[test]
fn simple_insert() {
	let p = Path::new(".");
	let manager = buffer::BufferManager::new(1024, p.clone());
	let mut bt = BTree::new(23, Rc::new(Mutex::new(manager)));
	let some_tid = schema::TID::new(0, 0);
	bt.insert(42, some_tid);
	println!("Lookup: {}", bt.lookup(&42));

	assert!(false);
}

#[test]
fn lookup_nonexisting() {
	let p = Path::new(".");
	let manager = buffer::BufferManager::new(1024, p.clone());
	let bt = BTree::new(23, Rc::new(Mutex::new(manager)));
	let result = bt.lookup(&42);
	assert_eq!(result, None);
}

#[test]
fn create_leaf_page() {
	let p = Path::new(".");
	let mut manager = buffer::BufferManager::new(1024, p.clone());
	let pagelock = manager.fix_page(10).unwrap();
	let page = pagelock.read();
	//let lp: LeafNode<u64> = LeafNode::new(page.get_data(), Rc::new(Mutex::new(manager)), pagelock);
	//println!("lp len: {}", lp.entries.len());
	assert!(false);
}
