use std::cast;
use std::mem::size_of;
use std::raw::Slice;
use std::num::Zero;
use std::rc::Rc;
use sync::Mutex;

mod buffer;
mod schema;

static LEAF_MARKER: u8 = 0b11111111;
static BRANCH_MARKER: u8 = 0b0;

/* simple type alias to simplify signatures */
type ConcurrentManager = Rc<Mutex<buffer::BufferManager>>;

/* a new trait which specifies which traits our keys should implement */
trait Keyish: TotalOrd + Zero + Clone {}
impl<T: TotalOrd + Zero + Clone> Keyish for T {}

struct BTree<'a, K> {
	segment: u64,
	manager: ConcurrentManager,
	tree: LazyNode,
	next_free_page: u64,
}

impl<'a, K: Keyish> BTree<'a, K> {
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
		// it is not allowed for the key to be the Zero value, that is used
		// as marker for invalid data
		assert!(!key.is_zero());
		let node = self.tree.load(self.manager.clone());
		// try insertion and see if the root was split
		let candidate = match node {
			Branch(mut n) => n.insert_value(self, key, value),
			Leaf(mut n) => n.insert_value(self, key, value),
		};
		// set new tree root if it was split
		match candidate {
			Some(new_node) => fail!("TODO: root split"),
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

	fn lookup(&self, key: &K) -> Option<schema::TID> {
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
	fn load<'a, K: Keyish>(&self, manager: ConcurrentManager) -> Node<'a, K> {
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

struct Overflowed<K>(K, u64);

struct LeafNode<'a, K> {
	capacity: uint,
	entries: &'a mut [LeafEntry<K>],
	manager: ConcurrentManager,
	frame: buffer::ConcurrentFrame,
}

impl<'a, K: Keyish> LeafNode<'a, K> {
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
			//println!("Entry {:?}", r[i]);
			if !r[i].key.is_zero() && !r[i].tid.is_invalid() {
				capacity -= 1;
			}
		}

		println!("Instantiating leaf node with capacity {}", capacity);
		LeafNode {
			entries: r,
			capacity: capacity,
			manager: manager,
			frame: frame,
		}
	}

	fn insert_value(&mut self, tree: &mut BTree<K>, key: K, tid: schema::TID) -> Option<Overflowed<K>> {
		println!("Leaf insertion, remaining capacity {}", self.capacity);
		if self.capacity == 0 {
			let lazy_node = tree.create_leaf_page();
			let new_node: Node<K> = lazy_node.load(self.manager.clone());
			let mut new_leaf = match new_node {
				Leaf(n) => n,
				Branch(_) => fail!("Got branch node where leaf was expected"),
			};
			let num_elements_to_move = self.entries.len()/2;
			let maximum = self.entries[num_elements_to_move].key.clone();

			// copy them over first
			for i in range(0, num_elements_to_move) {
				new_leaf.insert_value(tree,
					self.entries[i].key.clone(),
					self.entries[i].tid);
			}
			// erase them second
			for i in range(0, num_elements_to_move).rev() {
				let k = self.entries[i].key.clone();
				self.erase(&k);
			}

			// now let's actually insert that value
			if key <= maximum {
				// insert into new
				new_leaf.insert_value(tree, key, tid);
			} else {
				// insert into this
				self.insert_value(tree, key, tid);
			}
			// no worries, those can't overflow

			let overflow = Overflowed(maximum, lazy_node.page_id);
			return Some(overflow);
		}

		// find place to insert
		let location = self.find_slot(&key);
		println!("Location found: {}", location);
		// free that spot
		self.shift_from(location);
		// and put it in
		self.entries[location].key = key;
		self.entries[location].tid = tid;
		self.capacity -= 1;

		// insertion went fine, done
		None
	}

	fn erase(&mut self, key: &K) {
		for i in range(0, self.entries.len()) {
			if &self.entries[i].key == key {
				self.entries[i].key = Zero::zero();
				self.entries[i].tid = schema::TID::new(0, 0);
				self.capacity += 1;
				self.shift_to(i);
				break;
			}
		}
	}

	/* finds the location at which a key should be inserted */
	fn find_slot(&self, key: &K) -> uint {
		let mut found = None;
		for i in range(0, self.entries.len()) {
			//println!("Checking {:?} against {:?}", key, self.entries[i].key);
			if self.entries[i].key.is_zero() {
				found = Some(i);
				break;
			}
			// there is no prev element to compare
			if i == 0 {
				if key < &self.entries[i].key {
					found = Some(i);
					break;
				}
				continue;
			}

			if &self.entries[i - 1].key < key && key < &self.entries[i].key {
				found = Some(i);
				break;
			}
		}

		found.unwrap()
	}

	/* moves all items from `index` one number back */
	fn shift_from(&mut self, index: uint) {
		// actually, this rotates right by 1 starting from index
		let last_elem = self.entries.len() - 1;
		for i in range(index, last_elem) {
			self.entries.swap(i, last_elem);
		}
	}

	fn shift_to(&mut self, index: uint) {
		let last_elem = self.entries.len() - 1;
		for i in range(index, last_elem) {
			self.entries.swap(i, i+1);
		}
	}

	fn lookup(self, key: &K) -> Option<schema::TID> {
		for i in range(0, self.entries.len()) {
			//println!("Checking {:?} for {:?}", self.entries[i], key);
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
		//println!("Writing back leaf page {:?}", self.frame);
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

impl<'a, K: Keyish> BranchNode<'a, K> {
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

	fn insert_branch(&mut self, tree: &mut BTree<K>, key: K, value: u64) -> Option<Overflowed<K>> {
		if self.capacity == 0 {
			let lazy_node = tree.create_branch_page();
			let new_node = lazy_node.load(self.manager.clone());
			let mut new_branch = match new_node {
				Branch(b) => b,
				Leaf(_) => fail!("Got leaf node where branch was expected"),
			};
			let num_elements_to_move = self.entries.len()/2;
			let maximum = self.entries[num_elements_to_move].key.clone();
			// copy them over first
			for i in range(0, num_elements_to_move) {
				new_branch.insert_branch(tree,
					self.entries[i].key.clone(),
					self.entries[i].page_id);
			}
			// erase them second
			for i in range(0, num_elements_to_move).rev() {
				let k = self.entries[i].key.clone();
				self.erase(&k);
			}

			// now let's actually insert that value
			if key <= maximum {
				// insert into new
				new_branch.insert_branch(tree, key, value);
			} else {
				// insert into this
				self.insert_branch(tree, key, value);
			}
			// no worries, those can't overflow

			let overflow = Overflowed(maximum, lazy_node.page_id);
			println!("Overflow {:?}", overflow);
			//fail!("splitting branches TODO");
			return Some(overflow);
		}
		let index = self.find_slot(&key);
		println!("Adding new page reference at {}", index);
		self.shift_from(index);

		self.entries[index].page_id = value;
		self.entries[index].key = key;
		self.capacity -= 1;
		None
	}

	fn erase(&mut self, key: &K) {
		for i in range(0, self.entries.len()) {
			if &self.entries[i].key == key {
				self.entries[i].key = Zero::zero();
				self.entries[i].page_id = 0;
				self.capacity += 1;
				self.shift_to(i);
				break;
			}
		}
	}

	// duplicated from LeafNode
	fn shift_to(&mut self, index: uint) {
		let last_elem = self.entries.len() - 1;
		for i in range(index, last_elem) {
			self.entries.swap(i, i+1);
		}
	}

	// duplicated from LeafNode, Rust doesn't do inheritance
	fn find_slot(&self, key: &K) -> uint {
		let mut found = None;
		for i in range(0, self.entries.len()) {
			//println!("Checking {:?} against {:?}", key, self.entries[i].key);
			if self.entries[i].key.is_zero() {
				found = Some(i);
				break;
			}
			// there is no prev element to compare
			if i == 0 {
				if key < &self.entries[i].key {
					found = Some(i);
					break;
				}
				continue;
			}

			if &self.entries[i - 1].key < key && key < &self.entries[i].key {
				found = Some(i);
				break;
			}
		}

		found.unwrap()
	}

	// duplicated from LeafNode
	fn shift_from(&mut self, index: uint) {
		// actually, this rotates right by 1 starting from index
		let last_elem = self.entries.len() - 1;
		for i in range(index, last_elem) {
			self.entries.swap(i, last_elem);
		}
	}

	/* might return a new branch node if this one was split */
	fn insert_value(&mut self, tree: &mut BTree<K>, key: K, value: schema::TID) -> Option<Overflowed<K>> {
		// locate the place where to insert
		let mut place = None;
		for i in range(0, self.entries.len()) {
			if self.entries[i].page_id == 0 {
				continue
			}
			if key <= self.entries[i].key {
				println!("Found candidate, {:?} <= {:?}", key, self.entries[i].key);
				place = Some(i);
				break;
			}
		}

		match place {
			/* no already existing place to insert exists */
			None => {
				// there is no such page, but it should be created
				let lazy_node = tree.create_leaf_page();
				let new_node = lazy_node.load(tree.manager.clone());
				match new_node {
					Leaf(mut n) => n.insert_value(tree, key.clone(), value),
					Branch(_) => fail!("Did not create a leaf page"),
				};
				self.insert_branch(tree, key, lazy_node.page_id)
			},
			/* a place to insert it was found */
			Some(index) => {
				let lazy_node = LazyNode::new(self.entries[index].page_id);
				let new_node = lazy_node.load(tree.manager.clone());
				let overflowed = match new_node {
					Leaf(mut n) => n.insert_value(tree, key, value),
					Branch(mut n) => n.insert_value(tree, key, value),
				};
				match overflowed {
					None => None,
					Some(Overflowed(max, page)) => self.insert_branch(tree, max, page),
				}
			}
		}
	}

	fn lookup(self, manager: ConcurrentManager, key: &K) -> Option<schema::TID> {
		// find the page to descend to
		let mut next_page = None;
		for i in range(0, self.entries.len()) {
			// skip all empty fields
			if self.entries[i].key.is_zero() && self.entries[i].page_id == 0 {
				continue
			}
			if i == 0 {
				if key <= &self.entries[i].key {
					next_page = Some(self.entries[i].page_id);
				}
				continue;
			}
			println!("Entry {}: {:?}", i, self.entries[i]);
			if &self.entries[i-1].key < key && key <= &self.entries[i].key {
				// found the branch into which to descend
				next_page = Some(self.entries[i].page_id);
				break;
			}
		}
		println!("Going for {:?}", next_page);

		match next_page {
			// if there is no page to descend to, it can't be found
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
		//println!("Ohai, unfixing page");
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
	let res = bt.lookup(&42).unwrap();
	assert_eq!(some_tid, res);
}

#[test]
fn split_leaf_insert() {
	split_insert(true);
}

fn split_insert(leaf: bool) {
	let p = Path::new(".");
	let manager = buffer::BufferManager::new(1024, p.clone());
	let mut bt = BTree::new(23, Rc::new(Mutex::new(manager)));
	let some_tid = schema::TID::new(23, 42);

	// this causes it to fill the leaf first
	if leaf {
		bt.insert(301, some_tid);
	}

	for i in range(1, 260) {
		bt.insert(i, some_tid);
		let res = match bt.lookup(&i) {
			Some(v) => v,
			None => fail!("Couldn't find value previously inserted into {}", i),
		};
		assert_eq!(some_tid, res);
	}
	assert!(false);
}

#[test]
fn split_branch_insert() {
	split_insert(false);
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
