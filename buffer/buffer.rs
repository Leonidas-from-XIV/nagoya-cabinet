extern crate collections;
extern crate sync;
extern crate rand;
use collections::HashMap;
use std::io::{File, Open, Read, Write, TempDir};
use std::num::Zero;
use std::comm::{Data, Empty, Disconnected};
use sync::{Arc, RWLock};
use sync::Future;
use rand::task_rng;
use rand::distributions::{IndependentSample, Range};
use rand::distributions::range::SampleRange;

struct BufferManager {
	size: uint,
	entries: HashMap<u64, BufferEntry>,
	path: Path,
}

struct BufferEntry {
	frame: Arc<RWLock<BufferFrame>>,
}

#[deriving(Eq)]
enum Status {
	Free,
	Fixed(uint),
}

struct BufferFrame {
	page_id: u64,
	data: Vec<u8>,
	fixed: Status,
}

/* Destructor trait implementation */
impl Drop for BufferManager {
	fn drop(&mut self) {
		println!("Dropping");
		// TODO
	}
}

impl BufferManager {
	pub fn new(size: uint, path: Path) -> BufferManager {
		let h = HashMap::with_capacity(size);
		BufferManager {size: size, entries: h, path: path}
	}

	fn open_or_create(&self, page_id: u64) -> File {
		let file_path = self.path.join(page_id.to_str());
		match File::open_mode(&file_path, Open, Read) {
			Ok(f) => f,
			Err(_) => {
				match File::open_mode(&file_path, Open, Write) {
					Ok(mut f) => {
						match f.write([0_u8, ..4 * 1024]) {
							Ok(_) => (),
							Err(e) => fail!("writing page failed: {}", e),
						}
						match File::open_mode(&file_path, Open, Read) {
							Ok(f) => f,
							Err(e) => fail!("failed reading file: {}", e),
						}
					},
					Err(e) => fail!("Writing file failed: {}", e),
				}
			},
		}
	}

	/*
	 * returns true if page could be loaded
	 */
	fn load_page(&mut self, page_id: u64) -> bool {
		if self.entries.len() == self.size {
			if !self.evict_page() {
				return false;
			}
		}

		let mut file_handle = self.open_or_create(page_id);
		//let content = match file_handle.read_exact(4*1024) {
		let content = match file_handle.read_exact(10) {
			Ok(c) => Vec::from_slice(c),
			Err(e) => fail!("Couldn't read from page: {}", e),
		};

		let frame = BufferFrame {data: content, page_id: page_id, fixed: Free};
		let entry = BufferEntry {frame: Arc::new(RWLock::new(frame))};
		self.entries.insert(page_id, entry);
		true
	}

	/*
	 * return false if no page could be evicted
	 */
	fn evict_page(&mut self) -> bool {
		let random_key = {
			let mut free_keys = self.entries.iter().filter_map(|e| {
				// iterate over entries, check if they are free
				// and return the keys of the free entries
				let (k,v) = e;
				let frame = v.frame.read();
				if frame.fixed == Free {
					Some(k)
				} else {
					None
				}
			});
			// pick a random key
			sample(&mut free_keys).map(|v| v.clone())
		};

		match random_key {
			None => false,
			Some(key) => self.entries.remove(&key),
		}
	}
	
	pub fn fix_page(&mut self, page_id: u64) -> Option<Arc<RWLock<BufferFrame>>> {
		if !self.entries.contains_key(&page_id) {
			if !self.load_page(page_id) {
				return None;
			}
		}
		let entry = self.entries.get(&page_id);
		{
			let mut f = entry.frame.write();
			f.fixed = match f.fixed {
				Free => Fixed(1),
				Fixed(n) => Fixed(n+1),
			};
		}
		// Arcs can be cloned and they will all point to the same RWLock
		Some(entry.frame.clone())
	}

	pub fn unfix_page(&mut self, frame: Arc<RWLock<BufferFrame>>, is_dirty: bool) {
		{
			let mut frame = frame.write();
			frame.fixed = match frame.fixed {
				Fixed(1) => Free,
				Fixed(n) => Fixed(n-1),
				Free => fail!("Unfixing unfixed page"),
			};
		}

		if !is_dirty {
			return;
		}
		let frame = frame.read();
		if frame.fixed == Free {
			println!("writing back {}", frame.page_id);
			self.write_page(frame.page_id, frame.get_data());
		}
	}

	fn write_page(&mut self, page_id: u64, data: &[u8]) {
		let file_path = self.path.join(page_id.to_str());
		let mut handle = match File::open_mode(&file_path, Open, Write) {
			Ok(handle) => handle,
			Err(e) => fail!("Opening file for writing failed: {}", e),
		};

		match handle.write(data) {
			Ok(_) => (),
			Err(e) => fail!("Writing to file failed: {}", e),
		};
	}
}

/*
 * A buffer frame, the unit that gets the data
 */
impl BufferFrame {
	/*
	 * get_mut_data returns a borrowed reference to a mutable slice to the vector
	 * contents.
	 */
	pub fn get_mut_data<'a>(&'a mut self) -> &'a mut [u8] {
		self.data.as_mut_slice()
	}
	pub fn get_data<'a>(&'a self) -> &'a [u8] {
		self.data.as_slice()
	}
}

fn sample<'a, T, I:Iterator<T>>(from: &'a mut I) -> Option<T> {
	let from: ~[T] = from.collect();
	let l = from.len();
	if l == 0 {
		return None;
	}
	let index = randrange(l);
	Some(from[index])
}

fn randrange<X: SampleRange + Ord + Zero>(high: X) -> X {
	let between: Range<X> = Range::new(Zero::zero(), high);
	let mut rng = rand::task_rng();
	between.ind_sample(&mut rng)
}

#[test]
fn test_create() {
	let dir = match TempDir::new("buffermanager") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};

	let mut bm = BufferManager::new(16, dir.path().clone());
	let pageref = match bm.fix_page(42) {
		Some(p) => p,
		None => fail!("Getting page failed"),
	};
	{
		let mut page = pageref.write();
		let data = page.get_mut_data();
		data[0] = 42;
		//println!("data: {}", Vec::from_slice(data));
	}
	bm.unfix_page(pageref, true);
	fail!("always");
}

#[test]
fn test_threads() {
	use rand::random;
	use std::task::spawn;

	let pages_in_ram = 1;
	let pages_on_disk: u64 = 20;
	let thread_count = 3;
	let dir = match TempDir::new("buffermanager") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};
	let p = dir.path();
	let p = Path::new(".");

	let mut buffermanager = BufferManager::new(pages_in_ram, p.clone());

	for i in range(0, pages_on_disk) {
		let bf = match buffermanager.fix_page(i) {
			Some(frame) => frame,
			None => fail!("Couldn't fix page {}", i),
		};
		{
			let mut lock = bf.write();
			lock.get_mut_data()[0] = 0;
		}
		buffermanager.unfix_page(bf, true);
	}
	let bm = Arc::new(RWLock::new(buffermanager));

	// start scan thread
	let (tx, rx) = channel();
	let bm_scan = bm.clone();
	let mut scan = Future::spawn(proc() {
		let mut counters = Vec::from_elem(pages_on_disk as uint, 0_u8);
		loop {
			println!("Running scan thread...");
			match rx.try_recv() {
				Empty => {
					let page_number = randrange(pages_on_disk);
					let mut bm = bm_scan.write();
					let bf = match bm.fix_page(page_number) {
						Some(frame) => frame,
						None => fail!("Couldn't scan/fix page"),
					};
					let current_val = {
						let lock = bf.read();
						lock.get_data()[0]
					};
					bm.unfix_page(bf, false);
					bm.downgrade();
					// check if the value is going up
					assert!(&current_val >= counters.get(page_number as uint));
					// set to the new value
					let v = counters.get_mut(page_number as uint);
					*v = current_val;

				},
				/*
				 * terminate on both disconnect and when any kind of
				 * data arrives
				 */
				Disconnected => break,
				Data(_) => break,
			};
		}
	});

	// start read/write threads
	let mut rw_threads: Vec<Future<int>> = Vec::new();
	for _ in range(0, thread_count) {
		let bm = bm.clone();
		rw_threads.push(Future::spawn(proc() {
			let is_write = random::<bool>();
			println!("Creating new {} task",
				if is_write {"write"} else {"read"});
			let page_number = randrange(pages_on_disk);
			let mut bm = bm.write();
			if is_write {
				let bf = match bm.fix_page(page_number) {
					Some(frame) => frame,
					None => fail!("Couldn't fix page"),
				};
				{
					println!("Wrote to page");
					let mut lock = bf.write();
					let data = lock.get_mut_data();
					data[0] = data[0] + 1;
					println!("data: {}", Vec::from_slice(data));
				}
				bm.unfix_page(bf, is_write);
			} else {
				let bf = match bm.fix_page(page_number) {
					Some(frame) => frame,
					None => fail!("Couldn't fix page"),
				};
				bm.unfix_page(bf, is_write);
			}
			// return whether we wrote (1) or read (0) as future
			if is_write {1} else {0}
		}));
	}

	// Rust does not have join, but we can wait on Futures which does the same
	let total_count = rw_threads.mut_iter().fold(0, |acc, val| acc + val.get());
	println!("total_count: {}", total_count);

	// terminate the scan thread and wait until it has completed
	tx.send("terminate");
	scan.get();

	// re-open the pages and check whether all numbers got saved
	let mut bm = BufferManager::new(pages_in_ram, p.clone());
	let mut total_count_on_disk = 0;
	for i in range(0, pages_on_disk) {
		let bf = match bm.fix_page(i) {
			Some(frame) => frame,
			None => fail!("Couldn't fix page")
		};
		let value = {
			let lock = bf.read();
			let data = lock.get_data();
			data[0]
		};
		bm.unfix_page(bf, false);
		// cast up from u8 to int
		total_count_on_disk += value as int;
	}
	println!("Total count on disk: {}", total_count_on_disk);
	assert_eq!(total_count, total_count_on_disk);

	fail!("always");
}
