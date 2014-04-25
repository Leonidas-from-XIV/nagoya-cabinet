extern crate collections;
extern crate sync;
extern crate rand;
use collections::HashMap;
use std::io::{File, Open, Read, Write, TempDir};
use std::num::Zero;
use sync::{Arc, RWLock};
use sync::Future;
use rand::task_rng;
use rand::distributions::{IndependentSample, Range};
use rand::distributions::range::SampleRange;

struct BufferManager {
	size: uint,
	entries: HashMap<u64, BufferEntry>,
	directory: TempDir,
}

struct BufferEntry {
	frame: Arc<RWLock<BufferFrame>>,
}

struct BufferFrame {
	page_id: u64,
	data: Vec<u8>,
}

/* Destructor trait implementation */
impl Drop for BufferManager {
	fn drop(&mut self) {
		println!("Dropping");
		// TODO
	}
}

impl BufferManager {
	pub fn new(size: uint) -> BufferManager {
		let h = HashMap::with_capacity(size);
		let dir = match TempDir::new("buffermanager") {
			Some(temp_dir) => temp_dir,
			None => fail!("creation of temporary directory"),
		};
		BufferManager {size: size, entries: h, directory: dir}
	}

	fn open_or_create(&self, page_id: u64) -> File {
		let page_path = self.directory.path();
		let file_path = page_path.join(page_id.to_str());
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

	fn load_page(&mut self, page_id: u64) {
		if self.entries.len() == self.size {
			self.evict_page();
		}

		let mut file_handle = self.open_or_create(page_id);
		//let content = match file_handle.read_exact(4*1024) {
		let content = match file_handle.read_exact(10) {
			Ok(c) => Vec::from_slice(c),
			Err(e) => fail!("Couldn't read from page: {}", e),
		};

		let frame = BufferFrame {data: content, page_id: page_id};
		let entry = BufferEntry {frame: Arc::new(RWLock::new(frame))};
		self.entries.insert(page_id, entry);
	}

	fn evict_page(&mut self) {
		let random_key = {
			let mut iter = self.entries.keys();
			sample(&mut iter).map(|v| v.clone())
		};

		match random_key {
			None => false,
			Some(key) => self.entries.remove(&key),
		};
	}
	
	pub fn fix_page(&mut self, page_id: u64) -> Option<Arc<RWLock<BufferFrame>>> {
		if !self.entries.contains_key(&page_id) {
			self.load_page(page_id);
		}
		let entry = self.entries.get(&page_id);
		// Arcs can be cloned and they will all point to the same RWLock
		Some(entry.frame.clone())
	}

	pub fn unfix_page(&mut self, frame: Arc<RWLock<BufferFrame>>, is_dirty: bool) {
		if !is_dirty {
			return;
		}
		let frame = frame.read();
		println!("writing back {}", frame.page_id);
		self.write_page(frame.page_id, frame.get_data());
	}

	fn write_page(&mut self, page_id: u64, data: &[u8]) {
		let page_path = self.directory.path();
		let file_path = page_path.join(page_id.to_str());
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
	let mut bm = BufferManager::new(16);
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
	let mut buffermanager = BufferManager::new(pages_in_ram);

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
					None => fail!("Cound't fix page"),
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
					None => fail!("Cound't fix page"),
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

	// TODO: add scan thread

	//let mut bm = BufferManager::new(pages_in_ram);
	let mut bm = bm.write();
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
		total_count_on_disk += value;
	}
	println!("Total count on disk: {}", total_count_on_disk);

	fail!("always");
}
