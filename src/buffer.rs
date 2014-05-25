use collections::HashMap;
use std::io::{Open, Read, Write, TempDir};
use std::num::Zero;
use std::comm::{Data, Empty, Disconnected};
use sync::{Arc, RWLock};
use sync::Future;
use rand::task_rng;
use rand::distributions::{IndependentSample, Range};
use rand::distributions::range::SampleRange;
use native::io::file;
use native::io::file::FileDesc;
use std::rt::rtio::RtioFileStream;

/* Linux seems to use 4K segments, that's a good bet */
pub static PAGE_SIZE: uint = 4 * 1024;
/*
 * how much of the page_id should be reserved for pages?
 * the rest goes for segments. The bigger this is, the larger the segment files are
 * and the more pages they contain. (2^PAGE_BITS)*PAGE_SIZE gives you segment size.
 *
 * Adjust as required.
 */
pub static PAGE_BITS: uint = 32;

pub type ConcurrentFrame = Arc<RWLock<BufferFrame>>;

pub struct BufferManager {
	size: uint,
	entries: HashMap<u64, BufferEntry>,
	path: Path,
}

struct BufferEntry {
	frame: ConcurrentFrame,
	written: Cleanliness,
}

#[deriving(Eq)]
enum Status {
	Free,
	Fixed(uint),
}

#[deriving(Eq)]
enum Cleanliness {
	Clean,
	Dirty,
}

pub struct BufferFrame {
	page_id: u64,
	data: Vec<u8>,
	fixed: Status,
}

/* Destructor trait implementation */
impl Drop for BufferManager {
	fn drop(&mut self) {
		for (page, entry) in self.entries.iter() {
			info!("Drop k == {}, dirty? {:?}", page, entry.written);
			match entry.written {
				Clean => (),
				Dirty => {
					let frame = entry.frame.read();
					self.write_page(frame.page_id, frame.get_data());
				},
			}
		}
	}
}

impl BufferManager {
	pub fn new(size: uint, path: Path) -> BufferManager {
		let h = HashMap::with_capacity(size);
		BufferManager {size: size, entries: h, path: path}
	}

	fn open_or_create(&self, page_id: u64) -> FileDesc {
		let (segment, _) = split_segment(page_id);
		let file_path = self.path.join(segment.to_str());
		match file::open(&file_path.to_c_str(), Open, Read) {
			Ok(f) => return f,
			Err(_) => self.create(page_id),
		};
		match file::open(&file_path.to_c_str(), Open, Read) {
			Ok(f) => f,
			Err(e) => fail!("Failed reopening written file: {}", e),
		}
	}

	fn create(&self, page_id: u64) {
		let (segment, offset) = split_segment(page_id);
		let path = self.path.join(segment.to_str());

		match file::open(&path.to_c_str(), Open, Write) {
			Ok(mut f) => {
				match f.pwrite([0_u8, ..PAGE_SIZE], offset * PAGE_SIZE as u64) {
					Ok(_) => (),
					//Err(e) => fail!("Failed pwriting to file {}: {}", &path.to_c_str().as_str(), e),
					// in this case, err means everything is fine, since there is a bug in Rust' libnative
					Err(_) => (),
				};
			},
			Err(e) => fail!("Failed opening file for write: {}", e),
		};
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
		let (_, offset) = split_segment(page_id);

		let mut file_handle = self.open_or_create(page_id);
		let mut buf = [0_u8, ..PAGE_SIZE];
		let content = match file_handle.pread(buf, offset * PAGE_SIZE as u64) {
			Ok(_) => {
				Vec::from_slice(buf)
				/*
				if n == PAGE_SIZE as int {
					Vec::from_slice(buf)
				} else {
					fail!("pread failed: wanted {}, got {}",
						PAGE_SIZE, n)
				}*/
			},
			Err(e) => fail!("Couldn't read from page: {}", e),
		};

		let frame = BufferFrame {data: content, page_id: page_id, fixed: Free};
		let entry = BufferEntry {frame: Arc::new(RWLock::new(frame)), written: Clean};
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
			Some(key) => {
				match self.entries.pop(&key) {
					None => false,
					Some(entry) => {
						info!("Evicting entry: {:?}", entry);
						if entry.written == Dirty {
							let frame = entry.frame.read();
							self.write_page(frame.page_id, frame.get_data());
						}
						true
					},
				}
			},
		}
	}
	
	pub fn fix_page(&mut self, page_id: u64) -> Option<ConcurrentFrame> {
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

	pub fn unfix_page(&mut self, frame: ConcurrentFrame, is_dirty: bool) {
		{
			let mut frame = frame.write();
			frame.fixed = match frame.fixed {
				Fixed(1) => Free,
				Fixed(n) => Fixed(n-1),
				Free => fail!("Unfixing unfixed page"),
			};
		}

		if is_dirty {
			let frame = frame.read();
			let entry = self.entries.get_mut(&frame.page_id);
			entry.written = Dirty;
		}
	}

	fn write_page(&self, page_id: u64, data: &[u8]) {
		let (segment, offset) = split_segment(page_id);
		let file_path = self.path.join(segment.to_str());
		let mut handle = match file::open(&file_path.to_c_str(), Open, Write) {
			Ok(handle) => handle,
			Err(e) => fail!("Opening file for writing failed: {}", e),
		};

		info!("Writing to segment {}, offset {}", segment, offset);
		match handle.pwrite(data, offset * PAGE_SIZE as u64) {
			Ok(_) => (),
			//Err(e) => fail!("Writing to file failed: {}", e),
			// again Rust 0.10 pwrite error
			Err(_) => (),
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
	let mut rng = task_rng();
	between.ind_sample(&mut rng)
}

fn split_segment(num: u64) -> (u64, u64) {
	let high = num >> PAGE_BITS;
	let low = num & ((1 << PAGE_BITS) - 1);
	(high, low)
}

pub fn join_segment(segment: u64, page: u64) -> u64 {
	// TODO do proper masking before
	let high = segment << PAGE_BITS;
	let low = page;
	high ^ low
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
	}
	bm.unfix_page(pageref, true);
}

#[test]
fn test_threads() {
	use rand::random;
	use std::task::spawn;
	use std::os;

	let pages_in_ram: uint = match os::getenv("PAGES_IN_RAM") {
		Some(v) => from_str(v).expect("PAGES_IN_RAM expects integer"),
		None => 1,
	};
	let pages_on_disk: u64 = match os::getenv("PAGES_ON_DISK") {
		Some(v) => from_str(v).expect("PAGES_ON_DISK expects integer"),
		None => 20,
	};
	let thread_count: uint = match os::getenv("THREADS") {
		Some(v) => from_str(v).expect("THREADS expects integer"),
		None => 3,
	};
	let dir = match TempDir::new("buffermanager") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};
	let p = dir.path();
	//let p = Path::new(".");

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
			info!("Running scan thread...");
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
			info!("Creating new {} task",
				if is_write {"write"} else {"read"});
			let page_number = randrange(pages_on_disk);
			let mut bm = bm.write();
			if is_write {
				let bf = match bm.fix_page(page_number) {
					Some(frame) => frame,
					None => fail!("Couldn't fix page"),
				};
				{
					let mut lock = bf.write();
					let data = lock.get_mut_data();
					data[0] = data[0] + 1;
					info!("Wrote to page {}", page_number);
					debug!("data: {}", Vec::from_slice(data));
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
	info!("Total count in RAM: {}", total_count);

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
	info!("Total count on disk: {}", total_count_on_disk);
	assert_eq!(total_count, total_count_on_disk);
}
