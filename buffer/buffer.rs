extern crate collections;
extern crate sync;
use collections::HashMap;
use std::io::{File, Open, Read, Write, TempDir};
use sync::{Arc, RWLock};
#[cfg(test)]
use std::task::spawn;

struct BufferManager {
	size: uint,
	entries: HashMap<u64, BufferEntry>,
	directory: TempDir,
}

struct BufferEntry {
	frame: Arc<RWLock<BufferFrame>>,
}

struct BufferFrame {
	pageId: u64,
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

	fn openOrCreate(&self, pageId: u64) -> File {
		let page_path = self.directory.path();
		let file_path = page_path.join(pageId.to_str());
		match File::open_mode(&file_path, Open, Read) {
			Ok(f) => f,
			Err(e) => {
				match File::open_mode(&file_path, Open, Write) {
					Ok(mut f) => {
						f.write([0_u8, ..4 * 1024]);
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

	fn loadPage(&mut self, pageId: u64) {
		if self.entries.len() == self.size {
			self.evictPage();
		}

		let mut file_handle = self.openOrCreate(pageId);
		let content = match file_handle.read_exact(4*1024) {
			Ok(c) => Vec::from_slice(c),
			Err(e) => fail!("Couldn't read from page: {}", e),
		};

		let frame = BufferFrame {data: content, pageId: pageId};
		let entry = BufferEntry {frame: Arc::new(RWLock::new(frame))};
		self.entries.insert(pageId, entry);
	}

	fn evictPage(&mut self) {
		// TODO delete a random page
		// evicting is hard, let's go shopping
	}
	
	pub fn fixPage(&mut self, pageId: u64, exclusive: bool) -> Option<Arc<RWLock<BufferFrame>>> {
		// TODO
		if !self.entries.contains_key(&pageId) {
			self.loadPage(pageId);
		}
		let entry = self.entries.get(&pageId);
		// Arcs can be cloned and they will all point to the same RWLock
		Some(entry.frame.clone())
	}

	pub fn unfixPage(&mut self, frame: Arc<RWLock<BufferFrame>>, isDirty: bool) {
		if !isDirty {
			return;
		}
		let frame = frame.read();
		self.writePage(frame.pageId, frame.get_data());
	}

	fn writePage(&mut self, pageId: u64, data: &[u8]) {
		// TODO: write page back to disk
		println!("Writing {}", data);
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

#[test]
fn test_create() {
	let mut bm = BufferManager::new(16);
	let pageref = match bm.fixPage(42, false) {
		Some(p) => p,
		None => fail!("Getting page failed"),
	};
	{
		let mut page = pageref.write();
		let mut data = page.get_mut_data();
		data[0] = 42;
		println!("data: {}", Vec::from_slice(data));
	}
	bm.unfixPage(pageref, true);
	fail!("always");
}

#[test]
fn test_threads() {
	let pages_in_ram = 20;
	let pages_on_disk = 20;
	let thread_count = 10;
	let mut bm = BufferManager::new(pages_in_ram);

	for _ in range(0, thread_count) {
		spawn(proc() {
			println!("I'm a new task");
		})
	}
}
