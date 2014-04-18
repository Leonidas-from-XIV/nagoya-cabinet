extern crate collections;
use collections::HashMap;
use std::io::{File, Open, Read, Write, TempDir};

enum State {
	Available,
	Exclusive,
}

struct BufferManager {
	size: uint,
	entries: HashMap<u64, BufferEntry>,
	directory: TempDir,
}

struct BufferEntry {
	state: State,
	frame: BufferFrame,
}

struct BufferFrame {
	data: Vec<u8>,
}

impl BufferManager {
	pub fn new(size: uint) -> BufferManager {
		let h = HashMap::with_capacity(size);
		let dir = match TempDir::new("externalsort") {
			Some(temp_dir) => temp_dir,
			None => fail!("creation of temporary directory"),
		};
		BufferManager {size: size, entries: h, directory: dir}
	}

	fn openOrCreate(&self, pageId: u64) -> File {
		let page_path = self.directory.path();
		let file_path = page_path.join(pageId.to_str());
		let mut file_handle = match File::open_mode(&file_path, Open, Read) {
			Ok(f) => f,
			Err(e) => {
				match File::open_mode(&file_path, Open, Write) {
					Ok(mut f) => {
						f.write([0_u8, ..4 * 1024]);
						match File::open_mode(&file_path, Open, Read) {
							Ok(f) => f,
							Err(e) => fail!("failed reading file: {}, e"),
						}
					},
					Err(e) => fail!("Writing file failed: {}", e),
				}
			},
		};
		file_handle
	}

	fn loadPage(&mut self, pageId: u64) {
		if self.entries.len() == self.size {
			self.evictPage();
		}

		// TODO: open file, load page
		let mut file_handle = self.openOrCreate(pageId);
		let buffer = [0_u8, ..4*1024];

		let content = match file_handle.read_exact(4*1024) {
			Ok(c) => Vec::from_slice(c),
			Err(e) => fail!("Couldn't read from page: {}", e),
		};

		let frame = BufferFrame {data: content};
		let entry = BufferEntry {state: Available, frame: frame};
		self.entries.insert(pageId, entry);
	}

	fn evictPage(&mut self) {
		// TODO delete a random page
	}
	
	pub fn fixPage(pageId: u64, exclusive: bool) -> Option<BufferFrame> {
		// TODO
		None
	}

	pub fn unfixPage(frame: BufferFrame, isDirty: bool) {
		// TODO
	}
}

impl BufferFrame {
	pub fn getData<'a>(&'a mut self) -> &'a mut [u8] {
		self.data.as_mut_slice()
	}
}

fn main() {
	println!("Rewrite me into a test");
}
