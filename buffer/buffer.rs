extern crate collections;
use collections::HashMap;

enum State {
	Clean,
	Dirty
}

struct BufferManager<'a> {
	size: uint,
	frames: HashMap<u64, BufferEntry<'a>>,
}

struct BufferEntry<'a> {
	state: State,
	frame: BufferFrame<'a>,
}

struct BufferFrame<'a> {
	data: &'a mut [u8],
}

impl<'a> BufferManager<'a> {
	pub fn new(size: uint) -> BufferManager {
		let h = HashMap::with_capacity(size);
		BufferManager {size: size, frames: h}
	}
	
	pub fn fixPage(pageId: u64, exclusive: bool) -> Option<BufferFrame> {
		None
	}

	pub fn unfixPage(frame: BufferFrame, isDirty: bool) {
	}
}

impl<'a> BufferFrame<'a> {
	pub fn getData() -> ~[u8] {
		~[0_u8, ..1024]
	}
}

fn main() {
	println!("Rewrite me into a test");
}
