extern crate collections;
use collections::HashMap;

struct BufferManager {
	size: uint,
	frames: HashMap<u64, BufferFrame>,
}

struct BufferFrame;

impl BufferManager {
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

impl BufferFrame {
	pub fn getData() -> ~[u8] {
		~[0_u8, ..1024]
	}
}

fn main() {
	println!("Rewrite me into a test");
}
