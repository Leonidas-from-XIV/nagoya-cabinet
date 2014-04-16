struct BufferManager;

struct BufferFrame;

impl BufferManager {
	pub fn new(size: uint) -> BufferManager {
		BufferManager
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
