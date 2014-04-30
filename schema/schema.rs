struct Record {
	// TODO: do we even need this?
	len: uint,
	data: Vec<u8>,
}

impl Record {
	/* move semantics, no copy */
	pub fn new(len: uint, data: Vec<u8>) -> Record {
		Record {len: len, data: data}
	}
	
	pub fn get_data<'a>(&'a self) -> &'a [u8] {
		self.data.as_slice()
	}
}

struct SPSegment;

/* our newtype struct: create a TID type as an alias to u64 */
struct TID(u64);

impl SPSegment {
	pub fn insert(r: Record) -> TID {
		TID(0_u64)
	}

	pub fn remove(tid: TID) -> bool {
		false
	}

	pub fn lookup(tid: TID) -> Record {
		// TODO
		Record {len: 1, data: vec!(1)}
	}

	pub fn update(tid: TID, r: Record) -> bool {
		false
	}
}


#[test]
fn main() {
}
