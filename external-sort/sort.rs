extern crate collections;
use std::io::{File, Open, Write, TempDir};
use collections::priority_queue::PriorityQueue;

/*
 * an entry is the number that we read and the file it came from, so after we took
 * out one Entry from the priority queue, we need to know where to take the next number
 * from.
 */
struct Entry {
	value: u64,
	source: File,
}

/* The priority queue depends on the ordering relation and always returns the biggest */
impl Ord for Entry {
	// lesser than implementation
	fn lt(&self, other: &Entry) -> bool {
		// we invert: for us the smallest will have the highest priority
		!(self.value < other.value)
	}
}

/* ordering needs equality for some reason, so there */
impl Eq for Entry {
	fn eq(&self, other: &Entry) -> bool {
		self.value == other.value
	}
}

/* A struct to hold our queue */
struct PriorityFile {
	queue: PriorityQueue<Entry>,
}

/*
 * 'Class' that takes a list of files and does a k-way merge of the smallest values
 * from the files
 */
impl PriorityFile {
	pub fn new(files: Vec<File>) -> PriorityFile {
		let mut queue: PriorityQueue<Entry> = PriorityQueue::new();
		// now that we got the queue, populate it with initial values

		// move_iter is required to get the values themselves, not borrowed pointers to them
		for mut f in files.move_iter() {
			// read from file, append to queue
			let number = match f.read_le_u64() {
				Ok(num) => num,
				Err(e) => fail!("failed to read u64 from file: {}", e),
			};
			queue.push(Entry {value: number, source: f});
		};
		PriorityFile {queue: queue}
	}

	/* Returns the next smallest number from the list of files or None if exhausted */
	pub fn next(&mut self) -> Option<u64> {
		match self.queue.maybe_pop() {
			Some(mut res) => {
				let v = res.value;
				match res.source.read_le_u64() {
					Ok(num) => self.queue.push(Entry {value: num, source: res.source}),
					Err(_) => (),
				};
				Some(v)
			},
			None => None,
		}
	}
}

fn externalSort(mut fdInput: File, size: u64, mut fdOutput: File, memSize: u64) {
	let runs = size / memSize;
	let items_per_run = (memSize / 8) as uint;

	println!("There will be {} runs with {} elements each.", runs, items_per_run);

	/* initialize a Vector. Rust Vectors grow but this one will stay fixed */
	let mut run: Vec<u64> = Vec::with_capacity(items_per_run);
	// preallocate it with zeroes
	for _ in range(0, items_per_run) {
		run.push(0_u64);
	}

	// let's create a temporary directory to store the sorted chunks
	// neat: TempDir deletes the directory and its contents when it goes out of
	// scope
	let overflow_dir = match TempDir::new("externalsort") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};
	let overflow_path = overflow_dir.path();
	println!("temp dir path {}", overflow_path.display());

	// start the sorting runs
	for n in range(0, runs) {
		// iterate over our 'run' buffer and read from the input file
		// I suppose the values are little endian, which considering we are most
		// likely on x86, is a reasonable assumption
		for element in run.mut_iter() {
			let number = match fdInput.read_le_u64() {
				Ok(num) => num,
				Err(e) => fail!("failed to read u64 from file: {}", e),
			};
			*element = number;
		}
		// O(n log n) sort from the stdlib, hopefully more or less equivalent to
		// C++ std::sort
		run.sort();

		// now write out the results into numbered files
		let file_path = overflow_path.join(n.to_str());
		let mut file_file = match File::open_mode(&file_path, Open, Write) {
			Ok(f) => f,
			Err(e) => fail!("overflow file failed opening for write: {}", e),
		};

		for &element in run.iter() {
			match file_file.write_le_u64(element) {
				Ok (_) => (),
				Err(e) => fail!("writing overflow failed: {}", e),
			};
		};
	};

	/* sorting done, now merging */

	// collect all file handles. Hopefully less than 4096
	let mut files = Vec::with_capacity(runs as uint);
	for n in range(0, runs) {
		let file_path = overflow_path.join(n.to_str());
		let fd = match File::open(&file_path) {
			Ok(f) => f,
			Err(e) => fail!("overflow file opening error {}", e),
		};
		files.push(fd);
	}

	// we got a nice class here that spits out sorted numbers until exhausted
	let mut pf = PriorityFile::new(files);
	loop {
		match pf.next() {
			// if we got a number, write it to the output
			Some (number) => match fdOutput.write_le_u64(number) {
				Ok(_) => (),
				Err(e) => fail!("writing output file failed: {}", e),
			},
			None => break,
		};
	};
}

fn main() {
	let input_file_path = &Path::new("input");
	let size = match input_file_path.stat() {
		Ok(stat) => stat.size,
		Err(e) => fail!("Couldn't read {}", e),
	};
	println!("Input file size {}", size);
	let fin = match File::open(input_file_path) {
		Ok(f) => f,
		Err(e) => fail!("input file error: {}", e),
	};
	let fout = match File::open_mode(&Path::new("output"), Open, Write) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};
	externalSort(fin, size, fout, 800);
}
