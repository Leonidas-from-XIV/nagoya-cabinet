#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
use std::io::{File, Open, Write, TempDir};
use std::os::args;
use std::cast;
use std::raw::Slice;
use collections::priority_queue::PriorityQueue;
#[cfg(not(test))]
use std::from_str::from_str;
#[cfg(test)]
use std::io::{ReadWrite, SeekSet, EndOfFile};

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

fn read_u64(from: &mut File, n: uint) -> Vec<u64> {
	// rust post 0.10 will directly return a Vec<u8> here
	let buf = match from.read_exact(n*8) {
		Ok(b) => b,
		Err(e) => fail!("reading failed {}", e),
	};
	//let u = buf.as_ptr();
	let u = buf.as_slice();

	let mut r: Slice<u64> = unsafe { cast::transmute(u) };
	assert_eq!(r.len % 8, 0);
	r.len /= 8;
	Vec::from_slice(unsafe { cast::transmute(r) })
}

fn write_u64(to: &mut File, items: &Vec<u64>) {
	let s = items.as_slice();
	let mut r: Slice<u8> = unsafe { cast::transmute(s) };
	r.len *= 8;
	let buf: &[u8] = unsafe { cast::transmute(r) };

	match to.write(buf) {
		Ok(_) => (),
		Err(e) => fail!("writing failed: {}", e),
	};
}

fn externalSort(mut fdInput: File, size: u64, mut fdOutput: File, memSize: u64) {
	let mut runs = size / memSize;
	let items_per_run = (memSize / 8) as uint;
	let over = ((size / 8) - (items_per_run as u64 * runs)) as uint;

	info!("There will be {} runs with {} elements each.", runs, items_per_run);
	if over > 0 {
		info!("And one additional run with {} items.", over);
	}

	// let's create a temporary directory to store the sorted chunks
	// neat: TempDir deletes the directory and its contents when it goes out of
	// scope
	let overflow_dir = match TempDir::new("externalsort") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};
	let overflow_path = overflow_dir.path();
	info!("temp dir:\ncd {}", overflow_path.display());

	// start the sorting runs
	for n in range(0, runs) {
		// iterate over our 'run' buffer and read from the input file
		// I suppose the values are little endian, which considering we are most
		// likely on x86, is a reasonable assumption
		info!("Run {}, starting read", n);
		let mut run = read_u64(&mut fdInput, items_per_run);
		info!("Run {}, read finished", n);
		// O(n log n) sort from the stdlib, hopefully more or less equivalent to
		// C++ std::sort
		info!("Run {}, starting sort", n);
		run.sort();
		info!("Run {}, sort finished", n);

		// now write out the results into numbered files
		let file_path = overflow_path.join(n.to_str());
		let mut file_file = match File::open_mode(&file_path, Open, Write) {
			Ok(f) => f,
			Err(e) => fail!("overflow file failed opening for write: {}", e),
		};

		info!("Run {}, starting write", n);
		write_u64(&mut file_file, &run);
		info!("Run {}, write finished", n);
	};

	/* additional run to catch remaining objects */
	if over > 0 {
		info!("overrun starting read");
		let mut run = read_u64(&mut fdInput, over);
		info!("overrun read finished");
		info!("overrun starting sort");
		run.sort();
		info!("overrun sort finished");

		let file_path = overflow_path.join(runs.to_str());
		let mut file_file = match File::open_mode(&file_path, Open, Write) {
			Ok(f) => f,
			Err(e) => fail!("overflow file failed opening for write: {}", e),
		};
		info!("overrun starting write");
		write_u64(&mut file_file, &run);
		info!("overrun finished write");

		runs += 1;
	};

	/* sorting done, now merging */
	info!("Pre-sorting done, now k-way merge")

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
	// put the sorted numbers in a cache first, so they can all be written in bulk
	let mut write_cache = Vec::with_capacity(items_per_run);
	loop {
		match pf.next() {
			Some (number) => {
				// a new number was found, add it to the cache
				// and if the cache is full, write that to disk
				write_cache.push(number);
				debug!("Gathered element {}", number);
				if write_cache.len() >= items_per_run {
					info!("Writing out {} elements from cache", write_cache.len());
					write_u64(&mut fdOutput, &write_cache);
					info!("Write done");
					write_cache.clear();
				};
			},
			None => {
				info!("Ending, writing out remaining {} elements from cache", write_cache.len());
				// we finished, write remaining cache to disk
				write_u64(&mut fdOutput, &write_cache);
				info!("Write done");
				break;
			},
		};
	};
}

#[cfg(not(test))]
fn main() {
	let argv = args();
	if argv.len() < 4 {
		fail!("Argument error: <inputFile> <outputFile> <memoryBufferInMB>");
	}
	let input_file_path = &Path::new(argv[1].as_slice());
	let size = match input_file_path.stat() {
		Ok(stat) => stat.size,
		Err(e) => fail!("Couldn't read {}", e),
	};
	let fin = match File::open(input_file_path) {
		Ok(f) => f,
		Err(e) => fail!("input file error: {}", e),
	};
	let fout = match File::open_mode(&Path::new(argv[2].as_slice()), Open, Write) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};
	let buffer_size:u64 = match from_str(args()[3].as_slice()) {
		Some(num) => num,
		None => fail!("Not numeric input"),
	};
	info!("Input file size {}", size);

	externalSort(fin, size, fout, buffer_size * 1024 * 1024);
}

#[test]
fn generate_5gb_and_sort() {
	let number_gigabytes = 5;

	let mut random = match File::open(&Path::new("/dev/urandom")) {
		Ok(f) => f,
		Err(e) => fail!("Error opening urandom: {}", e),
	};

	let test_dir = match TempDir::new("externalsort-test") {
		Some(temp_dir) => temp_dir,
		None => fail!("creation of temporary directory"),
	};
	let test_path = test_dir.path();

	let mut unordered_nums = match File::open_mode(&test_path.join("input"), Open, ReadWrite) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};

	let mut buffer = [0, .. 1024*1024];
	let mut left = number_gigabytes * 1024;
	while left > 0 {
		match random.read(buffer) {
			Ok(_) => match unordered_nums.write(buffer) {
				Ok(_) => (),
				Err(e) => fail!("failed writing random numbers {}", e),
			},
			Err(e) => fail!("failed reading random numbers {}", e),
		};
		left -= 1;
	}

	match unordered_nums.seek(0, SeekSet) {
		Ok(_) => (),
		Err(e) => fail!("Failed to seek: {}", e),
	};

	let ordered_nums = match File::open_mode(&test_path.join("output"), Open, Write) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};

	externalSort(unordered_nums, 1024 * 1024 * 1024 * number_gigabytes, ordered_nums, 1024 * 1024 * 100);
	let mut ordered_nums = match File::open(&test_path.join("output")) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};

	let mut last = 0_u64;
	loop {
		let number = match ordered_nums.read_le_u64() {
			Ok(num) => num,
			Err(e) => {
				match e.kind {
					EndOfFile => break,
					_ => fail!("Error reading from file: {}", e),
				}
			}
		};
		assert!(number >= last);
		last = number;
	}
}
