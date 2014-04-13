extern crate collections;
use std::io::{File, Open, Write, TempDir};
use collections::priority_queue::PriorityQueue;

struct PriorityFile {
	files: Vec<File>,
	queue: ~PriorityQueue<u64>,
}

impl PriorityFile {
	pub fn new(mut files: Vec<File>) -> PriorityFile {
		let mut queue: PriorityQueue<u64> = PriorityQueue::new();
		for f in files.mut_iter() {
			let number = match f.read_le_u64() {
				Ok(num) => num,
				Err(e) => fail!("failed to read u64 from file: {}", e),
			};
			queue.push(number);
		}
		PriorityFile {files: files, queue: ~queue}
	}

	pub fn next(&mut self) -> Option<u64> {
		let res = self.queue.maybe_pop();
		res
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

	let overflow_path = match TempDir::new("externalsort") {
		Some(path) => path,
		None => fail!("creation of temporary directory"),
	};

	println!("tdpath {}", overflow_path.path().display());

	for n in range(0, runs) {
		for element in run.mut_iter() {
			let number = match fdInput.read_le_u64() {
				Ok(num) => num,
				Err(e) => fail!("failed to read u64 from file: {}", e),
			};
			//println!("read {} byte", number);
			*element = number;
		}
		run.sort();

		let file_path = overflow_path.path().join(n.to_str());
		println!("path: {}", file_path.display());
		let mut file_file = match File::open_mode(&file_path, Open, Write) {
			Ok(f) => f,
			Err(e) => fail!("output file error: {}", e),
		};

		for &element in run.iter() {
			match file_file.write_le_u64(element) {
				Ok (_) => (),
				Err(e) => fail!("writing overflow failed: {}", e),
			};
		};
	};

	let mut files = Vec::with_capacity(runs as uint);
	for n in range(0, runs) {
		let file_path = overflow_path.path().join(n.to_str());
		let fd = match File::open(&file_path) {
			Ok(f) => f,
			Err(e) => fail!("overflow file open error {}", e),
		};
		files.push(fd);
	}

	let mut pf = PriorityFile::new(files);
	loop {
		match pf.next() {
			Some (number) => match fdOutput.write_le_u64(number) {
				Ok(_) => (),
				Err(e) => fail!("Didn't write to file {}", e),
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
	externalSort(fin, 8000, fout, 800);
}
