extern crate collections;
use std::io::{File, Open, Write};
use std::u64;
use collections::priority_queue::PriorityQueue;

fn externalSort(mut fdInput: File, size: u64, mut fdOutput: File, memSize: u64) {
	let items_per_run = (size / memSize) as uint;
	let mut run: Vec<u64> = Vec::with_capacity(items_per_run);
	for _ in range(0, items_per_run) {
		run.push(0_u64);
	}
	for element in run.mut_iter() {
		let number = match fdInput.read_le_u64() {
			Ok(num) => num,
			Err(e) => fail!("failed to read u64 from file: {}", e),
		};
		println!("read {} byte", number);
		*element = number;
	}
	run.sort();

	let data = ~[1,2,3];
	let pq = PriorityQueue::from_vec(data);
}

fn main() {
	let fin = match File::open(&Path::new ("input")) {
		Ok(f) => f,
		Err(e) => fail!("input file error: {}", e),
	};
	let fout = match File::open_mode(&Path::new("output"), Open, Write) {
		Ok(f) => f,
		Err(e) => fail!("output file error: {}", e),
	};
	externalSort(fin, 16, fout, 8);
	println!("Ohai");
}
