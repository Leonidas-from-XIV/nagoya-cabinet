#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;

mod buffer;
mod schema;
mod btree;

#[test]
fn main() {
	println!("Please recompile with the --test option");
	assert!(false);
}
