#![feature(phase)]
#[phase(syntax, link)] extern crate log;
extern crate collections;
extern crate sync;
extern crate rand;
extern crate serialize;

mod buffer;
mod schema;
mod btree;
mod operators;

#[cfg(not(test))]
fn main() {
	println!("Please recompile with the --test option");
}
