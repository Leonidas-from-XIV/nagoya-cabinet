nagoya-cabinet
==============

A DBMS, sortof. Implemented in Rust, 0.10-ish.

How to build
------------

Until I figure out what the best way to build the code is, calling the compiler
`rustc` should be easiest.

Either grab a Rust 0.10 package from your package manager (Arch Linux ships it
in [community]), other distros might offer PPAs or COPRs or other third-party
repositories. Alternatively, grab the source (takes long to compile) or a
precompiled package from [rust-lang.org](http://www.rust-lang.org/) and install
it.

I'll be trying to be compatible with a released version of Rust and not a
nightly for convenience. Stuff is changing fast.

Build sorting
-------------

```sh
cd external-sort
# now there's two alternatives: either build the binary or the unit test
# start with the binary
rustc sort.rs
# run sort
./sort input output blockSizeInMb
# need debug/info output?
RUST_LOG=info ./sort …
RUST_LOG=debug ./sort …
# here's how to build the unit test
rustc --test sort.rs
# run unit test
./sort
```
