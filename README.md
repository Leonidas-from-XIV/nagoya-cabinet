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
./sort inputFile outputFile memoryBufferInMB
# need debug/info output? debug prints *every single number*, takes forever
RUST_LOG=info ./sort …
RUST_LOG=debug ./sort …
# here's how to build the unit test
rustc --test sort.rs
# run unit test
./sort
# this test generates random data, sorts in 100MB chunks, merges the chunks
# and verifies that the result is ordered.
```

Build buffer
------------

The buffer implementation can be tested with the built-in testsuite.
Unfortunately, it doesn't take command line arguments, but you can specify
environment variables to specify some parameters if you like. Or don't, some
defaults are provided.

```sh
cd buffer
rustc --test buffer.rs
env THREADS=a PAGES_ON_DISK=b PAGES_IN_RAM=c ./buffer
```
