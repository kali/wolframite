extern crate capnpc;

use std::path::Path;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/wiki.capnp")
        .run().expect("schema compiler command");
}
