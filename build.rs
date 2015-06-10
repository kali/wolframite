extern crate capnpc;

use std::path::Path;

fn main() {
    ::capnpc::compile(Path::new("src/schema"), &[Path::new("src/schema/wiki.capnp")]).unwrap();
}
