extern crate wiki;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use wiki::cap;
use wiki::WikiError;
use wiki::helpers::*;
use std::fs;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];
    let cap_root = data_dir_for("cap", lang, date);
    let glob = cap_root.clone() + "/*cap.snappy";
    for entry in try!(::glob::glob(&glob)) {
        let input:fs::File = try!(fs::File::open(try!(entry)));
        try!(cap::read_pages(SnappyFramedDecoder::new(input, Ignore)));
    }
    Ok( () )
}
