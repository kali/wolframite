extern crate wiki;
extern crate glob;
extern crate bzip2;
extern crate snappy_framed;

use wiki::WikiError;
use wiki::helpers::*;
use wiki::cap;

use std::io;
use std::io::prelude::*;
use std::fs;

use std::path;

use bzip2::reader::BzDecompressor;
use snappy_framed::write::SnappyFramedEncoder;

fn main() {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];
    capitanize(lang, date).unwrap();
}

pub fn capitanize(lang:&str, date:&str) -> Result<(), WikiError> {
    let source_root = data_dir_for("download", lang, date);
    let target_root = data_dir_for("cap", lang, date);
    try!(fs::create_dir_all(target_root.clone()));
    let glob = source_root.clone() + "/*.bz2";
    for entry in try!(::glob::glob(&glob)) {
        let entry = try!(entry);
        let mut target = path::PathBuf::from(
            target_root.clone()+&entry.to_str().unwrap()[source_root.len() ..]
        );
        target.set_extension(".cap.snap");
        try!(capitanize_file(&*entry, &path::Path::new(&*target)));
    };
    Ok( () )
}

pub fn capitanize_file(src:&path::Path, dst:&path::Path) -> Result<(), WikiError> {
    let mut input = BzDecompressor::new(try!(fs::File::open(src)));
    //let output = SnappyFramedEncoder::new(fs::File::create(dst));
    let mut output = try!(fs::File::create(dst));
    println!("{:?}", src);
    try!(cap::capitanize(input, output));
    Ok( () )
}
