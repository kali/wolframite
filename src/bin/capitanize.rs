extern crate wiki;
extern crate glob;
extern crate bzip2;
extern crate snappy_framed;
extern crate simple_parallel;
extern crate num_cpus;

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
    let mut pool = simple_parallel::Pool::new(2*num_cpus::get());
    let jobs:Result<Vec<(path::PathBuf,path::PathBuf)>,WikiError> =
        try!(::glob::glob(&glob)).map( |entry| {
            let entry:String = try!(entry).to_str().unwrap().to_string();
            let mut target =
                target_root.clone()
                + &entry[source_root.len() .. entry.len()-7]
                + "cap.snappy";
            Ok((path::PathBuf::from(&*entry), path::PathBuf::from(&target)))
    }).collect();
    let task = |job:(path::PathBuf,path::PathBuf)| { capitanize_file(&*job.0, &*job.1) };
    let result:Result<Vec<()>,WikiError> = unsafe { pool.map(try!(jobs), &task).collect() };
    try!(result);
    Ok( () )
}

pub fn capitanize_file(src:&path::Path, dst:&path::Path) -> Result<(), WikiError> {
    let mut input = BzDecompressor::new(try!(fs::File::open(src)));
    let output = try!(SnappyFramedEncoder::new(try!(fs::File::create(dst))));
    try!(cap::capitanize(input, output));
    Ok( () )
}
