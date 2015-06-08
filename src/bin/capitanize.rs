#![feature(path_ext)]

extern crate wiki;
extern crate glob;
extern crate bzip2;
extern crate snappy_framed;
extern crate simple_parallel;
extern crate num_cpus;

use wiki::WikiError;
use wiki::helpers::*;
use wiki::cap;

use std::fs;
use std::fs::PathExt;

use std::path;

use bzip2::reader::BzDecompressor;

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
    let mut pool = simple_parallel::Pool::new(1+num_cpus::get());
    let jobs:Result<Vec<(path::PathBuf,path::PathBuf)>,WikiError> =
        try!(::glob::glob(&glob)).map( |entry| {
            let entry:String = try!(entry).to_str().unwrap().to_string();
            let target =
                target_root.clone()
                + &entry[source_root.len() .. entry.find(".").unwrap()];
            Ok((path::PathBuf::from(&*entry), path::PathBuf::from(&target)))
    }).collect();
    let mut jobs = try!(jobs);
    jobs.sort_by( |a,b| b.0.metadata().unwrap().len().cmp(&a.0.metadata().unwrap().len()));
    let task = |job:(path::PathBuf,path::PathBuf)| { capitanize_file(&*job.0, &*job.1) };
    let result:Result<Vec<()>,WikiError> = unsafe { pool.map(jobs, &task).collect() };
    try!(result);
    Ok( () )
}

pub fn capitanize_file(src:&path::Path, dst:&path::Path) -> Result<(), WikiError> {
    let input = BzDecompressor::new(try!(fs::File::open(src)));
//    let output = try!(SnappyFramedEncoder::new(try!(fs::File::create(dst))));
    try!(cap::capitanize_and_slice(input, dst));
    Ok( () )
}
