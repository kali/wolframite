#![feature(path_ext)]

extern crate wolframite;
extern crate glob;
extern crate bzip2;
extern crate snappy_framed;
extern crate simple_parallel;
extern crate num_cpus;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::cap;

use std::fs;
use std::fs::PathExt;

use std::path;

use bzip2::reader::BzDecompressor;

fn main() {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let date:String = if args[2] == "latest" {
        helpers::latest("download", lang).unwrap().unwrap()
    } else {
        args[2].to_string()
    };
    capitanize(lang, &*date).unwrap();
}

pub fn capitanize(lang:&str, date:&str) -> Result<(), WikiError> {
    let source_root = helpers::data_dir_for("download", lang, date);
    let target_root = helpers::data_dir_for("cap", lang, date);
    try!(fs::remove_dir_all(target_root.clone()));
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
    fs::File::create(format!("data/cap/{}/{}/ok", lang, &*date)).unwrap();
    Ok( () )
}

pub fn capitanize_file(src:&path::Path, dst:&path::Path) -> Result<(), WikiError> {
    let input = BzDecompressor::new(try!(fs::File::open(src)));
    try!(cap::capitanize_and_slice(input, dst));
    Ok( () )
}
