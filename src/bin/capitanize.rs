extern crate wolframite;
extern crate glob;
extern crate bzip2;
extern crate flate2;
extern crate snappy_framed;
extern crate simple_parallel;
extern crate num_cpus;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::capitanize_wikidata;
use wolframite::capitanize_wiki;

use std::fs;

use std::process;

use std::path;


fn main() {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let date:String = if args.len() < 3 || args[2] == "latest" {
        helpers::latest("download", lang).unwrap().unwrap()
    } else {
        args[2].to_string()
    };
    capitanize(lang, &*date).unwrap();
}

pub fn capitanize(lang:&str, date:&str) -> Result<(), WikiError> {
    let source_root = helpers::data_dir_for("download", lang, date);
    let target_root = helpers::data_dir_for("cap", lang, date);
    let _ = fs::remove_dir_all(target_root.clone());
    try!(fs::create_dir_all(target_root.clone()));
    let extension = if lang == "wikidata" { "json.gz" } else { "bz2" };
    let glob = source_root.clone() + "/*." + extension;
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
    let mut pool = simple_parallel::Pool::new(1+num_cpus::get());
    let task = |job:(path::PathBuf,path::PathBuf)| {
        if lang != "wikidata" {
            let input = bzip2::read::BzDecoder::new(try!(fs::File::open(&*job.0)));
            capitanize_wiki::process(input, &*job.1)
        } else {
            let cmd = try!(process::Command::new("gzcat")
                    .arg("-d").arg(&*job.0)
                    .stdout(process::Stdio::piped())
                    .spawn());
            try!(capitanize_wikidata::process(cmd.stdout.unwrap(), &*job.1));
            Ok(())
        }
    };
    let result:Result<Vec<()>,WikiError> = unsafe { pool.map(jobs, &task).collect() };
    try!(result);
    let _ = fs::File::create(format!("data/cap/{}/{}/ok", lang, &*date));
    Ok( () )
}

