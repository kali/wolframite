#![feature(path_ext)]
extern crate hyper;
extern crate regex;
extern crate wiki;

use std::io;
use std::io::prelude::*;
use std::fs;
use std::path;

use regex::Regex;
use hyper::Client;
use hyper::header::ContentLength;

use wiki::helpers::*;

fn main() {
    let prefix = "http://dumps.wikimedia.org";

    let mut client = Client::new();
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];
    fs::create_dir_all(data_dir_for("download", lang, date)).unwrap();
    let summary_url = format!("{}/{}/{}/", prefix, lang, date);

    let res = client.get(&summary_url).send().unwrap();

    let buffered = io::BufReader::new(res);
    let expr = format!(r#"href="(/{}/{}/{}-{}-pages-articles\d[^\\"]*)""#,
        lang, date, lang, date);
    let re = Regex::new(&*expr).unwrap();

    for line in buffered.lines() {
        let line = line.unwrap();
        for cap in re.captures_iter(&*line) {
            let filename = cap.at(1).unwrap();
            let url = prefix.to_string() + "/" + filename;
            let local_filename = "data/download".to_string() + filename;
            let path = path::Path::new(&*local_filename);

            let mut res = client.get(&*url).send().unwrap();
            let size:Option<u64>
                = res.headers.get::<ContentLength>().map( |x| **x );
            if size.is_some() && path.exists() &&
                path.metadata().map( |m| m.len() ).unwrap_or(0)
                == size.unwrap() {
                println!("skip {} (size: {})", filename, size.unwrap());
            } else {
                let mut file = fs::File::create(path).unwrap();
                io::copy(&mut res, &mut file).unwrap();
            }
        }
    }

}
