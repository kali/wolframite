#![feature(path_ext)]
extern crate hyper;
extern crate regex;
extern crate wolframite;

use std::io;
use std::io::prelude::*;
use std::fs;
use std::path;

use regex::Regex;
use hyper::Client;
use hyper::header::ContentLength;

use wolframite::helpers;

const PREFIX:&'static str = "http://dumps.wikimedia.org";

fn latest_available(lang:&str, item:&str) -> Option<String> {
    let mut client = Client::new();
    let rss_url = format!("{}/{}/latest/{}-latest-{}-rss.xml", PREFIX, lang, lang, item);
    let res = client.get(&rss_url).send().unwrap();
    let buffered = io::BufReader::new(res);
    let re = Regex::new(r#"<link>.*/(20\d+)</link>"#).unwrap();
    for line in buffered.lines() {
        let line = line.unwrap();
        for cap in re.captures_iter(&*line) {
            return Some(cap.at(1).unwrap().to_string());
        }
    }
    None
}



fn main() {

    let mut client = Client::new();
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let mut date = args[2].to_string();
    if date == "latest" {
        date = latest_available(lang, "pages-articles.xml.bz2").unwrap();
    }
    println!("lang:{} date:{}", lang, &*date);
    fs::create_dir_all(helpers::data_dir_for("download", lang, &*date)).unwrap();

    let summary_url = format!("{}/{}/{}/", PREFIX, lang, &*date);

    let res = client.get(&summary_url).send().unwrap();

    let buffered = io::BufReader::new(res);
    let expr = format!(r#"href="(/{}/{}/{}-{}-pages-articles\d[^\\"]*)""#,
        lang, date, lang, date);
    let re = Regex::new(&*expr).unwrap();

    let mut files = vec![];
    for line in buffered.lines() {
        let line = line.unwrap();
        for cap in re.captures_iter(&*line) {
            files.push(cap.at(1).unwrap().to_string());
        }
    }

    if files.len() == 0 {
        files.push(format!("/{}/{}/{}-{}-pages-articles.xml.bz2", lang, date, lang, date));
    }

    for filename in files {
        let url = PREFIX.to_string() + "/" + &*filename;
        let local_filename = "data/download".to_string() + &*filename;
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
    fs::File::create(format!("data/download/{}/{}/ok", lang, &*date)).unwrap();
}
