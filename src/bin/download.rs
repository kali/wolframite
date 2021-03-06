extern crate regex;
extern crate reqwest;
extern crate time;
extern crate wolframite;

use std::io;
use std::io::prelude::*;
use std::fs;
use std::path;

use regex::Regex;

use wolframite::helpers;

const PREFIX: &'static str = "http://dumps.wikimedia.org";

fn latest_available(lang: &str, item: &str) -> Option<String> {
    let rss_url = format!("{}/{}/latest/{}-latest-{}-rss.xml",
                          PREFIX,
                          lang,
                          lang,
                          item);
    let res = reqwest::get(&rss_url).unwrap();
    let buffered = io::BufReader::new(res);
    let re = Regex::new(r#"<link>.*/(20\d+)</link>"#).unwrap();
    for line in buffered.lines() {
        let line = line.unwrap();
        if let Some(found) = re.captures(&line) {
            return Some(found[1].to_string());
        }
    }
    None
}

fn latest_wikidata_available() -> Option<String> {
    let res = reqwest::get("http://dumps.wikimedia.org/other/wikidata/").unwrap();
    let buffered = io::BufReader::new(res);
    let re = Regex::new(r#"href="(\d*)\.json\.gz"#).unwrap();
    buffered
        .lines()
        .flat_map(|line| {
                      let line: String = line.unwrap();
                      re.captures(&*line).map(|cap| cap[1].to_string())
                  })
        .last()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let date = if args.len() >= 3 && args[2].to_string() != "latest" {
        Some(args[2].to_string())
    } else {
        None
    };
    if lang == "wikidata" {
        download_wikidata(date)
    } else if lang == "pagecounts" {
        download_pagecounts(date)
    } else {
        download_wiki(lang, date)
    }
}

fn download_pagecounts(date: Option<String>) {
    let date: String = date.unwrap_or_else(|| {
                                               let yesterday = time::now_utc() -
                                                               time::Duration::days(1);
                                               time::strftime("%Y%m%d", &yesterday).unwrap()
                                           });
    let index_url = format!("http://dumps.wikimedia.org/other/pagecounts-raw/{}/{}-{}/",
                            &date[0..4],
                            &date[0..4],
                            &date[4..6]);
    let dir = helpers::data_dir_for("download", "pagecounts", &*date);
    fs::create_dir_all(&*dir).unwrap();
    let index = reqwest::get(&index_url).unwrap();
    let buffered = io::BufReader::new(index);
    let expr = format!(r#"href="(pagecounts-{}-.*\.gz)""#, date);
    let re = Regex::new(&*expr).unwrap();
    let mut files: Vec<String> = vec![];
    for line in buffered.lines() {
        let line = line.unwrap();
        if let Some(cap) = re.captures(&*line) {
            files.push(cap[1].to_string());
        }
    }
    for filename in files {
        let url = index_url.clone() + "/" + &*filename;
        let local_filename = dir.clone() + "/" + &*filename;
        download_if_smaller(url, local_filename);
    }
    let _ = fs::File::create(format!("{}/ok", &*dir));
}

fn download_wikidata(date: Option<String>) {
    let date: String = date.or_else(|| latest_wikidata_available()).unwrap();
    let url = format!("http://dumps.wikimedia.org/other/wikidata/{}.json.gz",
                      &*date);
    let dir = helpers::data_dir_for("download", "wikidata", &*date);
    fs::create_dir_all(&*dir).unwrap();
    let filename = format!("{}/wikidata-{}.json.gz", &*dir, &*date);
    download_if_smaller(url, filename);
    let _ = fs::File::create(format!("{}/ok", &*dir));
}

fn download_wiki(lang: &String, optdate: Option<String>) {
    let date: String = optdate
        .or_else(|| latest_available(lang, "pages-articles1.xml.bz2"))
        .or_else(|| latest_available(lang, "pages-articles.xml.bz2"))
        .unwrap();
    let dir = helpers::data_dir_for("download", &*lang, &*date);
    fs::create_dir_all(&*dir).unwrap();

    let summary_url = format!("{}/{}/{}/", PREFIX, lang, &*date);

    let res = reqwest::get(&summary_url).unwrap();

    let buffered = io::BufReader::new(res);
    let expr = format!(r#"href="(/{}/{}/{}-{}-pages-articles\d[^\\"]*)""#,
                       lang,
                       date,
                       lang,
                       date);
    let re = Regex::new(&*expr).unwrap();

    let mut files = vec![];
    for line in buffered.lines() {
        let line = line.unwrap();
        for cap in re.captures_iter(&*line) {
            files.push(cap[1].to_string());
        }
    }

    if files.len() == 0 {
        files.push(format!("/{}/{}/{}-{}-pages-articles.xml.bz2",
                           lang,
                           date,
                           lang,
                           date));
    }

    for filename in files {
        let url = PREFIX.to_string() + "/" + &*filename;
        let local_filename = "data/download".to_string() + &*filename;
        download_if_smaller(url, local_filename);
    }
    let _ = fs::File::create(format!("{}/ok", &*dir));
}

fn download_if_smaller(url: String, filename: String) {
    let path = path::Path::new(&*filename);
    let mut res = reqwest::get(&*url).unwrap();
    let size: Option<u64> = res.headers()
        .get::<reqwest::header::ContentLength>()
        .map(|x| **x);
    if size.is_some() && path.exists() &&
       path.metadata().map(|m| m.len()).unwrap_or(0) == size.unwrap() {
        println!("skip {} (size: {})", filename, size.unwrap());
    } else {
        let mut file = fs::File::create(path).unwrap();
        io::copy(&mut res, &mut file).unwrap();
    }
}
