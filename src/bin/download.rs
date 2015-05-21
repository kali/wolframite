extern crate hyper;
extern crate regex;

use std::io::prelude::*;
use std::io::BufReader;
use std::fs;

use regex::Regex;
use hyper::Client;

fn main() {
    let prefix = "http://dumps.wikimedia.org";

    let mut client = Client::new();
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];
    fs::create_dir_all(format!("data/download/{}/{}", lang, date)).unwrap();
    let summary_url = format!("{}/{}/{}/", prefix, lang, date);

    let mut res = client.get(&summary_url).send().unwrap();

    let buffered = BufReader::new(res);
    let expr = format!(r#"href="(/{}/{}/{}-{}-pages-articles\d[^\\"]*)""#,
        lang, date, lang, date);
    let re = Regex::new(&*expr).unwrap();

    for line in buffered.lines() {
        let line = line.unwrap();
        for cap in re.captures_iter(&*line) {
            let filename = cap.at(1).unwrap();
            let url = prefix.to_string() + "/" + filename;
            let local_filename = "data/download".to_string() + filename;
            let mut file = fs::File::create(local_filename).unwrap();
            let mut res = client.get(&*url).send().unwrap();
            ::std::io::copy(&mut res, &mut file).unwrap();
        }
    }

}
