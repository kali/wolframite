extern crate wolframite;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;
extern crate regex;

use wolframite::cap;
use wolframite::WikiError;
use wolframite::helpers;
use std::fs;

use regex::Regex;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
    let re = Regex::new(r#"\[(http.*?) .*\]"#).unwrap();
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let date:String = if args[2] == "latest" {
        helpers::latest("cap", lang).unwrap().unwrap()
    } else {
        args[2].to_string()
    };
    let cap_root = helpers::data_dir_for("cap", lang, &*date);
    let glob = cap_root.clone() + "/*cap.snap";
    for entry in try!(::glob::glob(&glob)) {
        let input:fs::File = try!(fs::File::open(try!(entry)));
        let input = SnappyFramedDecoder::new(input, Ignore);
        let reader = cap::PagesReader::new(input);
        for page in reader {
            use wolframite::wiki_capnp::page::Which::{Text,Redirect};
            let page = try!(page);
            let reader = try!(page.as_page_reader());
            match try!(reader.which()) {
                Text(text) => {
                    let text = try!(text);
                    for cap in re.captures_iter(text) {
                        println!("{}\t{}\t{}\t{}", lang, date, try!(reader.get_title()), cap.at(1).unwrap());
                    }
                },
                Redirect(_) => {}
            }
        }
    }
    Ok( () )
}
