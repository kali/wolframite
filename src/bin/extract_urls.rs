extern crate wiki;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use wiki::cap;
use wiki::WikiError;
use wiki::helpers::*;
use std::fs;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let date:String = if args[2] == "latest" {
        latest("cap", lang).unwrap().unwrap()
    } else {
        args[2].to_string()
    };
    let cap_root = data_dir_for("cap", lang, &*date);
    let glob = cap_root.clone() + "/*cap.snap";
    for entry in try!(::glob::glob(&glob)) {
        let input:fs::File = try!(fs::File::open(try!(entry)));
        let input = SnappyFramedDecoder::new(input, Ignore);
        let reader = cap::PagesReader::new(input);
        for page in reader {
            let page = try!(page);
            let reader = try!(page.as_page_reader());
            println!("{}", try!(reader.get_title()));
        }
    }
    Ok( () )
}
