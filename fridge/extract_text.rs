extern crate wolframite;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use wolframite::wiki;
use wolframite::WikiError;
use wolframite::helpers;
use std::fs;

use wolframite::wiki::Page::Which::{Text,Redirect};

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
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
        let reader = wiki::PagesReader::new(input);
        for page in reader {
            let page = try!(page);
            let reader = try!(page.as_page_reader());
            if try!(reader.get_title()) == args[3] {
                match try!(reader.which()) {
                    Text(text) => { println!("{}", try!(text)); },
                    Redirect(red) => { println!("REDIRECT {}", try!(red)); }
                }
                return Ok( () )
            }
        }
    }
    Err(WikiError::Other("not found".to_string()))
}
