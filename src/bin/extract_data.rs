extern crate wolframite;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use wolframite::cap;
use wolframite::WikiError;
use wolframite::helpers;
use std::fs;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use wolframite::wiki_capnp::page::Which::{Text,Redirect};

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
    let args:Vec<String> = std::env::args().collect();
    let date:String = if args[1] == "latest" {
        helpers::latest("cap", "wikidatawiki").unwrap().unwrap()
    } else {
        args[1].to_string()
    };
    let cap_root = helpers::data_dir_for("cap", "wikidatawiki", &*date);
    let glob = cap_root.clone() + "/*cap.snap";
//     for entry in try!(::glob::glob(&glob)) {
//         let input:fs::File = try!(fs::File::open(try!(entry)));
//         let input = SnappyFramedDecoder::new(input, Ignore);
//         let reader = cap::PagesReader::new(input);
//         for page in reader {
//             let page = try!(page);
//             let reader = try!(page.as_page_reader());
//             match try!(reader.which()) {
//                 Text(text) => {
//                     let text = try!(text);
//                     for cap in re.captures_iter(text) {
//                         println!("wikidatawiki\t{}\t{}\t{}", date, try!(reader.get_title()), cap.at(1).unwrap());
//                     }
//                 },
//                 Redirect(_) => {}
//             }
//         }
//     }
    Ok( () )
}
