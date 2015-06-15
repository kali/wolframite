extern crate wolframite;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::wikidata::EntityHelpers;
use std::fs;

pub use wolframite::wiki_capnp::monolingual_text as MongolingualText;

pub type WikiResult<T> = Result<T,WikiError>;


use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

fn main() {
    run().unwrap();
}

fn run() -> Result<(), WikiError> {
    let date:String = helpers::latest("cap", "wikidata").unwrap().unwrap();
    let cap_root = helpers::data_dir_for("cap", "wikidata", &*date);
    let glob = cap_root.clone() + "/*cap.snap";
    for entry in try!(::glob::glob(&glob)) {
        let input:fs::File = try!(fs::File::open(try!(entry)));
        let input = SnappyFramedDecoder::new(input, Ignore);
        let reader = wolframite::wikidata::EntityReader::new(input);
        for message in reader {
            let message = try!(message);
            println!("{:?} {:?}", try!(message.get_label("en")), try!(message.get_description("en")));
        }
    }
    Ok( () )
}
