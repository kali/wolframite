extern crate wolframite;
extern crate glob;
extern crate capnp;
extern crate snappy_framed;

use wolframite::WikiError;
use wolframite::helpers;
use std::fs;

pub use wolframite::wiki_capnp::monolingual_text as MongolingualText;

pub type WikiResult<T> = Result<T,WikiError>;

trait MapWrapper {
    fn get(&self, key:&str) -> WikiResult<Option<&str>>;
}

impl <'a> MapWrapper for wolframite::wiki_capnp::map::Reader<'a> {
    fn get(&self, key:&str) -> WikiResult<Option<&str>> {
        let entries = try!(self.get_entries());
        for entry in entries.iter() {
            let this_key:&str = try!(entry.get_key().get_as());
            if this_key == key {
                let value:MongolingualText::Reader = try!(entry.get_value().get_as());
                match try!(value.which()) {
                    MongolingualText::Value(t) => return Ok(Some(try!(t))),
                    MongolingualText::Removed(_) => return Ok(None)
                }
            }
        }
        Ok(None)
    }
}

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
            let entity = try!(message.as_entity_reader());
            println!("{:?}", try!(entity.get_labels()).get("en"));
        }
    }
    Ok( () )
}
