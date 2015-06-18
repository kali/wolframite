extern crate wolframite;
extern crate tinycdb;

use std::path;
use tinycdb::Cdb;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

pub use wolframite::wiki_capnp::monolingual_text as MongolingualText;

pub type WikiResult<T> = Result<T,WikiError>;

fn main() {
    run().unwrap();
}

fn run() -> WikiResult<()> {
/*
    let date:String = helpers::latest("cap", "wikidata").unwrap().unwrap();
    let it = wikidata::for_date(&*date);
    for message in it.unwrap() {
        let message = try!(message);
        println!("{:?} {:?}", try!(message.get_label("en")), try!(message.get_description("en")));
    }
    Ok( () )
*/
    let mut wd = wikidata::Wikidata::latest_compiled().unwrap();
    println!("{:?}", wd.get_label("Q42"));
    Ok( () )
}
