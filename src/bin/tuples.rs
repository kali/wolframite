extern crate wolframite;
extern crate tinycdb;

use std::path;
use tinycdb::Cdb;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

pub use wolframite::wiki_capnp::monolingual_text as MongolingualText;
pub use wolframite::wiki_capnp::claim as Claim;
pub use wolframite::wiki_capnp::snak as Snak;

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
    for entity in try!(wd.entities()) {
        let entity = try!(entity);
        println!("{} {:?}", entity.get_id().unwrap(), wd.get_label(try!(entity.get_id())));
        for claim in try!(entity.get_claims()) {
            let key:&str = try!(claim.get_key().get_as());
            println!("  {} {:?}", key, wd.get_label(key));
/*
            let value:Claim::Reader = try!(claim.get_value().get_as());
            let snak = try!(value.get_mainsnak());
            match try!(snak.which()) {
                Snak::Value(v) => println!("    has a value"),
                Snak::Somevalue(_) => println!("    some value"),
                Snak::Novalue(_) => (),
            }
*/
        }
    }
    Ok( () )
}
