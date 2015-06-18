extern crate wolframite;

use wolframite::WikiError;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

pub type WikiResult<T> = Result<T,WikiError>;

fn main() {
    run().unwrap();
}

fn run() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    for entity in try!(wd.entities()) {
        let entity = try!(entity);
        for tuple in try!(entity.get_relations()) {
            println!("{}\t{}\t{}", entity.get_id().unwrap(),
                tuple.0.get_id(), tuple.1.get_id())
        }
    }
    Ok( () )
}
