extern crate wolframite;

use wolframite::WikiError;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

pub type WikiResult<T> = Result<T,WikiError>;

fn main() {
    run().unwrap();
}

fn run() -> WikiResult<()> {
    let mut wd = wikidata::Wikidata::latest_compiled().unwrap();
    for e in try!(wd.entities()) {
        let e = try!(e);
        for t in try!(e.triplets()) {
            if t.1 == wikidata::EntityRef::P(106) && t.2 == wikidata::EntityRef::Q(33999) {
                println!("{} {}",
                    t.1.get_id(),
                    wd.get_label(try!(e.get_id())).unwrap_or("no label"));
            }
        }
    }
    Ok( () )
}
