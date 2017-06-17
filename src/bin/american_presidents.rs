#![allow(non_snake_case)]

extern crate wolframite;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred;
use wolframite::wikidata::EntityRef;

fn main() {
    count().unwrap()
}

fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    mapred::par_foreach(wd.entity_iter_iter().unwrap(),
                        &|e: WikiResult<wikidata::EntityMessage>| {
        let e = e.unwrap();
        let mut relations = e.get_relations().unwrap();
        if relations.any(|t| t == (EntityRef::P(39), EntityRef::Q(11696))) {
            let mut relations = e.get_relations().unwrap();
            let fiction = relations.any(|t| {
                                            t == (EntityRef::P(31), EntityRef::Q(95074)) ||
                                            t == (EntityRef::P(31), EntityRef::Q(15632617))
                                        });
            if fiction {
                let work = e.get_relations()
                    .unwrap()
                    .find(|t| t.0 == EntityRef::P(1441))
                    .and_then(|t| wd.get_label(&*t.1.get_id()));
                println!("{} ({})",
                         e.get_a_label().unwrap(),
                         &*work.unwrap_or("unknown work".to_string()));
            } else {
                println!("{}, REAL", e.get_a_label().unwrap());
            }
        }
    });
    Ok(())
}
