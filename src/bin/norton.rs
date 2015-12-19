#![allow(non_snake_case)]

extern crate wolframite;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred;
use wolframite::wikidata::EntityRef;

fn main() { count().unwrap() }

fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    mapred::par_foreach(wd.entity_iter_iter().unwrap(),
        &|e:WikiResult<wikidata::EntityMessage>| {
            let e = e.unwrap();
            let mut relations = e.get_relations().unwrap();
            if relations.any(|t| t == (EntityRef::P(161),EntityRef::Q(125904))) {
                println!("{}", e.get_a_label().unwrap());
            }
        }
    );
    Ok(())
}

