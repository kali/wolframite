#![allow(non_snake_case)]

extern crate wolframite;
#[macro_use]
extern crate itertools;
extern crate pad;

use itertools::Itertools;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred::MapReduceOp;
use wolframite::wikidata::EntityRef;

fn main() { count().unwrap() }

fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    let mro = MapReduceOp::new_map_reduce(
        |e:WikiResult<wikidata::EntityMessage>| {
            let e = e.unwrap();
            let v:Vec<(EntityRef,usize)> = e.get_relations().unwrap()
                .map(|t| (t.0, 1) )
                .collect();
            Box::new(v.into_iter())
        },
        |a,b| { *a + *b }
    );
    let itit = try!(wd.entity_iter_iter());
    let r = mro.run(itit);

    r.iter().foreach(|entry| {
        println!("{}\t{}\t{}", entry.1, entry.0.get_id(), &*wd.get_label(&entry.0.get_id()).unwrap_or("no label".to_string()));
    });

    Ok( () )
}

