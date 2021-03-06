extern crate wolframite;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred::MapReduceOp;
use wolframite::wikidata::EntityRef;

fn main() {
    count().unwrap()
}

#[allow(dead_code)]
fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    let mro = MapReduceOp::new_map_reduce(|e: WikiResult<wikidata::EntityMessage>| {
        let e = e.unwrap();
        Box::new(vec![((),
                       e.get_relations()
                           .unwrap()
                           .any(|t| {
                                    (t.0 == EntityRef::P(31) && t.1 == EntityRef::Q(11424) &&
                                     e.get_claim(EntityRef::P(1258)).unwrap().is_some())
                                }) as usize)]
                         .into_iter())
    },
                                          |a: &usize, b: &usize| a + b);
    let itit = try!(wd.entity_iter_iter());
    let r = mro.run(itit);
    println!("results: {:?}", r);
    Ok(())
}
