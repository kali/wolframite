extern crate wolframite;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred::MapReduceOp;

fn main() {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    let result = MapReduceOp::map_reduce(|e: WikiResult<wikidata::EntityMessage>| {
                                             let e = e.unwrap();
                                             let en = e.get_label("en").unwrap();
                                             let de = e.get_label("de").unwrap();
                                             let identical:bool = en.is_some() && de == en;
                                             Box::new(Some((identical, 1)).into_iter())
                                         },
                                         |a, b| a + b,
                                         wd.entity_iter_iter().unwrap());

    println!("results: {:?}", result);
}
