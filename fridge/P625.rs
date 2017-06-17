#![allow(non_snake_case)]
extern crate wolframite;
extern crate capn_wiki;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred;
use wolframite::wikidata::EntityRef;

use capn_wiki::wiki_capnp::snak as Snak;
use capn_wiki::wiki_capnp::data_value as DataValue;

fn main() {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    mapred::par_foreach(wd.entity_iter_iter().unwrap(),
                        &|e: WikiResult<wikidata::EntityMessage>| {
                            let e = e.unwrap();
                            if let Ok(Some(claims)) = e.get_claim(EntityRef::P(625)) {
                                for claim in claims.iter() {
                                    let snak = claim.get_mainsnak().unwrap();
                                    if let Ok(Snak::Value(Ok(v))) = snak.which() {
                                        if let Ok(DataValue::Globecoordinate(Ok(co))) = v.which() {
                                            println!("{} {} {}",
                                                     e.get_a_label().unwrap(),
                                                     co.get_latitude(),
                                                     co.get_longitude());
                                        }
                                    }
                                }
                            }
                        });
}
