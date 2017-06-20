#![allow(non_snake_case)]

extern crate wolframite;
extern crate itertools;
extern crate pad;
extern crate num_cpus;
extern crate simple_parallel;

use std::fs;
use std::io::prelude::*;

use wolframite::WikiResult;
use wolframite::wikidata::Wikidata;
use wolframite::helpers;
use wolframite::wikidata::EntityHelpers;

fn main() { run().unwrap() }

fn run() -> WikiResult<()> {
    let date:String = helpers::latest("cap", "wikidata").unwrap().unwrap();
    let target_root = helpers::data_dir_for("csv", "wikidata", &*date);
    let _ = fs::remove_dir_all(target_root.clone());
    try!(fs::create_dir_all(target_root.clone()));
    let node_filename = format!("{}/titles.csv", target_root);
    let tuple_filename = format!("{}/tuples.csv", target_root);
    let mut node = fs::File::create(node_filename).unwrap();
    let mut tuples = fs::File::create(tuple_filename).unwrap();
    writeln!(node, "id,label").unwrap();
    writeln!(tuples, "source,prop,dest").unwrap();

    let it = Wikidata::entity_iter_for_date(&*date).unwrap();

    for (i, entity) in it.enumerate() {
        let entity = entity.unwrap();
        let label = entity.get_a_label().unwrap_or("/no label".to_string()).replace(",","").replace("\n","");
        writeln!(node, "{},{}", entity.get_id().unwrap(), &*label).unwrap();
        for tuple in entity.triplets().unwrap() {
            writeln!(tuples, "{},{},{}", tuple.0.get_id(), tuple.1.get_id(), tuple.2.get_id()).unwrap();
        }
        if i % 100000 == 0 {
            println!("done {}", i);
        }
    };
    Ok( () )
}

