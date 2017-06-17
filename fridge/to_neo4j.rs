#![allow(non_snake_case)]

extern crate wolframite;
#[macro_use]
extern crate itertools;
extern crate pad;
extern crate num_cpus;
extern crate simple_parallel;

use std::fs;
use std::path;
use std::io::prelude::*;

use wolframite::WikiResult;
use wolframite::wikidata::Wikidata;
use wolframite::helpers;
use wolframite::wikidata::EntityHelpers;

fn main() { run().unwrap() }

fn run() -> WikiResult<()> {
    let wd = Wikidata::latest_compiled().unwrap();
    let target_root = helpers::data_dir_for("neo4j", "wikidata", &*wd.date);
    let _ = fs::remove_dir_all(target_root.clone());
    try!(fs::create_dir_all(target_root.clone()+"/nodes"));
    try!(fs::create_dir_all(target_root.clone()+"/tuples"));
    let task = |pair:(usize,WikiResult<path::PathBuf>)| {
        let node_filename = format!("{}/nodes/part{:04}.csv", target_root, pair.0);
        let mut node = fs::File::create(node_filename).unwrap();
        let tuple_filename = format!("{}/tuples/part{:04}.csv", target_root, pair.0);
        let mut tuples = fs::File::create(tuple_filename).unwrap();
        writeln!(node, "qid:ID;label:LABEL").unwrap();
        writeln!(tuples, ":START_ID;:TYPE;:END_ID").unwrap();
        for entity in Wikidata::entity_iter_for_file(pair.1.unwrap()).unwrap() {
            let entity = entity.unwrap();
            let label = entity.get_a_label().unwrap_or("/no label".to_string()).replace("'","").replace("\n","");
            writeln!(node, "{};'{}'", entity.get_id().unwrap(), &*label).unwrap();
            for tuple in entity.triplets().unwrap() {
                writeln!(tuples, "{};{};{}", tuple.0.get_id(), tuple.1.get_id(), tuple.2.get_id()).unwrap();
            }
        }
    };
    let mut pool = simple_parallel::Pool::new(1+num_cpus::get());
    let jobs:Vec<(usize,WikiResult<path::PathBuf>)> =
        wd.cap_files().unwrap().enumerate().collect();
    let _result:Vec<()> = unsafe { pool.map(jobs.into_iter(), &task) }.collect();
    Ok( () )
}

