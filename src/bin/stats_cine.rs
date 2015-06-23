#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;
extern crate regex;

extern crate rustc_serialize;

use simple_parallel::pool::Pool;
use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

use wolframite::wikidata::EntityRef;


fn main() { count().unwrap() }

#[allow(dead_code)]
fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.entity_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wikidata::MessageAndEntity>>+Send>| -> usize {
        it.map(|e| {
            let e = e.unwrap();
            if e.get_relations().unwrap().any(|t|
                t.0 == EntityRef::P(31) && t.1 == EntityRef::Q(11424) &&
                e.get_claim(EntityRef::P(1258)).unwrap().is_some()
            ) {
                1
            } else {
                0
            }
        }).fold(0, |a,b|a+b)
    };
    let counters:Vec<usize> = unsafe { pool.map(chunks, &each).collect() };
    println!("{}", counters.iter().fold(0usize, |a,b| a+b));
    Ok( () )
}

