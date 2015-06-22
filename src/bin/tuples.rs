extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;

use simple_parallel::pool::Pool;
use wolframite::WikiResult;
use wolframite::wikidata;

fn main() {
    run().unwrap();
}


fn run() -> WikiResult<()> {
    let block_counter = |it:wikidata::BoxedIter<wikidata::WikidataTriplet>| -> u64 {
        let sub = it.map(|t:wikidata::WikidataTriplet| {
            if  t.1 == wikidata::EntityRef::P(106) &&
                t.2 == wikidata::EntityRef::Q(33999) {
                1u64
            } else {
                0u64
            }
        });
        sub.fold(0,|a,b|a+b)
    };
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.triplets_iter_iter());
    let halfway = unsafe { pool.map(chunks, &block_counter) };
    let count:u64 = halfway.fold(0,|a,b|a+b);
    println!("count: {}", count);
    Ok( () )
}

