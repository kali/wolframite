extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;

use simple_parallel::pool::Pool;
use wolframite::WikiResult;
use wolframite::wikidata;

fn main() {
    run().unwrap();
}

fn reducer<T:Iterator<Item=u64>>(it:T) -> u64 {
    it.fold(0,|a,b| a+b)
}

fn mapper<'a,T:Iterator<Item=wikidata::WikidataTriplet>>(it:T) -> Box<Iterator<Item=u64>+'a> 
    where T:'a
{
    Box::new(it.filter(|t|
       t.1 == wikidata::EntityRef::P(106) &&
       t.2 == wikidata::EntityRef::Q(3399)
    ).map(|_| 1u64))
}

fn run() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.triplets_iter_iter());

    let block_counter = |it:wikidata::BoxedIter<wikidata::WikidataTriplet>| -> u64 {
        reducer(mapper(it))
    };
    let halfway = unsafe { pool.map(chunks, &block_counter) };
    let count = reducer(halfway);

    println!("count: {}", count);
    Ok( () )
}

