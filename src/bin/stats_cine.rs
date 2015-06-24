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
use wolframite::BoxedIter;
use wolframite::wikidata::EntityHelpers;

use wolframite::wikidata::EntityRef;

fn main() { count().unwrap() }

type BI<A> = Box<Iterator<Item=A>+Send>;

struct MapReduceOp<M, R>
    where   M: 'static + Sync + Fn(usize) -> usize,
            R: 'static + Sync + Fn(BI<usize>) -> usize
{
    mapper:&'static M,
    reducer:&'static R
}

impl <M,R> MapReduceOp<M,R>
    where   M: 'static + Sync + Fn(usize) -> usize,
            R: 'static + Sync + Fn(BI<usize>) -> usize
{
    fn run(&self, chunks:BI<BI<usize>>) -> usize {
        let mut pool = Pool::new(1 + num_cpus::get());
        let mapper = self.mapper;
        let reducer = self.reducer;
        let each = move |it:BI<usize>| -> usize {
            reducer(Box::new(it.map(move |e| { mapper(e) })))
        };
        let counters:Vec<usize> = unsafe { pool.map(chunks, &each).collect() };
        (self.reducer)(Box::new(counters.into_iter()))
    }
}


#[allow(dead_code)]
fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.entity_iter_iter());

    fn mapper(e:WikiResult<wikidata::MessageAndEntity>) -> usize {
        let e = e.unwrap();
        if e.get_relations().unwrap().any(|t|
            t.0 == EntityRef::P(31) && t.1 == EntityRef::Q(11424) &&
            e.get_claim(EntityRef::P(1258)).unwrap().is_some()
        ) {
            1
        } else {
            0
        }
    }

    let reducer = |it:BoxedIter<usize>| -> usize {
        it.fold(0, |a,b| a+b)
    };

    let each = |it:Box<Iterator<Item=WikiResult<wikidata::MessageAndEntity>>+Send>| -> usize {
        reducer(Box::new(it.map(|e| { mapper(e) })))
    };

    let counters:Vec<usize> = unsafe { pool.map(chunks, &each).collect() };
    println!("{}", reducer(Box::new(counters.into_iter())));
    Ok( () )
}

