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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use wolframite::wikidata::EntityRef;

fn main() { count().unwrap() }

type BI<'a,A> = Box<Iterator<Item=A> + Send + 'a>;

struct MapReduceOp<M,R,A,K,V>
    where   M: 'static + Sync + Fn(A) -> (K,V),
            R: 'static + Sync + Fn(&V,&V) -> V,
            A:Send,
            K:Send + Eq + ::std::hash::Hash,
            V:Clone+Send
{
    mapper: M,
    reducer: R,
    _phantom: ::std::marker::PhantomData<A>
}

impl <M,R,A,K,V> MapReduceOp<M,R,A,K,V>
    where   M: 'static + Sync + Fn(A) -> (K,V),
            R: 'static + Sync + Fn(&V,&V) -> V,
            A:Send,
            K:Send + Eq + ::std::hash::Hash + Clone,
            V:Clone+Send
{
    fn run(&self, chunks:BI<BI<A>>) -> HashMap<K,V> {
        let reducer = &self.reducer;
        let mapper = &self.mapper;
        let each = |it: BI<A>| -> HashMap<K,V> {
            let mut aggregates:HashMap<K,V> = HashMap::new();
            for (k,v) in it.map(|e| { mapper(e) }) {
                let val = aggregates.entry(k.clone());
                match val {
                    Entry::Occupied(prev) => {
                        let next = reducer(prev.get(), &v);
                        *(prev.into_mut()) = next;
                    }
                    Entry::Vacant(vac) => { vac.insert(v); }
                };
            };
            aggregates
        };
        let mut pool = Pool::new(1 + num_cpus::get());
        let halfway:Vec<HashMap<K,V>> = unsafe { pool.map(chunks, &each).collect() };
        let mut result:HashMap<K,V> = HashMap::new();
        for h in halfway.iter() {
            for (k,v) in h.iter() {
                let val = result.entry(k.clone());
                match val {
                    Entry::Occupied(prev) => {
                        let next = reducer(prev.get(), v);
                        *(prev.into_mut()) = next.clone();
                    }
                    Entry::Vacant(vac) => { vac.insert(v.clone()); }
                };
            };
        };
        result
    }

    fn new_map_reduce(map:M, reduce:R) -> MapReduceOp<M,R,A,K,V> {
        MapReduceOp {
            mapper: map, reducer: reduce,
            _phantom: ::std::marker::PhantomData
        }
    }

    fn map_reduce(map:M, reduce:R, chunks:BI<BI<A>>) -> HashMap<K,V> {
        MapReduceOp::new_map_reduce(map,reduce).run(chunks)
    }
}

#[allow(dead_code)]
fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    let mro = MapReduceOp::new_map_reduce(
        |e:WikiResult<wikidata::MessageAndEntity>| {
            let e = e.unwrap();
            ((), e.get_relations().unwrap().any(|t|
                (t.0 == EntityRef::P(31) && t.1 == EntityRef::Q(11424) &&
                e.get_claim(EntityRef::P(1258)).unwrap().is_some())) as usize
            )
        },
        |a:&usize,b:&usize| { a+b }
    );
    let biter = try!(wd.entity_iter_iter());
    let r = mro.run(biter);
    println!("results: {:?}", r);
    Ok( () )
}

