#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;
extern crate regex;
extern crate url_aggregator;

extern crate rustc_serialize;
extern crate docopt;

#[macro_use] extern crate scan_fmt;

use std::collections::{ BTreeSet, HashSet };
use std::sync::Mutex;
use regex::Regex;

use docopt::Docopt;

use simple_parallel::pool::Pool;
use wolframite::{ WikiResult, BoxedIter };
use wolframite::wikidata;
use wolframite::wiki;
use wolframite::wikidata::EntityHelpers;
use wolframite::wiki::Page::Which::{Text,Redirect};

use wolframite::wikidata::EntityRef;

docopt!(Args derive Debug, "
Usage: smart_urls_prefix <wiki-name> <pair>...
");

fn main() {
let args:Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let pairs:Vec<(EntityRef,EntityRef)> = args.arg_pair.iter().map(|p| {
        let (a,b) = scan_fmt!(p, "P{}Q{}", u32, u32);
        (EntityRef::P(a.unwrap()), EntityRef::Q(b.unwrap()))
    }).collect();
    let mut pages = Mutex::new(HashSet::new());
    grep_entities(&*pairs, &mut pages).unwrap();
    println!("entities: {}", pages.lock().unwrap().len());
    let urls = Mutex::new(BTreeSet::new());
    grep_urls(&*args.arg_wiki_name, &*pages.lock().unwrap(), &urls).unwrap();
    println!("distinct urls: {}", urls.lock().unwrap().len());
    let result = url_aggregator::aggregate_urls(&*urls.lock().unwrap());
    for u in result {
        println!("{:5} {:5} {}", u.1, u.2, u.0);
    }
}

fn grep_urls(wikiname:&str, set:&HashSet<String>, urls:&Mutex<BTreeSet<String>>) -> WikiResult<()> {
    let re = Regex::new(r#"\[(http.*?) .*\]"#).unwrap();
    let wiki = try!(wiki::Wiki::latest_compiled(wikiname));
    let mut pool = Pool::new(1+num_cpus::get());
    let chunks = try!(wiki.page_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wiki::MessageAndPage>>+Send>| -> () {
        for page in it {
            let page = page.unwrap();
            let reader = page.as_page_reader().unwrap();
            let title = reader.get_title().unwrap();
            if set.contains(title) {
                match reader.which().unwrap() {
                    Text(text) => {
                        let text = text.unwrap();
                        for cap in re.captures_iter(text) {
                            urls.lock().unwrap().insert(cap.at(1).unwrap().to_owned());
                        }
                    },
                    Redirect(_) => {}
                }
            }
        }
    };
    let _ = unsafe { pool.map(chunks, &each).count() };
    Ok( () )
}

fn grep_entities(pairs:&[(EntityRef,EntityRef)], set: &mut Mutex<HashSet<String>>) -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.entity_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wikidata::MessageAndEntity>>+Send>| -> () {
        for e in it {
            let e = e.unwrap();
            if e.get_relations().unwrap().any(|t:(wikidata::EntityRef,wikidata::EntityRef)|
                pairs.iter().any(|p| t == *p)
            ) {
                if let Some(sitelink) = e.get_sitelink("enwiki").unwrap() {
                    let mut locked = set.lock().unwrap();
                    locked.insert(sitelink);
                }
            }
        }
    };
    let _ = unsafe { pool.map(chunks, &each).count() };
    Ok( () )
}

#[allow(dead_code)]
fn count() -> WikiResult<()> {
    fn reducer<T:Iterator<Item=u64>>(it:T) -> u64 {
        it.fold(0,|a,b| a+b)
    }

    fn mapper<'a,T:Iterator<Item=wikidata::WikidataTriplet>>(it:T) -> Box<Iterator<Item=u64>+'a> 
        where T:'a
    {
        Box::new(it.filter(|t|
           t.1 == wikidata::EntityRef::P(106) &&
           t.2 == wikidata::EntityRef::Q(33999)
        ).map(|_| 1u64))
    }

    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.triplets_iter_iter());

    let block_counter = |it:BoxedIter<wikidata::WikidataTriplet>| -> u64 {
        reducer(mapper(it))
    };
    let halfway = unsafe { pool.map(chunks, &block_counter) };
    let count = reducer(halfway);

    println!("count: {}", count);
    Ok( () )
}

