extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;
extern crate regex;

use std::collections::{ BTreeSet, HashSet };
use std::sync::Mutex;
use regex::Regex;

use simple_parallel::pool::Pool;
use wolframite::{ WikiResult, WikiError, BoxedIter };
use wolframite::wikidata;
use wolframite::wikidata::Wikidata;
use wolframite::cap::Wiki;
use wolframite::wikidata::EntityHelpers;
use wolframite::wiki_capnp::page::Which::{Text,Redirect};

fn main() {
    let mut pages = Mutex::new(HashSet::new());
    grep_entities(&mut pages).unwrap();
    let mut urls = Mutex::new(BTreeSet::new());
    grep_urls("enwiki", &*pages.lock().unwrap(), &urls).unwrap();
}

fn grep_urls(wikiname:&str, set:&HashSet<String>, urls:&Mutex<BTreeSet<String>>) -> WikiResult<()> {
    let re = Regex::new(r#"\[(http.*?) .*\]"#).unwrap();
    let wiki = try!(Wiki::latest_compiled(wikiname));
    let chunks = try!(wiki.page_iter_iter());
    chunks.map(|chunk| {
        for page in chunk {
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
    }).count();
    Ok( () )
}

fn grep_entities(set: &mut Mutex<HashSet<String>>) -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.entity_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wikidata::MessageAndEntity>>+Send>| -> () {
        for e in it {
            let e = e.unwrap();
            if e.get_relations().unwrap().any(|t:(wikidata::EntityRef,wikidata::EntityRef)|
               t.0 == wikidata::EntityRef::P(106) &&
               t.1 == wikidata::EntityRef::Q(33999)
            ) {
                if let Some(sitelink) = e.get_sitelink("enwiki").unwrap() {
                    let mut locked = set.lock().unwrap();
                    locked.insert(sitelink);
                }
            }
        }
    };
    let _ = unsafe { pool.map(chunks, &each).count() };
//    chunks.map(|it| each(Box::new(it))).count();
    {
        let locked = set.lock().unwrap();
        println!("count: {}", locked.len());
    }
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

