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

use std::collections::{ BTreeSet, HashSet, HashMap };
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
Usage: smart_urls_prefix <wiki-name> <pairs>...
    pairs: in the form ident:pair1,pair2,pair3
");

type Filter = (String,Vec<(EntityRef,EntityRef)>);
type Pages = HashMap<String,HashSet<String>>;
type Urls = HashMap<String,BTreeSet<String>>;

fn main() {
let args:Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let filters:Vec<Filter> = args.arg_pairs.iter().map(|filter| {
        let (ident,filters) = scan_fmt!(filter, "{}:{}", String, String);
        let filter = filters.unwrap().split(",").map(|p| {
            let (a,b) = scan_fmt!(p, "P{}Q{}", u32, u32);
            (EntityRef::P(a.unwrap()), EntityRef::Q(b.unwrap()))
        }).collect();
        (ident.unwrap(),filter)
    }).collect();
    let mut pages = Mutex::new(HashMap::new());
    let mut urls:Mutex<Urls> = Mutex::new(HashMap::new());
    {
        let mut h = pages.lock().unwrap();
        let mut h2 = urls.lock().unwrap();
        for filter in filters.iter() {
            h.insert(filter.0.to_owned(), HashSet::new());
            h2.insert(filter.0.to_owned(), BTreeSet::new());
        }
    }
    grep_pages(&*filters, &mut pages).unwrap();
    grep_urls(&*args.arg_wiki_name, &*pages.lock().unwrap(), &mut urls).unwrap();
    let urls = urls.lock().unwrap();
    let pages = pages.lock().unwrap();
    for (label, _) in urls.iter() {
        println!("{}: {} {}", label,
            pages.get(label).unwrap().len(),
            urls.get(label).unwrap().len()
        );
    }
    let mut aggregated = HashMap::new();
    for (label,urlset) in urls.iter() {
        aggregated.insert(label, url_aggregator::aggregate_urls(urlset));
    }
    let mut in_common = HashMap::new();
    for (_label,urlset) in aggregated.iter() {
        for &(ref url,_,_) in urlset.iter() {
            let previous = *in_common.get(&*url).unwrap_or(&(0usize));
            in_common.insert(url, previous + 1);
        }
    }
    for (label,urlset) in aggregated.iter() {
        for &(ref url,_,_) in urlset.iter() {
            if *in_common.get(url).unwrap() < filters.len() / 2 {
                println!("{}\t{}\t{}", args.arg_wiki_name, label, url);
            }
        }
    }
}

fn grep_urls(wikiname:&str, pages:&Pages, urls:&mut Mutex<Urls>) -> WikiResult<()> {
    let re = Regex::new(r#"\[(http.*?) .*\]"#).unwrap();
    let wiki = try!(wiki::Wiki::latest_compiled(wikiname));
    let mut pool = Pool::new(1+num_cpus::get());
    let chunks = try!(wiki.page_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wiki::MessageAndPage>>+Send>| -> () {
        for page in it {
            let page = page.unwrap();
            let reader = page.as_page_reader().unwrap();
            let title = reader.get_title().unwrap();
            for (ident,set) in pages {
                if set.contains(title) {
                    match reader.which().unwrap() {
                        Text(text) => {
                            let text = text.unwrap();
                            for cap in re.captures_iter(text) {
                                let mut urlset = urls.lock().unwrap();
                                urlset.get_mut(ident).unwrap().insert(cap.at(1).unwrap().to_owned());
                            }
                        },
                        Redirect(_) => {}
                    }
                }
            }
        }
    };
    let _ = unsafe { pool.map(chunks, &each).count() };
    Ok( () )
}

fn grep_pages(filters:&[Filter], pages: &mut Mutex<Pages>) -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.entity_iter_iter());

    let each = |it:Box<Iterator<Item=WikiResult<wikidata::MessageAndEntity>>+Send>| -> () {
        for e in it {
            let e = e.unwrap();
            for filter in filters {
                if e.get_relations().unwrap().any(|t:(wikidata::EntityRef,wikidata::EntityRef)|
                    filter.1.iter().any(|p| t == *p)
                ) {
                    if let Some(sitelink) = e.get_sitelink("enwiki").unwrap() {
                        let mut locked = pages.lock().unwrap();
                        locked.get_mut(&*filter.0).unwrap().insert(sitelink);
                    }
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

