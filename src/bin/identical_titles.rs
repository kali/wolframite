extern crate wolframite;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred::MapReduceOp;

fn main() { count().unwrap() }

#[allow(dead_code)]
fn count() -> WikiResult<()> {
    let wd = wikidata::Wikidata::latest_compiled().unwrap();

    let mro = MapReduceOp::new_map_reduce(
        |e:WikiResult<wikidata::MessageAndEntity>| {
            let e = e.unwrap();
            let en = e.get_label("en");
            let de = e.get_label("de");
            let identical:bool = en.is_ok() && de.is_ok() && de.unwrap() == en.unwrap();
            Box::new(vec!((identical, 1)).into_iter())
        },
        |a:&usize,b:&usize| { a+b }
    );
    let biter = try!(wd.entity_iter_iter());
    let r = mro.run(biter);
    println!("results: {:?}", r);
    Ok( () )
}

