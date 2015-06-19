extern crate wolframite;
extern crate simple_parallel;
extern crate num_cpus;

use simple_parallel::pool::Pool;
use wolframite::WikiError;
use wolframite::wikidata;
//use wolframite::wikidata::EntityHelpers;

pub type WikiResult<T> = Result<T,WikiError>;

fn main() {
    run().unwrap();
}


fn run() -> WikiResult<()> {
    let mut i = ::std::sync::atomic::AtomicUsize::new(1);
    let block_counter = |it:wikidata::BoxedIter<wikidata::WikidataTriplet>| -> u64 {
        let me = i.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        println!("starting {}", me);
        let sub = it.map(|t:wikidata::WikidataTriplet| {
            if  t.1 == wikidata::EntityRef::P(106) &&
                t.2 == wikidata::EntityRef::Q(33999) {
                1u64
            } else {
                0u64
            }
        }).fold(0,|a,b|a+b);
        println!("finishing {} : {}", me, sub);
        sub
    };
    let wd = wikidata::Wikidata::latest_compiled().unwrap();
    let mut pool = Pool::new(1 + num_cpus::get());
    let chunks = try!(wd.triplets_iter_iter());
    let halfway = unsafe { pool.map(chunks, &block_counter) };
    let count:u64 = halfway.fold(0,|a,b|a+b);
    println!("count: {}", count);
    Ok( () )
}

/*                
println!("{} {}",
                    t.1.get_id(),
                    wd.get_label(&*t.0.get_id()).unwrap_or("no label"));
*/
