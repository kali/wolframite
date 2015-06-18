extern crate wolframite;
extern crate tinycdb;

use std::fs;
use std::path;

use tinycdb::Cdb;

use wolframite::WikiError;
use wolframite::helpers;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;

pub use wolframite::wiki_capnp::monolingual_text as MongolingualText;

pub type WikiResult<T> = Result<T,WikiError>;

fn main() {
    run().unwrap();
}

fn run() -> WikiResult<()> {
    let date:String = helpers::latest("cap", "wikidata").unwrap().unwrap();
    let target_root = helpers::data_dir_for("labels", "wikidata", &*date);
    try!(fs::create_dir_all(target_root.clone()));
    let filename = target_root.clone()+"/labels";
    Cdb::new(path::Path::new(&*filename), |creator| {
        let it = wikidata::entity_reader(&*date).unwrap();
        for (i, message) in it.enumerate() {
            let message = message.unwrap();
            creator.add(&*message.get_id().unwrap().as_bytes(),
                &*message.get_label("en").unwrap().unwrap_or("<no en label>".to_string()).as_bytes()
            ).unwrap();
            if i % 100000 == 0 {
                println!("done {}", i);
            }
        }
    }).unwrap();
    let _ = fs::File::create(format!("{}/ok", target_root));
    Ok( () )
}
