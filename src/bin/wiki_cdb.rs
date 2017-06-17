extern crate wolframite;
extern crate snappy_framed;
extern crate tinycdb;

extern crate byteorder;
#[macro_use]
extern crate clap;
extern crate glob;

use std::fs;
use std::path;

use tinycdb::Cdb;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use wolframite::wiki;
use wolframite::WikiError;
use wolframite::helpers;

use byteorder::ByteOrder;
use wolframite::wiki::Page::Which::{Text, Redirect};

pub type WikiResult<T> = Result<T, WikiError>;

fn main() {
    let matches = clap_app!(wiki_cdb =>
        (@arg wiki: -w --wiki +takes_value "Pick a wiki")
        (@arg date: -d --date +takes_value "Pick a date")
    )
            .get_matches();
    let wiki = matches.value_of("wiki").unwrap_or("enwiki");
    let date: String = helpers::latest("cap", wiki).unwrap().unwrap();
    run(wiki, &*date).unwrap();
}

fn normalize_title(title: &str) -> String {
    let mut result: String = title.chars().next().unwrap().to_lowercase().collect();
    result.push_str(&*title.chars().skip(1).collect::<String>());
    result
}

fn run(wiki: &str, date: &str) -> WikiResult<()> {
    let cdb_path = helpers::data_dir_for("cdb", wiki, date);
    let cap_path = path::PathBuf::from(helpers::data_dir_for("cap", wiki, date));
    let cdb = path::Path::new(&cdb_path);
    if cdb.exists() {
        fs::remove_dir_all(&cdb)?
    }
    println!("deleted");
    fs::create_dir_all(&cdb)?;
    println!("created");

    Cdb::new(&*cdb.join("title"), |title| {
        Cdb::new(&*cdb.join("text"), |text| {
            Cdb::new(&*cdb.join("ix_title"),
                     |ix_title| for entry in ::glob::glob(cap_path
                                                              .join("*.cap.snap")
                                                              .to_str()
                                                              .unwrap())
                                 .unwrap() {
                         let entry = entry.unwrap();
                         println!("loop: {:?}", entry);
                         let input: fs::File = fs::File::open(entry).unwrap();
                         let input = SnappyFramedDecoder::new(input, Ignore);
                         let reader = wiki::PagesReader::new(input);

                         for page in reader {
                             let page = page.unwrap();
                             let reader = page.as_page_reader().unwrap();
                             let mut id = [0u8; 8];
                             byteorder::LittleEndian::write_u64(&mut id, reader.get_id());
                             let tit = reader.get_title().unwrap();
                             title.add(&id, tit.as_bytes()).unwrap();
                             ix_title
                                 .add(normalize_title(tit).as_bytes(), &id)
                                 .unwrap();
                             match reader.which().unwrap() {
                                     Text(t) => text.add(&id, t.unwrap().as_bytes()),
                                     Redirect(red) => {
                                         text.add(&id,
                                                  format!("REDIRECT {}", red.unwrap()).as_bytes())
                                     }
                                 }
                                 .unwrap();
                         }
                     })
                    .unwrap();
        })
                .unwrap();
    })
            .unwrap();
    // let it = Wikidata::entity_iter_for_date(&*date).unwrap();
    // for (i, message) in it.enumerate() {
    // let message = message.unwrap();
    // let label = message.get_a_label().unwrap();
    // creator.add(&*message.get_id().unwrap().as_bytes(),
    // label.as_bytes()).unwrap();
    // if i % 100000 == 0 {
    // println!("done {}", i);
    // }
    // }
    // }).unwrap();
    //
    let _ = fs::File::create(helpers::data_dir_for("cdb/ok", wiki, date));
    Ok(())
}
