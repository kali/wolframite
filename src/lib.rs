extern crate glob;
extern crate bzip2;
#[macro_use]
extern crate error_chain;
extern crate flate2;
extern crate xml;
extern crate snappy_framed;
extern crate serde;
extern crate serde_json;
extern crate num_cpus;
extern crate simple_parallel;

extern crate capnp;

extern crate capn_wiki;
extern crate url_aggregator;

extern crate tinycdb;
extern crate itertools;

pub mod helpers;
pub mod wiki;
pub mod wikidata;
pub mod capitanize_wikidata;
pub mod capitanize_wiki;
pub mod mapred;

error_chain! {
    types { WikiError, WikiErrorKind, WikiErrorExt, WikiResult; }
    foreign_links {
        Io(::std::io::Error);
        GlobPattern(::glob::PatternError);
        Glob(::glob::GlobError);
        Capnp(::capnp::Error);
        CapnpNotInSchema(::capnp::NotInSchema);
        Serde(::serde_json::Error);
    }
}

pub type BoxedIter<Item> = Box<Iterator<Item=Item>+Send>;

