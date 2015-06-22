#![feature(str_char)]

extern crate glob;
extern crate bzip2;
extern crate xml;
extern crate snappy_framed;
extern crate serde;

extern crate capnp;

extern crate capn_wiki;
extern crate url_aggregator;

extern crate tinycdb;

use std::io;
use std::error::Error;

pub use capn_wiki::wiki_capnp as wiki_capnp;
pub mod helpers;
pub mod cap;
pub mod wikidata;

#[derive(Debug)]
pub enum WikiError {
    Io(io::Error),
    GlobPattern(glob::PatternError),
    Glob(glob::GlobError),
    Capnp(capnp::Error),
    Other(String)
}

pub type WikiResult<T> = Result<T,WikiError>;
pub type BoxedIter<Item> = Box<Iterator<Item=Item>+Send>;

impl From<io::Error> for WikiError {
    fn from(err: io::Error) -> WikiError {
        WikiError::Io(err)
    }
}

impl From<glob::PatternError> for WikiError {
    fn from(err: glob::PatternError) -> WikiError {
        WikiError::GlobPattern(err)
    }
}

impl From<glob::GlobError> for WikiError {
    fn from(err: glob::GlobError) -> WikiError {
        WikiError::Glob(err)
    }
}

impl From<capnp::Error> for WikiError {
    fn from(err: capnp::Error) -> WikiError {
        WikiError::Capnp(err)
    }
}

impl From<capnp::NotInSchema> for WikiError {
    fn from(_err: capnp::NotInSchema) -> WikiError {
        WikiError::Other("Not in cap schema.".to_string())
    }
}

impl From<serde::json::error::Error> for WikiError {
    fn from(err: serde::json::error::Error) -> WikiError {
        WikiError::Other(format!("Json decode error: {}", err.description()))
    }
}

impl <'a> From<&'a str> for WikiError {
    fn from(err: &str) -> WikiError {
        WikiError::Other(err.to_string())
    }
}

impl From<String> for WikiError {
    fn from(err: String) -> WikiError {
        WikiError::Other(err)
    }
}

impl From<tinycdb::CdbError> for WikiError {
    fn from(err: tinycdb::CdbError) -> WikiError {
        WikiError::Other(format!("{:?}", err))
    }
}
