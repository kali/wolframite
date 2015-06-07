extern crate glob;
extern crate bzip2;
extern crate xml;
extern crate snappy_framed;

extern crate capnp;
extern crate capnpc;

use std::io;
//use std::fs;

pub mod helpers;
pub mod cap;

mod wiki_capnp {
    include!("./schema/wiki_capnp.rs");
}


#[derive(Debug)]
pub enum WikiError {
    Io(io::Error),
    GlobPattern(glob::PatternError),
    Glob(glob::GlobError),
    Capnp(capnp::Error)
}

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
