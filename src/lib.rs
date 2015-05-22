extern crate glob;
extern crate bzip2;
extern crate xml;
extern crate snzip;

use std::io;
//use std::fs;

pub mod helpers;

#[derive(Debug)]
pub enum WikiError {
    Io(io::Error),
    GlobPattern(glob::PatternError),
    Glob(glob::GlobError),
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
