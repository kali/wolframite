use WikiError;
use WikiResult;
use BoxedIter;
use helpers;

use std::io;
use std::fs;
use std::error::Error;
use std::io::prelude::*;

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use capnp;
use capnp::serialize_packed;
use capnp::serialize::OwnedSegments;
use capnp::message::Reader;

pub use capn_wiki::wiki_capnp::page as Page;

pub struct Wiki {
    wiki:String,
    date:String,
}

impl Wiki {
    pub fn for_date(wiki:&str, date:&str) -> WikiResult<Wiki> {
        Ok( Wiki{ wiki:wiki.to_string(), date:date.to_string() } )
    }

    pub fn latest_compiled(wiki:&str) -> WikiResult<Wiki> {
        let date = helpers::latest("cap", wiki).unwrap().unwrap();
        Wiki::for_date(wiki, &*date)
    }

    pub fn page_iter(&self) -> WikiResult<BoxedIter<WikiResult<MessageAndPage>>> {
        let it = try!(self.page_iter_iter());
        Ok(Box::new(it.flat_map(|i| i)))
    }

    pub fn page_iter_iter(&self) -> WikiResult<BoxedIter<BoxedIter<WikiResult<MessageAndPage>>>> {
        let cap_root = helpers::data_dir_for("cap", &*self.wiki, &*self.date);
        let glob = cap_root.clone() + "/*cap.snap";
        let mut readers:Vec<BoxedIter<WikiResult<MessageAndPage>>> = vec!();
        for file in try!(::glob::glob(&glob)) {
            let file = file.unwrap();
            readers.push(Box::new(PagesReader::new(SnappyFramedDecoder::new(fs::File::open(file).unwrap(), Ignore))));
        };
        Ok(Box::new(readers.into_iter()))
    }
}

pub struct MessageAndPage {
    message:Reader<OwnedSegments>
}

impl MessageAndPage {
    pub fn as_page_reader(&self) -> WikiResult<Page::Reader> {
        self.message.get_root().map_err( |e| WikiError::from(e))
    }
}

pub struct PagesReader<R:io::Read> {
    options: capnp::message::ReaderOptions,
    stream: io::BufReader<R>,
}

impl <R:io::Read> PagesReader<R> {
    pub fn new(r:R) -> PagesReader<R> {
        PagesReader {
            options:capnp::message::ReaderOptions::new(),
            stream:io::BufReader::new(r),
        }
    }
}

impl <R:io::Read> Iterator for PagesReader<R> {
    type Item = WikiResult<MessageAndPage>;

    fn next(&mut self) -> Option<WikiResult<MessageAndPage>> {
        match serialize_packed::read_message(&mut self.stream, self.options) {
            Ok(msg) => { Some(Ok(MessageAndPage { message:msg })) },
            Err(err) => {
                if err.description().contains("Premature EOF") {
                    return None
                } else {
                    return Some(Err(WikiError::from(err)))
                }
            }
        }
    }
}
