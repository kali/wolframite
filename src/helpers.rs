use std::io;
use std::io::prelude::*;
use std::fs;

use bzip2::read::BzDecoder;
use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode;

use xml::reader::EventReader;
use xml::reader::Events;
use xml::reader::XmlEvent;

use WikiError;

pub fn latest(state:&str, lang:&str) -> Result<Option<String>,WikiError> {
    let glob = format!("data/{}/{}/*/ok", state, lang);
    let mut dirs:Vec<_> = try!(try!(::glob::glob(&glob)).collect());
    dirs.sort();
    Ok(dirs.last()
        .and_then(|p| p.iter().nth(3))
        .and_then(|t| t.to_str())
        .map(|s| s.to_string()))
}

pub fn data_dir_for(state:&str, lang:&str, date:&str) -> String {
    format!("data/{}/{}/{}", state, lang, date)
}

pub type WikiResult<R> = Result<R, WikiError>;

#[derive(Debug)]
pub struct Page {
    title:String,
    text:String,
}

pub struct PagesFromXml<R : Read> {
    parser:Events<R>
}

impl <R : Read> Iterator for PagesFromXml<R> {
    type Item = WikiResult<Page>;

    fn next(&mut self) -> Option<WikiResult<Page>> {
        #[derive(PartialEq)]
        enum State {
            Nowhere, InTitle, InText
        };
        let mut state:State = State::Nowhere;
        let mut page = Page { title: String::new(), text: String::new() };
        while let Some(ref e) = self.parser.next() {
            match e {
                &Ok(XmlEvent::StartElement { ref name, .. })
                    if state == State::Nowhere && name.local_name == "title" => {
                    state = State::InTitle;
                }
                &Ok(XmlEvent::StartElement { ref name, .. })
                    if state == State::Nowhere && name.local_name == "text" => {
                    state = State::InText;
                }
                &Ok(XmlEvent::EndElement { ref name })
                    if state == State::InTitle && name.local_name == "title" => {
                    state = State::Nowhere;
                }
                &Ok(XmlEvent::EndElement { ref name })
                    if state == State::InText && name.local_name == "text" => {
                    state = State::Nowhere;
                }
                &Ok(XmlEvent::EndElement { ref name }) if name.local_name == "page" => {
                    return Some(Ok(page))
                }
                &Ok(XmlEvent::Characters(ref content)) if state == State::InTitle =>
                    page.title.push_str(&*content),
                &Ok(XmlEvent::Characters(ref content)) if state == State::InText =>
                    page.text.push_str(&*content),
                &Ok(XmlEvent::Whitespace(ref content)) if state == State::InTitle =>
                    page.title.push_str(&*content),
                &Ok(XmlEvent::Whitespace(ref content)) if state == State::InText =>
                    page.text.push_str(&*content),
                &Err(ref e) => {
                    println!("Error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        None
    }
}

pub fn pages_from_xml<R:Read>(read:R) -> WikiResult<PagesFromXml<R>> {
    let parser = EventReader::new(read);
    Ok(PagesFromXml{ parser:parser.into_iter() })
}

pub struct ReadChain<T:Read> {
    position: usize,
    inner: Vec<T>
}

impl<T:Read> ReadChain<T> {
    pub fn new(readers:Vec<T>) -> ReadChain<T> {
        return ReadChain{ position:0, inner:readers}
    }
}

impl<T:Read> Read for ReadChain<T> {

    fn read(&mut self, buf: &mut [u8]) -> Result<usize,io::Error> {
        while self.position < self.inner.len() {
            let sent = try!(self.inner[self.position].read(buf));
            if sent > 0 {
                return Ok(sent);
            }
            self.position += 1;
        }
        Ok(0)
    }
}

pub fn bzcat(lang:&str, date:&str) -> Result<ReadChain<BzDecoder<fs::File>>, WikiError> {
    let glob = data_dir_for("download", lang, date) + "/*.bz2";
    let decompressors:Result<Vec<BzDecoder<fs::File>>,WikiError> =
        try!(::glob::glob(&glob)).map(|entry| {
            let file = try!(fs::File::open(try!(entry)));
            Ok(BzDecoder::new(file))
    }).collect();
    let decompressors = try!(decompressors);
    Ok(ReadChain::new(decompressors))
}

pub fn snappycat(lang:&str, date:&str) -> Result<ReadChain<SnappyFramedDecoder<fs::File>>, WikiError> {
    let glob = data_dir_for("snappy", lang, date) + "/*.sz";
    let decompressors:Result<Vec<SnappyFramedDecoder<fs::File>>,WikiError> =
        try!(::glob::glob(&glob)).map(|entry| {
            let file = try!(fs::File::open(try!(entry)));
            Ok(SnappyFramedDecoder::new(file, CrcMode::Ignore))
    }).collect();
    let decompressors = try!(decompressors);
    Ok(ReadChain::new(decompressors))
}
