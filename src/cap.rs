use xml::reader::{ EventReader, Events };
use xml::reader::events::*;

use WikiError;
use std::io;
use std::fs;
use std::error::Error;
use std::io::prelude::*;
use std::iter;
use std::path;

use capnp;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::message::MessageReader;

pub use wiki_capnp::page as Page;

use snappy_framed::write::SnappyFramedEncoder;

pub type WikiResult<T> = Result<T,WikiError>;

pub struct MessageAndPage {
    message:capnp::serialize::OwnedSpaceMessageReader
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

pub fn read_pages<R:io::Read>(mut r:R) -> Result<(), WikiError> {
    let options = capnp::message::ReaderOptions::new();
    let mut stream = io::BufReader::new(r);
    loop {
        match serialize_packed::read_message(&mut stream, options) {
            Ok(msg) => {
                let page:Page::Reader = try!(msg.get_root());
            },
            Err(err) => {
                if err.description().contains("Premature EOF") {
                    return Ok( () )
                } else {
                    return Err(WikiError::from(err))
                }
            }
        }
    }
}

pub fn capitanize_and_slice<R:io::Read>(mut input:R, output:&path::Path) -> Result<(),WikiError> {
    let mut parser = EventReader::new(input);
    let mut iterator = parser.events();
    let mut counter = 0;
    let size = 100000;
    let open_one = |counter| {
        let filename = format!("{}-part-{:05}.cap.snap",
            output.to_str().unwrap(), counter/size);
        SnappyFramedEncoder::new(fs::File::create(filename).unwrap()).unwrap()
    };
    let mut part:SnappyFramedEncoder<_> = open_one(counter);
    while let Some(ref e) = iterator.next() {
        match e {
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "page" => {
                let mut message = MallocMessageBuilder::new_default();
                {
                    let mut page = message.init_root::<Page::Builder>();
                    try!(consume_page(&mut iterator, &mut page));
                }
                if counter % size == 0 {
                    part = open_one(counter);
                }
                try!(serialize_packed::write_message(&mut part, &mut message));
                counter += 1;
            },
            _ => ()
        }
    }
    Ok(())
}

pub fn capitanize<R:io::Read, W:io::Write>(mut input:R, mut output:W) -> Result<(),WikiError> {
    let mut parser = EventReader::new(input);
    let mut iterator = parser.events();
    while let Some(ref e) = iterator.next() {
        match e {
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "page" => {
                let mut message = MallocMessageBuilder::new_default();
                {
                    let mut page = message.init_root::<Page::Builder>();
                    try!(consume_page(&mut iterator, &mut page));
                }
                try!(serialize_packed::write_message(&mut output, &mut message))
            },
            _ => ()
        }
    }
    Ok(())
}

fn consume_page<R:io::Read>(events:&mut Events<R>, page:&mut Page::Builder) -> io::Result<()> {
    while let Some(ref e) = events.next() {
        match e {
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "title" => {
                page.set_title(&*try!(consume_string(events)));
            }
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "revision" => {
                try!(consume_revision(events, page));
            }
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "redirect" => {
                page.set_redirect(&*try!(consume_string(events)));
            }
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "id" => {
                page.set_id(try!(consume_string(events)
                    .and_then(|s|
                        s.parse().or_else( |e| {
                            println!("Error: {} {}", s, e);
                            Err(io::Error::new(io::ErrorKind::Other, "can not parse int (id)"))
                        })
                    )
                ));
            }
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "ns" => {
                page.set_ns(try!(consume_string(events)
                    .and_then(|s|
                        s.parse().or_else( |e| {
                            println!("Error: {} {}", s, e);
                            Err(io::Error::new(io::ErrorKind::Other, "can not parse int (ns)"))
                        })
                    )
                ));
            }
            &XmlEvent::EndElement { ref name, .. }
                if name.local_name == "page" => return Ok(()),
            _ => ()
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "eof?"))
}

fn consume_revision<R:io::Read>(events:&mut Events<R>, page:&mut Page::Builder) -> io::Result<()> {
    while let Some(ref e) = events.next() {
        match e {
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "text" => {
                page.set_text(&*try!(consume_string(events)));
            }
            &XmlEvent::EndElement { ref name, .. }
                if name.local_name == "revision" => return Ok(()),
            _ => ()
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "eof?"))
}

fn consume_string<R:io::Read>(events:&mut Events<R>) -> io::Result<String> {
    let mut text = String::new();
    while let Some(ref e) = events.next() {
        match e {
            &XmlEvent::Characters(ref content) => text.push_str(&*content),
            &XmlEvent::Whitespace(ref content) => text.push_str(&*content),
            &XmlEvent::EndElement { .. } => return Ok(text),
            _ => ()
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "EOF?"))
}


