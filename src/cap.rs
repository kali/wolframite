use xml::reader::{ EventReader, Events };
use xml::reader::events::*;

use WikiError;
use std::io;
use std::error::Error;
use std::io::prelude::*;
use std::iter;

use capnp;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::message::MessageReader;

use wiki_capnp::page as Page;

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


