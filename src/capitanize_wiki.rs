use xml::reader::{ EventReader, Events, XmlEvent };

use std::{ io, fs, path};
use std::io::prelude::*;

use snappy_framed::write::SnappyFramedEncoder;

use capnp::serialize_packed;
use capnp::message::Builder;

use WikiResult;

pub use capn_wiki::wiki_capnp::page as Page;

pub fn process<R:io::Read>(input:R, output:&path::Path) -> WikiResult<()> {
    let mut parser = EventReader::new(input).into_iter();
    let mut part_counter = 0;
    let mut counter = 0u64;
    let mut path = path::PathBuf::new();
    let mut part:Option<SnappyFramedEncoder<_>> = None; //open_one(counter);
    while let Some(ref e) = parser.next() {
        match e {
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "page" => {
                if part.is_none() || (counter % 1000 == 0 &&
                        try!(fs::metadata(&path)).len() > 250_000_000) {
                    path = path::PathBuf::from(format!("{}-part-{:05}.cap.snap",
                        output.to_str().unwrap(), part_counter));
                    part_counter+=1;
                    part =
                        Some(SnappyFramedEncoder::new(fs::File::create(path.as_os_str()).unwrap()).unwrap());
                }
                let mut message = Builder::new_default();
                {
                    let mut page = message.init_root::<Page::Builder>();
                    try!(consume_page(&mut parser, &mut page));
                }
                counter += 1;
                try!(serialize_packed::write_message(&mut part.as_mut().unwrap(), &mut message));
            },
            _ => ()
        }
    }
    Ok(())
}

fn consume_page<R:io::Read>(events:&mut Events<R>, page:&mut Page::Builder) -> io::Result<()> {
    while let Some(ref e) = events.next() {
        match e {
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "title" => {
                page.set_title(&*try!(consume_string(events)));
            }
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "revision" => {
                try!(consume_revision(events, page));
            }
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "redirect" => {
                page.set_redirect(&*try!(consume_string(events)));
            }
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "id" => {
                page.set_id(try!(consume_string(events)
                    .and_then(|s|
                        s.parse().or_else( |_| {
                            Err(io::Error::new(io::ErrorKind::Other, "can not parse int (id)"))
                        })
                    )
                ));
            }
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "ns" => {
                page.set_ns(try!(consume_string(events)
                    .and_then(|s|
                        s.parse().or_else( |_| {
                            Err(io::Error::new(io::ErrorKind::Other, "can not parse int (ns)"))
                        })
                    )
                ));
            }
            &Ok(XmlEvent::EndElement { ref name, .. })
                if name.local_name == "page" => return Ok(()),
            _ => ()
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "eof?"))
}

fn consume_revision<R:io::Read>(events:&mut Events<R>, page:&mut Page::Builder) -> io::Result<()> {
    while let Some(ref e) = events.next() {
        match e {
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "model" => {
                page.set_model(match &*try!(consume_string(events)) {
                    "wikitext" => Page::Model::Wikitext,
                    "wikibase-item" => Page::Model::Wikibaseitem,
                    "css" => Page::Model::Css,
                    "json" => Page::Model::Json,
                    "flow-board" => Page::Model::Flowboard,
                    "javascript" => Page::Model::Javascript,
                    "Scribunto" => Page::Model::Scribunto,
                    m => return Err(io::Error::new(io::ErrorKind::Other, "invalid model : ".to_string() + m))
                })
            }
            &Ok(XmlEvent::StartElement { ref name, .. }) if name.local_name == "text" => {
                page.set_text(&*try!(consume_string(events)));
            }
            &Ok(XmlEvent::EndElement { ref name, .. })
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
            &Ok(XmlEvent::Characters(ref content)) => text.push_str(&*content),
            &Ok(XmlEvent::Whitespace(ref content)) => text.push_str(&*content),
            &Ok(XmlEvent::EndElement { .. }) => return Ok(text),
            _ => ()
        }
    }
    Err(io::Error::new(io::ErrorKind::Other, "EOF?"))
}
