
use xml::reader::{ EventReader, Events };
use xml::reader::events::*;

use WikiError;
use std::io;
use std::fs;
use std::error::Error;
use std::io::prelude::*;
use std::path;

use serde::json;

use capnp;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::message::MessageReader;

pub use wiki_capnp::page as Page;
pub use wiki_capnp::entity as Entity;
pub use wiki_capnp::map as Map;
pub use wiki_capnp::map::entry as MapEntry;
pub use wiki_capnp::localized_text as LocalizedText;
pub use wiki_capnp::site_link as SiteLink;
pub use wiki_capnp::{ EntityType };
use capnp::struct_list as StructList;

macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

use snappy_framed::write::SnappyFramedEncoder;

pub type WikiResult<T> = Result<T,WikiError>;

// READ PAGE FROM CAP

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

// PARSE WIKI XML -> CAP SNAP

pub fn capitanize_and_slice<R:io::Read>(input:R, output:&path::Path) -> Result<(),WikiError> {
    let mut parser = EventReader::new(input);
    let mut iterator = parser.events();
    let mut part_counter = 0;
    let mut counter = 0u64;
    let mut path = path::PathBuf::new();
    let mut part:Option<SnappyFramedEncoder<_>> = None; //open_one(counter);
    while let Some(ref e) = iterator.next() {
        match e {
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "page" => {
                if part.is_none() || (counter % 1000 == 0 &&
                        try!(fs::metadata(&path)).len() > 250_000_000) {
                    path = path::PathBuf::from(format!("{}-part-{:05}.cap.snap",
                        output.to_str().unwrap(), part_counter));
                    part_counter+=1;
                    part =
                        Some(SnappyFramedEncoder::new(fs::File::create(path.as_os_str()).unwrap()).unwrap());
                }
                let mut message = MallocMessageBuilder::new_default();
                {
                    let mut page = message.init_root::<Page::Builder>();
                    try!(consume_page(&mut iterator, &mut page));
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
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "model" => {
                page.set_model(match &*try!(consume_string(events)) {
                    "wikitext" => Page::Model::Wikitext,
                    "wikibase-item" => Page::Model::Wikibaseitem,
                    "css" => Page::Model::Css,
                    "javascript" => Page::Model::Javascript,
                    "Scribunto" => Page::Model::Scribunto,
                    m => return Err(io::Error::new(io::ErrorKind::Other, "invalid model : ".to_string() + m))
                })
            }
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

// PARSE JSON DATA DUMP TO CAP

pub fn capitanize_and_slice_wikidata<R:io::Read>(input:R, output:&path::Path) -> Result<(),WikiError> {
    let input = io::BufReader::new(input);
    let mut path = path::PathBuf::new();
    let mut part:Option<SnappyFramedEncoder<_>> = None; //open_one(counter);
    let mut part_counter = 0;
    let mut counter = 0;
    for line in input.lines() {
        let mut line = try!(line);
        if part.is_none() || (counter % 1000 == 0 &&
                try!(fs::metadata(&path)).len() > 250_000_000) {
            path = path::PathBuf::from(format!("{}-part-{:05}.cap.snap",
                output.to_str().unwrap(), part_counter));
            part_counter+=1;
            part =
                Some(SnappyFramedEncoder::new(fs::File::create(path.as_os_str()).unwrap()).unwrap());
        }
        if line == "[" || line == "]" {
        } else {
            counter += 1;
            if line.pop() == Some('}') {
                line.push('}')
            }
            let value:json::Value = try!(json::from_str(&*line).or_else(|e| {
                println_stderr!("##### JSON ERROR @{} #####", counter);
                println_stderr!("{:?}", e);
                try!(io::copy(&mut io::Cursor::new(line.as_bytes()), &mut io::stderr()));
                Err(e)
            }));
            let mut message = MallocMessageBuilder::new_default();
            {
                match consume_item(&value, &mut message) {
                    Err(e) => {
                        println_stderr!("##### ERROR @{} #####", counter);
                        println_stderr!("{:?}", e);
                        try!(json::ser::to_writer_pretty(&mut io::stderr(),&value));
                        return Err(e);
                    },
                    _ => ()
                }
            }
            try!(serialize_packed::write_message(&mut part.as_mut().unwrap(), &mut message));
        }
    }
    Ok( () )
}

fn consume_item(value:&json::Value, message:&mut MallocMessageBuilder) -> Result<(),WikiError> {
    let mut entity = message.init_root::<Entity::Builder>();
    let id = try!(value.find("id").ok_or("id expected"));
    entity.set_id(try!(id.as_string().ok_or("id is expected to be a string")));
    let typ = try!(value.find("type").ok_or("type field expected"));
    match typ.as_string() {
        Some("item") => entity.set_type(EntityType::Item),
        Some("property") => entity.set_type(EntityType::Property),
        _ => return Err(WikiError::Other(format!("type expected to be a string (\"item\", or \"property\") got: {:?}", typ))),
    };
    value.find("labels").map(|labels| {
        build_map(labels, entity.borrow().init_labels(),
            |v,b| build_localized_text(v, b.init_as())
        )
    });
    value.find("descriptions").map(|vs| {
        build_map(vs, entity.borrow().init_descriptions(),
            |v,b| build_localized_text(v, b.init_as())
        )
    });
    value.find("sitelinks").map(|vs| {
        build_map(vs, entity.borrow().init_sitelinks(),
            |v,b| build_sitelink(v, b.init_as())
        )
    });
    value.find("aliases").map(|vs| {
        build_map(vs, entity.borrow().init_aliases(), |v,b| {
            let array = try!(v.as_array().ok_or("expect an array"));
            let mut list_builder:StructList::Builder<LocalizedText::Builder> =
                b.init_as_sized(array.len() as u32);
            for (i,item) in array.iter().enumerate() {
                try!(build_localized_text(item, list_builder.borrow().get(i as u32)));
            };
            Ok( () )
        })
    });
    Ok( () )
}

fn build_map<F>(map_of_maps:&json::Value, mut map:Map::Builder, inner:F) 
                -> WikiResult<()>
            where F: Fn(&json::Value, ::capnp::any_pointer::Builder) -> WikiResult<()> {
    let map_of_maps = try!(map_of_maps.as_object().ok_or("map of localized text is expected as json object"));
    map.borrow().init_entries(map_of_maps.len() as u32);
    let mut entries = try!(map.borrow().get_entries());
    {
        for (i, (l, v)) in map_of_maps.iter().enumerate() {
            let mut entry = entries.borrow().get(i as u32);
            entry.borrow().init_key();
            try!(entry.borrow().get_key().set_as(&**l));
            try!(inner(v, entry.get_value()));
        }
    }
    Ok( () )
}

fn build_localized_text(json:&json::Value, mut builder:LocalizedText::Builder) -> WikiResult<()> {
    json.find("language").and_then(|v| v.as_string()).map(|v| builder.set_language(v));
    json.find("value").and_then(|v| v.as_string()).map(|v| builder.set_value(v));
    builder.set_removed(json.find("removed").is_some());
    Ok( () )
}

fn build_sitelink(json:&json::Value, mut builder:SiteLink::Builder) -> WikiResult<()> {
    json.find("site").and_then(|v| v.as_string()).map(|v| builder.set_site(v));
    json.find("title").and_then(|v| v.as_string()).map(|v| builder.set_title(v));
    Ok( () )
}
