
use xml::reader::{ EventReader, Events };
use xml::reader::events::*;

use WikiError;
use WikiResult;
use BoxedIter;
use helpers;

use std::io;
use std::fs;
use std::error::Error;
use std::io::prelude::*;
use std::path;

use serde::json;
use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use capnp;
use capnp::serialize_packed;
use capnp::{MessageBuilder, MallocMessageBuilder};
use capnp::message::MessageReader;

use capnp::struct_list as StructList;
pub use wiki_capnp::page as Page;
pub use wiki_capnp::entity as Entity;
pub use wiki_capnp::map as Map;
pub use wiki_capnp::map::entry as MapEntry;
pub use wiki_capnp::monolingual_text as MongolingualText;
pub use wiki_capnp::site_link as SiteLink;
pub use wiki_capnp::claim as Claim;
pub use wiki_capnp::snak as Snak;
pub use wiki_capnp::data_value as DataValue;
pub use wiki_capnp::wikibase_entity_ref as WikibaseEntityRef;
pub use wiki_capnp::time as Time;
pub use wiki_capnp::quantity as Quantity;
pub use wiki_capnp::globe_coordinate as GlobeCoordinate;
pub use wiki_capnp::{ EntityType };

macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

use snappy_framed::write::SnappyFramedEncoder;

// READ PAGE FROM CAP

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
                        s.parse().or_else( |_| {
                            Err(io::Error::new(io::ErrorKind::Other, "can not parse int (id)"))
                        })
                    )
                ));
            }
            &XmlEvent::StartElement { ref name, .. } if name.local_name == "ns" => {
                page.set_ns(try!(consume_string(events)
                    .and_then(|s|
                        s.parse().or_else( |_| {
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
                    "json" => Page::Model::Json,
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
                let job = consume_item(&value, &mut message);
                try!(job.or_else(|e| {
                    println!("error handler");
                    println_stderr!("##### ERROR @{} #####", counter);
                    println_stderr!("{:?}", e);
                    try!(json::ser::to_writer_pretty(&mut io::stderr(),&value));
                    return Err(e);
                }));
            }
/*
            try!(json::ser::to_writer_pretty(&mut io::stderr(),&value));
            return Err(WikiError::Other("blah".to_string()));
*/
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
    try!(value.find("labels").map(|labels| {
        build_map(labels, entity.borrow().init_labels(),
            |v,b| build_monolingual_text(v, b.init_as())
        )
    }).unwrap_or(Ok(())));
    try!(value.find("descriptions").map(|vs| {
        build_map(vs, entity.borrow().init_descriptions(),
            |v,b| build_monolingual_text(v, b.init_as())
        )
    }).unwrap_or(Ok(())));
    try!(value.find("sitelinks").map(|vs| {
        build_map(vs, entity.borrow().init_sitelinks(),
            |v,b| build_sitelink(v, b.init_as())
        )
    }).unwrap_or(Ok(())));
    try!(value.find("aliases").map(|vs| {
        build_map(vs, entity.borrow().init_aliases(), |v,b| {
            let array = try!(v.as_array().ok_or("expect an array"));
            let mut list_builder:StructList::Builder<MongolingualText::Builder> =
                b.init_as_sized(array.len() as u32);
            for (i,item) in array.iter().enumerate() {
                try!(build_monolingual_text(item, list_builder.borrow().get(i as u32)));
            };
            Ok( () )
        })
    }).unwrap_or(Ok(())));
    try!(value.find("claims").map(|vs| {
        build_map(vs, entity.borrow().init_claims(), |v,b| {
            let array = try!(v.as_array().ok_or("expect an array"));
            let mut list_builder:StructList::Builder<Claim::Builder> =
                b.init_as_sized(array.len() as u32);
            for (i,item) in array.iter().enumerate() {
                try!(build_claim(item, list_builder.borrow().get(i as u32)));
            };
            Ok( () )
        })
    }).unwrap_or(Ok(())));
    Ok( () )
}

fn build_map<F>(map_of_maps:&json::Value, mut map:Map::Builder, inner:F)
                -> WikiResult<()>
            where F: Fn(&json::Value, ::capnp::any_pointer::Builder) -> WikiResult<()> {
    let map_of_maps = try!(map_of_maps.as_object().ok_or("map of monolingual text is expected as json object"));
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


fn build_monolingual_text(json:&json::Value, mut builder:MongolingualText::Builder) -> WikiResult<()> {
    json.find("language").and_then(|v| v.as_string()).map(|v| builder.set_language(v));
    if json.find("removed").is_some() {
        builder.set_removed( () );
    } else {
        json.find("value").and_then(|v| v.as_string()).map(|v| builder.set_value(v));
    }
    Ok( () )
}

fn build_sitelink(json:&json::Value, mut builder:SiteLink::Builder) -> WikiResult<()> {
    json.find("site").and_then(|v| v.as_string()).map(|v| builder.set_site(v));
    json.find("title").and_then(|v| v.as_string()).map(|v| builder.set_title(v));
    Ok( () )
}

fn build_claim(json:&json::Value, mut builder:Claim::Builder) -> WikiResult<()> {
    json.find("id").and_then(|v| v.as_string()).map(|v| builder.set_id(v));
    match json.find("type").and_then(|v| v.as_string()) {
        Some("statement") => builder.set_type(Claim::Type::Statement),
        Some("claim") => builder.set_type(Claim::Type::Claim),
        Some(e) =>
            return Err(WikiError::from(format!("unexpected value for claim type {}", e))),
        _ => ()
    }
    match json.find("rank").and_then(|v| v.as_string()) {
        Some("preferred") => builder.set_rank(Claim::Rank::Preferred),
        Some("normal") => builder.set_rank(Claim::Rank::Normal),
        Some("deprecated") => builder.set_rank(Claim::Rank::Deprecated),
        Some(e) =>
            return Err(WikiError::from(format!("unexpected value for claim rank {}", e))),
        _ => ()
    }
    let snak = try!(json.find("mainsnak").ok_or("I do expect a mainsnak."));
    try!(build_snak(snak, builder.init_mainsnak()));
    Ok( () )
}

fn build_snak(json:&json::Value, mut builder:Snak::Builder) -> WikiResult<()> {
    json.find("property").and_then(|v| v.as_string()).map(|v| builder.set_property(v));
    json.find("datatype").and_then(|v| v.as_string()).map(|v| builder.set_datatype(v));
    let snaktype = try!(json.find("snaktype").and_then(|v|v.as_string()).ok_or("expect a snaktype"));
    match snaktype {
        "value" => {
            let value = try!(json.find("datavalue").ok_or("no datatype in snak"));
            try!(build_data_value(value, builder.init_value()));
        },
        "novalue" => builder.set_novalue(()),
        "somevalue" => builder.set_somevalue(()),
        e => return Err(WikiError::from(format!("unexpected snaktype {}", e))),
    }
    Ok( () )
}

fn build_data_value(json:&json::Value, mut builder:DataValue::Builder) -> WikiResult<()> {
    let t = try!(json.find("type").and_then(|v| v.as_string()).ok_or("expect a type"));
    let v = try!(json.find("value").ok_or("expect a value"));
    match t {
        "string" => builder.set_string(
            try!(v.as_string().ok_or("expected a string"))),
        "wikibase-entityid" =>
            try!(build_wikibase_entity_ref(v, builder.init_wikibaseentityid())),
        "time" => try!(build_time(v, builder.init_time())),
        "quantity" => try!(build_quantity(v, builder.init_quantity())),
        "globecoordinate" => try!(build_globecoordinate(v, builder.init_globecoordinate())),
        "monolingualtext" => try!(build_monolingual_text(v, builder.init_monolingualtext())),
        e => {
            return Err(WikiError::Other(format!("unexpected datavalue type:{} {:?}", e, json)))
        }
    }
    Ok( () )
}

fn build_wikibase_entity_ref(json:&json::Value, mut builder:WikibaseEntityRef::Builder) -> WikiResult<()> {
    let typ = try!(json.find("entity-type").and_then(|v| v.as_string()).ok_or("expect an entity-type"));
    builder.set_type(try!(build_entity_type(typ)));
    builder.set_id(try!(
        json.find("numeric-id").and_then(|v| v.as_u64()).ok_or("expect a numeric id")) as u32);
    Ok( () )
}

fn build_time(json:&json::Value, mut builder:Time::Builder) -> WikiResult<()> {
    json.find("time").and_then(|v| v.as_string()).map(|v| builder.set_time(v));
    json.find("calendarmodel").and_then(|v| v.as_string()).map(|v| builder.set_calendarmodel(v));
    json.find("precision").and_then(|v| v.as_u64()).map(|v| builder.set_precision(v as u8));
    json.find("timezone").and_then(|v| v.as_u64()).map(|v| builder.set_timezone(v as i16));
    json.find("before").and_then(|v| v.as_u64()).map(|v| builder.set_before(v as u64));
    json.find("after").and_then(|v| v.as_u64()).map(|v| builder.set_after(v as u64));
    Ok( () )
}

fn build_quantity(json:&json::Value, mut builder:Quantity::Builder) -> WikiResult<()> {
    json.find("amount").and_then(|v| v.as_f64()).map(|v| builder.set_amount(v));
    json.find("lowerBound").and_then(|v| v.as_f64()).map(|v| builder.set_lower_bound(v));
    json.find("upperBound").and_then(|v| v.as_f64()).map(|v| builder.set_upper_bound(v));
    json.find("unit").and_then(|v| v.as_string()).map(|v| builder.set_unit(v));
    Ok( () )
}

fn build_globecoordinate(json:&json::Value, mut builder:GlobeCoordinate::Builder)
        -> WikiResult<()> {
    json.find("latitude").and_then(|v| v.as_f64()).map(|v| builder.set_latitude(v));
    json.find("longitude").and_then(|v| v.as_f64()).map(|v| builder.set_longitude(v));
    json.find("altitude").and_then(|v| v.as_f64()).map(|v| builder.set_altitude(v));
    json.find("precision").and_then(|v| v.as_f64()).map(|v| builder.set_precision(v));
    json.find("globe").and_then(|v| v.as_string()).map(|v| builder.set_globe(v));
    Ok( () )
}

fn build_entity_type(typ:&str) -> WikiResult<EntityType> {
    match typ {
        "item" => Ok(EntityType::Item),
        "property" => Ok(EntityType::Property),
        _ => Err(WikiError::Other(format!("type expected to be a string (\"item\", or \"property\") got: {:?}", typ))),
    }
}
