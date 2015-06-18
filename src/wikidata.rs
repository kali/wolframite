use std::io;
use std::fs;
use std::path;
use std::io::prelude::*;
use std::error::Error;

use helpers;

use capnp;
use capnp::message::MessageReader;
use WikiError;

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

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use tinycdb::Cdb;

pub type WikiResult<T> = Result<T,WikiError>;

pub struct Wikidata {
    date:String,
    labels:Box<Cdb>
}

pub type WikidataReader = EntityReader<SnappyFramedDecoder<helpers::ReadChain<fs::File>>>;

impl Wikidata {
    fn for_date(date:&str) -> WikiResult<Wikidata> {
        let labels_file = helpers::data_dir_for("labels", "wikidata", date) + "/labels";
        let labels = try!(Cdb::open(path::Path::new(&*labels_file)));
        Ok(Wikidata { date: date.to_string(), labels:labels })
    }

    pub fn latest_compiled() -> WikiResult<Wikidata> {
        let date1 = helpers::latest("labels", "wikidata").unwrap().unwrap();
        let date2 = helpers::latest("cap", "wikidata").unwrap().unwrap();
        if date1 != date2 {
            Err(WikiError::Other("latest wikidate seems only partially compiled".to_string()))
        } else {
            Wikidata::for_date(&*date1)
        }
    }

    pub fn get_label(&mut self, key:&str) -> Option<&str> {
        (*self.labels).find(key.as_bytes()).map(|x| ::std::str::from_utf8(x).unwrap())
    }

    pub fn entities(&self) -> WikiResult<WikidataReader> {
        entity_reader(&*self.date)
    }

    pub fn tuples(&self) -> Box<Iterator<Item=(EntityRef,EntityRef,EntityRef)>> {
        Box::new(
            self.entities().unwrap().flat_map(|e| {
                let e = e.unwrap();
                let source = EntityRef::from_id(e.get_id().unwrap());
                let relations = e.get_relations().unwrap();
                let threes:Vec<(EntityRef,EntityRef,EntityRef)> =
                    relations.iter().map(|pair| (source, pair.0, pair.1)).collect();
                threes
            }))
    }

}



pub fn entity_reader(date:&str) ->
        WikiResult<EntityReader<SnappyFramedDecoder<helpers::ReadChain<fs::File>>>> {
    let cap_root = helpers::data_dir_for("cap", "wikidata", date);
    let glob = cap_root.clone() + "/*cap.snap";
    let mut files:Vec<fs::File> = vec!();
    for entry in try!(::glob::glob(&glob)) {
        files.push(try!(fs::File::open(try!(entry))));
    }
    let chain = helpers::ReadChain::new(files);
    let input = SnappyFramedDecoder::new(chain, Ignore);
    Ok(EntityReader::for_reader(input))
}

trait MapWrapper {
    fn get(&self, key:&str) -> WikiResult<Option<&str>>;
}

impl <'a> MapWrapper for Map::Reader<'a> {
    fn get(&self, key:&str) -> WikiResult<Option<&str>> {
        let entries = try!(self.get_entries());
        for entry in entries.iter() {
            let this_key:&str = try!(entry.get_key().get_as());
            if this_key == key {
                let value:MongolingualText::Reader = try!(entry.get_value().get_as());
                match try!(value.which()) {
                    MongolingualText::Value(t) => return Ok(Some(try!(t))),
                    MongolingualText::Removed(_) => return Ok(None)
                }
            }
        }
        Ok(None)
    }
}

#[derive(Clone,Copy)]
pub enum EntityRef { Property(u32), Item(u32) }

impl EntityRef {
    pub fn from_id(id:&str) -> EntityRef {
        let (first,rest) = id.slice_shift_char().unwrap();
        let i:u32 = rest.parse().unwrap();
        match first {
            'P' => EntityRef::Property(i),
            'Q' => EntityRef::Item(i),
            _   => panic!("id must start by P or Q")
        }
    }
    fn from_wikibaseentityid(r:WikibaseEntityRef::Reader) -> EntityRef {
        let id = r.get_id();
        match r.get_type().unwrap() {
            EntityType::Property => EntityRef::Property(id),
            EntityType::Item => EntityRef::Item(id)
        }
    }
    pub fn get_id(&self) -> String {
        match self {
            &EntityRef::Property(id) => format!("P{}", id),
            &EntityRef::Item(id) => format!("Q{}", id),
        }
    }
}


pub struct MessageAndEntity {
    message:capnp::serialize::OwnedSpaceMessageReader
}

pub trait EntityHelpers {
    fn as_entity_reader(&self) -> WikiResult<Entity::Reader>;

    fn get_id(&self) -> WikiResult<&str> {
        Ok(try!(try!(self.as_entity_reader()).get_id()))
    }

    fn get_labels(&self) -> WikiResult<Map::Reader> {
        Ok(try!(try!(self.as_entity_reader()).get_labels()))
    }

    fn get_descriptions(&self) -> WikiResult<Map::Reader> {
        Ok(try!(try!(self.as_entity_reader()).get_descriptions()))
    }

    fn get_label(&self, lang:&str) -> WikiResult<Option<String>> {
        let labels = try!(self.get_labels());
        Ok(try!(labels.get(lang)).map(|s| s.to_string()))
    }

    fn get_description(&self, lang:&str) -> WikiResult<Option<String>> {
        let descriptions = try!(self.get_descriptions());
        Ok(try!(descriptions.get(lang)).map(|s| s.to_string()))
    }

    fn get_claims<'a>(&'a self) ->
        WikiResult<capnp::traits::ListIter<::capnp::struct_list::Reader<'a, MapEntry::Reader<'a>>, MapEntry::Reader<'a>>> {
        let claims = try!(try!(try!(self.as_entity_reader()).get_claims()).get_entries());
        Ok(claims.iter())
    }

    fn get_relations<'a>(&'a self) -> WikiResult<Vec<(EntityRef,EntityRef)>> {
        let mut result = vec!();
        for claim in try!(self.get_claims()) {
            let values: ::capnp::struct_list::Reader<Claim::Reader> =
                try!(claim.get_value().get_as());
            for value in values.iter() {
                let snak = try!(value.get_mainsnak());
                match try!(snak.which()) {
                    Snak::Somevalue(_) => (),
                    Snak::Novalue(_) => (),
                    Snak::Value(v) => match try!(try!(v).which()) {
                        DataValue::Wikibaseentityid(t) =>
                            result.push((
                                EntityRef::from_id(try!(snak.get_property())),
                                EntityRef::from_wikibaseentityid(try!(t))
                            )),
                        _ => ()
                    }
                }
            }
        }
        Ok(result)
    }
}


impl EntityHelpers for MessageAndEntity {
    fn as_entity_reader(&self) -> WikiResult<Entity::Reader> {
        self.message.get_root().map_err( |e| WikiError::from(e))
    }
}

pub struct EntityReader<R:io::Read> {
    options: capnp::message::ReaderOptions,
    stream: io::BufReader<R>,
}

impl <R:io::Read> EntityReader<R> {
    pub fn for_reader(r:R) -> EntityReader<R> {
        EntityReader {
            options:capnp::message::ReaderOptions::new(),
            stream:io::BufReader::new(r),
        }
    }
}

impl <R:io::Read> Iterator for EntityReader<R> {
    type Item = WikiResult<MessageAndEntity>;

    fn next(&mut self) -> Option<WikiResult<MessageAndEntity>> {
        match capnp::serialize_packed::read_message(&mut self.stream, self.options) {
            Ok(msg) => { Some(Ok(MessageAndEntity { message:msg })) },
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
