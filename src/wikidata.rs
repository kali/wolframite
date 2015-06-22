use std::io;
use std::fs;
use std::path;
use std::io::prelude::*;
use std::error::Error;

use helpers;

use capnp;
use capnp::message::MessageReader;
use WikiError;
use WikiResult;
use BoxedIter;

pub use capn_wiki::wiki_capnp::page as Page;
pub use capn_wiki::wiki_capnp::entity as Entity;
pub use capn_wiki::wiki_capnp::map as Map;
pub use capn_wiki::wiki_capnp::map::entry as MapEntry;
pub use capn_wiki::wiki_capnp::monolingual_text as MongolingualText;
pub use capn_wiki::wiki_capnp::site_link as SiteLink;
pub use capn_wiki::wiki_capnp::claim as Claim;
pub use capn_wiki::wiki_capnp::snak as Snak;
pub use capn_wiki::wiki_capnp::data_value as DataValue;
pub use capn_wiki::wiki_capnp::wikibase_entity_ref as WikibaseEntityRef;
pub use capn_wiki::wiki_capnp::time as Time;
pub use capn_wiki::wiki_capnp::quantity as Quantity;
pub use capn_wiki::wiki_capnp::globe_coordinate as GlobeCoordinate;
pub use capn_wiki::wiki_capnp::{ EntityType };

use snappy_framed::read::SnappyFramedDecoder;
use snappy_framed::read::CrcMode::Ignore;

use tinycdb::Cdb;

pub type WikidataTriplet = (EntityRef,EntityRef,EntityRef);
pub type EntityIter = Iterator<Item=WikiResult<MessageAndEntity>>+Send;
pub type EntityIterIter = Iterator<Item=Box<EntityIter>>+Send;

pub struct Wikidata {
    date:String,
    labels:Box<Cdb>
}

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

    pub fn entity_iter(&self)
            -> WikiResult<BoxedIter<WikiResult<MessageAndEntity>>> {
        entity_iter(&*self.date)
    }

    pub fn entity_iter_iter(&self)
            -> WikiResult<BoxedIter<BoxedIter<WikiResult<MessageAndEntity>>>> {
        entity_iter_iter(&*self.date)
    }

    pub fn triplets_iter_iter(&self) ->
            WikiResult<BoxedIter<BoxedIter<WikidataTriplet>>> {
        Ok(Box::new(try!(self.entity_iter_iter()).map(
            |entity_iter:Box<EntityIter>| -> Box<Iterator<Item=WikidataTriplet>+Send> {
                Box::new(entity_iter.flat_map(
                    |e:WikiResult<MessageAndEntity>| e.unwrap().triplets().unwrap()
                ))
            }
        )))
    }

    pub fn triplets_iter(&self) -> WikiResult<BoxedIter<WikidataTriplet>> {
        let iter_iter = try!(self.triplets_iter_iter());
        Ok(Box::new(iter_iter.flat_map(|it| it)))
    }
}

pub fn entity_iter(date:&str) -> WikiResult<Box<EntityIter>> {
    let reader = try!(entity_iter_iter(date)).flat_map(|it| it);
    Ok(Box::new(reader))
}

pub fn entity_iter_iter(date:&str) -> WikiResult<Box<EntityIterIter>> {
    let cap_root = helpers::data_dir_for("cap", "wikidata", date);
    let glob = cap_root.clone() + "/*cap.snap";
    let mut files:Vec<Box<EntityIter>> = vec!();
    for entry in try!(::glob::glob(&glob)) {
        files.push(Box::new(EntityReader::for_reader(SnappyFramedDecoder::new(try!(fs::File::open(try!(entry))), Ignore))));
    }
    Ok(Box::new(files.into_iter()))
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

#[derive(Clone,Copy,PartialEq,Debug)]
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
    #[allow(non_snake_case)]
    pub fn Q(id:u32) -> EntityRef { EntityRef::Item(id) }
    #[allow(non_snake_case)]
    pub fn P(id:u32) -> EntityRef { EntityRef::Property(id) }
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

    fn get_sitelinks(&self) -> WikiResult<Map::Reader> {
        Ok(try!(try!(self.as_entity_reader()).get_sitelinks()))
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

    fn get_sitelink(&self, lang:&str) -> WikiResult<Option<String>> {
        let sitelinks = try!(self.get_sitelinks());
        Ok(try!(sitelinks.get(lang)).map(|s| s.to_string()))
    }

    fn get_claims<'a>(&'a self) ->
        WikiResult<capnp::traits::ListIter<::capnp::struct_list::Reader<'a, MapEntry::Reader<'a>>, MapEntry::Reader<'a>>> {
        let claims = try!(try!(try!(self.as_entity_reader()).get_claims()).get_entries());
        Ok(claims.iter())
    }

    fn get_relations(&self) ->
            WikiResult<Box<Iterator<Item=(EntityRef,EntityRef)>+Send>> {
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
        Ok(Box::new(result.into_iter()))
    }

    fn as_ref(&self) -> EntityRef {
        EntityRef::from_id(&self.get_id().unwrap())
    }

    fn triplets(& self) -> WikiResult<Box<Iterator<Item=WikidataTriplet>+Send>> {
        let my_ref = self.as_ref();
        let it = try!(self.get_relations())
            .map(move |pair| (my_ref, pair.0, pair.1));
        Ok(Box::new(it))
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
