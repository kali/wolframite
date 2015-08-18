use std::io;
use std::path;
use std::io::prelude::*;
use std::error::Error;

use std::sync::Mutex;

use helpers;

use capnp;
use capnp::{ text, traits };
use capnp::message::Reader;
use capnp::serialize::OwnedSegments;
use WikiError;
use WikiResult;
use BoxedIter;

pub use capn_wiki::wiki_capnp::page as Page;
pub use capn_wiki::wiki_capnp::entity as Entity;
pub use capn_wiki::wiki_capnp::map as Map;
pub use capn_wiki::wiki_capnp::map::entry as MapEntry;
pub use capn_wiki::wiki_capnp::monolingual_text as MonolingualText;
pub use capn_wiki::wiki_capnp::site_link as SiteLink;
pub use capn_wiki::wiki_capnp::claim as Claim;
pub use capn_wiki::wiki_capnp::snak as Snak;
pub use capn_wiki::wiki_capnp::data_value as DataValue;
pub use capn_wiki::wiki_capnp::wikibase_entity_ref as WikibaseEntityRef;
pub use capn_wiki::wiki_capnp::time as Time;
pub use capn_wiki::wiki_capnp::quantity as Quantity;
pub use capn_wiki::wiki_capnp::globe_coordinate as GlobeCoordinate;
pub use capn_wiki::wiki_capnp::{ EntityType };

use tinycdb::Cdb;

pub type WikidataTriplet = (EntityRef,EntityRef,EntityRef);
pub type EntityIter = Iterator<Item=WikiResult<EntityMessage>>+Send;
pub type EntityIterIter = Iterator<Item=Box<EntityIter>>+Send;

pub struct Wikidata {
    pub date:String,
    labels:Mutex<Box<Cdb>>
}

impl Wikidata {
    fn for_date(date:&str) -> WikiResult<Wikidata> {
        let labels_file = helpers::data_dir_for("labels", "wikidata", date) + "/labels";
        let labels = try!(Cdb::open(path::Path::new(&*labels_file)));
        Ok(Wikidata {   date: date.to_string(),
                        labels:Mutex::new(labels) })
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

    // "static" iterators
    pub fn cap_files_for_date(date:&str) -> WikiResult<BoxedIter<WikiResult<path::PathBuf>>> {
        let cap_root = helpers::data_dir_for("cap", "wikidata", date);
        let glob = cap_root.clone() + "/*cap.gz";
        Ok(Box::new(try!(::glob::glob(&glob)).map(|f| f.map_err(|e| WikiError::from(e)))))
    }

    pub fn entity_iter_for_date(date:&str)
            -> WikiResult<BoxedIter<WikiResult<EntityMessage>>> {
        Ok(Box::new(try!(Wikidata::entity_iter_iter_for_date(date)).flat_map(|it| it)))
    }

    pub fn entity_iter_iter_for_date(date:&str)
            -> WikiResult<BoxedIter<BoxedIter<WikiResult<EntityMessage>>>> {
        let mut files:Vec<Box<EntityIter>> = vec!();
        for entry in try!(Wikidata::cap_files_for_date(date)) {
            files.push(try!(Wikidata::entity_iter_for_file(try!(entry))));
        }
        Ok(Box::new(files.into_iter()))
    }

    pub fn entity_iter_for_file(filename:path::PathBuf)
        -> WikiResult<BoxedIter<WikiResult<EntityMessage>>> {
        let cmd = try!(::std::process::Command::new("gzcat")
            .arg("-d").arg(&*filename)
            .stdout(::std::process::Stdio::piped())
            .spawn());
        Ok(Box::new(EntityReader::for_reader(cmd.stdout.unwrap())))
    }

    // members iterators
    pub fn cap_files(&self) -> WikiResult<BoxedIter<WikiResult<path::PathBuf>>> {
        Wikidata::cap_files_for_date(&self.date)
    }

    pub fn entity_iter(&self)
            -> WikiResult<BoxedIter<WikiResult<EntityMessage>>> {
        Wikidata::entity_iter_for_date(&self.date)
    }

    pub fn entity_iter_iter(&self)
            -> WikiResult<BoxedIter<BoxedIter<WikiResult<EntityMessage>>>> {
        Wikidata::entity_iter_iter_for_date(&self.date)
    }

    pub fn get_label(&self, key:&str) -> Option<String> {
        let mut lock = self.labels.lock().unwrap();
        (*lock).find(key.as_bytes()).map(|x| ::std::str::from_utf8(x).unwrap().to_string())
    }

    pub fn triplets_iter_iter(&self) ->
            WikiResult<BoxedIter<BoxedIter<WikidataTriplet>>> {
        Ok(Box::new(try!(self.entity_iter_iter()).map(
            |entity_iter:Box<EntityIter>| -> Box<Iterator<Item=WikidataTriplet>+Send> {
                Box::new(entity_iter.flat_map(
                    |e:WikiResult<EntityMessage>| e.unwrap().triplets().unwrap()
                ))
            }
        )))
    }

    pub fn triplets_iter(&self) -> WikiResult<BoxedIter<WikidataTriplet>> {
        let iter_iter = try!(self.triplets_iter_iter());
        Ok(Box::new(iter_iter.flat_map(|it| it)))
    }
}

/*
trait MapWrapper<V> {
    fn get(&self, key:&str) -> WikiResult<Option<V>>;
}

impl <'a> MapWrapper<MonolingualText::Reader<'a>> for Map::Reader<'a,text::Owned,MonolingualText::Owned> {

    fn get(&self, key:&str) -> WikiResult<Option<MonolingualText::Reader<'a>>> {
        let entries = try!(self.get_entries());
        for entry in entries.iter() {
            let this_key:&str = try!(entry.get_key());
            if this_key == key {
                let value = try!(entry.get_value());
                return Ok(Some(value));
/*
                let value:MonolingualText::Reader = try!(entry.get_value());
                match try!(value.which()) {
                    MonolingualText::Value(t) => return Ok(Some(try!(t))),
                    MonolingualText::Removed(_) => return Ok(None)
                }
*/
            }
        }
        Ok(None)
    }
}
*/

/*
impl <'a,V> MapWrapper<V> for Map::Reader<'a,text::Owned,V>
        where V: for<'x> traits::Owned<'x>
    {

    fn get(&self, key:&str) -> WikiResult<Option<V>> {
        let entries = try!(self.get_entries());
        for entry in entries.iter() {
            let this_key:&str = try!(entry.get_key());
            if this_key == key {
                let value = try!(entry.get_value());
                return Ok(Some(value));
/*
                let value:MonolingualText::Reader = try!(entry.get_value());
                match try!(value.which()) {
                    MonolingualText::Value(t) => return Ok(Some(try!(t))),
                    MonolingualText::Removed(_) => return Ok(None)
                }
*/
            }
        }
        Ok(None)
    }
}
*/

#[derive(Clone,Copy,PartialEq,Debug,Hash,Eq)]
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

impl ::std::fmt::Display for EntityRef {
    fn fmt(&self, f:&mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        write!(f, "{}", self.get_id())
    }
}

/*
impl ::std::hash::Hash for EntityRef {
}
*/


pub struct EntityMessage {
    message:Reader<OwnedSegments>
}

pub trait EntityHelpers {
    fn as_entity_reader(&self) -> WikiResult<Entity::Reader>;

    fn get_id(&self) -> WikiResult<&str> {
        Ok(try!(try!(self.as_entity_reader()).get_id()))
    }

    fn get_labels(&self) -> WikiResult<Map::Reader<text::Owned,MonolingualText::Owned>> {
        Ok(try!(try!(self.as_entity_reader()).get_labels()))
    }

/*
    fn get_sitelinks(&self) -> WikiResult<Map::Reader<text::Owned,SiteLink::Owned>> {
        Ok(try!(try!(self.as_entity_reader()).get_sitelinks()))
    }
*/
    fn get_descriptions(&self) -> WikiResult<Map::Reader<text::Owned,MonolingualText::Owned>> {
        Ok(try!(try!(self.as_entity_reader()).get_descriptions()))
    }

    fn lookup<'a,V>(map:Map::Reader<'a,text::Owned,V>, key:text::Reader) ->
        WikiResult<Option<MapEntry::Reader<'a,text::Owned,V>>>
            where V:for<'x> traits::Owned<'x>
    {
        for item in try!(map.get_entries()).iter() {
            if try!(item.borrow().get_key()) == key {
                return Ok(Some(item))
            }
        }
        Ok(None)
    }

    fn extract_monolingual_value(poly:Map::Reader<text::Owned,MonolingualText::Owned>, lang:&str) -> WikiResult<Option<String>>
    {
        for item in try!(poly.get_entries()).iter() {
            if try!(item.borrow().get_key()) == lang {
                let mono = try!(item.get_value());
                match try!(mono.which()) {
                    MonolingualText::Value(t) => return Ok(Some(try!(t).to_owned())),
                    _ => ()
                }
            }
        }
        return Ok(None)
    }

    fn get_label(&self, lang:&str) -> WikiResult<Option<String>> {
        Self::extract_monolingual_value(try!(self.get_labels()), lang)
    }

    fn get_a_label(&self) -> WikiResult<String> {
        for l in vec!("en", "fr", "es") {
            if let Some(label) = try!(self.get_label(l)) {
                return Ok(label);
            }
        }
        let labels = try!(self.get_labels().unwrap().get_entries());
        if let Some(entry) = labels.iter().next() {
            let key:&str = try!(entry.get_key());
            return Ok(self.get_label(key).unwrap().unwrap())
        }
        self.get_id().map(|s|s.to_string())
    }


    fn get_description(&self, lang:&str) -> WikiResult<Option<String>> {
        Self::extract_monolingual_value(try!(self.get_descriptions()), lang)
    }

    fn get_sitelink(&self, lang:&str) -> WikiResult<Option<SiteLink::Reader>> {
        let sitelinks = try!(try!(self.as_entity_reader()).get_sitelinks());
        let sitelink_item = try!(Self::lookup(sitelinks, lang));
        if sitelink_item.is_none() {
            return Ok(None)
        }
        let value = try!(sitelink_item.unwrap().get_value());
        Ok(Some(value))
    }
/*
    fn get_claims<'a>(&'a self) ->
        WikiResult<capnp::traits::ListIter<::capnp::struct_list::Reader<'a, MapEntry::Owned>, MapEntry::Reader<'a>>> {
        let claims = try!(try!(try!(self.as_entity_reader()).get_claims()).get_entries());
        Ok(claims.iter())
    }
*/

    fn get_claim<'a>(&'a self, prop:EntityRef) ->
        WikiResult<Option<::capnp::struct_list::Reader<Claim::Owned>>> {
        let claims = try!(try!(self.as_entity_reader()).get_claims());
        let prop_as_string:String = prop.get_id();
        let claim_entry = try!(Self::lookup(claims, &*prop_as_string));
        if claim_entry.is_none() {
            return Ok(None)
        }
        let value = try!(claim_entry.unwrap().get_value());
        Ok(Some(value))
    }

    fn get_relations(&self) ->
            WikiResult<Box<Iterator<Item=(EntityRef,EntityRef)>+Send>> {
        let mut result = vec!();
        for claim in try!(try!(try!(self.as_entity_reader()).get_claims()).get_entries()).iter() {
            for value in try!(claim.get_value()).iter() {
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


impl EntityHelpers for EntityMessage {
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
    type Item = WikiResult<EntityMessage>;

    fn next(&mut self) -> Option<WikiResult<EntityMessage>> {
        match capnp::serialize_packed::read_message(&mut self.stream, self.options) {
            Ok(msg) => { Some(Ok(EntityMessage { message:msg })) },
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
