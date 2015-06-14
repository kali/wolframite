use std::io;
use std::io::prelude::*;
use std::error::Error;

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

pub type WikiResult<T> = Result<T,WikiError>;

pub struct MessageAndEntity {
    message:capnp::serialize::OwnedSpaceMessageReader
}

impl MessageAndEntity {
    pub fn as_entity_reader(&self) -> WikiResult<Entity::Reader> {
        self.message.get_root().map_err( |e| WikiError::from(e))
    }
}

pub struct EntityReader<R:io::Read> {
    options: capnp::message::ReaderOptions,
    stream: io::BufReader<R>,
}

impl <R:io::Read> EntityReader<R> {
    pub fn new(r:R) -> EntityReader<R> {
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
