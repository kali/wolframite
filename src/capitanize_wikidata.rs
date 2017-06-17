use std::{ io, fs, path };
use std::io::prelude::*;

use serde_json;

use flate2;
use flate2::write::GzEncoder;

use capnp::serialize_packed;
use capnp::{ traits, text };
use capnp::message::{ Allocator, Builder};

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

use {WikiResult, WikiError};

macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

pub fn process<R:io::Read>(input:R, output:&path::Path) -> WikiResult<()> {
    let input = io::BufReader::new(input);
    let mut path = path::PathBuf::new();
//    let mut part:Option<SnappyFramedEncoder<_>> = None; //open_one(counter);
    let mut part:Option<GzEncoder<_>> = None;
    let mut part_counter = 0;
    let mut counter = 0;
    for line in input.lines() {
        let mut line = try!(line);
        if part.is_none() || (counter % 1000 == 0 &&
                try!(fs::metadata(&path)).len() > 250_000_000) {
            path = path::PathBuf::from(format!("{}-part-{:05}.cap.gz",
                output.to_str().unwrap(), part_counter));
            part_counter+=1;
            part =
                //Some(SnappyFramedEncoder::new(fs::File::create(path.as_os_str()).unwrap()).unwrap());
                Some(GzEncoder::new(fs::File::create(path.as_os_str()).unwrap(), flate2::Compression::Default));
        }
        if line == "[" || line == "]" {
        } else {
            counter += 1;
            if line.pop() == Some('}') {
                line.push('}')
            }
            let value:serde_json::value::Value = serde_json::from_str(&*line).map_err(|e| {
                println_stderr!("##### JSON ERROR @{} #####", counter);
                println_stderr!("{:?}", e);
                let _ = io::copy(&mut io::Cursor::new(line.as_bytes()), &mut io::stderr());
                e
            })?;
            let mut message = Builder::new_default();
            {
                let job = consume_item(&value, &mut message);
                try!(job.or_else(|e| {
                    println!("error handler");
                    println_stderr!("##### ERROR @{} #####", counter);
                    println_stderr!("{:?}", e);
                    try!(serde_json::ser::to_writer_pretty(&mut io::stderr(),&value));
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

fn consume_item<T:Allocator>(value:&serde_json::value::Value, message:&mut Builder<T>) -> WikiResult<()> {
    let mut entity = message.init_root::<Entity::Builder>();
    let id = try!(value.pointer("/id").ok_or("id expected"));
    entity.set_id(try!(id.as_str().ok_or("id is expected to be a string")));
    let typ = try!(value.pointer("/type").ok_or("type field expected"));
    match typ.as_str() {
        Some("item") => entity.set_type(EntityType::Item),
        Some("property") => entity.set_type(EntityType::Property),
        _ => Err(format!("type expected to be a string (\"item\", or \"property\") got: {:?}", typ))?,
    };
    try!(value.pointer("/labels").map(|labels| {
        build_map_to_mono(labels, entity.borrow().init_labels())
    }).unwrap_or(Ok(())));
    try!(value.pointer("/descriptions").map(|vs| {
        build_map_to_mono(vs, entity.borrow().init_descriptions())
    }).unwrap_or(Ok(())));
    try!(value.pointer("/sitelinks").map(|vs| {
        build_map(vs, entity.borrow().init_sitelinks(), |json,entry|
            build_sitelink(json, try!(entry.get_value()))
        )
    }).unwrap_or(Ok(())));
    try!(value.pointer("/aliases").map(|vs| {
        build_map(vs, entity.borrow().init_aliases(), |v,e| {
            let array = try!(v.as_array().ok_or("expect an array"));
            let mut list_builder = e.initn_value(array.len() as u32);
            for (i,item) in array.iter().enumerate() {
                try!(build_monolingual_text(item, list_builder.borrow().get(i as u32)));
            };
            Ok( () )
        })
    }).unwrap_or(Ok(())));
    try!(value.pointer("/claims").map(|vs| {
        build_map(vs, entity.borrow().init_claims(), |v,e| {
            let array = try!(v.as_array().ok_or("expect an array"));
            let mut list_builder = e.initn_value(array.len() as u32);
            for (i,item) in array.iter().enumerate() {
                try!(build_claim(item, list_builder.borrow().get(i as u32)));
            };
            Ok( () )
        })
    }).unwrap_or(Ok(())));
    Ok( () )
}

fn build_map_to_mono(map_of_maps:&serde_json::value::Value,
        map:Map::Builder<text::Owned,MongolingualText::Owned>) -> WikiResult<()> {
    build_map(map_of_maps, map, |json, entry| build_monolingual_text(json, try!(entry.get_value())))
}

fn build_map<F,V>(map_of_maps:&serde_json::value::Value, mut map:Map::Builder<text::Owned,V>, f:F) -> WikiResult<()>
        where   V: for<'x> traits::Owned<'x>,
                F: Fn(&serde_json::value::Value, MapEntry::Builder<text::Owned,V>) -> WikiResult<()>
    {
    let map_of_maps = try!(map_of_maps.as_object().ok_or("map of monolingual text is expected as json object"));
    map.borrow().init_entries(map_of_maps.len() as u32);
    let mut entries = try!(map.borrow().get_entries());
    {
        for (i, (l, v)) in map_of_maps.iter().enumerate() {
            let mut entry = entries.borrow().get(i as u32);
            entry.borrow().set_key(&**l).unwrap();
            try!(f(v, entry));
        }
    }
    Ok( () )
}

fn build_monolingual_text(json:&serde_json::value::Value, mut builder:MongolingualText::Builder) -> WikiResult<()> {
    json.pointer("/language").and_then(|v| v.as_str()).map(|v| builder.set_language(v));
    if json.pointer("/removed").is_some() {
        builder.set_removed( () );
    } else {
        json.pointer("/value").and_then(|v| v.as_str()).map(|v| builder.set_value(v));
    }
    Ok( () )
}

fn build_sitelink(json:&serde_json::value::Value, mut builder:SiteLink::Builder) -> WikiResult<()> {
    json.pointer("/site").and_then(|v| v.as_str()).map(|v| builder.set_site(v));
    json.pointer("/title").and_then(|v| v.as_str()).map(|v| builder.set_title(v));
    Ok( () )
}

fn build_claim(json:&serde_json::value::Value, mut builder:Claim::Builder) -> WikiResult<()> {
    json.pointer("/id").and_then(|v| v.as_str()).map(|v| builder.set_id(v));
    match json.pointer("/type").and_then(|v| v.as_str()) {
        Some("statement") => builder.set_type(Claim::Type::Statement),
        Some("claim") => builder.set_type(Claim::Type::Claim),
        Some(e) =>
            return Err(WikiError::from(format!("unexpected value for claim type {}", e))),
        _ => ()
    }
    match json.pointer("/rank").and_then(|v| v.as_str()) {
        Some("preferred") => builder.set_rank(Claim::Rank::Preferred),
        Some("normal") => builder.set_rank(Claim::Rank::Normal),
        Some("deprecated") => builder.set_rank(Claim::Rank::Deprecated),
        Some(e) =>
            return Err(WikiError::from(format!("unexpected value for claim rank {}", e))),
        _ => ()
    }
    let snak = try!(json.pointer("/mainsnak").ok_or("I do expect a mainsnak."));
    try!(build_snak(snak, builder.init_mainsnak()));
    Ok( () )
}

fn build_snak(json:&serde_json::value::Value, mut builder:Snak::Builder) -> WikiResult<()> {
    json.pointer("/property").and_then(|v| v.as_str()).map(|v| builder.set_property(v));
    json.pointer("/datatype").and_then(|v| v.as_str()).map(|v| builder.set_datatype(v));
    let snaktype = try!(json.pointer("/snaktype").and_then(|v|v.as_str()).ok_or("expect a snaktype"));
    match snaktype {
        "value" => {
            let value = try!(json.pointer("/datavalue").ok_or("no datatype in snak"));
            try!(build_data_value(value, builder.init_value()));
        },
        "novalue" => builder.set_novalue(()),
        "somevalue" => builder.set_somevalue(()),
        e => return Err(WikiError::from(format!("unexpected snaktype {}", e))),
    }
    Ok( () )
}

fn build_data_value(json:&serde_json::value::Value, mut builder:DataValue::Builder) -> WikiResult<()> {
    let t = try!(json.pointer("/type").and_then(|v| v.as_str()).ok_or("expect a type"));
    let v = try!(json.pointer("/value").ok_or("expect a value"));
    match t {
        "string" => builder.set_string(
            try!(v.as_str().ok_or("expected a string"))),
        "wikibase-entityid" =>
            try!(build_wikibase_entity_ref(v, builder.init_wikibaseentityid())),
        "time" => try!(build_time(v, builder.init_time())),
        "quantity" => try!(build_quantity(v, builder.init_quantity())),
        "globecoordinate" => try!(build_globecoordinate(v, builder.init_globecoordinate())),
        "monolingualtext" => try!(build_monolingual_text(v, builder.init_monolingualtext())),
        e => {
            Err(format!("unexpected datavalue type:{} {:?}", e, json))?
        }
    }
    Ok( () )
}

fn build_wikibase_entity_ref(json:&serde_json::value::Value, mut builder:WikibaseEntityRef::Builder) -> WikiResult<()> {
    let typ = try!(json.pointer("/entity-type").and_then(|v| v.as_str()).ok_or("expect an entity-type"));
    builder.set_type(try!(build_entity_type(typ)));
    builder.set_id(try!(
        json.pointer("/numeric-id").and_then(|v| v.as_u64()).ok_or("expect a numeric id")) as u32);
    Ok( () )
}

fn build_time(json:&serde_json::value::Value, mut builder:Time::Builder) -> WikiResult<()> {
    json.pointer("/time").and_then(|v| v.as_str()).map(|v| builder.set_time(v));
    json.pointer("/calendarmodel").and_then(|v| v.as_str()).map(|v| builder.set_calendarmodel(v));
    json.pointer("/precision").and_then(|v| v.as_u64()).map(|v| builder.set_precision(v as u8));
    json.pointer("/timezone").and_then(|v| v.as_u64()).map(|v| builder.set_timezone(v as i16));
    json.pointer("/before").and_then(|v| v.as_u64()).map(|v| builder.set_before(v as u64));
    json.pointer("/after").and_then(|v| v.as_u64()).map(|v| builder.set_after(v as u64));
    Ok( () )
}

fn build_quantity(json:&serde_json::value::Value, mut builder:Quantity::Builder) -> WikiResult<()> {
    json.pointer("/amount").and_then(|v| v.as_f64()).map(|v| builder.set_amount(v));
    json.pointer("/lowerBound").and_then(|v| v.as_f64()).map(|v| builder.set_lower_bound(v));
    json.pointer("/upperBound").and_then(|v| v.as_f64()).map(|v| builder.set_upper_bound(v));
    json.pointer("/unit").and_then(|v| v.as_str()).map(|v| builder.set_unit(v));
    Ok( () )
}

fn build_globecoordinate(json:&serde_json::value::Value, mut builder:GlobeCoordinate::Builder)
        -> WikiResult<()> {
    json.pointer("/latitude").and_then(|v| v.as_f64()).map(|v| builder.set_latitude(v));
    json.pointer("/longitude").and_then(|v| v.as_f64()).map(|v| builder.set_longitude(v));
    json.pointer("/altitude").and_then(|v| v.as_f64()).map(|v| builder.set_altitude(v));
    json.pointer("/precision").and_then(|v| v.as_f64()).map(|v| builder.set_precision(v));
    json.pointer("/globe").and_then(|v| v.as_str()).map(|v| builder.set_globe(v));
    Ok( () )
}

fn build_entity_type(typ:&str) -> WikiResult<EntityType> {
    match typ {
        "item" => Ok(EntityType::Item),
        "property" => Ok(EntityType::Property),
        _ => Err(format!("type expected to be a string (\"item\", or \"property\") got: {:?}", typ))?,
    }
}
