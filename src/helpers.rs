
use std::io;
use std::io::prelude::*;
use std::fs;

use bzip2::reader::BzDecompressor;

use xml::reader::EventReader;
use xml::reader::events::*;

use WikiError;

pub fn data_dir_for(state:&str, lang:&str, date:&str) -> String {
    format!("data/{}/{}/{}", state, lang, date)
}

fn indent(size: usize) -> String {
    const INDENT: &'static str = "    ";
    (0..size).map(|_| INDENT)
             .fold(String::with_capacity(size*INDENT.len()), |r, s| r + s)
}

pub fn pages<R:Read>(read:R) -> Result<(),WikiError> {
    let mut parser = EventReader::new(read);
    let mut depth = 0;
    for e in parser.events() {
        match e {
            XmlEvent::StartElement { name, .. } => {
                println!("{}+{}", indent(depth), name.local_name);
                depth+=1;
            }
            XmlEvent::EndElement { name } => {
                depth -= 1;
                println!("{}-{}", indent(depth), name.local_name);
            }
            XmlEvent::Error(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }
    Ok( () )
}

pub struct ReadChain<T:Read> {
    position: usize,
    inner: Vec<T>
}

impl<T:Read> ReadChain<T> {
    fn new(readers:Vec<T>) -> ReadChain<T> {
        return ReadChain{ position:0, inner:readers}
    }
}

impl<T:Read> Read for ReadChain<T> {

    fn read(&mut self, buf: &mut [u8]) -> Result<usize,io::Error> {
        while self.position < self.inner.len() {
            let sent = try!(self.inner[self.position].read(buf));
            if(sent > 0) {
                return Ok(sent);
            }
            self.position += 1;
        }
        Ok(0)
    }
}

pub fn cat(lang:&str, date:&str) -> Result<ReadChain<BzDecompressor<fs::File>>, WikiError> {
    let glob = data_dir_for("download", lang, date) + "/*.bz2";
    let decompressors:Result<Vec<BzDecompressor<fs::File>>,WikiError> =
        try!(::glob::glob(&glob)).map(|entry| {
            let file = try!(fs::File::open(try!(entry)));
            Ok(BzDecompressor::new(file))
    }).collect();
    let decompressors = try!(decompressors);
    Ok(ReadChain::new(decompressors))
}
