#![allow(dead_code)]
extern crate capnp;

pub mod wiki_capnp {
    include!(concat!(env!("OUT_DIR"), "/wiki_capnp.rs"));
}
