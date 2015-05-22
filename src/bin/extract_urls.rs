extern crate wiki;

use wiki::helpers::*;
use std::io;

fn main() {
    for page in pages_from_xml(io::stdin()).unwrap() {
        println!("{:?}", page);
    }
}
