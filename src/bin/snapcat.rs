extern crate wiki;

use wiki::helpers::*;
use std::io;

fn main() {
    let args:Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];

    let mut source = wiki::helpers::snappycat(lang, date).unwrap();
    io::copy(&mut source, &mut io::stdout()).unwrap();
}

