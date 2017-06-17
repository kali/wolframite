extern crate wolframite;

use wolframite::helpers;
use std::io;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ref lang = args[1];
    let ref date = args[2];

    let mut source = helpers::bzcat(lang, date).unwrap();
    io::copy(&mut source, &mut io::stdout()).unwrap();
}
