extern crate wiki;

use wiki::helpers::*;
use std::io;

fn main() {
    pages(io::stdin()).unwrap();
}
