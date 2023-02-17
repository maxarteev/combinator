#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]

use std::{
    fs::File, 
    io::prelude::*
};


fn open_file() {
    let mut file = File::open("F:/temp/csv/csv/1.csv");
    
    println!("{:?}", file);
}


fn main() {
    open_file()
}
