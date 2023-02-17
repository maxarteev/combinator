#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]

use std::{
    fs::{File, read}, 
    io::{prelude::*, Error}
};

fn parse_string_from_file(str: String) {
    for line in str.split_terminator("\r\n") {
        println!("{:?}", line);
    }
}

fn read_file(file: &mut File) -> Result<String, Error> {
    let mut buff = String::new() ;
    file.read_to_string(&mut buff)?;
    Ok(buff)
}
fn open_file(path: &str) -> Result<File, Error> {
    File::open(path)
}
fn open_files_in_dir(dir: String) {
    
}


fn main() {
    let mut vec_files: Vec<File> = Vec::new();

    let mut file = open_file("F:/temp/csv/csv/1.csv").expect("file not open");
    let string_from_file = read_file(&mut file).expect("can't read file");
    parse_string_from_file(string_from_file);

}
