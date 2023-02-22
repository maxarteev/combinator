#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]

use std::{
    time::{Instant},
    fs::{self, File, read}, 
    io::{self, prelude::*, Error}, path::PathBuf, collections::{HashMap, hash_map, BTreeMap}
};
fn create_merged_file(vec: Vec<String>, mut path: String, merge_file_name: &str) -> Result<(), Error> {
    let now = Instant::now();
    path.push_str(merge_file_name);
    // println!("{:?}", path);
    let mut file = File::create(path)?;
    for string in vec.into_iter() {
        write!(file, "{}\r\n", string);
    }
    println!("create_merged_file : {:.2?}", now.elapsed());
    Ok(())
}

fn merge_string(hash_map: BTreeMap<PathBuf, Vec<String>>, explode_line: char) -> Vec<String> {
    let now = Instant::now();
    let main_key = hash_map.keys().next().unwrap().clone();
    let mut merge_vec: Vec<String> = Vec::new();
    
    for (path, vec) in hash_map.into_iter() {
        for (iter,strq) in vec.into_iter().enumerate() {
            if merge_vec.len() == 0 && iter == 0 {
                merge_vec.push(strq.clone())
            }
            if iter != 0 {
                merge_vec.push(strq);
            }
        }
    }
    let elapsed = now.elapsed();
    println!("merge_string : {:.2?}", elapsed);
    merge_vec
}

fn check_equaluty_header(hash_map: &BTreeMap<PathBuf, Vec<String>>) {
    let hash = hash_map.values().next().unwrap().first();
    for (path, vec) in hash_map.iter() {
        if vec.first() != hash {
            println!("Заголовки не совпадають! {:?} and {:?}", vec.first(), hash);
        }
    }
}

fn parse_string_from_files(hash_files:  BTreeMap<PathBuf, String>) -> BTreeMap<PathBuf, Vec<String>> {
    let now = Instant::now();
    
    let mut strings_in_files: BTreeMap<PathBuf, Vec<String>> = BTreeMap::new();
    for (path, str) in hash_files.into_iter(){
        let mut vec = Vec::new();
        for line in str.split_terminator("\r\n").into_iter() {
            // println!("{:?}", line);
            vec.push(line.to_string());
        }
        strings_in_files.insert(path, vec);
    }
    let elapsed = now.elapsed();
    println!("parse_string_from_files : {:.2?}", elapsed);
    strings_in_files
}

fn read_files(files: BTreeMap<PathBuf, File>) -> BTreeMap<PathBuf, String> {
    let mut strings_in_files: BTreeMap<PathBuf, String> = BTreeMap::new();
    for (path, mut file) in files.into_iter() {
        let mut buff = String::new();
        file.read_to_string(&mut buff).expect("1111");
        strings_in_files.insert(path, buff);
    }
    // println!("{:?}", strings_in_files);
    strings_in_files
}

fn open_files(vec: Vec<PathBuf>) -> BTreeMap<PathBuf, File> {
    let mut open_files = BTreeMap::new();
    for (iter, path) in vec.iter().enumerate() {
        open_files.insert(path.clone(), File::open(path).expect("wada"));
    }
    open_files
}

fn get_files_path_in_dir(dir: &String, escape_file: &str) -> Result<Vec<PathBuf>, Error>{
    let mut entries = fs::read_dir(dir)?
    .filter(|res| 
        match res {
            Ok(e) if e.file_name() != escape_file => true,
            _ => false
        }
    )
    .map(|res| res.map(|e| e.path()))
    .collect::<Result<Vec<_>, io::Error>>()?;
    Ok(entries)
}

fn main() {
    let now = Instant::now();
    let explode_line = ',';
    let path = "F:/temp/csv/csv/".to_string();
    let merge_file_name = "merge.csv";

    let mut vec_files = 
        get_files_path_in_dir(&path, merge_file_name)
            .and_then(move |vec| Ok(open_files(vec)))
            .and_then(|files| Ok(read_files(files)))
            .and_then(|hash_files| Ok(parse_string_from_files(hash_files)))
            .expect("222");
    check_equaluty_header(&vec_files);
    let vec = merge_string(vec_files, explode_line);
    create_merged_file(vec, path, merge_file_name);

    let elapsed = now.elapsed();
    println!("Main : {:.2?}", elapsed);
    // println!("{:?}", vec_files);
    
    //let string_from_file = read_file(&mut file).expect("can't read file");
    // parse_string_from_file(string_from_file);

}
