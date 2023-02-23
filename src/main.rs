#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]


#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::{
    time::{Instant},
    fs::{self, File, read}, 
    io::{self, prelude::*, Error, ErrorKind}, 
    path::PathBuf, 
    collections::{BTreeMap}
};

struct CustomCollectError(Vec<Error>);

impl CustomCollectError {
    fn new() -> Self {
        CustomCollectError(vec![])
    }
    fn push(&mut self, error: String) {
        self.0.push(Error::new(ErrorKind::Other, error));
    }
    fn r#return(self) -> Result<(), Vec<Error>> {
        if self.0.len() > 0 { return Err(self.0); }
        Ok(())
    }
}

fn create_merge_file(files: &BTreeMap<PathBuf, String>, mut path: String, merge_file_name: &str) -> Result<(), Error> {
    let now = Instant::now();

    path.push_str(merge_file_name); 
    let mut new_file = File::create(path)?;
    
    for (iter_files, (path, mut buff)) in files.iter().enumerate() {
        let first_line = buff.find("\r\n").unwrap();

        if iter_files == 0 {
            write!(new_file, "{}\r\n", buff);
        }else{
            write!(new_file, "{}\r\n", buff.get(first_line..).unwrap());
        }
    }
    println!("create_merged_file : {:.2?}", now.elapsed());
    Ok(())
}


fn check_difference(files: &BTreeMap<PathBuf, String>, explode_line: &char) -> Result<(), Vec<Error>> {
    let now = Instant::now();

    let mut head_for_diff = String::new();
    let (path_base, _) = files.first_key_value().expect("Path not found");
    let mut custom_result= CustomCollectError::new();

    for (iter_files, (path, mut buff)) in files.iter().enumerate() {
        let first_line_index = buff.find("\r\n").unwrap();
        let first_line =  buff.get(0..first_line_index).unwrap().trim();

        if iter_files == 0 {
            head_for_diff = first_line.to_string();
            continue;
        }
        let (head_for_diff_count, first_line_count) = (head_for_diff.split(*explode_line).count(), first_line.split(*explode_line).count());
        if head_for_diff_count == first_line_count {
            for (column_one, column_two) in head_for_diff.split_terminator(*explode_line).zip(first_line.split_terminator(*explode_line)) {
                if column_one == column_two {continue};
                // Result::Err(123);
                // println!("Заголовки в файлах: {:?} и {:?} отличаются. Различия {:?} с {:?}", path_base ,path, column_one, column_two);
                custom_result.push(format!("Заголовки в файлах: `{:?}` и {:?} отличаются. Различия {} с {}", path, path_base , column_one, column_two));
            }
            continue;
        }
        custom_result.push(format!("В файлах: `{:?}` и `{:?}` количество заглавных столбцов отличается. В первом случае их {}, во втором {} ", path, path_base , head_for_diff_count, first_line_count));
    
        for (line_index, line) in buff.split_terminator("\r\n").enumerate() {
            // println!("111");
            if line_index == 0 { continue; }
            if line.split(*explode_line).count() == head_for_diff_count { continue; }
            custom_result.push(format!("В файле: `{:?}` и `{:?}` количество столбцов отличается. На линии {} ", path, path_base , line_index + 1));
        }
    }
    println!("create_merged_file : {:.2?}", now.elapsed());
    custom_result.r#return()

}

fn files_to_string(files: BTreeMap<PathBuf, File>) -> BTreeMap<PathBuf, String> {
    // let now = Instant::now();
    let mut btree = BTreeMap::new();
    for (iter_files, (path, mut file)) in files.into_iter().enumerate() {
        let mut buff = String::new();
        
        file.read_to_string(&mut buff).expect("1111");

        btree.insert(path, buff);
    }
    // println!("create_merged_file : {:.2?}", now.elapsed());
    btree
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
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let now = Instant::now();
    let explode_line = ',';
    let path = "F:/temp/csv/csv/".to_string();
    let merge_file_name = "merge.csv";

    let mut buff = get_files_path_in_dir(&path, merge_file_name)
        .and_then(move | vec| Ok(open_files(vec)))
        .and_then(move | btree| Ok(files_to_string(btree)))
        .expect("222");

    let res = check_difference(&buff, &explode_line);

    match res {
        Err(vec) => {
            vec.into_iter().for_each(|err| println!("{}", err.into_inner().unwrap()));
        }
        _ => ()
    }
    // println!("{:?}", res);
    
    create_merge_file(&buff, path, &merge_file_name);



}