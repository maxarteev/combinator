#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]


#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::{
    time::{Instant},
    fs::{self, File, read}, 
    io::{self, prelude::*, Error}, path::PathBuf, collections::{HashMap, hash_map, BTreeMap}
};

fn create_merge_file(files: &BTreeMap<PathBuf, String>, mut path: String, merge_file_name: &str) -> Result<(), Error>{
    let now = Instant::now();
    
    path.push_str(merge_file_name); 
    let mut new_file = File::create(path)?;
    
    for (iter_files, (path, mut buff)) in files.iter().enumerate() {
        
        // let mut buff = String::new();
        
        // file.read_to_string(&mut buff).expect("1111");

        let first_line = buff.find("\r\n").unwrap();
        // println!("buff append {:?}", gg.unwrap().get(0..));


        if iter_files == 0 {
            write!(new_file, "{}\r\n", buff);
        }else{
            write!(new_file, "{}\r\n", buff.get(first_line..).unwrap());
        }
        
        // println!("buff prepend {:?}", buff);
        // println!("{:?}", buff);
        // for (line, str) in buff.split_terminator("\r\n").enumerate() {
        //     // println!("STR {:?}", str);
            
        //     if iter_files == 0 && line == 0 {
        //         // write!(new_file, "{}\r\n", str);
        //         // new_buff.push_str(str);
        //         // new_buff.push_str("\r\n");
                
        //         // buff.replace_range(0..str.clone().len(), "");
        //         continue;
        //     }else if line != 0 {
        //         // write!(new_file, "{}\r\n", str);
        //         // new_buff.push_str(str);
        //         // new_buff.push_str("\r\n");
        //     }
        // }
        // write!(new_file, "{}\r\n", buff);
        // println!("buff append {:?}", new_buff);
    }
    println!("create_merged_file : {:.2?}", now.elapsed());
    Ok(())
}

fn check_difference(files: &BTreeMap<PathBuf, String>, explode_line: &char) -> Result<(), Error> {
    let mut head_for_diff = String::new();
    
    'outer: for (iter_files, (path, mut buff)) in files.iter().enumerate() {
        // let mut buff = String::new();
        // let mut buff2 = String::new();
        // // println!("1 {:?}", file);
        // // file.
        // file.read_to_string(&mut buff).expect("1111");
        // file.read_to_string(&mut buff2).expect("1111");
        // println!("1 {:?}", buff);
        // println!("2 {:?}", buff2);
        let first_line_index = buff.find("\r\n").unwrap();
        // let first_line_index = buff.find("\r\n").unwrap();

        let first_line =  buff.get(0..first_line_index).unwrap().trim();

        if iter_files == 0 {
            head_for_diff = first_line.to_string();
            continue;
        }
        if head_for_diff.split(*explode_line).count() == first_line.split(*explode_line).count() {
            for (column_one, column_two) in head_for_diff.split_terminator(*explode_line).zip(first_line.split_terminator(*explode_line)) {
                if column_one == column_two {continue};
                
                let (path_base, _) = files.first_key_value().unwrap();
                println!("Заголовки в файлах: {:?} и {:?} отличаются. Различия {:?} с {:?}", path_base ,path, column_one, column_two);
            }
            continue;
        }
        
        // println!("Заголовки в файлах: {:?}, {:?} отличаются. ", head_for_diff.split(*explode_line).count());
        // println!("column_two : {:?}", first_line.split(*explode_line).count());

        // for (column_one, column_two) in head_for_diff.split_terminator(*explode_line).zip(first_line.split_terminator(*explode_line)) {
        //     println!("column_one : {:?}, column_two : {:?}", column_one, column_two);
        // }

        // println!("не равно : {:?} и {:?}", head_for_diff, first_line);
        // println!("head_for_diff : {:?}", head_for_diff);
        
        

        // 'inner: for (line, str) in buff.split_terminator("\r\n").enumerate() {
        //     if iter_files == 0  && line == 0 {
        //         head_for_diff = str.clone().trim().to_string();
        //         continue;
        //     }
    // if line != 0 {
    //     if head_for_diff != str.trim() {
    //             if line != 0 {
    //                 code 1+1;
    //                     if(!= )
    //                     return
    //                 continue;
    //             }
    //             if head_for_diff != str.trim() {
    //                 code();
    //             }

    //                 if something{
    //                     awdlkawdjlkasd
    //                 }

    //         if line == 0 && head_for_diff != str.trim() {
    //             // тута надо доделать проверку не на строку, а на каждый столбец по отдельности, чтобы конкретно символы совпадали
    //             // for word in head_for_diff.split_terminator(*explode_line) {
                    
    //             // }
    //             println!("Заголовки не совпадают в файле {:?} на строке {:?}. Ожидается заголовок {:?}, а найден: {:?}", path, line, head_for_diff, str);
    //             break 'outer;
    //         }

            // тут было бы неплохо проверку на кол-во строк и т.д.
        // }
        // println!("outer {:?}", head_for_diff);
    }

    // let hash = files.values().next().unwrap().first();
    // for (path, vec) in files.iter() {
    //     if vec.first() != hash {
    //         println!("Заголовки не совпадають! {:?} and {:?}", vec.first(), hash);
    //     }
    // }
    Ok(())

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

    let mut buff = 
        get_files_path_in_dir(&path, merge_file_name)
        .and_then(move |vec| Ok(open_files(vec)))
        .and_then(move |btree| Ok(files_to_string(btree)))
        .expect("222");

    check_difference(&buff, &explode_line);
    
    create_merge_file(&buff, path, &merge_file_name);



}