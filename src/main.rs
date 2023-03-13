#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{self, prelude::*, Error, ErrorKind},
    path::PathBuf,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Instant, Duration},
};

#[derive(Debug)]
struct CustomCollectError(Vec<Error>);

impl CustomCollectError {
    fn new() -> Self {
        CustomCollectError(vec![])
    }
    fn push(&mut self, error: String) {
        self.0.push(Error::new(ErrorKind::Other, error));
    }
    fn print_err(&self) {
        for err in self.0.iter() {
            println!("{}", err);
        }
    }
}
fn check_difference_btree(
    path: Arc<PathBuf>,
    buff: Arc<String>,
    head_for_diff: Arc<String>,
    path_base: &Arc<PathBuf>,
    explode_line: &char,
) {
    let explode_line = *explode_line;
    let mut custom_result = CustomCollectError::new();
    let first_line_index = buff.find("\r\n").unwrap();
    let first_line = buff.get(0..first_line_index).unwrap().trim();

    let (head_for_diff_count, first_line_count) = (
        head_for_diff.split(explode_line).count(),
        first_line.split(explode_line).count(),
    );
    if head_for_diff_count == first_line_count {
        for (column_one, column_two) in head_for_diff
            .split_terminator(explode_line)
            .zip(first_line.split_terminator(explode_line))
        {
            if column_one != column_two {
                custom_result.push(format!(
                    "Заголовки в файлах: `{:?}` и {:?} отличаются. Различия {} с {}",
                    path, path_base, column_one, column_two
                ));
            };
            
        }
    } else {
        custom_result.push(
            format!("В файлах: `{:?}` и `{:?}` количество заглавных столбцов отличается. В первом случае их {}, во втором {} ", 
            path, path_base , head_for_diff_count, first_line_count
        ));
    }
    for (line_index, line) in buff.split_terminator("\r\n").enumerate() {
        // 
        if line_index != 0 {
            if line.split(explode_line).count() != head_for_diff_count {
                // println!("path {:?}", path);
                // println!("path {:?}", path_base);
                // println!("line.split(explode_line).count() {:?}", line.split(explode_line).count());
                // println!("head_for_diff_count {:?}", head_for_diff_count);
                custom_result.push(format!(
                    "В файле: `{:?}` и `{:?}` количество столбцов отличается. На линии {} ",
                    path,
                    path_base,
                    line_index + 1
                ));
            }
        }
    }
    custom_result.print_err();
}

struct ThreadPool {
    threads: Vec<Option<thread::JoinHandle<()>>>,
    sender: Option<Sender<Box<dyn FnOnce() + Send + 'static>>>,
}
impl Drop for ThreadPool {
    fn drop(&mut self) {
        let now = Instant::now();
        drop(self.sender.take());
        for worker in &mut self.threads {
            if let Some(thread) = worker.take() {
                thread.join().unwrap();
            }
        }
        println!("thread : {:.2?}", now.elapsed());
        thread::sleep(Duration::from_millis(4000));
    }
}
impl ThreadPool {
    fn new(size: usize) -> ThreadPool {
        let mut threads = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel::<Box<dyn FnOnce() + Send + 'static>>();
        let rec_arc = Arc::new(Mutex::new(receiver));

        for _ in 0..size {
            let rec = Arc::clone(&rec_arc);

            threads.push(
                Some(
                    thread::spawn(move || loop {
                        let message = rec.lock().unwrap().recv();
                        match message {
                            Ok(closure) => {
                                closure();
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    })
                )
            );
        }
        ThreadPool {
            threads,
            sender: Some(sender),
        }
    }
}

fn create_merge_file_thread(
    file_for_result: Arc<Mutex<File>>,
    buff: Arc<String>,
) {
    let first_line = buff.find("\r\n").unwrap();
    write!(file_for_result.lock().unwrap(), "{}\r\n", buff.get(first_line..).unwrap()).unwrap();
}

fn create_merge_file(
    files: &BTreeMap<Arc<PathBuf>, Arc<String>>,
    mut path: PathBuf,
    merge_file_name: &str,
    _pool_size: usize,
) -> Result<(), Error> {
    let mut new_file = File::create(path)?;

    for (iter_files, (_, buff)) in files.iter().enumerate() {
        
        let first_line = buff.find("\r\n").unwrap();

        if iter_files == 0 {
            write!(new_file, "{}\r\n", buff).unwrap();
        } else {
            write!(new_file, "{}\r\n", buff.get(first_line..).unwrap()).unwrap();
        }
    }
    Ok(())
}

fn check_difference_multi_threads(
    files: &BTreeMap<Arc<PathBuf>, Arc<String>>,
    explode_line: &char,
    pool_size: usize,
) -> Result<(), Error> {
    let mut pool = ThreadPool::new(pool_size);

    let (path_base_arc, string_base_arc) = files.first_key_value().expect("Path not found");
    let string_base = Arc::clone(string_base_arc);
    let base_line_index = string_base.find("\r\n").unwrap();
    let base_line = string_base.get(0..base_line_index).unwrap().trim();
    let head_for_diff_arc = Arc::new(base_line.to_string());
    let sender = pool.sender.take().unwrap();

    for (path_arc, buff_arc) in files.iter() {
        let path_base = Arc::clone(path_base_arc);
        let buff = Arc::clone(buff_arc);
        let path = Arc::clone(path_arc);
        let explode_line = *explode_line;
        let head_for_diff = Arc::clone(&head_for_diff_arc);

        let closure = move || {
            let mut custom_result = CustomCollectError::new();
            let first_line_index = buff.find("\r\n").unwrap();
            let first_line = buff.get(0..first_line_index).unwrap().trim();

            let (head_for_diff_count, first_line_count) = (
                head_for_diff.split(explode_line).count(),
                first_line.split(explode_line).count(),
            );
            if head_for_diff_count == first_line_count {
                for (column_one, column_two) in head_for_diff
                    .split_terminator(explode_line)
                    .zip(first_line.split_terminator(explode_line))
                {
                    if column_one == column_two {
                        continue;
                    };
                    custom_result.push(format!(
                        "Заголовки в файлах: `{:?}` и {:?} отличаются. Различия {} с {}",
                        path, path_base, column_one, column_two
                    ));
                }
            } else {
                custom_result.push(
                    format!("В файлах: `{:?}` и `{:?}` количество заглавных столбцов отличается. В первом случае их {}, во втором {} ", 
                    path, path_base , head_for_diff_count, first_line_count
                ));
            }

            for (line_index, line) in buff.split_terminator("\r\n").enumerate() {
                if line_index == 0 {
                    continue;
                }
                if line.split(explode_line).count() == head_for_diff_count {
                    continue;
                }
                custom_result.push(format!(
                    "В файле: `{:?}` и `{:?}` количество столбцов отличается. На линии {} ",
                    path,
                    path_base,
                    line_index + 1
                ));
            }
            custom_result.print_err();
        };
        sender.send(Box::new(closure)).unwrap();
    }
    Ok(())
}

fn files_to_string(files: BTreeMap<PathBuf, File>) -> BTreeMap<Arc<PathBuf>, Arc<String>> {
    let mut btree = BTreeMap::new();
    for (path, mut file) in files.into_iter() {
        let mut buff = String::new();
        file.read_to_string(&mut buff).expect("1111");
        btree.insert(Arc::new(path), Arc::new(buff));
    }
    btree
}

fn open_files(vec: Vec<PathBuf>) -> BTreeMap<PathBuf, File> {
    let mut open_files = BTreeMap::new();
    for path in vec.into_iter() {
        open_files.insert(path.clone(), File::open(path).expect("wada"));
    }
    open_files
}
fn open_files_for_threads(path: PathBuf) -> (PathBuf, File) {
    (path.clone(), File::open(path).expect("wada"))
}

fn get_files_path_in_dir(dir: &PathBuf, escape_file: &str) -> Result<Vec<PathBuf>, Error> {
    let entries = fs::read_dir(dir)?
        .filter(|res| matches!(res, Ok(e) if e.file_name() != escape_file))
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    Ok(entries)
}


fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let now = Instant::now();
    let explode_line = ',';
    let path = PathBuf::from("F:/temp/csv/csv3/");
    let base_file = "1.csv";
    let base_file_for_eq = path.clone().join(base_file);
    let merge_file_name = "merge.csv";
    let merge_file_path = path.clone().join(merge_file_name);
    let pool_size = 8;




/////-----------------------------------------------

    let mut pool = ThreadPool::new(pool_size);
    let sender = pool.sender.take().unwrap();

    let files : Vec<(PathBuf, File)> = get_files_path_in_dir(&path, merge_file_name).unwrap()
    .into_iter().filter(|x| *x != base_file_for_eq)
    .map(|path |open_files_for_threads(path)).collect();
    
    let mut base_string = String::new();
    File::open(base_file_for_eq.clone()).expect("wada").read_to_string(&mut base_string).expect("1111");
    
    let path_base_arc = Arc::new(base_file_for_eq);

    let base_line_index = base_string.find("\r\n").unwrap();
    let base_line = base_string.get(0..base_line_index).unwrap().trim();
    let head_for_diff_arc = Arc::new(base_line.to_string());

    let file_for_result_arc = Arc::new(Mutex::new(File::create(merge_file_path).unwrap()));
    write!(file_for_result_arc.lock().unwrap(), "{}\r\n", base_string).unwrap();



    for (path_, mut file) in files.into_iter() {
        let path_arc = Arc::new(path_.clone());
        let path= Arc::clone(&path_arc);
        let path_base= Arc::clone(&path_base_arc);
        let head_for_diff = Arc::clone(&head_for_diff_arc);
        let mut buff_string = String::new();
        file.read_to_string(&mut buff_string).expect("1111");
        let buff = Arc::new(buff_string);
        let file_for_result = Arc::clone(&file_for_result_arc);
        
        let closure = move || {
            check_difference_btree(Arc::clone(&path), Arc::clone(&buff), head_for_diff, &path_base, &explode_line);
            if path != path_base {
                create_merge_file_thread(file_for_result, Arc::clone(&buff));
            }
            
        };
        sender.send(Box::new(closure)).unwrap();
    }

    println!("done : {:.2?}", now.elapsed());
    
/////-----------------------------------------------
  


    // let files = get_files_path_in_dir(&path, merge_file_name)
    //     .map(open_files)
    //     .map(files_to_string)
    //     .expect("Что-то пошло не так");
    // check_difference_multi_threads(&files, &explode_line,pool_size).unwrap();
    // create_merge_file(&files, merge_file_path, merge_file_name, pool_size).unwrap();
    // println!("done : {:.2?}", now.elapsed());
}
