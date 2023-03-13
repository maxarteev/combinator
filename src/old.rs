// #![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, prelude::*, Error, ErrorKind},
    path::PathBuf,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
    time::Instant,
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

struct ThreadPool {
    threads: Vec<Option<thread::JoinHandle<()>>>,
    sender: Option<Sender<Box<dyn FnOnce() + Send + 'static>>>,
}
impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.threads {
            if let Some(thread) = worker.take() {
                thread.join().unwrap();
            }
        }
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

fn create_merge_file(
    files: &BTreeMap<Arc<PathBuf>, Arc<String>>,
    mut path: String,
    merge_file_name: &str,
    _pool_size: usize,
) -> Result<(), Error> {
    let now = Instant::now();
    // let mut pool = ThreadPool::new(pool_size);
    path.push_str(merge_file_name);
    let mut new_file = File::create(path)?;

    for (iter_files, (_, buff)) in files.iter().enumerate() {
        
        let first_line = buff.find("\r\n").unwrap();

        if iter_files == 0 {
            write!(new_file, "{}\r\n", buff).unwrap();
        } else {
            write!(new_file, "{}\r\n", buff.get(first_line..).unwrap()).unwrap();
        }
    }
    println!("create_merged_file : {:.2?}", now.elapsed());
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
    for path in vec.iter() {
        open_files.insert(path.clone(), File::open(path).expect("wada"));
    }
    open_files
}

fn get_files_path_in_dir(dir: &String, escape_file: &str) -> Result<Vec<PathBuf>, Error> {
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
    let path = "F:/temp/csv/csv3/".to_string();
    let merge_file_name = "merge.csv";
    let pool_size = 40;
    

    let files = get_files_path_in_dir(&path, merge_file_name)
        .map(open_files)
        .map(files_to_string)
        .expect("Что-то пошло не так");
    println!("chet delaet : {:.2?}", now.elapsed());
    let now = Instant::now();
    check_difference_multi_threads(&files, &explode_line,pool_size).unwrap();
    println!("check: {:.2?}", now.elapsed());
    let now = Instant::now();
    create_merge_file(&files, path, merge_file_name, pool_size).unwrap();
    println!("merge : {:.2?}", now.elapsed());
}
