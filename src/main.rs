// какая то херня для профилировщика =)
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::{
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

// кастомный сбор ошибок в вектор
#[derive(Debug)]
struct CustomCollectError(Vec<Error>);

impl CustomCollectError {
    fn new() -> Self {
        // инициализируем коллекцию ошибок
        CustomCollectError(vec![])
    }
    fn push(&mut self, error: String) {
        // помещаем ошибки в вектор
        self.0.push(Error::new(ErrorKind::Other, error));
    }
    fn print_all_err(&self) {
        // вывод ошибок на печать
        for err in self.0.iter() {
            println!("{}", err);
        }
    }
}

// структура для пула потоков
struct ThreadPool {
    threads: Vec<Option<thread::JoinHandle<()>>>,
    sender: Option<Sender<Box<dyn FnOnce() + Send + 'static>>>,
}
impl Drop for ThreadPool {
    fn drop(&mut self) {
        // при завершении первого потока, вызывается ожидание завершения остальных потоков, вот такие мы хитрые
        let now = Instant::now();
        // чекаем завершения остальных потоков
        for worker in &mut self.threads {
            if let Some(thread) = worker.take() {
                thread.join().unwrap();
            }
        }
        // считаем время на завершения всех потоков
        println!("threads drop: {:.2?}", now.elapsed());
    }
}
impl ThreadPool {
    fn new(size: usize) -> ThreadPool {
        let mut threads = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel::<Box<dyn FnOnce() + Send + 'static>>();
        let rec_arc = Arc::new(Mutex::new(receiver));

        for _ in 0..size {
            let rec = Arc::clone(&rec_arc);

            threads.push(Some(thread::spawn(move || loop {
                let message = rec.lock().unwrap().recv();
                match message {
                    Ok(closure) => {
                        closure();
                    }
                    Err(_) => {
                        break;
                    }
                }
            })));
        }
        ThreadPool {
            threads,
            sender: Some(sender),
        }
    }
}

fn check_difference(
    path: Arc<PathBuf>,
    buff: Arc<String>,
    head_for_diff: Arc<String>,
    path_base: &Arc<PathBuf>,
    explode_line: &char,
) {
    let explode_line = *explode_line;
    let mut error_collection = CustomCollectError::new();
    let first_line_index = buff.find("\r\n").unwrap();
    let first_line = buff.get(0..first_line_index).unwrap().trim();

    let (head_for_diff_count, first_line_count) = (
        head_for_diff.split(explode_line).count(),
        first_line.split(explode_line).count(),
    );
    // если заголовки файлов не отличаются по количеству столбцов
    if head_for_diff_count == first_line_count {
        // чисто для проверки расхождения заголовков
        for (column_one, column_two) in head_for_diff
            .split_terminator(explode_line)
            .zip(first_line.split_terminator(explode_line))
        {
            // проверяем, вдруг они отличаются по названию ! сук
            if column_one != column_two {
                error_collection.push(format!(
                    "Заголовки в файлах: `{:?}` и {:?} отличаются. Различия {} с {}",
                    path, path_base, column_one, column_two
                ));
            };
        }
    } else {
        // ну тут пишем если даже количество столбцов заголовков отличается
        error_collection.push(
            format!("В файлах: `{:?}` и `{:?}` количество заглавных столбцов отличается. В первом случае их {}, во втором {} ", 
            path, path_base , head_for_diff_count, first_line_count
        ));
    }
    // проверяем разницу количества строк остальных файлов с нашим базовым файлом
    for (line_index, line) in buff.split_terminator("\r\n").enumerate() {
        // тут два if, да, херово выглядит! А че сделать, подскажите.
        if line_index != 0 {
            if line.split(explode_line).count() != head_for_diff_count {
                error_collection.push(format!(
                    "В файле: `{:?}` и `{:?}` количество столбцов отличается. На линии {} ",
                    path,
                    path_base,
                    line_index + 1
                ));
            }
        }
    }
    // ну и собственно просим поток распечатать ошибки
    error_collection.print_all_err();
}

fn get_files_path_in_dir(dir: &PathBuf, escape_file: &str) -> Result<Vec<PathBuf>, Error> {
    let entries = fs::read_dir(dir)?
        .filter(|res| matches!(res, Ok(e) if e.file_name() != escape_file)) // тут пропускает уже смерженный файл, если запуск не первый и конечный файл уже существует
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    Ok(entries)
}

fn open_files(path: PathBuf) -> (PathBuf, File) {
    (path.clone(), File::open(path).expect("wada"))
}

fn create_merge_file(file_for_result: Arc<Mutex<File>>, buff: Arc<String>) {
    let first_line = buff.find("\r\n").unwrap();
    // пишем в файл, пропуская первую заглавную строку
    write!(
        file_for_result.lock().unwrap(),
        "{}",
        buff.get(first_line..).unwrap()
    )
    .unwrap();
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let now = Instant::now();
    let explode_line = ','; // разделить
    let path = PathBuf::from("F:/temp/csv/csv2/"); // папка с файлами .csv
    let base_file_name = "csv1.csv"; // какой файл берем за основу для проверок и мержа
    let merge_file_name = "merge.csv"; // конечное название создаваемого файла
    let pool_size = 8; // количество потоков, пока 8, чтобы забить весь проц уахаха

    // соединяем общий путь с названием файла
    let base_file_path = path.join(base_file_name);
    let merge_file_path = path.join(merge_file_name);

    // создаем потоки от pool_size
    let mut pool = ThreadPool::new(pool_size);
    // инициализируем и забираем отправщика для последующего отправления замыканий
    let sender = pool.sender.take().unwrap();

    // открываем файлы из папки и берем их названия (пропускаем первый - базовый файл, чтобы не было дублей)
    let files: Vec<(PathBuf, File)> = get_files_path_in_dir(&path, merge_file_name)
        .unwrap()
        .into_iter()
        .map(open_files)
        .collect(); // во как красиво можно вызывать функцию на каждое значение. Раст сука умный!

    let mut base_string = String::new();
    File::open(base_file_path.clone())
        .expect("Базовый файл не найден!")
        .read_to_string(&mut base_string)
        .expect("Базовый файл не может быть прочитан!");

    let path_base_arc = Arc::new(base_file_path);
    let base_line_index = base_string.find("\r\n").unwrap();
    let base_line = base_string.get(0..base_line_index).unwrap().trim();
    let head_for_diff_arc = Arc::new(base_line.to_string());

    let file_for_result_arc = Arc::new(Mutex::new(File::create(merge_file_path).unwrap()));
    write!(file_for_result_arc.lock().unwrap(), "{}", base_string).unwrap();

    for (path_, mut file) in files.into_iter() {
        // читаем файлы в строки
        let mut buff_string = String::new();
        file.read_to_string(&mut buff_string)
            .expect("Не получается прочитать файл {path}");

        // ну тут арки =)
        let path_arc = Arc::new(path_);
        let path_base = Arc::clone(&path_base_arc);
        let head_for_diff = Arc::clone(&head_for_diff_arc);
        let buff = Arc::new(buff_string);
        let file_for_result = Arc::clone(&file_for_result_arc);

        // кложура, которую потом передадим в sender
        let closure = move || {
            // первая функция проверяет все файлы на различие заголовка базового файла со всеми остальнымы (с самим собой в том числе).
            check_difference(
                Arc::clone(&path_arc),
                Arc::clone(&buff),
                head_for_diff,
                &path_base,
                &explode_line,
            );
            // а тут создается конечный файл и туда пропихиваются всё без обработки. if - чтобы базовый файл не попал второй раз в конечный файл - результат.
            if Arc::clone(&path_arc) != path_base {
                create_merge_file(file_for_result, Arc::clone(&buff));
            }
        };
        // отправляет в поток
        sender.send(Box::new(closure)).unwrap();
    }
    // чекаем проход основного потока
    println!("done main thread : {:.2?}", now.elapsed());
}
