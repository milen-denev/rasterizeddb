#![allow(unused_imports)]
#![allow(dead_code)]

use futures::future::join_all;
use log::LevelFilter;
use rand::Rng;
use rasterizeddb_core::{
    client::DbClient,
    core::{
        db_type::DbType,
        row::{
            row::{vec_into_rows, ColumnWritePayload, RowWrite},
            row_pointer::RowPointer,
        },
        storage_providers::traits::StorageIO,
        support_types::QueryExecutionResult,
    },
};
use std::{
    io::stdin,
    sync::atomic::AtomicU64,
    sync::Arc,
};

const QUERY: &str = "salary >= 249999.0";

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Error)
        .init();

    loop {
        let choice = prompt_menu_choice();
        match choice {
            1 => {
                let client = Arc::new(DbClient::new(Some("127.0.0.1")).await.unwrap());

                let create_result = create_table(&client).await;
                println!("Create result: {:?}", create_result);
                pause("Press any key to continue...");
                insert_rows(&client).await;
                pause("Press any key to continue...");
                run_queries(&client).await;
            }
            2 => {
                let client = Arc::new(DbClient::new(Some("127.0.0.1")).await.unwrap());

                pause("Press any key to continue...");
                insert_rows(&client).await;
                pause("Press any key to continue...");
                run_queries(&client).await;
            }
            3 => {
                let client = Arc::new(DbClient::new(Some("127.0.0.1")).await.unwrap());

                pause("Press any key to continue...");
                run_queries(&client).await;
            }
            4 => {
                pause("Press any key to start generating 5,000,000 rows...");
                generate_rows_and_pointers_to_disk().await?;
            }
            _ => unreachable!(),
        }
        
        println!("Press any key to try again...");
        let mut buffer = String::new(); 
        stdin().read_line(&mut buffer).unwrap();
    }
    
    Ok(())
}

fn prompt_menu_choice() -> u8 {
    println!(
        "Choose an option:\n\
1. Create Table, Add rows and query\n\
2. Add rows and Query\n\
3. Query only\n\
4. Generate 5,000,000 rows + pointers to disk (in-memory build)\n"
    );

    loop {
        println!("Enter 1, 2, 3, or 4:");
        let mut buffer = String::new();
        stdin().read_line(&mut buffer).unwrap();
        match buffer.trim() {
            "1" => return 1,
            "2" => return 2,
            "3" => return 3,
            "4" => return 4,
            _ => println!("Invalid choice."),
        }
    }
}

#[derive(Clone)]
struct InMemoryVecStorage {
    data: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    name: String,
    hash: u32,
}

impl InMemoryVecStorage {
    fn new(name: &str, initial_capacity: usize) -> Self {
        Self {
            data: std::sync::Arc::new(std::sync::Mutex::new(Vec::with_capacity(initial_capacity))),
            name: name.to_string(),
            hash: 0,
        }
    }

    fn take_all(&self) -> Vec<u8> {
        let mut guard = self.data.lock().unwrap();
        std::mem::take(&mut *guard)
    }
}

impl StorageIO for InMemoryVecStorage {
    async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
        true
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        let mut guard = self.data.lock().unwrap();
        let end = position as usize + buffer.len();
        if guard.len() < end {
            guard.resize(end, 0);
        }
        guard[position as usize..end].copy_from_slice(buffer);
    }

    async fn write_data_seek(&self, seek: std::io::SeekFrom, buffer: &[u8]) {
        match seek {
            std::io::SeekFrom::Start(pos) => self.write_data(pos, buffer).await,
            _ => panic!("InMemoryVecStorage::write_data_seek only supports SeekFrom::Start"),
        }
    }

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> Result<(), std::io::Error> {
        let guard = self.data.lock().unwrap();
        let start = *position as usize;
        let end = start + buffer.len();
        if end > guard.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end",
            ));
        }
        buffer.copy_from_slice(&guard[start..end]);
        *position += buffer.len() as u64;
        Ok(())
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> Result<(), std::io::Error> {
        for (pos, buf) in reads {
            let mut p = *pos;
            self.read_data_into_buffer(&mut p, buf).await?;
        }
        Ok(())
    }

    async fn append_data(&self, buffer: &[u8], _immediate: bool) {
        let mut guard = self.data.lock().unwrap();
        guard.extend_from_slice(buffer);
    }

    async fn get_len(&self) -> u64 {
        let guard = self.data.lock().unwrap();
        guard.len() as u64
    }

    fn exists(_location: &str, _table_name: &str) -> bool {
        true
    }

    async fn create_temp(&self) -> Self {
        Self::new("temp", 0)
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {
        panic!("swap_temp not supported for InMemoryVecStorage")
    }

    fn get_location(&self) -> Option<String> {
        None
    }

    async fn create_new(&self, name: String) -> Self {
        Self::new(&name, 0)
    }

    fn drop_io(&self) {
        // no-op
    }

    fn get_hash(&self) -> u32 {
        self.hash
    }

    async fn start_service(&self) {}

    fn get_name(&self) -> String {
        self.name.clone()
    }
}

fn make_bytes_payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut data_mb = rasterizeddb_core::memory_pool::MEMORY_POOL.acquire(bytes.len());
    let slice = data_mb.into_slice_mut();
    slice.copy_from_slice(bytes);

    ColumnWritePayload {
        data: data_mb,
        write_order,
        column_type: column_type.clone(),
        size: column_type.get_size(),
    }
}

fn make_string_payload(value: &str, write_order: u32) -> ColumnWritePayload {
    let bytes = value.as_bytes();
    let mut data_mb = rasterizeddb_core::memory_pool::MEMORY_POOL.acquire(bytes.len());
    let slice = data_mb.into_slice_mut();
    slice.copy_from_slice(bytes);

    ColumnWritePayload {
        data: data_mb,
        write_order,
        column_type: DbType::STRING,
        size: DbType::STRING.get_size(),
    }
}

fn make_employee_row_write(i: u64) -> RowWrite {
    // Keep strings relatively small and mostly fixed-size to make memory usage predictable.
    let name = format!("Employee {:07}", i);
    let job_title = "Software Engineer";
    let department = "Engineering";
    let manager = "Alice Smith";
    let location = "New York";
    let degree = "BSc Computer Science";
    let skills = "Rust, SQL, Git";
    let current_project = "Migration Tool";

    let id = (i + 1) as u64;
    let salary: f32 = 50_000.0 + ((i % 200_000) as f32);
    let age: i32 = 18 + (i % 59) as i32;
    let hire_date: u64 = 1_000_000 + (i % 8_000_000);
    let performance_score: f32 = ((i % 500) as f32) / 100.0;
    let is_active: u8 = if (i % 10) != 0 { 1 } else { 0 };
    let created_at: u64 = 1_000_000 + (i % 8_000_000);
    let updated_at: u64 = created_at + (i % 1_000);
    let is_fired: u8 = if is_active == 0 && (i % 2) == 0 { 1 } else { 0 };

    let mut row_write = RowWrite {
        columns_writing_data: Default::default(),
    };

    // write_order MUST match table column order.
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&id.to_le_bytes(), 0, DbType::U64));
    row_write.columns_writing_data.push(make_string_payload(&name, 1));
    row_write
        .columns_writing_data
        .push(make_string_payload(job_title, 2));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&salary.to_le_bytes(), 3, DbType::F32));
    row_write
        .columns_writing_data
        .push(make_string_payload(department, 4));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&age.to_le_bytes(), 5, DbType::I32));
    row_write.columns_writing_data.push(make_string_payload(manager, 6));
    row_write
        .columns_writing_data
        .push(make_string_payload(location, 7));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&hire_date.to_le_bytes(), 8, DbType::U64));
    row_write.columns_writing_data.push(make_string_payload(degree, 9));
    row_write.columns_writing_data.push(make_string_payload(skills, 10));
    row_write
        .columns_writing_data
        .push(make_string_payload(current_project, 11));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&performance_score.to_le_bytes(), 12, DbType::F32));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&[is_active], 13, DbType::BOOL));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&created_at.to_le_bytes(), 14, DbType::U64));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&updated_at.to_le_bytes(), 15, DbType::U64));
    row_write
        .columns_writing_data
        .push(make_bytes_payload(&[is_fired], 16, DbType::BOOL));

    row_write
}

async fn generate_rows_and_pointers_to_disk() -> std::io::Result<()> {
    #[cfg(target_os = "windows")]
    const ROWS_PATH: &str = r"G:\Databases\Production\employees.db";
    #[cfg(target_os = "windows")]
    const POINTERS_PATH: &str = r"G:\Databases\Production\employees_pointers.db";

    #[cfg(target_os = "linux")]
    const ROWS_PATH: &str = "/home/milen-denev/Documents/db/employees.db";
    
    #[cfg(target_os = "linux")]
    const POINTERS_PATH: &str = "/home/milen-denev/Documents/db/employees_pointers.db";

    const ROW_COUNT: u64 = 5_000_000;

    // Very rough capacity hints to reduce reallocations.
    // If you increase average string sizes, memory usage will grow significantly.
    let rows_io = rclite::Arc::new(InMemoryVecStorage::new("employees_rows", 512 * 1024 * 1024));
    let pointers_io = rclite::Arc::new(InMemoryVecStorage::new(
        "employees_pointers",
        (ROW_COUNT as usize).saturating_mul(24),
    ));

    let last_id = AtomicU64::new(0);
    let table_length = AtomicU64::new(0);

    println!("Generating {} rows in memory...", ROW_COUNT);
    let start = std::time::Instant::now();

    for i in 0..ROW_COUNT {
        let row_write = make_employee_row_write(i);

        let _pointer = RowPointer::write_row(
            pointers_io.clone(),
            rows_io.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            0,
            &row_write,
        )
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}")))?;

        if i > 0 && i % 100_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rows_per_sec = (i as f64) / elapsed.max(0.0001);
            println!(
                "  {} / {} rows (≈{:.0} rows/s)",
                i,
                ROW_COUNT,
                rows_per_sec
            );
        }
    }

    let gen_elapsed = start.elapsed();
    println!("Generation complete in {:.2?}", gen_elapsed);

    // Ensure target directory exists.
    if let Some(parent) = std::path::Path::new(ROWS_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!("Flushing buffers to disk...");
    let rows_bytes = rows_io.take_all();
    let pointers_bytes = pointers_io.take_all();

    {
        use std::io::Write;
        let mut f = std::fs::File::create(ROWS_PATH)?;
        f.write_all(&rows_bytes)?;
    }
    {
        use std::io::Write;
        let mut f = std::fs::File::create(POINTERS_PATH)?;
        f.write_all(&pointers_bytes)?;
    }

    println!(
        "Wrote rows: {} bytes, pointers: {} bytes",
        rows_bytes.len(),
        pointers_bytes.len()
    );
    println!("Rows file: {ROWS_PATH}");
    println!("Pointers file: {POINTERS_PATH}");

    Ok(())
}

fn pause(message: &str) {
    println!("{}", message);
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();
}

async fn create_table(client: &Arc<DbClient>) -> Result<QueryExecutionResult, std::io::Error> {
    let query = r##"
        CREATE TABLE employees (
            id UBIGINT,
            name VARCHAR,
            job_title VARCHAR,
            salary REAL,
            department VARCHAR,
            age INTEGER,
            manager VARCHAR,
            location VARCHAR,
            hire_date UBIGINT,
            degree VARCHAR,
            skills VARCHAR,
            current_project VARCHAR,
            performance_score REAL,
            is_active BOOLEAN,
            created_at UBIGINT,
            updated_at UBIGINT,
            is_fired BOOLEAN
        );
    "##;

    client.execute_query(query).await
}

async fn insert_rows(client: &Arc<DbClient>) {
    for _i in 0..50 {
        let mut features = vec![];

        let semaphore = Arc::new(tokio::sync::Semaphore::new(16));

        for i in 0..100_000 {
            let person = generate_person();
            let query = format!(
                r##"
                INSERT INTO employees (
                    id, name, job_title, salary, department, age, manager, location, hire_date,
                    degree, skills, current_project, performance_score, is_active,
                    created_at, updated_at, is_fired
                )
                VALUES (
                    {}, '{}', '{}', {}, '{}', {}, '{}', '{}', {},
                    '{}', '{}', '{}', {}, {}, {}, {}, {}
                );
                "##,
                (i + 1) + (_i * 100_000),
                person.name,
                person.job_title,
                person.salary,
                person.department,
                person.age,
                person.manager,
                person.location,
                person.hire_date,
                person.degree,
                person.skills,
                person.current_project,
                person.performance_score,
                person.is_active,
                person.created_at,
                person.updated_at,
                person.is_fired
            );

            let client_clone = Arc::clone(client);
            let semaphore_clone = Arc::clone(&semaphore);

            features.push(tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                let _insert_result = client_clone.execute_query(&query).await;
            }));
        }

        join_all(features).await;

        println!("Batch {} inserted.", _i + 1);
    }

    println!("Finished inserting records.");
}

async fn run_queries(client: &Arc<DbClient>) {
    for _ in 0..5 {
        let query = format!(r##"
            SELECT id, salary, age, name FROM employees
            WHERE {}
        "##, QUERY);

        let instant = std::time::Instant::now();
        let select_result = client.execute_query(&query).await;
        let elapsed = instant.elapsed().as_micros();

        println!("Query executed in {} μs", elapsed);

        match select_result {
            Ok(QueryExecutionResult::RowsResult(rows)) => {
                println!("Rows fetched successfully.");
                let rows = vec_into_rows(&rows).unwrap();
                println!("Total rows: {}", rows.len());
            }
            Ok(QueryExecutionResult::Error(err)) => {
                eprintln!("Error occurred: {}", err);
            }
            Ok(_) => {
                eprintln!("Unexpected result type");
            }
            Err(err) => {
                eprintln!("Query failed: {}", err);
            }
        }
    }
}

// A simple struct to hold the generated data.
struct Person {
    name: String,
    job_title: String,
    salary: f32,
    department: String,
    age: u32,
    manager: String,
    location: String,
    hire_date: u64,
    degree: String,
    skills: String,
    current_project: String,
    performance_score: f32,
    is_active: bool,
    created_at: u64,
    updated_at: u64,
    is_fired: bool,
}

// Function to generate a new `Person` with random data.
fn generate_person() -> Person {
    // Create a new thread-local random number generator.
    let mut rng = rand::rng();

    // Arrays of possible names and job titles.
    let first_names = [
        "James",
        "Mary",
        "Robert",
        "Patricia",
        "John",
        "Jennifer",
        "Michael",
        "Linda",
        "William",
        "Elizabeth",
        "David",
        "Susan",
        "Richard",
        "Jessica",
        "Thomas",
        "Sarah",
        "Christopher",
        "Karen",
        "Daniel",
        "Nancy",
        "Paul",
        "Lisa",
        "Mark",
        "Betty",
        "Donald",
        "Dorothy",
        "George",
        "Helen",
        "Steven",
        "Sandra",
        "Edward",
        "Ashley",
        "Kenneth",
        "Donna",
        "Joseph",
        "Kimberly",
        "Brian",
        "Carol",
        "Ronald",
        "Michelle",
        "Anthony",
        "Emily",
        "Jason",
        "Amanda",
        "Jeff",
        "Deborah",
        "Ryan",
        "Stephanie",
        "Gary",
        "Laura",
        "Nicholas",
        "Cynthia",
        "Eric",
        "Kathleen",
        "Jacob",
        "Amy",
        "Angela",
        "Melissa",
        "Brenda",
        "Rebecca",
        "Andrew",
        "Janet",
        "Joshua",
        "Sharon",
        "Matthew",
        "Christine",
        "Kevin",
        "Anna",
        "Jason",
        "Shirley",
        "Dennis",
        "Pamela",
        "Walter",
        "Debra",
        "Patrick",
        "Rachel",
        "Peter",
        "Nicole",
        "Douglas",
        "Catherine",
        "Henry",
        "Samantha",
        "Carl",
        "Theresa",
        "Arthur",
        "Gloria",
        "Jerry",
        "Evelyn",
        "Harold",
        "Frances",
        "Timothy",
        "Christina",
        "Frank",
        "Judith",
        "Raymond",
        "Rose",
        "Adam",
        "Beverly",
        "Gregory",
        "Jean",
        "Larry",
        "Cheryl",
        "Jose",
        "Hannah",
        "Jeremy",
        "Doris",
        "Stephen",
        "Julia",
        "Billy",
        "Marie",
        "Kyle",
        "Diane",
        "Benjamin",
        "Alice",
        "Keith",
        "Heather",
        "Roger",
        "Victoria",
        "Gerald",
        "Judith",
        "Craig",
        "Lauren",
        "Scott",
        "Brittany",
        "Joe",
        "Kelly",
        "Sam",
        "Natalie",
        "Jonathan",
        "Lois",
    ];

    let last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
        "Thomas",
        "Taylor",
        "Moore",
        "Jackson",
        "White",
        "Harris",
        "Martin",
        "Thompson",
        "Young",
        "Allen",
        "King",
        "Wright",
        "Scott",
        "Torres",
        "Nguyen",
        "Hill",
        "Flores",
        "Green",
        "Adams",
        "Nelson",
        "Baker",
        "Hall",
        "Rivera",
        "Campbell",
        "Mitchell",
        "Carter",
        "Roberts",
        "Phillips",
        "Collins",
        "Ramirez",
        "Stewart",
        "Russell",
        "Perez",
        "Cook",
        "Morgan",
        "Bennett",
        "Bell",
        "Coleman",
        "Reed",
        "Watson",
        "Diaz",
        "Washington",
        "Cooper",
        "Wood",
        "Price",
        "Hughes",
        "Patterson",
        "Kelly",
        "James",
        "Ryan",
        "Walker",
        "Cox",
        "Graham",
        "Gray",
        "Henderson",
        "Kim",
        "Howard",
        "Peterson",
        "Brooks",
        "Mitchell",
        "Price",
        "Parker",
        "Morris",
        "Sanders",
        "Butler",
        "Barnes",
        "Fisher",
        "Murphy",
        "Simmons",
        "Bailey",
        "Richardson",
        "Diaz",
        "Butler",
        "Stone",
        "Phillips",
        "Ford",
        "Graham",
        "Hicks",
        "Alexander",
        "Mason",
        "Stone",
        "Cole",
        "Payne",
        "Spencer",
        "Chavez",
        "Kennedy",
        "Lane",
        "Andrews",
        "Myers",
        "Hunter",
        "Reid",
        "Marshall",
        "Stevens",
        "Elliott",
        "Snyder",
        "Palmer",
        "Bishop",
        "Harper",
        "Gordon",
        "Mills",
        "Franklin",
        "Fields",
        "West",
        "Porter",
        "Gilbert",
        "Owens",
        "Holmes",
        "Powell",
        "Banks",
        "Carroll",
        "Fowler",
        "Wallace",
        "Nichols",
        "Grant",
    ];

    let job_titles = [
        "Software Engineer",
        "Data Scientist",
        "Product Manager",
        "UX Designer",
        "Financial Analyst",
        "Marketing Specialist",
        "Human Resources Manager",
        "Customer Service Representative",
        "Project Coordinator",
        "DevOps Engineer",
        "Accountant",
        "Architect",
        "Teacher",
        "Nurse",
        "Graphic Designer",
        "Civil Engineer",
        "Mechanical Engineer",
        "Electrical Engineer",
        "Web Developer",
        "System Administrator",
        "Cybersecurity Analyst",
        "Operations Manager",
        "Supply Chain Manager",
        "Account Executive",
        "Recruiter",
        "Paralegal",
        "Librarian",
        "Social Worker",
        "Journalist",
        "Editor",
        "Physician",
        "Physical Therapist",
        "Veterinarian",
        "Chef",
        "Bartender",
        "Plumber",
        "Electrician",
        "Real Estate Agent",
        "Pilot",
        "Flight Attendant",
        "Business Analyst",
        "Network Engineer",
        "Technical Writer",
        "Game Designer",
        "Art Director",
        "Biomedical Engineer",
        "Chemist",
        "Economist",
        "Geologist",
        "Mathematician",
        "Meteorologist",
        "Urban Planner",
        "Forester",
        "Hydrologist",
        "Zoologist",
        "Anthropologist",
        "Historian",
        "Curator",
        "Archivist",
        "Criminologist",
        "Political Scientist",
        "Sociologist",
        "Urban Planner",
        "Psychologist",
        "Speech-Language Pathologist",
        "Dietitian",
        "Physical Trainer",
        "Occupational Therapist",
        "Chiropractor",
        "Anesthesiologist",
        "Cardiologist",
        "Dermatologist",
        "Neurologist",
        "Orthopedic Surgeon",
        "Pediatrician",
        "Urologist",
    ];
    let departments = [
        "Engineering",
        "Marketing",
        "Sales",
        "HR",
        "Finance",
        "Support",
        "Legal",
        "Operations",
        "Product",
        "Design",
        "QA",
        "IT",
        "Customer Success",
        "R&D",
        "Business Development",
    ];
    let locations = [
        "New York",
        "San Francisco",
        "London",
        "Berlin",
        "Tokyo",
        "Toronto",
        "Sydney",
        "Paris",
        "Bangalore",
        "Shanghai",
        "Singapore",
        "Moscow",
        "Dubai",
    ];
    let degrees = [
        "BSc Computer Science",
        "MBA",
        "BA Psychology",
        "BEng Mechanical",
        "BS Finance",
        "PhD Physics",
        "MS Statistics",
        "BFA Design",
        "MA English",
        "JD Law",
    ];
    let skills_list = [
        "Rust, SQL, Git",
        "Python, ML, Docker",
        "JavaScript, React, CSS",
        "Excel, Accounting",
        "Recruiting, Interviewing",
        "Photoshop, Illustrator",
        "Project Management, Scrum",
        "Linux, Networking",
        "Copywriting, SEO",
        "Negotiation, Sales",
    ];
    let projects = [
        "Migration Tool",
        "Website Redesign",
        "Mobile App",
        "Infrastructure Upgrade",
        "Marketing Campaign",
        "HR Portal",
        "Data Warehouse",
        "Security Audit",
    ];
    let manager_names = [
        "Alice Smith",
        "Bob Johnson",
        "Carol Brown",
        "Dave Davis",
        "Eva Wilson",
        "Frank Thomas",
    ];

    // Pick a random name from the arrays.
    let first_name = first_names[rng.random_range(0..first_names.len())];
    let last_name = last_names[rng.random_range(0..last_names.len())];
    let name = format!("{} {}", first_name, last_name);

    // Generate a random salary as an f32 within a realistic range.
    let min_salary = 20000.0;
    let max_salary = 250000.0;
    let salary = rng.random_range(min_salary..max_salary);

    let job_title = job_titles[rng.random_range(0..job_titles.len())].to_string();
    let department = departments[rng.random_range(0..departments.len())].to_string();
    let age = rng.random_range(18..=76);
    let manager = manager_names[rng.random_range(0..manager_names.len())].to_string();
    let location = locations[rng.random_range(0..locations.len())].to_string();
    let hire_date = rng.random_range(1000000u64..9000000u64);
    let degree = degrees[rng.random_range(0..degrees.len())].to_string();
    let skills = skills_list[rng.random_range(0..skills_list.len())].to_string();
    let current_project = projects[rng.random_range(0..projects.len())].to_string();
    let performance_score = rng.random_range(0.0..5.0);
    let is_active = rng.random_bool(0.9); // 90% chance active
    let created_at = rng.random_range(1000000u64..9000000u64);
    let updated_at = rng.random_range(created_at..9000000u64);
    let is_fired = !is_active && rng.random_bool(0.5);

    // Return the new Person struct.
    Person {
        name,
        job_title,
        salary,
        department,
        age,
        manager,
        location,
        hire_date,
        degree,
        skills,
        current_project,
        performance_score,
        is_active,
        created_at,
        updated_at,
        is_fired,
    }
}
