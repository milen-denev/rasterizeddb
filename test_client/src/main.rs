#![allow(unused_imports)]
#![allow(dead_code)]

use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use futures::future::join_all;
use log::LevelFilter;
use rand::Rng;
use rasterizeddb_core::{client::DbClient, core::{database::QueryExecutionResult, db_type::DbType, row_v2::row::vec_into_rows}};

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Error)
        .init();

    let client = Arc::new(DbClient::new(Some("127.0.0.1")).await.unwrap());

    let _query = r##"
        CREATE TABLE employees (
            id UBIGINT,
            name VARCHAR,
            position VARCHAR,
            salary REAL
        );
    "##;

    let create_result = client.execute_query(_query).await;

    println!("Create result: {:?}", create_result);

    let mut features = vec![];

    let semaphore = Arc::new(tokio::sync::Semaphore::new(1)); 

    for i in 0..1000 {
        let person = generate_person();
        let query = format!(
            r##"
            INSERT INTO employees (id, name, position, salary)
            VALUES ({}, '{}', '{}', {});
            "##,
            i + 1, person.name, person.job_title, person.salary
        );

        let client_clone = Arc::clone(&client);
        let semaphore_clone = Arc::clone(&semaphore);

        features.push(tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();
            let _insert_result = client_clone.execute_query(&query).await;
            drop(_permit);
        }));
    }

    join_all(features).await;

    // tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // return Ok(());

    // let client_clone = Arc::clone(&client);

    // let total_type: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // let total_queries = 10;

    // let mut features = vec![];

    // for _ in 0..total_queries {
    //     let client_clone_2 = Arc::clone(&client_clone);
    //     let total_type_clone = Arc::clone(&total_type);

    //     features.push(tokio::spawn(async move {
    //         let query = r##"
    //             SELECT id FROM employees
    //             WHERE id = 50
    //         "##;

    //         let instant = std::time::Instant::now();
    //         let _select_result = client_clone_2.execute_query(query).await;
    //         let elapsed = instant.elapsed().as_millis();
    //         total_type_clone.fetch_add(elapsed as u64, Ordering::SeqCst);
    //     }));
    // }

    // join_all(features).await;

    // println!("Total time for {} queries: {} ms", total_queries, total_type.load(Ordering::SeqCst));

    let query = r##"
        SELECT id, name, position, salary FROM employees
        WHERE id = 999
    "##;

    let instant = std::time::Instant::now();
    let select_result = client.execute_query(query).await;
    let elapsed = instant.elapsed().as_millis();

    println!("Query executed in {} ms", elapsed);

    match select_result.unwrap() {
        QueryExecutionResult::RowsResult(rows) => {
            println!("Rows fetched successfully.");
            let rows = vec_into_rows(&rows).unwrap();
            println!("Total rows: {}", rows.len());
            for row in rows {
                for column in &row.columns {
                    if column.column_type == DbType::STRING {
                        let value = String::from_utf8(column.data.into_slice().to_vec()).unwrap();
                        println!("Name / Position: {}", value);
                    } else if column.column_type == DbType::U64 {
                        println!("Id: {}", u64::from_le_bytes(column.data.into_slice().try_into().unwrap()));
                    } else if column.column_type == DbType::F32 {
                        println!("Salary: {}", f32::from_le_bytes(column.data.into_slice().try_into().unwrap()));
                    }
                }
            }
        }
        QueryExecutionResult::Error(err) => {
            eprintln!("Error occurred: {}", err);
        }
        _ => {
            eprintln!("Unexpected result type");
        }
    }

    // let rebuild_result = client.execute_query("BEGIN SELECT FROM test_db REBUILD_INDEXES END").await.unwrap();

    // println!("Rebuild indexes result: {:?}", rebuild_result);

    // for i in 0..1_500_000 {
    //     let query = format!(
    //         r#"
    //         BEGIN
    //         INSERT INTO test_db (COL(F64), COL(F32), COL(I32), COL(STRING), COL(CHAR), COL(U128))
    //         VALUES ({}, {}, {}, {}, 'D', {})
    //         END
    //     "#, i, i as f64 * -1 as f64, i as i32, "Milen Denev", u128::MAX - i);
                
    //     let db_response = client.execute_query(&query).await.unwrap();

    //     // println!("Insert result: {:?}", db_response);
    // }

    // println!("Done inserting rows.");

    // let mut stopwatch = Stopwatch::new();
    // stopwatch.start();

    // let query2 = format!(
    //     r#"
    //     BEGIN
    //     SELECT FROM test_db
    //     WHERE COL(0,I32) = 12440 
    //     LIMIT 1000000
    //     END
    // "#);

    // let db_response2 = client.execute_query(&query2).await.unwrap();
    // let result = DbClient::extract_rows(db_response2).unwrap().unwrap();

    // stopwatch.stop();

    // for row in result.iter() {
    //     let columns = row.columns().unwrap();
    //     for (i, column) in columns.iter().enumerate() {
    //         println!("Column ({}): {}", i, column.into_value());
    //     }
    // }

    // println!("Elapsed {:?}", stopwatch.elapsed());

    // match result {
    //     ReturnResult::Rows(rows) => {
    //         println!("Total rows: {}", rows.len());
    //     },
    //     ReturnResult::HtmlView(html_string) => {
    //         println!("Html string: {}", html_string);
    //     }
    // }

    // println!("Press any key to continue...");

    // let mut buffer = String::new();
    // stdin().read_line(&mut buffer).unwrap();

    return Ok(());
}

// A simple struct to hold the generated data.
struct Person {
    name: String,
    job_title: String,
    salary: f32,
}

// Function to generate a new `Person` with random data.
fn generate_person() -> Person {
    // Create a new thread-local random number generator.
    let mut rng = rand::rng();

    // Arrays of possible names and job titles.
    let first_names = [
        "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
        "William", "Elizabeth", "David", "Susan", "Richard", "Jessica", "Thomas", 
        "Sarah", "Christopher", "Karen", "Daniel", "Nancy", "Paul", "Lisa", "Mark", 
        "Betty", "Donald", "Dorothy", "George", "Helen", "Steven", "Sandra", "Edward", 
        "Ashley", "Kenneth", "Donna", "Joseph", "Kimberly", "Brian", "Carol", "Ronald", 
        "Michelle", "Anthony", "Emily", "Jason", "Amanda", "Jeff", "Deborah", "Ryan", 
        "Stephanie", "Gary", "Laura", "Nicholas", "Cynthia", "Eric", "Kathleen", "Jacob", 
        "Amy", "Angela", "Melissa", "Brenda", "Rebecca", "Andrew", "Janet", "Joshua", 
        "Sharon", "Matthew", "Christine", "Kevin", "Anna", "Jason", "Shirley", "Dennis", 
        "Pamela", "Walter", "Debra", "Patrick", "Rachel", "Peter", "Nicole", "Douglas", 
        "Catherine", "Henry", "Samantha", "Carl", "Theresa", "Arthur", "Gloria", "Jerry", 
        "Evelyn", "Harold", "Frances", "Timothy", "Christina", "Frank", "Judith", "Raymond", 
        "Rose", "Adam", "Beverly", "Gregory", "Jean", "Larry", "Cheryl", "Jose", "Hannah", 
        "Jeremy", "Doris", "Stephen", "Julia", "Billy", "Marie", "Kyle", "Diane", "Benjamin",
        "Alice", "Keith", "Heather", "Roger", "Victoria", "Gerald", "Judith", "Craig", 
        "Lauren", "Scott", "Brittany", "Joe", "Kelly", "Sam", "Natalie", "Jonathan", "Lois"
    ];

    let last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", 
        "Thomas", "Taylor", "Moore", "Jackson", "White", "Harris", "Martin", "Thompson", 
        "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", 
        "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", 
        "Carter", "Roberts", "Phillips", "Collins", "Ramirez", "Stewart", "Russell", "Perez", 
        "Cook", "Morgan", "Bennett", "Bell", "Coleman", "Reed", "Watson", "Diaz", "Washington", 
        "Cooper", "Wood", "Price", "Hughes", "Patterson", "Kelly", "James", "Ryan", "Walker", 
        "Cox", "Graham", "Gray", "Henderson", "Kim", "Howard", "Peterson", "Brooks", "Mitchell", 
        "Price", "Parker", "Morris", "Sanders", "Butler", "Barnes", "Fisher", "Murphy", "Simmons", 
        "Bailey", "Richardson", "Diaz", "Butler", "Stone", "Phillips", "Ford", "Graham", "Hicks", 
        "Alexander", "Mason", "Stone", "Cole", "Payne", "Spencer", "Chavez", "Kennedy", "Lane", 
        "Andrews", "Myers", "Hunter", "Reid", "Marshall", "Stevens", "Elliott", "Snyder", "Palmer",
        "Bishop", "Harper", "Gordon", "Mills", "Franklin", "Fields", "West", "Porter", "Gilbert", 
        "Owens", "Holmes", "Powell", "Banks", "Carroll", "Fowler", "Wallace", "Nichols", "Grant"
    ];

    let job_titles = [
        "Software Engineer", "Data Scientist", "Product Manager", "UX Designer",
        "Financial Analyst", "Marketing Specialist", "Human Resources Manager",
        "Customer Service Representative", "Project Coordinator", "DevOps Engineer",
        "Accountant", "Architect", "Teacher", "Nurse", "Graphic Designer", 
        "Civil Engineer", "Mechanical Engineer", "Electrical Engineer", "Web Developer", 
        "System Administrator", "Cybersecurity Analyst", "Operations Manager", 
        "Supply Chain Manager", "Account Executive", "Recruiter", "Paralegal", 
        "Librarian", "Social Worker", "Journalist", "Editor", "Physician", 
        "Physical Therapist", "Veterinarian", "Chef", "Bartender", "Plumber", 
        "Electrician", "Real Estate Agent", "Pilot", "Flight Attendant", 
        "Business Analyst", "Network Engineer", "Technical Writer", "Game Designer",
        "Art Director", "Biomedical Engineer", "Chemist", "Economist", "Geologist", 
        "Mathematician", "Meteorologist", "Urban Planner", "Forester", "Hydrologist", 
        "Zoologist", "Anthropologist", "Historian", "Curator", "Archivist", 
        "Criminologist", "Political Scientist", "Sociologist", "Urban Planner",
        "Psychologist", "Speech-Language Pathologist", "Dietitian", "Physical Trainer", 
        "Occupational Therapist", "Chiropractor", "Anesthesiologist", "Cardiologist",
        "Dermatologist", "Neurologist", "Orthopedic Surgeon", "Pediatrician", "Urologist"
    ];

    // Pick a random name from the arrays.
    let first_name = first_names[rng.random_range(0..first_names.len())];
    let last_name = last_names[rng.random_range(0..last_names.len())];
    let name = format!("{} {}", first_name, last_name);

    // Pick a random job title.
    let job_title = job_titles[rng.random_range(0..job_titles.len())].to_string();

    // Generate a random salary as an f32 within a realistic range.
    let min_salary = 20000.0;
    let max_salary = 250000.0;
    let salary = rng.random_range(min_salary..max_salary);

    // Return the new Person struct.
    Person {
        name,
        job_title,
        salary,
    }
}
