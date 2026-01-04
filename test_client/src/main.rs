#![allow(unused_imports)]
#![allow(dead_code)]

use futures::future::join_all;
use log::LevelFilter;
use rand::Rng;
use rasterizeddb_core::{
    client::DbClient,
    core::{ db_type::DbType, row::row::vec_into_rows, support_types::QueryExecutionResult},
};
use std::{
    io::stdin,
    sync::Arc,
};

#[tokio::main(flavor = "multi_thread")]
#[allow(unreachable_code)]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Error)
        .init();

    loop {
        let client = Arc::new(DbClient::new(Some("127.0.0.1")).await.unwrap());

        let choice = prompt_menu_choice();
        match choice {
            1 => {
                let create_result = create_table(&client).await;
                println!("Create result: {:?}", create_result);
                pause("Press any key to continue...");
                insert_rows(&client).await;
                pause("Press any key to continue...");
                run_queries(&client).await;
            }
            2 => {
                pause("Press any key to continue...");
                insert_rows(&client).await;
                pause("Press any key to continue...");
                run_queries(&client).await;
            }
            3 => {
                pause("Press any key to continue...");
                run_queries(&client).await;
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
3. Query only\n"
    );

    loop {
        println!("Enter 1, 2, or 3:");
        let mut buffer = String::new();
        stdin().read_line(&mut buffer).unwrap();
        match buffer.trim() {
            "1" => return 1,
            "2" => return 2,
            "3" => return 3,
            _ => println!("Invalid choice."),
        }
    }
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
    for _i in 0..5 {
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
        let query = r##"
            SELECT id, salary, age, name FROM employees
            WHERE id = 100000 OR id = 200000 OR id = 300000 OR id = 400000 OR id = 500000
        "##;

        let instant = std::time::Instant::now();
        let select_result = client.execute_query(query).await;
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
