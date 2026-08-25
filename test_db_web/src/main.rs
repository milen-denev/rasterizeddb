#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::{fmt::Write, io};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::{response::Response, routing::get};
use once_cell::sync::Lazy;

use rand::Rng;

use rasterizeddb_core::client::DbClient;
use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::ReturnResult;
use tokio::sync::OnceCell;

static CLIENT: async_lazy::Lazy<Arc<DbClient>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        let client = DbClient::with_pool_settings_and_timeouts(
            Some("127.0.0.1"),
            64,
            128,
            Duration::from_secs(2),
            Duration::from_millis(250),
        )
            .await
            .unwrap();

        let _query = r##"
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

        client
            .execute_query(_query)
            .await
            .unwrap();

        _ = client.connect().await;

        Arc::new(client)
    })
});

static RESPONSE_CACHE: Lazy<Mutex<HashMap<u64, Arc<OnceCell<Bytes>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
struct HttpResponseError {
    status: StatusCode,
    message: String,
}

fn response_cache_cell(id: u64) -> Arc<OnceCell<Bytes>> {
    let mut cache = RESPONSE_CACHE.lock().expect("response cache mutex poisoned");
    cache.entry(id)
        .or_insert_with(|| Arc::new(OnceCell::new()))
        .clone()
}

fn text_response(status: StatusCode, body: impl Into<Body>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .body(body.into())
        .unwrap()
}

fn build_query(id: u64) -> String {
    let mut query = String::with_capacity(96);
    let _ = write!(
        &mut query,
        "SELECT id, salary, job_title, location, current_project FROM employees WHERE id = {}",
        id
    );
    query
}

fn render_rows(result: ReturnResult) -> Result<Bytes, io::Error> {
    let mut output = String::new();

    match result {
        ReturnResult::Rows(rows) => {
            for row in &rows {
                for (index, col) in row.columns.iter().enumerate() {
                    match (index, col.column_type.clone()) {
                        (0, DbType::U64) => {
                            let value = u64::from_le_bytes(
                                col.data
                                    .into_slice()
                                    .try_into()
                                    .expect("Failed to convert bytes to u64"),
                            );
                            let _ = write!(&mut output, "id: {}\t", value);
                        }
                        (1, DbType::F64) => {
                            let value = f64::from_le_bytes(
                                col.data
                                    .into_slice()
                                    .try_into()
                                    .expect("Failed to convert bytes to f64"),
                            );
                            let _ = write!(&mut output, "salary: {}\t", value);
                        }
                        (2, DbType::STRING) => {
                            let _ = write!(
                                &mut output,
                                "job title: {}\t",
                                String::from_utf8_lossy(&col.data.into_slice())
                            );
                        }
                        (3, DbType::STRING) => {
                            let _ = write!(
                                &mut output,
                                "location: {}\t",
                                String::from_utf8_lossy(&col.data.into_slice())
                            );
                        }
                        (4, DbType::STRING) => {
                            let _ = write!(
                                &mut output,
                                "project: {}\t",
                                String::from_utf8_lossy(&col.data.into_slice())
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => output.push_str("No rows returned."),
    }

    Ok(Bytes::from(output))
}

async fn fetch_response_body(id: u64) -> Result<Bytes, HttpResponseError> {
    let query = build_query(id);
    let client = CLIENT.force().await;

    let db_response = client.execute_query(&query).await.map_err(|err| HttpResponseError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: format!("db unavailable: {err}"),
    })?;

    let result = match DbClient::extract_rows(db_response) {
        Ok(Some(result)) => result,
        Ok(None) => ReturnResult::Rows(Vec::new()),
        Err(err) => {
            return Err(HttpResponseError {
                status: StatusCode::BAD_GATEWAY,
                message: format!("invalid db response: {err}"),
            });
        }
    };

    render_rows(result).map_err(|err| HttpResponseError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("failed to render response: {err}"),
    })
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/get/{id}", get(query));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:80").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

// get id from query param
async fn query(Path(id): Path<u64>) -> Response {
    let cache_cell = response_cache_cell(id);

    match cache_cell
        .get_or_try_init(|| async move { fetch_response_body(id).await })
        .await
    {
        Ok(body) => text_response(StatusCode::OK, Body::from(body.clone())),
        Err(err) => text_response(err.status, Body::from(err.message.clone())),
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
    let age = rng.random_range(21..=65);
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
