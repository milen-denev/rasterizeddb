#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fmt::Write, io};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, RawQuery};
use axum::http::{StatusCode, header};
use axum::{response::Response, routing::get};

use rand::RngExt;

use rasterizeddb_client::Login;
use rasterizeddb_client::orm::AutoMigrate;
use rasterizeddb_client::orm::Migrator;
use rasterizeddb_client::{Client, Options, Param, Rows};
use tokio::sync::OnceCell;

static CLIENT: async_lazy::Lazy<Arc<Client>> = async_lazy::Lazy::new(|| {
    Box::pin(async {
        // One connection shared by every request this server is handling,
        // in place of a pool of up to 128: the database runs each query on a
        // task of its own whichever connection it arrives on.
        // Where and as whom, from the environment: `RDB_ADDR` (default the
        // local server), and `RDB_USER` with `RDB_PASSWORD` for a server
        // that asks for a password (none given: no login exchange).
        let addr = std::env::var("RDB_ADDR").unwrap_or_else(|_| "127.0.0.1:61170".into());
        let login = match (std::env::var("RDB_USER"), std::env::var("RDB_PASSWORD")) {
            (Ok(user), Ok(password)) => Login::Password { user, password },
            _ => Login::Trust,
        };
        let client = Client::connect(
            &addr,
            Options {
                timeout: Some(Duration::from_secs(2)),
                connections: 2,
                login,
                max_message_size: 8_198_000,
                migrations: Some(Migrator::default()),
                auto_migrate: AutoMigrate::Off,
                idle_reconnect: Some(Duration::from_secs(120)),
                ..Options::default()
            },
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

        // A server error (the table is there from an earlier run) is fine; a
        // dead connection is not.
        match client.run(_query).await {
            Ok(_) | Err(rasterizeddb_client::Error::Server(_)) => {}
            Err(err) => panic!("create table: {err}"),
        }

        Arc::new(client)
    })
});

#[derive(Debug)]
struct HttpResponseError {
    status: StatusCode,
    message: String,
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

/// The labels of `build_query`'s select list, in its order: the native
/// protocol sends no column names, so a row is read by position.
const LABELS: [&str; 5] = ["id", "salary", "job title", "location", "project"];

fn render_rows(rows: &Rows) -> Result<Bytes, io::Error> {
    let mut output = String::new();
    if rows.is_empty() {
        output.push_str("No rows returned.");
    }
    for row in rows.iter() {
        let values = row
            .values()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        for (label, value) in LABELS.iter().zip(&values) {
            let _ = write!(&mut output, "{label}: {value}\t");
        }
    }
    Ok(Bytes::from(output))
}

/// A database error as an HTTP one: 502 for an error the server answered
/// with, 503 for one that never reached an answer.
fn db_error(err: rasterizeddb_client::Error) -> HttpResponseError {
    HttpResponseError {
        status: if matches!(err, rasterizeddb_client::Error::Server(_)) {
            StatusCode::BAD_GATEWAY
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        message: format!("db error: {err}"),
    }
}

async fn fetch_response_body(id: u64) -> Result<Bytes, HttpResponseError> {
    let query = build_query(id);
    let client = CLIENT.force().await;

    let rows = client.run(&query).await.map_err(db_error)?;

    render_rows(&rows).map_err(|err| HttpResponseError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("failed to render response: {err}"),
    })
}

/// The next unused id. `employees` has no serial column, so ids are handed
/// out here, starting after the largest one in the table when the first row
/// is added. Only this process counts: two copies of the server adding at
/// once would hand out the same ids.
static NEXT_ID: OnceCell<AtomicU64> = OnceCell::const_new();

/// Reserves `count` consecutive ids and returns the first.
async fn reserve_ids(client: &Client, count: u64) -> Result<u64, HttpResponseError> {
    let next = NEXT_ID
        .get_or_try_init(|| async {
            let rows = client
                .run("SELECT MAX(id) FROM employees")
                .await
                .map_err(db_error)?;
            // An empty table answers NULL (or no row at all).
            let max: Option<u64> = match rows.get(0) {
                Some(row) => row.get(0).map_err(|err| HttpResponseError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: format!("could not read MAX(id): {err}"),
                })?,
                None => None,
            };
            Ok::<_, HttpResponseError>(AtomicU64::new(max.map_or(1, |max| max + 1)))
        })
        .await?;
    Ok(next.fetch_add(count, Ordering::Relaxed))
}

/// The most rows one request may add.
const MAX_ADD: u64 = 100_000;
/// Rows per `INSERT` statement.
const INSERT_BATCH: usize = 1_000;

/// The column list every insert names, in the order `person_params` gives.
const INSERT_COLUMNS: &str = "id, name, job_title, salary, department, age, manager, location, \
     hire_date, degree, skills, current_project, performance_score, is_active, created_at, \
     updated_at, is_fired";
const INSERT_WIDTH: usize = 17;

fn person_params(id: u64, p: &Person) -> [Param<'_>; INSERT_WIDTH] {
    [
        Param::U64(id),
        Param::Text(&p.name),
        Param::Text(&p.job_title),
        Param::F32(p.salary),
        Param::Text(&p.department),
        Param::I32(p.age as i32),
        Param::Text(&p.manager),
        Param::Text(&p.location),
        Param::U64(p.hire_date),
        Param::Text(&p.degree),
        Param::Text(&p.skills),
        Param::Text(&p.current_project),
        Param::F32(p.performance_score),
        Param::Bool(p.is_active),
        Param::U64(p.created_at),
        Param::U64(p.updated_at),
        Param::Bool(p.is_fired),
    ]
}

/// `INSERT INTO employees (...) VALUES ($1, ..., $17), ($18, ...), ...` for
/// `rows` rows.
fn insert_statement(rows: usize) -> String {
    let mut sql = String::with_capacity(96 + rows * INSERT_WIDTH * 6);
    let _ = write!(&mut sql, "INSERT INTO employees ({INSERT_COLUMNS}) VALUES ");
    for row in 0..rows {
        sql.push_str(if row == 0 { "(" } else { ", (" });
        for column in 0..INSERT_WIDTH {
            if column > 0 {
                sql.push_str(", ");
            }
            let _ = write!(&mut sql, "${}", row * INSERT_WIDTH + column + 1);
        }
        sql.push(')');
    }
    sql
}

/// Adds `count` generated employees and answers with the ids they got.
async fn add_rows(count: u64) -> Result<Bytes, HttpResponseError> {
    if count == 0 || count > MAX_ADD {
        return Err(HttpResponseError {
            status: StatusCode::BAD_REQUEST,
            message: format!("count must be between 1 and {MAX_ADD}"),
        });
    }

    let client = CLIENT.force().await;
    let first = reserve_ids(client, count).await?;
    let last = first + count - 1;

    let people: Vec<Person> = (0..count).map(|_| generate_person()).collect();
    let mut added = 0u64;
    for (chunk_index, chunk) in people.chunks(INSERT_BATCH).enumerate() {
        let chunk_first = first + (chunk_index * INSERT_BATCH) as u64;
        let params: Vec<Param<'_>> = chunk
            .iter()
            .enumerate()
            .flat_map(|(i, person)| person_params(chunk_first + i as u64, person))
            .collect();
        match client.execute(&insert_statement(chunk.len()), &params).await {
            Ok(n) => added += n,
            Err(err) => {
                let mut error = db_error(err);
                error.message = format!(
                    "{} (after {added} of {count} rows were added, ids {first}..={})",
                    error.message,
                    first + added.saturating_sub(1)
                );
                return Err(error);
            }
        }
    }

    Ok(Bytes::from(format!("added {added} rows, ids {first}..={last}")))
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/get/{id}", get(query))
        // GET as well as POST, so a browser can call it like `/get`.
        .route("/add", get(add_one).post(add_one))
        .route("/add/{count}", get(add_many).post(add_many))
        // The in-memory key/value store (`KV ...` over the native protocol).
        .route("/kv/{key}", get(kv_get))
        .route("/kv/set/{key}/{value}", get(kv_set));

    // `RDB_WEB_ADDR` to listen elsewhere.
    let listen = std::env::var("RDB_WEB_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&listen).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn add_one() -> Response {
    add_response(1).await
}

async fn add_many(Path(count): Path<u64>) -> Response {
    add_response(count).await
}

async fn add_response(count: u64) -> Response {
    match add_rows(count).await {
        Ok(body) => text_response(StatusCode::OK, Body::from(body)),
        Err(err) => text_response(err.status, Body::from(err.message)),
    }
}

/// `?ttl=<seconds>` from the query string, if given. `Err` names a bad one.
fn ttl_from_query(query: Option<&str>) -> Result<Option<Duration>, Response> {
    let Some(query) = query else {
        return Ok(None);
    };
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("ttl=") {
            return match value.parse::<u64>() {
                Ok(secs) if secs > 0 => Ok(Some(Duration::from_secs(secs))),
                _ => Err(text_response(
                    StatusCode::BAD_REQUEST,
                    "ttl must be a whole number of seconds above 0",
                )),
            };
        }
    }
    Ok(None)
}

async fn kv_store(key: &str, value: &[u8], ttl: Option<Duration>) -> Response {
    let client = CLIENT.force().await;
    match client.kv().set(key.as_bytes(), value, ttl).await {
        Ok(()) => {
            let expiry = match ttl {
                Some(ttl) => format!(", expires in {}s", ttl.as_secs()),
                None => String::new(),
            };
            text_response(
                StatusCode::OK,
                format!("set {key} ({} bytes{expiry})", value.len()),
            )
        }
        Err(err) => {
            let err = db_error(err);
            text_response(err.status, err.message)
        }
    }
}

/// `GET /kv/set/{key}/{value}[?ttl=<seconds>]`.
async fn kv_set(
    Path((key, value)): Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Response {
    match ttl_from_query(query.as_deref()) {
        Ok(ttl) => kv_store(&key, value.as_bytes(), ttl).await,
        Err(response) => response,
    }
}

/// `GET /kv/{key}`: the value, or 404 when the key is not there (never set,
/// deleted, or expired).
async fn kv_get(Path(key): Path<String>) -> Response {
    let client = CLIENT.force().await;
    match client.kv().get(key.as_bytes()).await {
        Ok(Some(value)) => text_response(StatusCode::OK, value),
        Ok(None) => text_response(StatusCode::NOT_FOUND, format!("no key {key}")),
        Err(err) => {
            let err = db_error(err);
            text_response(err.status, err.message)
        }
    }
}

// get id from query param
async fn query(Path(id): Path<u64>) -> Response {
    match fetch_response_body(id).await {
        Ok(body) => text_response(StatusCode::OK, Body::from(body)),
        Err(err) => text_response(err.status, Body::from(err.message)),
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
