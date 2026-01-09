use rclite::Arc;

use crate::core::{
    database::Database,
    processor::concurrent_processor,
    rql::{
        executor,
        lexer_s1::{recognize_query_purpose, QueryPurpose},
    },
    row::row::{vec_into_rows, Row},
};

#[derive(Debug, Clone)]
struct Employee {
    id: u64,
    name: String,
    job_title: String,
    salary: f32,
    department: String,
    age: i32,
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

fn decode_error_message(result: &[u8]) -> String {
    if result.is_empty() {
        return "<empty result>".to_string();
    }
    if result[0] != 3 {
        return format!("<not an error result: status {}>", result[0]);
    }
    String::from_utf8_lossy(&result[1..]).to_string()
}

fn assert_ok(result: &[u8], context: &str) {
    assert!(
        !result.is_empty(),
        "{context}: expected non-empty result"
    );
    assert_eq!(
        result[0],
        0,
        "{context}: expected OK (0), got status {} (err={})",
        result[0],
        decode_error_message(result)
    );
}

fn assert_rows(result: &[u8], context: &str) -> Vec<Row> {
    assert!(
        !result.is_empty(),
        "{context}: expected non-empty result"
    );
    assert_eq!(
        result[0],
        2,
        "{context}: expected RowsResult (2), got status {} (err={})",
        result[0],
        decode_error_message(result)
    );
    vec_into_rows(&result[1..]).expect("decode rows")
}

fn assert_error_contains(result: &[u8], needle: &str, context: &str) {
    assert!(
        !result.is_empty(),
        "{context}: expected non-empty error result"
    );
    assert_eq!(
        result[0],
        3,
        "{context}: expected Error (3), got status {}",
        result[0]
    );
    let msg = decode_error_message(result);
    assert!(
        msg.contains(needle),
        "{context}: expected error to contain {needle:?}, got {msg:?}"
    );
}

fn mb_to_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 bytes"))
}

fn mb_to_i32(bytes: &[u8]) -> i32 {
    i32::from_le_bytes(bytes.try_into().expect("i32 bytes"))
}

fn mb_to_f32(bytes: &[u8]) -> f32 {
    f32::from_le_bytes(bytes.try_into().expect("f32 bytes"))
}

fn mb_to_bool(bytes: &[u8]) -> bool {
    bytes.first().copied().unwrap_or(0) != 0
}

fn mb_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

async fn exec(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("recognize_query_purpose failed for SQL: {sql}"));
    executor::execute(purpose, database.clone()).await
}

// Extended coverage test: end-to-end queries on an "employees" table.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_employees_end_to_end_queries() {
    // The query engine expects these globals to be initialized by the binary.
    // Tests run the engine directly, so set sane defaults here.
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = crate::ENABLE_SEMANTICS.get_or_init(|| true);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    // Keep the directory around (Windows file-locks can make auto-cleanup flaky).
    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_extcov_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    // Create table as specified.
    let create_sql = r##"CREATE TABLE employees (
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
        );"##;

    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE employees");

    // Sanity: table exists in the DB map.
    let table = database
        .tables
        .get("employees")
        .expect("employees table should exist after CREATE TABLE")
        .clone();

    assert_eq!(
        table.schema.fields.len(),
        17,
        "employees schema should have 17 columns"
    );

    // Insert random-ish values a few times (seeded for determinism).
    let mut rng = fastrand::Rng::with_seed(0xC0FFEE_u64);

    let departments = ["Engineering", "HR", "Sales", "Finance", "IT"];
    let job_titles = ["Engineer", "Manager", "Analyst", "Director"];
    let locations = ["NYC", "SF", "Remote", "LDN"];
    let degrees = ["BS", "MS", "PhD", "None"];
    let skills = ["Rust", "SQL", "Ops", "ML", "UI"];
    let projects = ["Apollo", "Zeus", "Hermes", "Athena"];

    let rows_to_insert = 75_usize;
    let mut expected: Vec<Employee> = Vec::with_capacity(rows_to_insert);

    for i in 0..rows_to_insert {
        let id = (i as u64) + 1;

        // Ensure salaries are unique to make ORDER BY assertions deterministic.
        let salary = (20_000.0 + rng.f32() * 120_000.0) + (i as f32) * 0.01;
        let performance_score = (rng.f32() * 100.0) + (i as f32) * 0.001;

        let age = rng.i32(18..=70);

        let department = departments[rng.usize(..departments.len())].to_string();
        let job_title = job_titles[rng.usize(..job_titles.len())].to_string();
        let location = locations[rng.usize(..locations.len())].to_string();
        let degree = degrees[rng.usize(..degrees.len())].to_string();
        let skill = skills[rng.usize(..skills.len())].to_string();
        let project = projects[rng.usize(..projects.len())].to_string();

        let name = format!("Emp{id:04}");
        let manager = format!("Mgr{}", rng.u32(1..=10));

        let hire_date = 1_600_000_000_u64 + rng.u64(0..=1_000_000);
        let created_at = 1_700_000_000_u64 + rng.u64(0..=1_000_000);
        let updated_at = created_at + rng.u64(0..=100_000);

        let is_active = rng.bool();
        let is_fired = rng.bool() && !is_active;

        let employee = Employee {
            id,
            name: name.clone(),
            job_title: job_title.clone(),
            salary,
            department: department.clone(),
            age,
            manager: manager.clone(),
            location: location.clone(),
            hire_date,
            degree: degree.clone(),
            skills: skill.clone(),
            current_project: project.clone(),
            performance_score,
            is_active,
            created_at,
            updated_at,
            is_fired,
        };

        let insert_sql = format!(
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
            employee.id,
            employee.name,
            employee.job_title,
            employee.salary,
            employee.department,
            employee.age,
            employee.manager,
            employee.location,
            employee.hire_date,
            employee.degree,
            employee.skills,
            employee.current_project,
            employee.performance_score,
            if employee.is_active { 1 } else { 0 },
            employee.created_at,
            employee.updated_at,
            if employee.is_fired { 1 } else { 0 },
        );

        let res = exec(&database, &insert_sql).await;
        assert_ok(&res, "INSERT INTO employees");
        expected.push(employee);
    }

    // Query 1: fetch all (no ORDER/LIMIT), assert every row matches by id.
    let q1 = r##"
        SELECT id, salary, age, name FROM employees
        WHERE id > 0 AND id < 1000000
    "##;
    let res = exec(&database, q1).await;
    let rows = assert_rows(&res, "SELECT all employees columns subset");
    assert_eq!(rows.len(), rows_to_insert, "row count mismatch");

    let schema = &table.schema.fields;

    let mut expected_by_id = std::collections::HashMap::<u64, Employee>::new();
    for e in &expected {
        expected_by_id.insert(e.id, e.clone());
    }

    for row in rows {
        let id_mb = row
            .get_column("id", schema)
            .expect("row should include id");
        let id = mb_to_u64(id_mb.into_slice());
        let exp = expected_by_id.get(&id).expect("id should exist");

        let salary_mb = row
            .get_column("salary", schema)
            .expect("row should include salary");
        let age_mb = row.get_column("age", schema).expect("row should include age");
        let name_mb = row
            .get_column("name", schema)
            .expect("row should include name");

        let salary = mb_to_f32(salary_mb.into_slice());
        let age = mb_to_i32(age_mb.into_slice());
        let name = mb_to_string(name_mb.into_slice());

        assert!(
            (salary - exp.salary).abs() < 0.0001,
            "salary mismatch for id {id}"
        );
        assert_eq!(age, exp.age, "age mismatch for id {id}");
        assert_eq!(name, exp.name, "name mismatch for id {id}");
    }

    // Query 2: WHERE with mixed numeric + string equality.
    let q2 = r##"
        SELECT id, department, age, is_active FROM employees
        WHERE department = 'Engineering' AND age >= 30
    "##;
    let res = exec(&database, q2).await;
    let rows = assert_rows(&res, "WHERE department + age");

    let expected_q2: Vec<&Employee> = expected
        .iter()
        .filter(|e| e.department == "Engineering" && e.age >= 30)
        .collect();

    assert_eq!(
        rows.len(),
        expected_q2.len(),
        "filtered row count mismatch"
    );

    for row in rows {
        let id = mb_to_u64(
            row.get_column("id", schema)
                .expect("id")
                .into_slice(),
        );
        let department = mb_to_string(
            row.get_column("department", schema)
                .expect("department")
                .into_slice(),
        );
        let age = mb_to_i32(
            row.get_column("age", schema)
                .expect("age")
                .into_slice(),
        );
        let is_active = mb_to_bool(
            row.get_column("is_active", schema)
                .expect("is_active")
                .into_slice(),
        );

        let exp = expected_by_id.get(&id).expect("id exists");
        assert_eq!(department, exp.department);
        assert_eq!(age, exp.age);
        assert_eq!(is_active, exp.is_active);
        assert_eq!(department, "Engineering");
        assert!(age >= 30);
    }

    // Query 3: ORDER BY + LIMIT (deterministic due to unique salary).
    let limit_n = 10_u64;
    let q3 = format!(
        r##"
        SELECT id, salary, name FROM employees
        WHERE id > 0
        ORDER BY salary
        LIMIT {limit_n}
    "##
    );
    let res = exec(&database, &q3).await;
    let rows = assert_rows(&res, "ORDER BY salary LIMIT");
    assert_eq!(rows.len(), limit_n as usize);

    let mut expected_sorted = expected.clone();
    expected_sorted.sort_by(|a, b| a.salary.partial_cmp(&b.salary).unwrap());

    for (idx, row) in rows.iter().enumerate() {
        let id = mb_to_u64(
            row.get_column("id", schema)
                .expect("id")
                .into_slice(),
        );
        let salary = mb_to_f32(
            row.get_column("salary", schema)
                .expect("salary")
                .into_slice(),
        );
        let name = mb_to_string(
            row.get_column("name", schema)
                .expect("name")
                .into_slice(),
        );

        let exp = &expected_sorted[idx];
        assert_eq!(id, exp.id, "id mismatch at sorted index {idx}");
        assert!(
            (salary - exp.salary).abs() < 0.0001,
            "salary mismatch at sorted index {idx}"
        );
        assert_eq!(name, exp.name, "name mismatch at sorted index {idx}");

        if idx > 0 {
            let prev_salary = mb_to_f32(
                rows[idx - 1]
                    .get_column("salary", schema)
                    .expect("salary")
                    .into_slice(),
            );
            assert!(prev_salary <= salary, "salary not non-decreasing");
        }
    }

    // Query 4: LIMIT without ORDER BY (don’t assume order, just assert row count and column presence).
    let q4 = r##"SELECT id FROM employees LIMIT 5"##;
    let res = exec(&database, q4).await;
    let rows = assert_rows(&res, "LIMIT only");
    assert_eq!(rows.len(), 5);
    for row in rows {
        let id = mb_to_u64(
            row.get_column("id", schema)
                .expect("id")
                .into_slice(),
        );
        assert!(expected_by_id.contains_key(&id), "id should be known");
    }

    // Query 5: ORDER BY on another numeric column.
    let q5 = r##"SELECT id, age FROM employees WHERE id > 0 ORDER BY age LIMIT 20"##;
    let res = exec(&database, q5).await;
    let rows = assert_rows(&res, "ORDER BY age LIMIT");
    assert_eq!(rows.len(), 20);
    let mut prev_age: Option<i32> = None;
    for row in rows {
        let age = mb_to_i32(
            row.get_column("age", schema)
                .expect("age")
                .into_slice(),
        );
        if let Some(prev) = prev_age {
            assert!(prev <= age, "age not non-decreasing");
        }
        prev_age = Some(age);
    }

    // Query 6: invalid column should return an error.
    let q6 = r##"SELECT does_not_exist FROM employees WHERE id > 0"##;
    let res = exec(&database, q6).await;
    // executor currently uses a generic message on schema mismatch.
    assert_error_contains(&res, "schema mismatch", "invalid column SELECT");

    // Query 7: invalid ORDER BY column should also fail.
    let q7 = r##"SELECT id FROM employees WHERE id > 0 ORDER BY does_not_exist LIMIT 1"##;
    let res = exec(&database, q7).await;
    assert_error_contains(&res, "schema mismatch", "invalid ORDER BY");

    // Query 8: UPDATE a single row and verify the mutation via SELECT.
    let q8 = r##"UPDATE employees SET salary = 12345.67, department = 'Engineering', is_active = 1 WHERE id = 1"##;
    let res = exec(&database, q8).await;
    assert_ok(&res, "UPDATE single employee");

    let q8_verify = r##"SELECT id, salary, department, is_active FROM employees WHERE id = 1"##;
    let res = exec(&database, q8_verify).await;
    let rows = assert_rows(&res, "SELECT updated employee");
    assert_eq!(rows.len(), 1, "expected exactly 1 updated row for id=1");
    let row = &rows[0];
    let id = mb_to_u64(
        row.get_column("id", schema)
            .expect("id")
            .into_slice(),
    );
    assert_eq!(id, 1);
    let salary = mb_to_f32(
        row.get_column("salary", schema)
            .expect("salary")
            .into_slice(),
    );
    assert!((salary - 12345.67).abs() < 0.01, "updated salary mismatch");
    let department = mb_to_string(
        row.get_column("department", schema)
            .expect("department")
            .into_slice(),
    );
    assert_eq!(department, "Engineering");
    let is_active = mb_to_bool(
        row.get_column("is_active", schema)
            .expect("is_active")
            .into_slice(),
    );
    assert!(is_active, "expected updated is_active=true");

    // Query 9: UPDATE multiple rows (string column), then verify all matching rows changed.
    let q9 = r##"UPDATE employees SET location = 'UPDATED_LOC' WHERE department = 'Sales'"##;
    let res = exec(&database, q9).await;
    assert_ok(&res, "UPDATE employees location for Sales");

    let q9_verify = r##"SELECT id, department, location FROM employees WHERE department = 'Sales'"##;
    let res = exec(&database, q9_verify).await;
    let rows = assert_rows(&res, "SELECT updated Sales employees");
    for row in rows {
        let department = mb_to_string(
            row.get_column("department", schema)
                .expect("department")
                .into_slice(),
        );
        assert_eq!(department, "Sales");
        let location = mb_to_string(
            row.get_column("location", schema)
                .expect("location")
                .into_slice(),
        );
        assert_eq!(location, "UPDATED_LOC");
    }

    // Query 10: DELETE a range of rows, then verify they are gone and total row count decreased.
    let q10 = r##"DELETE FROM employees WHERE id >= 71 AND id <= 75"##;
    let res = exec(&database, q10).await;
    assert_ok(&res, "DELETE employees id 71..75");

    let q10_verify_absent = r##"SELECT id FROM employees WHERE id >= 71 AND id <= 75"##;
    let res = exec(&database, q10_verify_absent).await;
    let rows = assert_rows(&res, "SELECT deleted employees should be absent");
    assert_eq!(rows.len(), 0, "deleted rows should not be returned");

    let q10_verify_count = r##"SELECT id FROM employees WHERE id > 0"##;
    let res = exec(&database, q10_verify_count).await;
    let rows = assert_rows(&res, "SELECT all employees after DELETE");
    assert_eq!(rows.len(), rows_to_insert - 5, "row count after DELETE mismatch");

    // Query 11: concurrent mixed workload (INSERT/UPDATE/DELETE/SELECT), then verify final state.
    // Keep operations on mostly-disjoint row sets to make the result deterministic.
    let base_count_after_q10 = rows_to_insert - 5;
    let concurrent_insert_n: u64 = 20;
    let insert_start_id: u64 = 1001;
    let delete_start_id: u64 = 50;
    let delete_end_id: u64 = 55;
    let update_start_id: u64 = 2;
    let update_end_id: u64 = 11;
    let update_salary: f32 = 77777.0;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Concurrent inserts.
    for i in 0..concurrent_insert_n {
        let database = database.clone();
        let id = insert_start_id + i;
        handles.push(tokio::spawn(async move {
            let insert_sql = format!(
                r##"INSERT INTO employees (id, name, job_title, salary, department, age, manager, location, hire_date, degree, skills, current_project, performance_score, is_active, created_at, updated_at, is_fired)
                    VALUES ({}, 'Conc{:04}', 'Engineer', 55555.5, 'Engineering', 33, 'ConcMgr', 'Remote', 1600000000, 'BS', 'Rust', 'Apollo', 99.9, 1, 1700000000, 1700000001, 0);"##,
                id, id
            );
            let res = exec(&database, &insert_sql).await;
            assert_ok(&res, "concurrent INSERT INTO employees");
        }));
    }

    // Concurrent updates on an existing, non-deleted range.
    {
        let database = database.clone();
        handles.push(tokio::spawn(async move {
            let update_sql = format!(
                r##"UPDATE employees SET manager = 'CONC_MGR', salary = {} WHERE id >= {} AND id <= {}"##,
                update_salary, update_start_id, update_end_id
            );
            let res = exec(&database, &update_sql).await;
            assert_ok(&res, "concurrent UPDATE employees");
        }));
    }

    // Concurrent deletes on a disjoint range.
    {
        let database = database.clone();
        handles.push(tokio::spawn(async move {
            let delete_sql = format!(
                r##"DELETE FROM employees WHERE id >= {} AND id <= {}"##,
                delete_start_id, delete_end_id
            );
            let res = exec(&database, &delete_sql).await;
            assert_ok(&res, "concurrent DELETE FROM employees");
        }));
    }

    // Concurrent queries (don’t assume stable row counts mid-flight; just validate they succeed).
    for _ in 0..8 {
        let database = database.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                let res = exec(&database, "SELECT id FROM employees WHERE id > 0 LIMIT 25").await;
                let _rows = assert_rows(&res, "concurrent SELECT LIMIT");
            }
        }));
    }

    for h in handles {
        h.await.expect("concurrent task panicked");
    }

    // Verify: inserts exist.
    let q11_verify_inserts = format!(
        r##"SELECT id FROM employees WHERE id >= {} AND id < {}"##,
        insert_start_id,
        insert_start_id + concurrent_insert_n
    );
    let res = exec(&database, &q11_verify_inserts).await;
    let rows = assert_rows(&res, "verify concurrent inserts");
    assert_eq!(rows.len(), concurrent_insert_n as usize, "inserted row count mismatch");

    // Verify: updates applied.
    let q11_verify_updates = format!(
        r##"SELECT id, manager, salary FROM employees WHERE id >= {} AND id <= {}"##,
        update_start_id, update_end_id
    );
    let res = exec(&database, &q11_verify_updates).await;
    let rows = assert_rows(&res, "verify concurrent updates");
    assert_eq!(rows.len(), (update_end_id - update_start_id + 1) as usize);
    for row in rows {
        let manager = mb_to_string(
            row.get_column("manager", schema)
                .expect("manager")
                .into_slice(),
        );
        let salary = mb_to_f32(
            row.get_column("salary", schema)
                .expect("salary")
                .into_slice(),
        );
        assert_eq!(manager, "CONC_MGR");
        assert!((salary - update_salary).abs() < 0.01, "concurrent updated salary mismatch");
    }

    // Verify: deletes applied.
    let q11_verify_deletes = format!(
        r##"SELECT id FROM employees WHERE id >= {} AND id <= {}"##,
        delete_start_id, delete_end_id
    );
    let res = exec(&database, &q11_verify_deletes).await;
    let rows = assert_rows(&res, "verify concurrent deletes");
    assert_eq!(rows.len(), 0, "deleted rows should be absent");

    // Verify: final row count matches expected.
    let expected_final_count = base_count_after_q10 + (concurrent_insert_n as usize)
        - ((delete_end_id - delete_start_id + 1) as usize);
    let res = exec(&database, "SELECT id FROM employees WHERE id > 0").await;
    let rows = assert_rows(&res, "final SELECT all employees after concurrent workload");
    assert_eq!(rows.len(), expected_final_count, "final row count mismatch");

    // Ensure we used the executor path the user wanted (not the network client).
    let _ = QueryPurpose::CreateTable(String::new());
}

async fn run_rowcount_case(rows_to_insert: usize) {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = crate::ENABLE_SEMANTICS.get_or_init(|| true);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_rowcount_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    let create_sql = r##"CREATE TABLE tiny_counts (
            id UBIGINT,
            age INTEGER
        );"##;

    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE tiny_counts");

    for i in 0..rows_to_insert {
        let id = (i as u64) + 1;
        let age = (i % 97) as i32;
        let insert_sql = format!(
            r##"INSERT INTO tiny_counts (id, age) VALUES ({}, {});"##,
            id, age
        );
        let res = exec(&database, &insert_sql).await;
        assert_ok(&res, "INSERT tiny_counts");
    }

    let table = database
        .tables
        .get("tiny_counts")
        .expect("tiny_counts table should exist after CREATE TABLE")
        .clone();

    let schema = &table.schema.fields;

    let q_all = format!(
        r##"SELECT id, age FROM tiny_counts WHERE id >= 1 AND id <= {}"##,
        (rows_to_insert as u64) + 5
    );
    let res = exec(&database, &q_all).await;
    let rows = assert_rows(&res, "rowcount full scan");
    assert_eq!(
        rows.len(),
        rows_to_insert,
        "rowcount={} full scan length",
        rows_to_insert
    );

    let subset_threshold = std::cmp::max(1, rows_to_insert / 2);
    let q_subset = format!(
        r##"SELECT id FROM tiny_counts WHERE id > {}"##,
        subset_threshold
    );
    let res = exec(&database, &q_subset).await;
    let subset_rows = assert_rows(&res, "rowcount subset");
    let expected_subset = rows_to_insert - subset_threshold;
    assert_eq!(
        subset_rows.len(),
        expected_subset,
        "rowcount={} subset length",
        rows_to_insert
    );

    let q_order = r##"SELECT id FROM tiny_counts WHERE id >= 1 ORDER BY id LIMIT 5"##;
    let res = exec(&database, q_order).await;
    let ordered_rows = assert_rows(&res, "rowcount order by limit");
    let expected_order_len = std::cmp::min(5, rows_to_insert);
    assert_eq!(ordered_rows.len(), expected_order_len);
}

#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_rowcount_varied_sizes() {
    for rows_to_insert in [100usize, 200, 256, 300, 1000, 5000, 10_000] {
        run_rowcount_case(rows_to_insert).await;
    }
}
