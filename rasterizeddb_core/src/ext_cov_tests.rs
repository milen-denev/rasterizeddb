use rclite::Arc;

use crate::core::{
    database::Database,
    processor::concurrent_processor,
    row::row::{Row, vec_into_rows},
    rql::{
        executor,
        lexer_s1::{QueryPurpose, recognize_query_purpose},
    },
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
    assert!(!result.is_empty(), "{context}: expected non-empty result");
    assert_eq!(
        result[0],
        0,
        "{context}: expected OK (0), got status {} (err={})",
        result[0],
        decode_error_message(result)
    );
}

fn assert_rows(result: &[u8], context: &str) -> Vec<Row> {
    assert!(!result.is_empty(), "{context}: expected non-empty result");
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
        result[0], 3,
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

    // The DDL declares no `NOT NULL`, so every column is nullable — Postgres'
    // default — and each carries a hidden nullability companion. The assertion
    // is about the declared columns, so it counts the visible ones.
    let visible = crate::core::rql::nullable::visible_fields(&table.schema.fields);
    assert_eq!(visible.len(), 17, "employees schema should have 17 columns");
    assert_eq!(
        table.schema.fields.len(),
        34,
        "each nullable column adds one companion"
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
        let id_mb = row.get_column("id", schema).expect("row should include id");
        let id = mb_to_u64(id_mb.into_slice());
        let exp = expected_by_id.get(&id).expect("id should exist");

        let salary_mb = row
            .get_column("salary", schema)
            .expect("row should include salary");
        let age_mb = row
            .get_column("age", schema)
            .expect("row should include age");
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

    assert_eq!(rows.len(), expected_q2.len(), "filtered row count mismatch");

    for row in rows {
        let id = mb_to_u64(row.get_column("id", schema).expect("id").into_slice());
        let department = mb_to_string(
            row.get_column("department", schema)
                .expect("department")
                .into_slice(),
        );
        let age = mb_to_i32(row.get_column("age", schema).expect("age").into_slice());
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
        let id = mb_to_u64(row.get_column("id", schema).expect("id").into_slice());
        let salary = mb_to_f32(
            row.get_column("salary", schema)
                .expect("salary")
                .into_slice(),
        );
        let name = mb_to_string(row.get_column("name", schema).expect("name").into_slice());

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
        let id = mb_to_u64(row.get_column("id", schema).expect("id").into_slice());
        assert!(expected_by_id.contains_key(&id), "id should be known");
    }

    // Query 5: ORDER BY on another numeric column.
    let q5 = r##"SELECT id, age FROM employees WHERE id > 0 ORDER BY age LIMIT 20"##;
    let res = exec(&database, q5).await;
    let rows = assert_rows(&res, "ORDER BY age LIMIT");
    assert_eq!(rows.len(), 20);
    let mut prev_age: Option<i32> = None;
    for row in rows {
        let age = mb_to_i32(row.get_column("age", schema).expect("age").into_slice());
        if let Some(prev) = prev_age {
            assert!(prev <= age, "age not non-decreasing");
        }
        prev_age = Some(age);
    }

    // Query 6: invalid column should return an error.
    let q6 = r##"SELECT does_not_exist FROM employees WHERE id > 0"##;
    let res = exec(&database, q6).await;
    // The parser's own message reaches the client now, so the error names the
    // column that could not be resolved instead of reporting a generic mismatch.
    assert_error_contains(&res, "does_not_exist", "invalid column SELECT");

    // Query 7: invalid ORDER BY column should also fail.
    let q7 = r##"SELECT id FROM employees WHERE id > 0 ORDER BY does_not_exist LIMIT 1"##;
    let res = exec(&database, q7).await;
    assert_error_contains(&res, "does_not_exist", "invalid ORDER BY");

    // Query 8: UPDATE a single row and verify the mutation via SELECT.
    let q8 = r##"UPDATE employees SET salary = 12345.67, department = 'Engineering', is_active = 1 WHERE id = 1"##;
    let res = exec(&database, q8).await;
    assert_ok(&res, "UPDATE single employee");

    let q8_verify = r##"SELECT id, salary, department, is_active FROM employees WHERE id = 1"##;
    let res = exec(&database, q8_verify).await;
    let rows = assert_rows(&res, "SELECT updated employee");
    assert_eq!(rows.len(), 1, "expected exactly 1 updated row for id=1");
    let row = &rows[0];
    let id = mb_to_u64(row.get_column("id", schema).expect("id").into_slice());
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

    let q9_verify =
        r##"SELECT id, department, location FROM employees WHERE department = 'Sales'"##;
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
    assert_eq!(
        rows.len(),
        rows_to_insert - 5,
        "row count after DELETE mismatch"
    );

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
    assert_eq!(
        rows.len(),
        concurrent_insert_n as usize,
        "inserted row count mismatch"
    );

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
        assert!(
            (salary - update_salary).abs() < 0.01,
            "concurrent updated salary mismatch"
        );
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

    let _table = database
        .tables
        .get("tiny_counts")
        .expect("tiny_counts table should exist after CREATE TABLE")
        .clone();

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

/// A WHERE expression that is undefined for some row must fail the query.
///
/// These used to be guarded by `debug_assert!`, which release builds compile
/// away. `WHERE age / 0` then reached a real integer division and panicked the
/// tokio worker evaluating the row. Because workers accumulate matches locally
/// and send them once at the end, and `join_all` discarded the `JoinError`, the
/// query reported *success* with whatever the surviving workers had — a
/// silently truncated result set.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_undefined_where_expression_reports_error() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_eval_err_")
        .tempdir()
        .expect("tempdir")
        .keep();
    let db_dir_str = db_dir.to_str().expect("utf-8 path").to_string();
    let database = Arc::new(Database::new(&db_dir_str).await);

    let res = exec(
        &database,
        r##"CREATE TABLE eval_err (id UBIGINT, age INTEGER, divisor INTEGER);"##,
    )
    .await;
    assert_ok(&res, "CREATE TABLE eval_err");

    // Row 7 has divisor = 0; every other row divides cleanly.
    for i in 1..=20u64 {
        let divisor = if i == 7 { 0 } else { 2 };
        let res = exec(
            &database,
            &format!(
                "INSERT INTO eval_err (id, age, divisor) VALUES ({}, {}, {});",
                i,
                i * 3,
                divisor
            ),
        )
        .await;
        assert_ok(&res, "INSERT eval_err");
    }

    // Constant zero divisor.
    let res = exec(&database, "SELECT id FROM eval_err WHERE age / 0 = 1").await;
    assert_error_contains(&res, "division by zero", "constant divide by zero");

    // Zero divisor reached only via one row's data — the case that would
    // otherwise return a short result set and look successful.
    let res = exec(&database, "SELECT id FROM eval_err WHERE age / divisor > 1").await;
    assert_error_contains(&res, "division by zero", "data-dependent divide by zero");

    // Integer overflow: release builds have `overflow-checks = false`, so this
    // used to wrap around and quietly return the wrong rows.
    let res = exec(
        &database,
        "SELECT id FROM eval_err WHERE age * 2147483647 > 1",
    )
    .await;
    assert_error_contains(&res, "integer out of range", "multiply overflow");

    // A well-defined expression over the same table still works, so the guards
    // above are not rejecting ordinary arithmetic.
    let res = exec(&database, "SELECT id FROM eval_err WHERE age / 3 = 5").await;
    let rows = assert_rows(&res, "valid division still works");
    assert_eq!(rows.len(), 1, "id 5 has age 15, 15 / 3 = 5");
}

/// Concurrent INSERTs must each end up pointing at their own bytes.
///
/// A row's byte offset is only correct if it comes from the append that wrote
/// the row. Deriving it from a separately reserved counter races: two inserts
/// can reserve in one order and append in the other, after which every pointer
/// describes a neighbour's bytes.
///
/// The payload strings are deliberately different lengths, so rows differ in
/// size and a swapped offset produces readable-but-wrong data rather than
/// silently landing on a same-sized row. Each row is checked for exact
/// id -> payload correspondence, not merely counted, since a count-only
/// assertion passes even when every row is corrupt.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_concurrent_inserts_keep_row_offsets() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_concurrent_offsets_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    let create_sql = r##"CREATE TABLE conc_rows (
            id UBIGINT,
            payload VARCHAR,
            age INTEGER
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE conc_rows");

    // `id` determines the payload length, so each row has a distinct size.
    fn payload_for(id: u64) -> String {
        let len = 1 + (id as usize % 37);
        std::iter::repeat(char::from(b'a' + (id % 26) as u8))
            .take(len)
            .collect()
    }

    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 40;
    const TOTAL: u64 = WRITERS * PER_WRITER;

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let database = database.clone();
        handles.push(tokio::spawn(async move {
            for k in 0..PER_WRITER {
                let id = w * PER_WRITER + k + 1;
                let insert_sql = format!(
                    r##"INSERT INTO conc_rows (id, payload, age) VALUES ({}, '{}', {});"##,
                    id,
                    payload_for(id),
                    (id % 97) as i32
                );
                let res = exec(&database, &insert_sql).await;
                assert_ok(&res, "concurrent INSERT conc_rows");
            }
        }));
    }

    for h in handles {
        h.await.expect("insert task panicked");
    }

    let q = format!(
        r##"SELECT id, payload, age FROM conc_rows WHERE id >= 1 AND id <= {}"##,
        TOTAL
    );
    let res = exec(&database, &q).await;
    let rows = assert_rows(&res, "read back concurrent inserts");

    assert_eq!(
        rows.len() as u64,
        TOTAL,
        "every concurrently inserted row must be readable"
    );

    let schema = &database
        .tables
        .get("conc_rows")
        .expect("conc_rows table should exist")
        .schema
        .fields;

    let mut seen: Vec<u64> = Vec::with_capacity(TOTAL as usize);
    for row in rows {
        let id = mb_to_u64(row.get_column("id", schema).expect("id").into_slice());
        let payload = mb_to_string(
            row.get_column("payload", schema)
                .expect("payload")
                .into_slice(),
        );
        let age = mb_to_i32(row.get_column("age", schema).expect("age").into_slice());

        assert_eq!(
            payload,
            payload_for(id),
            "row {} read back another row's payload — its pointer offset is wrong",
            id
        );
        assert_eq!(age, (id % 97) as i32, "row {} read back a wrong age", id);

        seen.push(id);
    }

    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len() as u64,
        TOTAL,
        "ids must be unique and complete after concurrent inserts"
    );
}

/// Every spelling of "give me a page" must return the same page.
///
/// `LIMIT n OFFSET m` is PostgreSQL's own, `OFFSET m ROWS FETCH NEXT n ROWS
/// ONLY` is the SQL:2008 one an ORM or a JDBC driver emits, and `SKIP m TAKE n`
/// is the LINQ-style one. A spelling the parser does not recognise used to be
/// dropped in silence, which returned the whole table where a page was asked
/// for — so the count is asserted as well as the ids.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_pagination_spellings_agree() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_pagination_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    let create_sql = r##"CREATE TABLE paged_rows (
            id UBIGINT,
            age INTEGER
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE paged_rows");

    const TOTAL: u64 = 20;
    for id in 1..=TOTAL {
        let insert_sql = format!(
            r##"INSERT INTO paged_rows (id, age) VALUES ({}, {});"##,
            id,
            (id % 97) as i32
        );
        let res = exec(&database, &insert_sql).await;
        assert_ok(&res, "INSERT paged_rows");
    }

    let table = database
        .tables
        .get("paged_rows")
        .expect("paged_rows table should exist")
        .clone();
    let schema = &table.schema.fields;

    let ids = |rows: Vec<Row>| -> Vec<u64> {
        rows.iter()
            .map(|row| {
                mb_to_u64(
                    row.get_column("id", schema)
                        .expect("row should include id")
                        .into_slice(),
                )
            })
            .collect()
    };

    // Rows 6..=10, written five different ways.
    let third_page = vec![6u64, 7, 8, 9, 10];
    for sql in [
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id LIMIT 5 OFFSET 5",
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY",
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id OFFSET 5 FETCH FIRST 5 ROWS ONLY",
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id SKIP 5 TAKE 5",
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id TAKE 5 SKIP 5",
    ] {
        let res = exec(&database, sql).await;
        let rows = assert_rows(&res, sql);
        assert_eq!(ids(rows), third_page, "page mismatch for: {sql}");
    }

    // `FETCH FIRST` without a count is one row, per SQL:2008.
    let res = exec(
        &database,
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id FETCH FIRST ROW ONLY",
    )
    .await;
    let rows = assert_rows(&res, "FETCH FIRST ROW ONLY");
    assert_eq!(ids(rows), vec![1u64]);

    // `LIMIT ALL` is "no cap", not "no rows" — the offset still applies.
    let res = exec(
        &database,
        "SELECT id FROM paged_rows WHERE id >= 1 ORDER BY id LIMIT ALL OFFSET 18",
    )
    .await;
    let rows = assert_rows(&res, "LIMIT ALL OFFSET 18");
    assert_eq!(ids(rows), vec![19u64, 20]);

    // A page asked for twice in one statement is a contradiction, not a hint.
    let res = exec(
        &database,
        "SELECT id FROM paged_rows WHERE id >= 1 LIMIT 5 FETCH FIRST 2 ROWS ONLY",
    )
    .await;
    assert_error_contains(&res, "multiple LIMIT", "LIMIT together with FETCH FIRST");

    // A column may still be named after one of the non-reserved keywords.
    let res = exec(
        &database,
        "SELECT id FROM paged_rows WHERE age > 0 ORDER BY id LIMIT 3",
    )
    .await;
    let rows = assert_rows(&res, "ordinary LIMIT still works");
    assert_eq!(rows.len(), 3);

    // A page on an aggregate caps the *groups*, never the rows fed to the fold.
    // Handing the cap to the scan would also switch on early scan termination,
    // and `COUNT(*)` would answer with the size of the page instead of the size
    // of the table. Both spellings of the page have to reach the fold, not the
    // scan.
    let count_of = |rows: &[Row]| -> i64 {
        assert_eq!(rows.len(), 1, "COUNT(*) with no GROUP BY is one group");
        i64::from_le_bytes(
            rows[0].columns[0]
                .data
                .into_slice()
                .try_into()
                .expect("COUNT is an i64"),
        )
    };

    for sql in [
        "SELECT COUNT(*) AS n FROM paged_rows WHERE id >= 1 LIMIT 3",
        "SELECT COUNT(*) AS n FROM paged_rows WHERE id >= 1 FETCH FIRST 3 ROWS ONLY",
        "SELECT COUNT(*) AS n FROM paged_rows WHERE id >= 1 TAKE 3",
    ] {
        let res = exec(&database, sql).await;
        let rows = assert_rows(&res, sql);
        assert_eq!(
            count_of(&rows),
            TOTAL as i64,
            "the page must not truncate the fold's input: {sql}"
        );
    }
}

/// `ORDER BY a, b DESC` must honour both keys.
///
/// Only the first was ever applied: a second key parsed and was then dropped,
/// so the rows within each `a` came back in scan order — an order that looks
/// deliberate and is not. Each `(dept, salary)` pair here is unique, so the
/// expected sequence is fully determined and a dropped key cannot coincide
/// with it.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_order_by_honours_every_key() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_order_by_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    let create_sql = r##"CREATE TABLE sorted_rows (
            id UBIGINT,
            dept VARCHAR,
            salary INTEGER
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE sorted_rows");

    // Three departments, round-robin, with a salary unique to each row.
    let dept_of = |id: u64| match id % 3 {
        1 => "a",
        2 => "b",
        _ => "c",
    };
    for id in 1..=12u64 {
        let insert_sql = format!(
            r##"INSERT INTO sorted_rows (id, dept, salary) VALUES ({}, '{}', {});"##,
            id,
            dept_of(id),
            id * 10
        );
        let res = exec(&database, &insert_sql).await;
        assert_ok(&res, "INSERT sorted_rows");
    }

    let table = database
        .tables
        .get("sorted_rows")
        .expect("sorted_rows table should exist")
        .clone();
    let schema = &table.schema.fields;

    let ids = |rows: &[Row]| -> Vec<u64> {
        rows.iter()
            .map(|row| {
                mb_to_u64(
                    row.get_column("id", schema)
                        .expect("row should include id")
                        .into_slice(),
                )
            })
            .collect()
    };

    // Departments ascending, best paid first within each one.
    let expected = vec![10u64, 7, 4, 1, 11, 8, 5, 2, 12, 9, 6, 3];

    // Neither sort key is selected here, so both have to be fetched for the
    // sort and trimmed off again before the rows are sent.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept, salary DESC",
    )
    .await;
    let rows = assert_rows(&res, "two keys, neither selected");
    assert_eq!(ids(&rows), expected);
    assert_eq!(
        rows[0].columns.len(),
        1,
        "the fetched sort keys must be trimmed off the result"
    );

    // The same order, written with an explicit ASC and with both keys selected.
    let res = exec(
        &database,
        "SELECT id, dept, salary FROM sorted_rows WHERE id >= 1 ORDER BY dept ASC, salary DESC",
    )
    .await;
    let rows = assert_rows(&res, "two keys, both selected");
    assert_eq!(ids(&rows), expected);

    // A third key breaks nothing when the first two already decide the order.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept, salary DESC, id",
    )
    .await;
    let rows = assert_rows(&res, "three keys");
    assert_eq!(ids(&rows), expected);

    // Each key keeps its own direction: reversing only the first swaps the
    // departments and leaves the salaries descending.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept DESC, salary DESC",
    )
    .await;
    let rows = assert_rows(&res, "leading key reversed");
    assert_eq!(ids(&rows), vec![12u64, 9, 6, 3, 11, 8, 5, 2, 10, 7, 4, 1]);

    // A page is taken from the fully sorted set, not from a prefix of it.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept, salary DESC LIMIT 3 OFFSET 2",
    )
    .await;
    let rows = assert_rows(&res, "sorted page");
    assert_eq!(ids(&rows), vec![4u64, 1, 11]);

    // The fold sorts its groups the same way. Every department has the same
    // count, so only the second key can decide the order.
    let res = exec(
        &database,
        "SELECT dept, COUNT(*) AS n FROM sorted_rows WHERE id >= 1 GROUP BY dept ORDER BY n DESC, dept",
    )
    .await;
    let rows = assert_rows(&res, "grouped, two keys");
    let depts: Vec<String> = rows
        .iter()
        .map(|r| mb_to_string(r.columns[0].data.into_slice()))
        .collect();
    assert_eq!(
        depts,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // `NULLS LAST` rides the expression-sort path now (the scan spec still
    // refuses it — the byte sort cannot place NULLs — so the planner routes
    // any term carrying a placement through the post-scan sort). Over a
    // NOT NULL key the explicit placement changes nothing, which is exactly
    // what this asserts.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept NULLS LAST, salary DESC",
    )
    .await;
    let rows = assert_rows(&res, "NULLS LAST over a NOT NULL key");
    assert_eq!(ids(&rows), expected);

    // An unknown column is still reported, whichever key position it is in.
    let res = exec(
        &database,
        "SELECT id FROM sorted_rows WHERE id >= 1 ORDER BY dept, does_not_exist",
    )
    .await;
    assert_error_contains(&res, "does_not_exist", "unknown second sort key");
}

/// Table-level `UNIQUE` constraints, single-column and composite, declared at
/// `CREATE TABLE` and added afterwards with `ALTER TABLE`.
///
/// The composite case is the one that cannot be expressed as a per-column flag:
/// `UNIQUE (org_id, user_id)` forbids repeating the *pair* while leaving either
/// column free to repeat on its own. Enforcing it column-by-column would reject
/// rows the constraint allows, so this asserts both halves — the duplicate pair
/// is refused *and* the repeated single values are accepted.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_table_level_unique_constraints() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_unique_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let db_dir_str = db_dir
        .to_str()
        .expect("db path should be valid UTF-8")
        .to_string();

    let database = Arc::new(Database::new(&db_dir_str).await);

    // --- composite constraint declared at CREATE TABLE --------------------
    let create_sql = r##"CREATE TABLE memberships (
            id UBIGINT,
            org_id INTEGER,
            user_id INTEGER,
            role VARCHAR,
            UNIQUE (org_id, user_id)
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE memberships");

    let insert = |id: u64, org: i32, user: i32, role: &str| {
        format!(
            r##"INSERT INTO memberships (id, org_id, user_id, role) VALUES ({}, {}, {}, '{}');"##,
            id, org, user, role
        )
    };

    let res = exec(&database, &insert(1, 1, 100, "admin")).await;
    assert_ok(&res, "first membership");

    // Same org, different user: the pair is new, so this is allowed. A
    // per-column reading of the constraint would wrongly refuse it.
    let res = exec(&database, &insert(2, 1, 200, "member")).await;
    assert_ok(&res, "same org_id, different user_id");

    // Same user, different org: likewise allowed.
    let res = exec(&database, &insert(3, 2, 100, "member")).await;
    assert_ok(&res, "same user_id, different org_id");

    // The pair repeats: refused.
    let res = exec(&database, &insert(4, 1, 100, "owner")).await;
    assert_error_contains(&res, "UNIQUE", "duplicate (org_id, user_id) pair");

    let res = exec(&database, "SELECT id FROM memberships WHERE id >= 1").await;
    assert_eq!(assert_rows(&res, "rows after composite checks").len(), 3);

    // An UPDATE that would create the duplicate is refused too — the check runs
    // on both write paths, not just on INSERT.
    let res = exec(
        &database,
        "UPDATE memberships SET user_id = 100 WHERE id = 2",
    )
    .await;
    assert_error_contains(&res, "UNIQUE", "UPDATE into a duplicate pair");

    // Deleting the row that held the pair frees it. Under MVCC a deleted row
    // keeps its `deleted` flag clear until reclamation runs, so without the
    // visibility filter the probe would still see it and refuse this.
    let res = exec(&database, "DELETE FROM memberships WHERE id = 1").await;
    assert_ok(&res, "delete the row holding the pair");
    let res = exec(&database, &insert(5, 1, 100, "owner")).await;
    assert_ok(&res, "reinsert a pair whose holder was deleted");

    // --- constraint added afterwards --------------------------------------
    let create_sql = r##"CREATE TABLE accounts (
            id UBIGINT,
            email VARCHAR,
            tenant INTEGER
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE accounts");

    let add_account = |id: u64, email: &str, tenant: i32| {
        format!(
            r##"INSERT INTO accounts (id, email, tenant) VALUES ({}, '{}', {});"##,
            id, email, tenant
        )
    };
    for (id, email, tenant) in [(1u64, "a@x.com", 1), (2, "b@x.com", 1), (3, "a@x.com", 2)] {
        let res = exec(&database, &add_account(id, email, tenant)).await;
        assert_ok(&res, "INSERT accounts");
    }

    // `email` already repeats, so the constraint was never true of this table
    // and adding it has to fail rather than appear to succeed.
    let res = exec(
        &database,
        "ALTER TABLE accounts ADD CONSTRAINT accounts_email_key UNIQUE (email)",
    )
    .await;
    assert_error_contains(&res, "duplicate", "ADD UNIQUE over existing duplicates");

    // The pair (email, tenant) is distinct across those rows, so this one holds.
    let res = exec(
        &database,
        "ALTER TABLE accounts ADD CONSTRAINT accounts_email_tenant_key UNIQUE (email, tenant)",
    )
    .await;
    assert_ok(&res, "ADD UNIQUE that the data satisfies");

    // And it is enforced from here on, without a restart.
    let res = exec(&database, &add_account(4, "a@x.com", 1)).await;
    assert_error_contains(&res, "UNIQUE", "duplicate after ALTER ADD CONSTRAINT");

    // A pair that is still new is accepted.
    let res = exec(&database, &add_account(5, "c@x.com", 1)).await;
    assert_ok(&res, "new pair after ALTER ADD CONSTRAINT");

    // Naming a column that does not exist is refused rather than stored.
    let res = exec(
        &database,
        "ALTER TABLE accounts ADD CONSTRAINT bad UNIQUE (does_not_exist)",
    )
    .await;
    assert_error_contains(&res, "does_not_exist", "ADD UNIQUE on an unknown column");

    // Adding the same constraint name twice is refused.
    let res = exec(
        &database,
        "ALTER TABLE accounts ADD CONSTRAINT accounts_email_tenant_key UNIQUE (tenant)",
    )
    .await;
    assert_error_contains(&res, "already exists", "duplicate constraint name");

    // --- dropping it -------------------------------------------------------
    let res = exec(
        &database,
        "ALTER TABLE accounts DROP CONSTRAINT accounts_email_tenant_key",
    )
    .await;
    assert_ok(&res, "DROP CONSTRAINT");

    // With the constraint gone the duplicate pair is allowed again.
    let res = exec(&database, &add_account(6, "a@x.com", 1)).await;
    assert_ok(&res, "duplicate accepted after DROP CONSTRAINT");
}

/// A `UNIQUE` constraint over a nullable column follows PostgreSQL's default of
/// `NULLS DISTINCT`: a tuple containing a `NULL` conflicts with nothing, so it
/// may repeat freely.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_unique_treats_nulls_as_distinct() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_unique_nulls_")
        .tempdir()
        .expect("tempdir")
        .keep();

    let database =
        Arc::new(Database::new(db_dir.to_str().expect("db path should be valid UTF-8")).await);

    let create_sql = r##"CREATE TABLE invites (
            id UBIGINT,
            code VARCHAR,
            claimed_by VARCHAR,
            UNIQUE (code, claimed_by)
        );"##;
    let res = exec(&database, create_sql).await;
    assert_ok(&res, "CREATE TABLE invites");

    for id in 1..=3u64 {
        let sql = format!(
            r##"INSERT INTO invites (id, code, claimed_by) VALUES ({}, 'SHARED', NULL);"##,
            id
        );
        let res = exec(&database, &sql).await;
        assert_ok(&res, "a tuple containing NULL may repeat");
    }

    let res = exec(&database, "SELECT id FROM invites WHERE id >= 1").await;
    assert_eq!(assert_rows(&res, "null tuples all stored").len(), 3);

    // With both columns present the constraint applies as normal.
    let res = exec(
        &database,
        r##"INSERT INTO invites (id, code, claimed_by) VALUES (4, 'SHARED', 'ann');"##,
    )
    .await;
    assert_ok(&res, "first fully-specified tuple");
    let res = exec(
        &database,
        r##"INSERT INTO invites (id, code, claimed_by) VALUES (5, 'SHARED', 'ann');"##,
    )
    .await;
    assert_error_contains(&res, "UNIQUE", "duplicate fully-specified tuple");
}

/// The anti-join, checked against an oracle computed outside the engine.
///
/// `LEFT JOIN … WHERE r.key IS NULL` once returned *every* left row, because a
/// single-relation `WHERE` conjunct was pushed into that relation's own scan —
/// which happens before the join, so filtering the right side to rows with a
/// null key emptied the hash index and left every left row looking unmatched.
///
/// A count-based assertion is no good here: it passes both when the answer is
/// right and when the bug returns everything. So the expected set is computed
/// in Rust from the two tables read whole, compared as a **set**, and asserted
/// to be non-empty *and* strictly smaller than the left table — a shape that
/// cannot be satisfied vacuously by either failure mode.
#[tokio::test(flavor = "multi_thread")]
async fn ext_cov_anti_join_matches_an_oracle() {
    let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
    let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
    let _ = concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

    let db_dir = tempfile::Builder::new()
        .prefix("rastdb_antijoin_")
        .tempdir()
        .expect("tempdir");
    let db_dir_str = db_dir.path().to_str().expect("utf-8 path").to_string();
    let database = Arc::new(Database::new(&db_dir_str).await);

    assert_ok(
        &exec(
            &database,
            "CREATE TABLE oj_left (id INTEGER NOT NULL, tag VARCHAR)",
        )
        .await,
        "create left",
    );
    assert_ok(
        &exec(
            &database,
            "CREATE TABLE oj_right (id INTEGER NOT NULL, lid INTEGER)",
        )
        .await,
        "create right",
    );

    // Every third left row gets a partner, so the answer is a proper, non-empty
    // subset either way the test could go wrong.
    const LEFT_ROWS: i32 = 30;
    let mut expected: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    for id in 1..=LEFT_ROWS {
        assert_ok(
            &exec(
                &database,
                &format!("INSERT INTO oj_left (id, tag) VALUES ({id}, 'row{id}')"),
            )
            .await,
            "insert left",
        );
        if id % 3 == 0 {
            assert_ok(
                &exec(
                    &database,
                    &format!(
                        "INSERT INTO oj_right (id, lid) VALUES ({}, {id})",
                        1000 + id
                    ),
                )
                .await,
                "insert right",
            );
        } else {
            expected.insert(id);
        }
    }

    // The oracle must not lean on any engine affordance beyond reading each
    // table whole, or it would inherit the very bug it is checking.
    let left_rows = assert_rows(
        &exec(&database, "SELECT id FROM oj_left").await,
        "read left",
    );
    let right_rows = assert_rows(
        &exec(&database, "SELECT lid FROM oj_right").await,
        "read right",
    );
    let matched: std::collections::BTreeSet<i32> = right_rows
        .iter()
        .filter_map(|r| r.columns.first().map(|c| mb_to_i32(c.data.into_slice())))
        .collect();
    let oracle: std::collections::BTreeSet<i32> = left_rows
        .iter()
        .filter_map(|r| r.columns.first().map(|c| mb_to_i32(c.data.into_slice())))
        .filter(|id| !matched.contains(id))
        .collect();
    assert_eq!(
        oracle, expected,
        "the oracle disagrees with how the data was built"
    );

    // Cannot pass vacuously: an empty answer and a whole-table answer are both
    // excluded before the engine is asked anything.
    assert!(!oracle.is_empty(), "oracle must not be empty");
    assert!(
        oracle.len() < LEFT_ROWS as usize,
        "oracle must be a proper subset, or the push-down bug would pass"
    );

    let actual_rows = assert_rows(
        &exec(
            &database,
            "SELECT l.id FROM oj_left l LEFT JOIN oj_right r ON r.lid = l.id WHERE r.id IS NULL",
        )
        .await,
        "anti-join",
    );
    let actual: std::collections::BTreeSet<i32> = actual_rows
        .iter()
        .filter_map(|r| r.columns.first().map(|c| mb_to_i32(c.data.into_slice())))
        .collect();

    assert_eq!(
        actual,
        oracle,
        "anti-join returned the wrong set: extra={:?}, missing={:?}",
        actual.difference(&oracle).collect::<Vec<_>>(),
        oracle.difference(&actual).collect::<Vec<_>>()
    );
}
