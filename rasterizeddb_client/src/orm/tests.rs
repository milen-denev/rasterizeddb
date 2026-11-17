use super::*;
use crate::{Numeric, Row, Rows, SqlEnum, Timestamp};

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "accounts")]
struct Account {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(unique)]
    email: String,
    #[rdb(index = "hash")]
    name: Option<String>,
    balance: Numeric,
    tags: Vec<String>,
    #[rdb(column = "order", default = "0")]
    position: i32,
    status: Status,
    #[rdb(skip)]
    cached: Option<u8>,
}

#[derive(SqlEnum, Debug, Clone, Copy, PartialEq)]
enum Status {
    Active,
    #[rdb(rename = "on-hold")]
    OnHold,
}

#[derive(crate::Model, Debug)]
struct OrderLine {
    #[rdb(primary_key)]
    order_id: i64,
    #[rdb(primary_key)]
    line: i32,
}

fn account() -> Account {
    Account {
        id: 0,
        email: "o'neil@example.com".into(),
        name: None,
        balance: Numeric::new("-10.50").unwrap(),
        tags: vec!["a b".into(), "q\"z".into()],
        position: 3,
        status: Status::OnHold,
        cached: Some(9),
    }
}

#[test]
fn columns_describe_the_struct() {
    assert_eq!(Account::TABLE, "accounts");
    assert_eq!(OrderLine::TABLE, "order_line");
    let names: Vec<&str> = Account::COLUMNS.iter().map(|c| c.name).collect();
    assert_eq!(
        names,
        ["id", "email", "name", "balance", "tags", "order", "status"]
    );
    let c = &Account::COLUMNS;
    assert!(c[0].primary_key && c[0].serial && c[0].sql_type == "BIGINT");
    assert!(c[1].unique && !c[1].nullable);
    assert!(c[2].nullable && c[2].index == Some(IndexMethod::Hash));
    assert!(c[3].read_as_text && c[3].sql_type == "NUMERIC");
    assert!(c[4].read_as_text && c[4].sql_type == "TEXT[]");
    assert_eq!(c[5].default, Some("0"));
    assert_eq!(c[6].sql_type, "TEXT");
    assert_eq!(Account::email.name(), "email");
    assert_eq!(Account::position.name(), "order");
}

#[test]
fn statements() {
    assert_eq!(
        select_list::<Account>().unwrap(),
        "id, email, name, balance::text, tags::text, \"order\", status"
    );
    assert_eq!(
        insert_sql(&[&account()]).unwrap(),
        "INSERT INTO accounts (email, name, balance, tags, \"order\", status) VALUES \
         ('o''neil@example.com', NULL, (-10.50), '{\"a b\",\"q\\\"z\"}', 3, 'on-hold') \
         RETURNING id, email, name, balance::text, tags::text, \"order\", status"
    );
    let upsert = upsert_sql(&account()).unwrap();
    assert!(
        upsert.contains("ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, "),
        "{upsert}"
    );
    assert!(
        upsert.starts_with("INSERT INTO accounts (id, email,"),
        "{upsert}"
    );
    assert_eq!(
        update_sql(&account()).unwrap(),
        "UPDATE accounts SET email = 'o''neil@example.com', name = NULL, balance = (-10.50), \
         tags = '{\"a b\",\"q\\\"z\"}', \"order\" = 3, status = 'on-hold' WHERE id = 0 \
         RETURNING id, email, name, balance::text, tags::text, \"order\", status"
    );
    assert_eq!(
        delete_sql(&account()).unwrap(),
        "DELETE FROM accounts WHERE id = 0"
    );
    let line = OrderLine {
        order_id: 7,
        line: -1,
    };
    assert_eq!(
        delete_sql(&line).unwrap(),
        "DELETE FROM order_line WHERE order_id = 7 AND line = (-1)"
    );
    assert!(
        upsert_sql(&line)
            .unwrap()
            .contains("ON CONFLICT (order_id, line) DO NOTHING")
    );
    assert!(update_sql(&line).is_err(), "nothing outside the key");
    assert!(
        find_sql::<OrderLine>(&[&7i64]).is_err(),
        "half a composite key"
    );
}

#[test]
fn ddl() {
    let ddl = create_table_sql::<Account>().unwrap();
    assert_eq!(
        ddl,
        vec![
            "CREATE TABLE IF NOT EXISTS accounts (id BIGSERIAL, email TEXT NOT NULL, \
             name TEXT, balance NUMERIC NOT NULL, tags TEXT[] NOT NULL, \"order\" INTEGER NOT NULL \
             DEFAULT 0, status TEXT NOT NULL, PRIMARY KEY (id))"
                .to_string(),
            "ALTER TABLE accounts ADD CONSTRAINT IF NOT EXISTS accounts_email_key UNIQUE (email)"
                .to_string(),
            "CREATE INDEX IF NOT EXISTS accounts_name_idx ON accounts USING hash (name)"
                .to_string(),
        ]
    );
}

#[test]
fn the_query_builder() {
    let q = Account::query()
        .filter(Account::balance.gt(Numeric::new("5").unwrap()) & !Account::name.is_null())
        .filter(Account::email.like("%@x.com") | Account::id.is_in([1i64, 2, 3]))
        .order_by(Account::position.desc().nulls_last())
        .limit(10)
        .offset(20);
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, email, name, balance::text, tags::text, \"order\", status FROM accounts \
         WHERE ((balance > 5) AND (NOT (name IS NULL))) AND ((email LIKE '%@x.com') OR \
         (id IN (1, 2, 3))) ORDER BY \"order\" DESC NULLS LAST LIMIT 10 OFFSET 20"
    );
    let none: [i64; 0] = [];
    assert!(
        Account::query()
            .filter(Account::id.is_in(none))
            .to_sql()
            .unwrap()
            .ends_with("WHERE 1 = 0")
    );
    assert!(
        Account::query()
            .filter(Account::email.eq("a\0"))
            .to_sql()
            .is_err(),
        "a NUL surfaces"
    );
    let raw = Expr::raw("lower(email) = $1", crate::params!["x"]);
    assert_eq!(raw.to_sql().unwrap(), "lower(email) = 'x'");
    let update = Account::query()
        .filter(Account::id.eq(1i64))
        .update()
        .set(Account::status, Status::Active)
        .set_raw(Account::position, "\"order\" + $1", crate::params![1]);
    assert_eq!(
        update.to_sql().unwrap(),
        "UPDATE accounts SET status = 'active', \"order\" = \"order\" + 1 WHERE id = 1 \
         RETURNING id, email, name, balance::text, tags::text, \"order\", status"
    );
    assert!(
        Account::query()
            .update()
            .set(Account::position, 1)
            .to_sql()
            .is_err(),
        "no filter"
    );
    assert!(
        Account::query()
            .every_row()
            .update()
            .set(Account::position, 1)
            .to_sql()
            .is_ok()
    );
    assert!(
        Account::query()
            .filter(Account::id.eq(1i64))
            .limit(1)
            .update()
            .set(Account::position, 1)
            .to_sql()
            .is_err()
    );
}

/// A row as the server would send it for `select_list::<Account>()`.
fn wire_row(cells: &[(u8, &[u8])]) -> Rows {
    let mut payload = Vec::new();
    for (t, d) in cells {
        payload.extend_from_slice(&(d.len() as u64).to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(d);
        payload.push(*t);
    }
    let mut data = (payload.len() as u32).to_le_bytes().to_vec();
    data.extend(payload);
    Rows::decode(data).unwrap()
}

#[test]
fn rows_read_into_the_struct() {
    use crate::tag;
    let rows = wire_row(&[
        (tag::I64, &5i64.to_le_bytes()),
        (tag::STRING, b"a@b.c"),
        (tag::NULL, b""),
        (tag::STRING, b"12.30"),
        (tag::STRING, b"{x,\"y z\"}"),
        (tag::I32, &4i32.to_le_bytes()),
        (tag::STRING, b"active"),
    ]);
    let row: Row<'_> = rows.get(0).unwrap();
    let a: Account = row.decode().unwrap();
    assert_eq!(
        a,
        Account {
            id: 5,
            email: "a@b.c".into(),
            name: None,
            balance: Numeric::new("12.30").unwrap(),
            tags: vec!["x".into(), "y z".into()],
            position: 4,
            status: Status::Active,
            cached: None,
        }
    );
    let short = wire_row(&[(tag::I64, &5i64.to_le_bytes())]);
    assert!(
        short.get(0).unwrap().decode::<Account>().is_err(),
        "a drifted select list fails"
    );
    let bad_enum = wire_row(&[(tag::STRING, b"gone")]);
    assert!(bad_enum.get(0).unwrap().get::<Status>(0).is_err());
    let _ = Timestamp::now();
}

/// A model that is neither `Send` nor `Sync`.
#[derive(crate::Model)]
struct Local {
    #[rdb(primary_key)]
    id: i64,
    #[rdb(skip)]
    memo: std::rc::Rc<()>,
}

fn assert_send<T: Send>(_: T) {}

/// Every ORM future is `Send` on any executor, for any model: the statement
/// is written before the future is made, so no row is held across a send.
#[allow(dead_code)]
fn orm_futures_are_send<E: crate::Executor>(db: &E, a: &Account, local: &Local) {
    assert_send(db.create_table::<Account>());
    assert_send(db.insert(a));
    assert_send(db.insert_many(std::slice::from_ref(a)));
    assert_send(db.upsert(a));
    assert_send(db.update(a));
    assert_send(db.delete(a));
    assert_send(db.find::<Account>(1i64));
    assert_send(db.all::<Account>());
    assert_send(db.insert(local));
    assert_send(db.insert_many(std::slice::from_ref(local)));
    assert_send(db.upsert(local));
    assert_send(db.delete(local));
    let q = Account::query();
    assert_send(q.all(db));
    assert_send(q.first(db));
    assert_send(q.count(db));
    assert_send(q.exists(db));
    assert_send(q.filter(Account::id.eq(1i64)).delete(db));
    let update = Account::query()
        .every_row()
        .update()
        .set(Account::position, 1);
    assert_send(update.run(db));
    assert_send(crate::kv::Kv::new(db).get(b"k"));
    assert_send(crate::kv::Kv::new(db).repo::<Account>().get(&[&1i64]));
    let _ = local.memo.clone();
}

/// A `&Client` and a `Client` are both executors: the call syntax of a
/// client and of a reference to one is the same.
#[allow(dead_code)]
async fn client_call_syntax(
    client: &crate::Client,
    owned: crate::Client,
    a: &Account,
) -> Result<()> {
    client.insert(a).await?;
    owned.insert(a).await?;
    Account::query().all(client).await?;
    Account::query().all(&owned).await?;
    Account::query().all(&client).await?;
    Ok(())
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
struct Doc {
    #[rdb(primary_key)]
    id: i64,
    body: String,
    #[rdb(version)]
    version: i32,
}

#[test]
fn a_version_column_guards_every_write() {
    assert_eq!(Doc::COLUMNS[2].role, ColumnRole::Version);
    assert_eq!(Doc::COLUMNS[0].role, ColumnRole::Plain);
    let doc = Doc {
        id: 7,
        body: "b".into(),
        version: 3,
    };
    assert_eq!(
        insert_sql(&[&doc]).unwrap(),
        "INSERT INTO doc (id, body, version) VALUES (7, 'b', 1) RETURNING id, body, version",
        "an insert starts at version 1, whatever the struct holds"
    );
    assert_eq!(
        update_sql(&doc).unwrap(),
        "UPDATE doc SET body = 'b', version = version + 1 WHERE id = 7 AND version = 3 \
         RETURNING id, body, version"
    );
    assert_eq!(
        delete_sql(&doc).unwrap(),
        "DELETE FROM doc WHERE id = 7 AND version = 3"
    );
    assert_eq!(
        upsert_sql(&doc).unwrap(),
        "INSERT INTO doc (id, body, version) VALUES (7, 'b', 1) ON CONFLICT (id) DO UPDATE SET \
         body = EXCLUDED.body, version = doc.version + 1 WHERE doc.version = 3 \
         RETURNING id, body, version"
    );
    let probe = VersionProbe::of(&doc).unwrap().unwrap();
    assert_eq!(probe.sql, "SELECT version FROM doc WHERE id = 7");
    assert!(VersionProbe::of(&account()).unwrap().is_none());

    let bulk = Doc::query()
        .filter(Doc::id.gt(1i64))
        .update()
        .set(Doc::body, "x");
    assert_eq!(
        bulk.to_sql().unwrap(),
        "UPDATE doc SET body = 'x', version = version + 1 WHERE id > 1 RETURNING id, body, version",
        "a bulk update moves the version too"
    );
    let explicit = Doc::query()
        .filter(Doc::id.gt(1i64))
        .update()
        .set(Doc::version, 0);
    assert_eq!(
        explicit.to_sql().unwrap(),
        "UPDATE doc SET version = 0 WHERE id > 1 RETURNING id, body, version"
    );
    // Unchanged for a model without one.
    assert_eq!(
        delete_sql(&account()).unwrap(),
        "DELETE FROM accounts WHERE id = 0"
    );
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "authors")]
struct Author {
    #[rdb(primary_key)]
    id: i64,
    name: String,
    #[rdb(has_many(Book, foreign_key = "author_id"))]
    books: Vec<Book>,
    #[rdb(many_to_many(
        Genre,
        through = "author_genres",
        local = "author_id",
        foreign = "genre_id"
    ))]
    genres: Vec<Genre>,
    #[rdb(count_of = "books")]
    book_count: i64,
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "books")]
struct Book {
    #[rdb(primary_key)]
    id: i64,
    #[rdb(references = "authors(id)", on_delete = "set null")]
    author_id: Option<i64>,
    title: String,
    #[rdb(belongs_to(Author, foreign_key = "author_id"))]
    author: Option<Box<Author>>,
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "genres")]
struct Genre {
    #[rdb(primary_key)]
    id: i64,
    name: String,
}

#[test]
fn relations_are_declared_and_filter_through_subqueries() {
    let names: Vec<&str> = Author::COLUMNS.iter().map(|c| c.name).collect();
    assert_eq!(
        names,
        ["id", "name"],
        "relation and count fields are not columns"
    );
    assert_eq!(Author::books.kind(), RelationKind::HasMany);
    assert_eq!(Book::author.kind(), RelationKind::BelongsTo);
    assert_eq!(
        Author::genres.through(),
        Some(Through {
            table: "author_genres",
            local: "author_id",
            foreign: "genre_id"
        })
    );
    assert_eq!(
        create_table_sql::<Book>().unwrap()[0],
        "CREATE TABLE IF NOT EXISTS books (id BIGINT NOT NULL, author_id BIGINT REFERENCES \
         authors (id) ON DELETE SET NULL, title TEXT NOT NULL, PRIMARY KEY (id))"
    );

    let q = Author::query().where_has(Author::books.filter(Book::title.like("R%")));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, name FROM authors WHERE id IN (SELECT author_id FROM books WHERE author_id \
         IS NOT NULL AND (title LIKE 'R%'))"
    );
    let q = Author::query().where_doesnt_have(Author::books);
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, name FROM authors WHERE (id IS NULL OR id NOT IN (SELECT author_id FROM \
         books WHERE author_id IS NOT NULL))"
    );
    let q = Author::query().where_has(Author::genres.filter(Genre::name.eq("poetry")));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, name FROM authors WHERE id IN (SELECT author_id FROM author_genres WHERE \
         author_id IS NOT NULL AND genre_id IN (SELECT id FROM genres WHERE name = 'poetry'))"
    );
    let q = Book::query().where_doesnt_have(Book::author);
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, author_id, title FROM books WHERE (author_id IS NULL OR author_id NOT IN \
         (SELECT id FROM authors WHERE id IS NOT NULL))",
        "a book with no author has none"
    );
    assert!(
        Author::query()
            .where_has(Author::books.limit(1))
            .to_sql()
            .is_err(),
        "a limit means nothing in a filter"
    );
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "sales")]
struct Sale {
    #[rdb(primary_key)]
    id: i64,
    region: String,
    units: i32,
    amount: i64,
    price: Numeric,
    rate: Option<f64>,
}

#[test]
fn projections_and_aggregates() {
    let q = Sale::query()
        .filter(Sale::units.gt(0))
        .select((
            Sale::region,
            count_rows(),
            Sale::amount.sum(),
            Sale::units.avg(),
            Sale::price.max(),
            Sale::rate.avg(),
            Sale::region.count_distinct(),
        ))
        .group_by(Sale::region)
        .having(Sale::amount.sum().gt(100i64) & count_rows::<Sale>().ge(2))
        .order_by(Sale::amount.sum().desc())
        .limit(5);
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT region, COUNT(*), CAST(SUM(amount) AS BIGINT), AVG(units)::text, \
         MAX(price)::text, CAST(AVG(rate) AS DOUBLE PRECISION), COUNT(DISTINCT region) \
         FROM sales WHERE units > 0 GROUP BY region HAVING (SUM(amount) > 100) AND \
         (COUNT(*) >= 2) ORDER BY SUM(amount) DESC LIMIT 5",
        "an integer sum is cast outside the SUM"
    );
    let one = Sale::query().select(Sale::price.sum());
    assert_eq!(one.to_sql().unwrap(), "SELECT SUM(price)::text FROM sales");
    let cols = Sale::query().select((Sale::id, Sale::price)).distinct();
    assert_eq!(
        cols.to_sql().unwrap(),
        "SELECT DISTINCT id, price::text FROM sales"
    );
    // The output types, checked by the compiler.
    fn output<M, P: Projection<M>>(_: &Projected<M, P>) -> std::marker::PhantomData<P::Output> {
        std::marker::PhantomData
    }
    let _: std::marker::PhantomData<Option<Numeric>> = output(&one);
    let _: std::marker::PhantomData<(i64, Numeric)> = output(&cols);
    let typed = Sale::query().select((Sale::units.sum(), Sale::rate.sum(), Sale::units.min()));
    let _: std::marker::PhantomData<(Option<i64>, Option<f64>, Option<i32>)> = output(&typed);
    assert!(
        Author::query()
            .with(Author::books)
            .select(Author::id)
            .to_sql()
            .is_err(),
        "relations do not load into a projection"
    );
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "events")]
struct Event {
    #[rdb(primary_key)]
    id: i64,
    kind: String,
    at: Timestamp,
    score: i32,
    note: Option<String>,
}

#[test]
fn keyset_pages_compare_row_values_or_chains() {
    use crate::Value;
    let c = |v: Vec<Value>| Cursor::new(v);
    // One direction: a row-value comparison, the key breaking ties the same
    // way.
    let q = Event::query()
        .order_by(Event::score.desc())
        .limit(10)
        .after(&c(vec![Value::I32(5), Value::I64(9)]));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, kind, at, score, note FROM events WHERE (score, id) < (5, 9) ORDER BY \
         score DESC, id DESC LIMIT 10"
    );
    // Before a cursor: the comparison and the order turn round.
    let q = Event::query()
        .order_by(Event::score.desc())
        .limit(10)
        .before(&c(vec![Value::I32(5), Value::I64(9)]));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, kind, at, score, note FROM events WHERE (score, id) > (5, 9) ORDER BY \
         score ASC, id ASC LIMIT 10"
    );
    // Mixed directions: an OR-chain, each column compared its own way.
    let q = Event::query()
        .filter(Event::kind.eq("x"))
        .order_by(Event::kind.asc())
        .order_by(Event::at.desc())
        .limit(2)
        .after(&c(vec![
            Value::Text("o'k".into()),
            Value::Timestamp(Timestamp { micros: 0 }),
            Value::I64(-3),
        ]));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, kind, at, score, note FROM events WHERE (kind = 'x') AND ((kind > 'o''k') OR \
         (kind = 'o''k' AND at < '1970-01-01 00:00:00+00') OR (kind = 'o''k' AND at = \
         '1970-01-01 00:00:00+00' AND id < (-3))) ORDER BY kind ASC, at DESC, id DESC LIMIT 2"
    );
    // No order: the primary key alone, no parentheses (a parenthesised
    // bare column is refused by the server).
    let q = Event::query().limit(3).after(&c(vec![Value::I64(7)]));
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT id, kind, at, score, note FROM events WHERE id > 7 ORDER BY id ASC LIMIT 3"
    );
    // Refusals: a nullable key, a cursor for another order, an offset.
    let refused = |q: Select<Event>| q.to_sql().unwrap_err().to_string();
    assert!(
        refused(Event::query().order_by(Event::note.asc()).after(&c(vec![]))).contains("nullable")
    );
    assert!(
        refused(Event::query().after(&c(vec![Value::I64(1), Value::I64(2)])))
            .contains("another order")
    );
    assert!(refused(Event::query().offset(5).after(&c(vec![Value::I64(1)]))).contains("offset"));

    // Cursors as strings: typed values only, rendered under escaping.
    let cursor = c(vec![
        Value::Text("'); DROP TABLE events; --".into()),
        Value::Timestamp(Timestamp {
            micros: 1_700_000_000_123_456,
        }),
        Value::I64(-1),
        Value::Numeric(Numeric::new("-1.50").unwrap()),
    ]);
    let text = cursor.encode().unwrap();
    assert!(
        text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.'),
        "{text}"
    );
    assert_eq!(Cursor::decode(&text).unwrap(), cursor);
    assert!(Cursor::decode("zz").is_err());
    assert!(Cursor::decode("l3").is_err());
    let hostile = c(vec![Value::Text("'; --".into()), Value::I64(1)]);
    let back = Cursor::decode(&hostile.encode().unwrap()).unwrap();
    let sql = Event::query()
        .order_by(Event::kind.asc())
        .after(&back)
        .to_sql()
        .unwrap();
    assert!(sql.contains("(kind, id) > ('''; --', 1)"), "{sql}");
}

#[derive(crate::Model, Debug, Clone, PartialEq)]
#[rdb(table = "notes")]
struct Note {
    #[rdb(primary_key)]
    id: i64,
    body: String,
    #[rdb(created_at)]
    created: Timestamp,
    #[rdb(updated_at)]
    updated: Timestamp,
    #[rdb(soft_delete)]
    deleted: Option<Timestamp>,
    #[rdb(version)]
    version: i64,
}

#[test]
fn timestamps_and_soft_delete() {
    let note = Note {
        id: 1,
        body: "b".into(),
        created: Timestamp { micros: 0 },
        updated: Timestamp { micros: 0 },
        deleted: None,
        version: 4,
    };
    let list = "id, body, created, updated, deleted, version";
    assert_eq!(
        insert_sql(&[&note]).unwrap(),
        format!(
            "INSERT INTO notes (id, body, created, updated, deleted, version) VALUES \
             (1, 'b', now(), now(), NULL, 1) RETURNING {list}"
        ),
        "the server's clock, not the struct's"
    );
    assert_eq!(
        update_sql(&note).unwrap(),
        format!(
            "UPDATE notes SET body = 'b', updated = now(), version = version + 1 WHERE id = 1 \
             AND version = 4 RETURNING {list}"
        ),
        "an update never writes the creation or deletion time"
    );
    assert_eq!(
        upsert_sql(&note).unwrap(),
        format!(
            "INSERT INTO notes (id, body, created, updated, deleted, version) VALUES \
             (1, 'b', now(), now(), NULL, 1) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, \
             deleted = EXCLUDED.deleted, updated = now(), version = notes.version + 1 WHERE \
             notes.version = 4 RETURNING {list}"
        )
    );
    assert_eq!(
        delete_sql(&note).unwrap(),
        "UPDATE notes SET deleted = now(), updated = now(), version = version + 1 WHERE id = 1 \
         AND version = 4 AND deleted IS NULL"
    );
    assert_eq!(
        force_delete_sql(&note).unwrap(),
        "DELETE FROM notes WHERE id = 1 AND version = 4"
    );
    assert_eq!(
        restore_sql(&note).unwrap(),
        format!(
            "UPDATE notes SET deleted = NULL, updated = now(), version = version + 1 WHERE id = 1 \
             AND version = 4 RETURNING {list}"
        )
    );
    assert!(restore_sql(&account()).is_err(), "nothing to restore");
    assert_eq!(
        find_sql::<Note>(&[&1i64]).unwrap(),
        format!("SELECT {list} FROM notes WHERE id = 1 AND deleted IS NULL")
    );
    assert_eq!(
        VersionProbe::of_live(&note).unwrap().unwrap().sql,
        "SELECT version FROM notes WHERE id = 1 AND deleted IS NULL"
    );

    // Queries see live rows, unless told otherwise.
    assert_eq!(
        Note::query().to_sql().unwrap(),
        format!("SELECT {list} FROM notes WHERE deleted IS NULL")
    );
    assert_eq!(
        Note::query()
            .filter(Note::id.gt(1i64))
            .only_deleted()
            .to_sql()
            .unwrap(),
        format!("SELECT {list} FROM notes WHERE (id > 1) AND deleted IS NOT NULL")
    );
    assert_eq!(
        Note::query().with_deleted().to_sql().unwrap(),
        format!("SELECT {list} FROM notes")
    );
    assert_eq!(
        Note::query()
            .filter(Note::id.gt(1i64))
            .update()
            .set(Note::body, "x")
            .to_sql()
            .unwrap(),
        format!(
            "UPDATE notes SET body = 'x', version = version + 1, updated = now() WHERE \
             (id > 1) AND deleted IS NULL RETURNING {list}"
        )
    );
    assert_eq!(
        Note::query().select(count_rows()).to_sql().unwrap(),
        "SELECT COUNT(*) FROM notes WHERE deleted IS NULL",
        "aggregates count live rows"
    );
}
