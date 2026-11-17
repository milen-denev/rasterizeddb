//! The ORM's features against a real server: optimistic locking, relations,
//! projections, keyset pages, timestamps and soft delete, upserts,
//! hooks, partitioning, the read-through cache and bulk updates.
//!
//! Needs `RDB_SERVER_BIN`, as `sdk_live.rs` does; without it each test says
//! so and passes.
#![cfg(feature = "derive")]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::prelude::*;
use tokio::process::{Child, Command};

struct Server {
    child: Child,
    dir: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn server(name: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_orm_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args([
            "--rdb-tcp",
            "off",
            "--rdb-socket-auth",
            "trust",
            "--socket-mode",
            "600",
        ])
        .args(["--log-level", "off"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server {
        child,
        dir,
        socket: format!("unix:{}", socket.display()),
    };
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

async fn connect(server: &Server) -> Client {
    Client::connect(
        &server.socket,
        Options {
            timeout: Some(Duration::from_secs(30)),
            ..Options::default()
        },
    )
    .await
    .expect("connect")
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(300), f)
            .await
            .expect("the test did not finish within 300 s")
    }
}

// --------------------------------------------------------- optimistic locking

#[derive(Model, Debug, Clone, PartialEq)]
struct Doc {
    #[rdb(primary_key)]
    id: i64,
    body: String,
    #[rdb(version)]
    version: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_version_column_catches_lost_updates() {
    bounded(async {
        let Some(server) = server("version").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Doc>().await.unwrap();

        let read = c
            .insert(&Doc {
                id: 1,
                body: "first".into(),
                version: 9,
            })
            .await
            .unwrap();
        assert_eq!(read.version, 1, "an insert starts at 1");

        let mut mine = read.clone();
        mine.body = "mine".into();
        let stored = c.update(&mine).await.unwrap();
        assert_eq!((stored.body.as_str(), stored.version), ("mine", 2));

        // The copy read before that update is stale now.
        let mut theirs = read.clone();
        theirs.body = "theirs".into();
        let e = c.update(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        assert!(
            e.to_string().contains("version 1") && e.to_string().contains("version 2"),
            "{e}"
        );
        let e = c.delete(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        let e = c.upsert(&theirs).await.unwrap_err();
        assert!(e.is_conflict(), "{e}");
        let now: Doc = c.find(1i64).await.unwrap().unwrap();
        assert_eq!(now, stored, "no stale write landed");

        // The current copy goes through, and moves the version each time.
        let mut current = now.clone();
        current.body = "upserted".into();
        let up = c.upsert(&current).await.unwrap();
        assert_eq!((up.body.as_str(), up.version), ("upserted", 3));
        let fresh = c
            .upsert(&Doc {
                id: 2,
                body: "new".into(),
                version: 0,
            })
            .await
            .unwrap();
        assert_eq!(fresh.version, 1);

        // A bulk update moves it too, so the copy above is stale after it.
        let bumped = Doc::query()
            .filter(Doc::id.eq(1i64))
            .update()
            .set(Doc::body, "bulk")
            .run(&c)
            .await
            .unwrap();
        assert_eq!(bumped[0].version, 4);
        assert!(c.update(&up).await.unwrap_err().is_conflict());

        // Gone is not a conflict: an update says no row, a delete false.
        let mut gone = fresh.clone();
        gone.id = 99;
        assert!(matches!(
            c.update(&gone).await,
            Err(Error::RowCount { got: 0, .. })
        ));
        assert!(!c.delete(&gone).await.unwrap());
        assert!(c.delete(&bumped[0]).await.unwrap());
        assert!(!c.delete(&bumped[0]).await.unwrap(), "already gone");

        // Two writers holding the same version: exactly one wins.
        let base = c
            .insert(&Doc {
                id: 3,
                body: "race".into(),
                version: 0,
            })
            .await
            .unwrap();
        let c = std::sync::Arc::new(c);
        let mut tasks = Vec::new();
        for n in 0..8 {
            let c = c.clone();
            let mut doc = base.clone();
            doc.body = format!("writer {n}");
            tasks.push(tokio::spawn(async move { c.update(&doc).await }));
        }
        let mut won = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(doc) => {
                    won += 1;
                    assert_eq!(doc.version, 2);
                }
                Err(e) => assert!(e.is_conflict(), "{e}"),
            }
        }
        assert_eq!(won, 1, "one writer per version");
        let last: Doc = c.find(3i64).await.unwrap().unwrap();
        assert_eq!(last.version, 2);
    })
    .await
}

// ------------------------------------------------------------------ relations

/// An executor that counts the statements sent through it, to show a load
/// is one statement per relation level, never one per row.
struct Counting<'a> {
    inner: &'a Client,
    sent: std::sync::atomic::AtomicUsize,
}

impl<'a> Counting<'a> {
    fn new(inner: &'a Client) -> Self {
        Counting {
            inner,
            sent: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn take(&self) -> usize {
        self.sent.swap(0, std::sync::atomic::Ordering::SeqCst)
    }
}

impl Executor for Counting<'_> {
    fn send_checked(&self, sql: &str) -> impl std::future::Future<Output = Result<Rows>> + Send {
        self.sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.send_checked(sql)
    }
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "authors")]
struct Author {
    #[rdb(primary_key)]
    id: i64,
    name: String,
    #[rdb(has_many(Book, foreign_key = "author_id"))]
    books: Vec<Book>,
    #[rdb(has_one(Bio, foreign_key = "author_id"))]
    bio: Option<Bio>,
    #[rdb(many_to_many(Genre, through = AuthorGenre, local = "author_id", foreign = "genre_id"))]
    genres: Vec<Genre>,
    #[rdb(count_of = "books")]
    books_count: i64,
    #[rdb(count_of = "genres")]
    genres_count: i64,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "books")]
struct Book {
    #[rdb(primary_key)]
    id: i64,
    #[rdb(index, references = "authors(id)", on_delete = "cascade")]
    author_id: Option<i64>,
    title: String,
    #[rdb(belongs_to(Author, foreign_key = "author_id"))]
    author: Option<Box<Author>>,
    #[rdb(has_many(Review, foreign_key = "book_id"))]
    reviews: Vec<Review>,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "bios")]
struct Bio {
    #[rdb(primary_key)]
    author_id: i64,
    text: String,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "reviews")]
struct Review {
    #[rdb(primary_key)]
    id: i64,
    #[rdb(references = "books(id)", on_delete = "cascade")]
    book_id: i64,
    stars: i32,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "genres")]
struct Genre {
    #[rdb(primary_key)]
    id: i64,
    name: String,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "author_genres")]
struct AuthorGenre {
    #[rdb(primary_key, references = "authors(id)", on_delete = "cascade")]
    author_id: i64,
    #[rdb(primary_key, references = "genres(id)", on_delete = "cascade")]
    genre_id: i64,
}

fn author(id: i64, name: &str) -> Author {
    Author {
        id,
        name: name.into(),
        books: vec![],
        bio: None,
        genres: vec![],
        books_count: 0,
        genres_count: 0,
    }
}

fn book(id: i64, author_id: Option<i64>, title: &str) -> Book {
    Book {
        id,
        author_id,
        title: title.into(),
        author: None,
        reviews: vec![],
    }
}

fn titles(books: &[Book]) -> Vec<&str> {
    books.iter().map(|b| b.title.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn relations_load_filter_count_and_link() {
    bounded(async {
        let Some(server) = server("relations").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Author>().await.unwrap();
        c.create_table::<Book>().await.unwrap();
        c.create_table::<Bio>().await.unwrap();
        c.create_table::<Review>().await.unwrap();
        c.create_table::<Genre>().await.unwrap();
        c.create_table::<AuthorGenre>().await.unwrap();

        let authors = c
            .insert_many(&[author(1, "Ada"), author(2, "Brontë"), author(3, "Cato")])
            .await
            .unwrap();
        c.insert_many(&[
            book(10, Some(1), "Notes"),
            book(11, Some(1), "Engines"),
            book(12, Some(2), "Jane"),
            book(13, None, "Anonymous"),
        ])
        .await
        .unwrap();
        c.insert(&Bio {
            author_id: 2,
            text: "Yorkshire".into(),
        })
        .await
        .unwrap();
        c.insert_many(&[
            Review {
                id: 1,
                book_id: 10,
                stars: 5,
            },
            Review {
                id: 2,
                book_id: 10,
                stars: 3,
            },
            Review {
                id: 3,
                book_id: 12,
                stars: 4,
            },
        ])
        .await
        .unwrap();
        let genres = c
            .insert_many(&[
                Genre {
                    id: 1,
                    name: "maths".into(),
                },
                Genre {
                    id: 2,
                    name: "novel".into(),
                },
                Genre {
                    id: 3,
                    name: "poetry".into(),
                },
            ])
            .await
            .unwrap();

        // many-to-many links: attach (a repeat is skipped), detach, sync.
        assert_eq!(
            Author::genres
                .attach(&authors[0], &genres[..2], &c)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            Author::genres
                .attach(&authors[0], &genres[..1], &c)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            Author::genres
                .attach(&authors[1], &genres[1..], &c)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            Author::genres
                .detach(&authors[0], &genres[1..2], &c)
                .await
                .unwrap(),
            1
        );
        let synced = Author::genres
            .sync(&authors[1], [&genres[1], &genres[0]], &c)
            .await
            .unwrap();
        assert_eq!(
            (synced.detached, synced.attached),
            (1, 1),
            "poetry out, maths in"
        );
        assert_eq!(
            Author::genres
                .attach(&authors[2], &genres[2..], &c)
                .await
                .unwrap(),
            1
        );
        assert_eq!(Author::genres.detach_all(&authors[2], &c).await.unwrap(), 1);
        let links: Vec<AuthorGenre> = AuthorGenre::query()
            .order_by(AuthorGenre::author_id.asc())
            .order_by(AuthorGenre::genre_id.asc())
            .all(&c)
            .await
            .unwrap();
        let pairs: Vec<(i64, i64)> = links.iter().map(|l| (l.author_id, l.genre_id)).collect();
        assert_eq!(pairs, [(1, 1), (2, 1), (2, 2)]);

        // Eager: one statement for the authors, one per relation.
        let counting = Counting::new(&c);
        let loaded = Author::query()
            .with(Author::books.order_by(Book::id.asc()))
            .with(Author::bio)
            .with(Author::genres.order_by(Genre::name.desc()))
            .with_count(Author::books)
            .with_count(Author::genres)
            .order_by(Author::id.asc())
            .all(&counting)
            .await
            .unwrap();
        // The authors; books; bio; the join rows and the genres; two counts.
        assert_eq!(counting.take(), 7);
        assert_eq!(titles(&loaded[0].books), ["Notes", "Engines"]);
        assert_eq!(titles(&loaded[1].books), ["Jane"]);
        assert!(loaded[2].books.is_empty());
        assert_eq!(loaded[0].bio, None);
        assert_eq!(loaded[1].bio.as_ref().unwrap().text, "Yorkshire");
        let names = |a: &Author| a.genres.iter().map(|g| g.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&loaded[0]), ["maths"]);
        assert_eq!(names(&loaded[1]), ["novel", "maths"], "per relation order");
        assert!(loaded[2].genres.is_empty());
        let counts: Vec<(i64, i64)> = loaded
            .iter()
            .map(|a| (a.books_count, a.genres_count))
            .collect();
        assert_eq!(counts, [(2, 1), (1, 2), (0, 0)]);

        // Nested, filtered, limited per parent, and belongs-to.
        let nested = Author::query()
            .filter(Author::id.is_in([1i64, 2]))
            .with(
                Author::books.order_by(Book::id.desc()).limit(1).with(
                    Book::reviews
                        .filter(Review::stars.ge(4))
                        .order_by(Review::id.asc()),
                ),
            )
            .order_by(Author::id.asc())
            .all(&counting)
            .await
            .unwrap();
        assert_eq!(counting.take(), 3, "authors, books, reviews");
        assert_eq!(
            titles(&nested[0].books),
            ["Engines"],
            "one per parent, the last"
        );
        assert_eq!(titles(&nested[1].books), ["Jane"]);
        assert_eq!(nested[1].books[0].reviews.len(), 1);
        let with_reviews = Book::query()
            .with(Book::reviews.filter(Review::stars.ge(4)))
            .with(Book::author)
            .order_by(Book::id.asc())
            .all(&c)
            .await
            .unwrap();
        let stars: Vec<Vec<i32>> = with_reviews
            .iter()
            .map(|b| b.reviews.iter().map(|r| r.stars).collect())
            .collect();
        assert_eq!(stars, [vec![5], vec![], vec![4], vec![]]);
        let owners: Vec<Option<&str>> = with_reviews
            .iter()
            .map(|b| b.author.as_deref().map(|a| a.name.as_str()))
            .collect();
        assert_eq!(owners, [Some("Ada"), Some("Ada"), Some("Brontë"), None]);

        // Lazy, into one row.
        let mut ada: Author = c.find(1i64).await.unwrap().unwrap();
        assert!(ada.books.is_empty());
        ada.load(Author::books.order_by(Book::title.asc()), &c)
            .await
            .unwrap();
        assert_eq!(titles(&ada.books), ["Engines", "Notes"]);
        let mut many = Author::query()
            .order_by(Author::id.asc())
            .all(&c)
            .await
            .unwrap();
        Author::genres
            .all()
            .load(&mut many, &counting)
            .await
            .unwrap();
        assert_eq!(counting.take(), 2, "the join rows, then the genres");
        assert_eq!(many[1].genres.len(), 2);

        // Filters on relations.
        let ids = |rows: Vec<Author>| rows.into_iter().map(|a| a.id).collect::<Vec<_>>();
        let has_books = Author::query()
            .where_has(Author::books)
            .order_by(Author::id.asc());
        assert_eq!(ids(has_books.all(&c).await.unwrap()), [1, 2]);
        let none = Author::query().where_doesnt_have(Author::books);
        assert_eq!(ids(none.all(&c).await.unwrap()), [3]);
        let starred = Author::query().where_has(Author::books.filter(Book::title.like("J%")));
        assert_eq!(ids(starred.all(&c).await.unwrap()), [2]);
        let novelists = Author::query()
            .where_has(Author::genres.filter(Genre::name.eq("novel")))
            .order_by(Author::id.asc());
        assert_eq!(ids(novelists.all(&c).await.unwrap()), [2]);
        let orphans: Vec<Book> = Book::query()
            .where_doesnt_have(Book::author)
            .all(&c)
            .await
            .unwrap();
        assert_eq!(titles(&orphans), ["Anonymous"], "a NULL key has no author");
        let by_ada: Vec<Book> = Book::query()
            .where_has(Book::author.filter(Author::name.eq("Ada")))
            .order_by(Book::id.asc())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(titles(&by_ada), ["Notes", "Engines"]);
        assert_eq!(
            Author::query()
                .where_has(Author::genres)
                .count(&c)
                .await
                .unwrap(),
            2
        );

        // A count needs a field to go in.
        let e = Book::query()
            .with_count(Book::reviews)
            .all(&c)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("count_of"), "{e}");

        // The foreign keys are the engine's: refused, and cascaded.
        let e = c.insert(&book(20, Some(99), "Ghost")).await.unwrap_err();
        assert!(e.to_string().contains("foreign key"), "{e}");
        assert!(c.delete(&authors[0]).await.unwrap());
        let left: Vec<Book> = Book::query()
            .order_by(Book::id.asc())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(
            titles(&left),
            ["Jane", "Anonymous"],
            "Ada's books went with her"
        );
        assert_eq!(
            Review::query().count(&c).await.unwrap(),
            1,
            "and their reviews"
        );
        assert_eq!(
            AuthorGenre::query().count(&c).await.unwrap(),
            2,
            "and her links"
        );
    })
    .await
}

// ------------------------------------------------- projections, aggregates

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "sales")]
struct Sale {
    #[rdb(primary_key)]
    id: i64,
    region: String,
    units: i32,
    amount: i64,
    #[rdb(sql_type = "NUMERIC(10,2)", text)]
    price: Numeric,
    rate: Option<f64>,
}

#[derive(FromRow, Debug, PartialEq)]
struct RegionTotal {
    region: String,
    sales: i64,
    amount: Option<i64>,
}

fn num(n: &Option<Numeric>) -> f64 {
    n.as_ref().unwrap().to_string().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn projections_and_aggregates_decode() {
    bounded(async {
        let Some(server) = server("aggregates").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Sale>().await.unwrap();
        let big = 3_000_000_000i64; // past i32: SUM(bigint) is NUMERIC on the server
        let sale =
            |id: i64, region: &str, units: i32, amount: i64, price: &str, rate: Option<f64>| Sale {
                id,
                region: region.into(),
                units,
                amount,
                price: Numeric::new(price).unwrap(),
                rate,
            };
        c.insert_many(&[
            sale(1, "north", 1, big, "10.50", Some(0.5)),
            sale(2, "north", 2, big, "20.25", None),
            sale(3, "south", 4, 7, "3.00", Some(1.5)),
            sale(4, "east", 5, 1, "99.99", Some(2.5)),
            sale(5, "east", 6, -2, "0.01", None),
        ])
        .await
        .unwrap();

        let (count, sum, avg_units, max_price, min_region, avg_rate, regions): (
            i64,
            Option<i64>,
            Option<Numeric>,
            Option<Numeric>,
            Option<String>,
            Option<f64>,
            i64,
        ) = Sale::query()
            .select((
                count_rows(),
                Sale::amount.sum(),
                Sale::units.avg(),
                Sale::price.max(),
                Sale::region.min(),
                Sale::rate.avg(),
                Sale::region.count_distinct(),
            ))
            .one(&c)
            .await
            .unwrap();
        assert_eq!(count, 5);
        assert_eq!(sum, Some(2 * big + 6), "an exact i64, cast outside the SUM");
        assert_eq!(num(&avg_units), 3.6);
        assert_eq!(max_price.unwrap().to_string(), "99.99");
        assert_eq!(min_region.as_deref(), Some("east"));
        assert_eq!(avg_rate, Some(1.5), "NULLs are not averaged");
        assert_eq!(regions, 3);

        let total: Option<Numeric> = Sale::query()
            .select(Sale::price.sum())
            .one(&c)
            .await
            .unwrap();
        assert_eq!(
            total.unwrap().to_string(),
            "133.75",
            "NUMERIC(10,2) keeps its scale as text"
        );
        let nothing: (Option<i64>, Option<Numeric>, Option<i32>) = Sale::query()
            .filter(Sale::id.gt(100i64))
            .select((Sale::amount.sum(), Sale::units.avg(), Sale::units.max()))
            .one(&c)
            .await
            .unwrap();
        assert_eq!(
            nothing,
            (None, None, None),
            "aggregates over no rows are NULL"
        );

        let grouped: Vec<(String, i64, Option<i64>)> = Sale::query()
            .select((Sale::region, count_rows(), Sale::amount.sum()))
            .group_by(Sale::region)
            .having(count_rows::<Sale>().ge(2))
            .order_by(Sale::amount.sum().desc())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(
            grouped,
            [
                ("north".to_string(), 2, Some(2 * big)),
                ("east".to_string(), 2, Some(-1)),
            ]
        );
        let structs: Vec<RegionTotal> = Sale::query()
            .filter(Sale::units.ge(2))
            .select((Sale::region, count_rows(), Sale::amount.sum()))
            .group_by(Sale::region)
            .order_by(Sale::region.asc())
            .all_as(&c)
            .await
            .unwrap();
        assert_eq!(
            structs,
            [
                RegionTotal {
                    region: "east".into(),
                    sales: 2,
                    amount: Some(-1)
                },
                RegionTotal {
                    region: "north".into(),
                    sales: 1,
                    amount: Some(big)
                },
                RegionTotal {
                    region: "south".into(),
                    sales: 1,
                    amount: Some(7)
                },
            ]
        );
        let columns: Vec<(i64, Numeric)> = Sale::query()
            .filter(Sale::region.eq("east"))
            .order_by(Sale::id.asc())
            .select((Sale::id, Sale::price))
            .all(&c)
            .await
            .unwrap();
        assert_eq!(columns[1].1.to_string(), "0.01");
        let distinct: Vec<String> = Sale::query()
            .select(Sale::region)
            .distinct()
            .order_by(Sale::region.asc())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(distinct, ["east", "north", "south"]);
    })
    .await
}

// ------------------------------------------------------- keyset and stream

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "events")]
struct Event {
    #[rdb(primary_key)]
    id: i64,
    kind: String,
    at: Timestamp,
    score: i32,
}

fn ids(events: &[Event]) -> Vec<i64> {
    events.iter().map(|e| e.id).collect()
}

/// Every page of `q` forwards, then every page backwards from the end:
/// both must be the whole result in order.
async fn walk(c: &Client, q: rasterizeddb_client::orm::Select<Event>) -> (Vec<i64>, Vec<i64>) {
    let mut forward = Vec::new();
    let mut page = q.clone().page(c).await.unwrap();
    assert!(
        page.prev_cursor.is_none(),
        "the first page has nothing before it"
    );
    let mut last_page;
    loop {
        forward.extend(ids(&page.items));
        last_page = page.clone();
        match &page.next_cursor {
            // Through the string form, as a web client would carry it.
            Some(cursor) => {
                let carried = Cursor::decode(&cursor.encode().unwrap()).unwrap();
                page = q.clone().after(&carried).page(c).await.unwrap();
            }
            None => break,
        }
    }
    let mut backward: Vec<i64> = ids(&last_page.items).into_iter().rev().collect();
    let mut page = last_page;
    while let Some(cursor) = page.prev_cursor.clone() {
        page = q.clone().before(&cursor).page(c).await.unwrap();
        assert!(page.next_cursor.is_some());
        backward.extend(ids(&page.items).into_iter().rev());
    }
    backward.reverse();
    (forward, backward)
}

use rasterizeddb_client::orm::Cursor;

#[tokio::test(flavor = "multi_thread")]
async fn keyset_pages_and_streams() {
    bounded(async {
        let Some(server) = server("keyset").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Event>().await.unwrap();
        let base = 1_700_000_000_000_000i64;
        let events: Vec<Event> = (1..=50)
            .map(|i| Event {
                id: i,
                kind: ["b", "a", "c"][(i % 3) as usize].to_string(),
                at: Timestamp {
                    micros: base + (i * 7919 % 50) * 1_000_000,
                },
                score: (i % 5) as i32,
            })
            .collect();
        c.insert_many(&events).await.unwrap();

        // One direction, many ties: (score, id) < (x, y).
        let by_score = Event::query().order_by(Event::score.desc()).limit(7);
        let whole = ids(&Event::query()
            .order_by(Event::score.desc())
            .order_by(Event::id.desc())
            .all(&c)
            .await
            .unwrap());
        let (forward, backward) = walk(&c, by_score).await;
        assert_eq!(forward, whole);
        assert_eq!(backward, whole);

        // Mixed directions, filtered: the OR-chain.
        let mixed = Event::query()
            .filter(Event::score.ne(0))
            .order_by(Event::kind.asc())
            .order_by(Event::at.desc())
            .limit(6);
        let whole = ids(&Event::query()
            .filter(Event::score.ne(0))
            .order_by(Event::kind.asc())
            .order_by(Event::at.desc())
            .order_by(Event::id.desc())
            .all(&c)
            .await
            .unwrap());
        assert_eq!(whole.len(), 40);
        let (forward, backward) = walk(&c, mixed).await;
        assert_eq!(forward, whole);
        assert_eq!(backward, whole);

        // A page that ends exactly at the end says so.
        let exact = Event::query().limit(50).page(&c).await.unwrap();
        assert_eq!(exact.items.len(), 50);
        assert!(exact.next_cursor.is_none());
        let e = Event::query().page(&c).await.unwrap_err();
        assert!(e.to_string().contains("limit"), "{e}");

        // A stream: every row in key order, one statement per batch.
        let counting = Counting::new(&c);
        let mut stream = Event::query()
            .order_by(Event::at.asc())
            .stream(&counting, 6);
        let mut streamed = Vec::new();
        while let Some(row) = stream.next().await {
            streamed.push(row.unwrap().id);
        }
        let whole = ids(&Event::query()
            .order_by(Event::at.asc())
            .order_by(Event::id.asc())
            .all(&c)
            .await
            .unwrap());
        assert_eq!(streamed, whole);
        assert_eq!(counting.take(), 9, "ceil(50 / 6) batches");
        let mut capped = Event::query()
            .filter(Event::score.eq(1))
            .limit(4)
            .stream(&c, 3);
        let mut first = Vec::new();
        while let Some(batch) = capped.next_batch().await {
            first.push(batch.unwrap().len());
        }
        assert_eq!(first, [3, 1], "a limit caps the stream");
    })
    .await
}

// --------------------------------------------- timestamps and soft delete

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "folders")]
struct Folder {
    #[rdb(primary_key)]
    id: i64,
    #[rdb(has_many(Note, foreign_key = "folder_id"))]
    notes: Vec<Note>,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "notes")]
struct Note {
    #[rdb(primary_key, serial)]
    id: i64,
    folder_id: i64,
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

fn note(folder_id: i64, body: &str) -> Note {
    Note {
        id: 0,
        folder_id,
        body: body.into(),
        created: Timestamp { micros: 0 },
        updated: Timestamp { micros: 0 },
        deleted: Some(Timestamp { micros: 0 }),
        version: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamps_come_from_the_server_and_deletes_are_soft() {
    bounded(async {
        let Some(server) = server("soft").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Folder>().await.unwrap();
        c.create_table::<Note>().await.unwrap();
        c.insert(&Folder {
            id: 1,
            notes: vec![],
        })
        .await
        .unwrap();

        let before = Timestamp::now().micros;
        let mut a = c.insert(&note(1, "a")).await.unwrap();
        assert!(
            a.created.micros >= before - 60_000_000,
            "the server's clock, not 1970"
        );
        // The server evaluates now() per call (PostgreSQL holds it for the
        // transaction), so the two may differ by microseconds.
        assert!(a.updated.micros >= a.created.micros);
        assert!(a.updated.micros - a.created.micros < 1_000_000);
        assert_eq!(
            a.deleted,
            Some(Timestamp { micros: 0 }),
            "inserted as given"
        );
        a = c.restore(&a).await.unwrap();
        assert_eq!(a.deleted, None);
        assert_eq!(a.version, 2, "restore is a versioned write");
        tokio::time::sleep(Duration::from_millis(20)).await;
        a.body = "a2".into();
        let a = c.update(&a).await.unwrap();
        assert!(a.updated.micros > a.created.micros, "updated moves");
        let stored: Note = c.find(a.id).await.unwrap().unwrap();
        assert_eq!(stored.created, a.created, "created never moves");

        let mut b = note(1, "b");
        b.deleted = None;
        let b = c.insert(&b).await.unwrap();
        let mut gone = note(1, "gone");
        gone.deleted = None;
        let gone = c.insert(&gone).await.unwrap();

        // A soft delete: the row stays, queries leave it out.
        assert!(c.delete(&b).await.unwrap());
        assert!(
            !c.delete(&b).await.unwrap(),
            "already deleted: false, not a conflict"
        );
        assert!(c.find::<Note>(b.id).await.unwrap().is_none());
        assert_eq!(Note::query().count(&c).await.unwrap(), 2);
        assert_eq!(Note::query().with_deleted().count(&c).await.unwrap(), 3);
        let deleted = Note::query().only_deleted().all(&c).await.unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].body, "b");
        assert!(deleted[0].deleted.is_some());
        assert_eq!(deleted[0].version, b.version + 1);

        // Relations see live rows too, unless told otherwise.
        let folders = Folder::query().with(Folder::notes).all(&c).await.unwrap();
        assert_eq!(folders[0].notes.len(), 2);
        let all = Folder::query()
            .with(Folder::notes.with_deleted())
            .all(&c)
            .await
            .unwrap();
        assert_eq!(all[0].notes.len(), 3);
        let with_deleted_b = Folder::query()
            .where_has(Folder::notes.filter(Note::body.eq("b")))
            .count(&c)
            .await
            .unwrap();
        assert_eq!(with_deleted_b, 0, "a deleted note does not count");
        let (n,): (i64,) = Note::query().select((count_rows(),)).one(&c).await.unwrap();
        assert_eq!(n, 2);

        // Restore, then force.
        let b = c.restore(&deleted[0]).await.unwrap();
        assert_eq!(b.deleted, None);
        assert!(c.find::<Note>(b.id).await.unwrap().is_some());
        assert!(c.force_delete(&gone).await.unwrap());
        assert_eq!(Note::query().with_deleted().count(&c).await.unwrap(), 2);

        // Bulk: delete marks, bumps versions and update times; force removes.
        let marked = Note::query()
            .filter(Note::body.eq("b"))
            .delete(&c)
            .await
            .unwrap();
        assert_eq!(marked, 1);
        assert_eq!(Note::query().count(&c).await.unwrap(), 1);
        let removed = Note::query()
            .filter(Note::folder_id.eq(1i64))
            .with_deleted()
            .force_delete(&c)
            .await
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(Note::query().with_deleted().count(&c).await.unwrap(), 0);
    })
    .await
}

// ------------------------------------------------------------- upserts

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "members")]
struct Member {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(unique)]
    email: String,
    name: String,
    visits: i64,
}

fn member(email: &str, name: &str, visits: i64) -> Member {
    Member {
        id: 0,
        email: email.into(),
        name: name.into(),
        visits,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_upsert_builder_absorbs_only_its_arbiter() {
    bounded(async {
        let Some(server) = server("upsert").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Member>().await.unwrap();

        // By email: the serial id is the server's, the second call updates.
        let ada = c
            .upsert(&member("ada@x", "Ada", 1))
            .on_conflict(Member::email)
            .await
            .unwrap();
        assert!(ada.id > 0, "the sequence assigned it");
        let again = c
            .upsert(&member("ada@x", "Ada L.", 5))
            .on_conflict(Member::email)
            .do_update(Member::name)
            .await
            .unwrap();
        assert_eq!(again.id, ada.id);
        assert_eq!(
            (again.name.as_str(), again.visits),
            ("Ada L.", 1),
            "only name moved"
        );
        let kept = c
            .upsert(&member("ada@x", "ignored", 9))
            .on_conflict(Member::email)
            .do_update_all_except(Member::name)
            .await
            .unwrap();
        assert_eq!((kept.name.as_str(), kept.visits), ("Ada L.", 9));
        let skipped = c
            .upsert(&member("ada@x", "nope", 0))
            .on_conflict(Member::email)
            .do_nothing()
            .await
            .unwrap();
        assert_eq!(skipped, None, "the key was taken");
        let new = c
            .upsert(&member("bob@x", "Bob", 0))
            .on_conflict(Member::email)
            .do_nothing()
            .await
            .unwrap();
        assert_eq!(new.as_ref().map(|m| m.name.as_str()), Some("Bob"));

        // The arbiter absorbs its own clash only: a clash on the email
        // under ON CONFLICT (id) is still an error, and writes nothing.
        let mut clash = member("ada@x", "Clash", 0);
        clash.id = 999;
        let e = c.upsert(&clash).await.unwrap_err();
        assert!(e.is_unique_violation(), "{e}");
        let (mut carol, mut dup) = (member("carol@x", "Carol", 0), member("ada@x", "Dup", 0));
        (carol.id, dup.id) = (100, 101);
        let e = c
            .insert_many(&[carol, dup])
            .on_conflict(Member::id)
            .do_nothing()
            .await
            .unwrap_err();
        assert!(e.is_unique_violation(), "{e}");
        assert_eq!(
            Member::query().count(&c).await.unwrap(),
            2,
            "carol was not half-written"
        );

        // Many rows: skipped, or merged.
        let inserted = c
            .insert_many(&[
                member("carol@x", "Carol", 1),
                member("ada@x", "skipped", 0),
                member("dan@x", "Dan", 1),
            ])
            .on_conflict(Member::email)
            .do_nothing()
            .await
            .unwrap();
        let names: Vec<&str> = inserted.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["Carol", "Dan"], "only the rows inserted come back");
        let merged = c
            .insert_many(&[member("carol@x", "Carol2", 7), member("eve@x", "Eve", 1)])
            .on_conflict(Member::email)
            .do_update((Member::name, Member::visits))
            .await
            .unwrap();
        assert_eq!(merged.len(), 2);
        let carol = Member::query()
            .filter(Member::email.eq("carol@x"))
            .one(&c)
            .await
            .unwrap();
        assert_eq!((carol.name.as_str(), carol.visits), ("Carol2", 7));
        let targetless = c
            .insert_many(&[member("eve@x", "x", 0), member("fay@x", "Fay", 0)])
            .do_nothing()
            .await
            .unwrap();
        assert_eq!(targetless.len(), 1, "any clash skipped without a target");
        assert_eq!(Member::query().count(&c).await.unwrap(), 6);
    })
    .await
}

// ---------------------------------------------------------------- RETURNING

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "prices")]
struct Price {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(sql_type = "NUMERIC(10,2)", text)]
    amount: Numeric,
    label: String,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "pairs")]
struct Pair {
    #[rdb(primary_key)]
    a: i64,
    #[rdb(primary_key)]
    b: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_return_rows_as_stored() {
    bounded(async {
        let Some(server) = server("returning").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Price>().await.unwrap();
        c.create_table::<Pair>().await.unwrap();
        let counting = Counting::new(&c);

        // The server's RETURNING lists, casts included, decode as the model.
        let sent = Price {
            id: 0,
            amount: Numeric::new("1.239").unwrap(),
            label: "x".into(),
        };
        let stored = counting.insert(&sent).await.unwrap();
        assert_eq!(counting.take(), 1, "no read after the write");
        assert!(stored.id > 0);
        let read: Price = c.find(stored.id).await.unwrap().unwrap();
        assert_eq!(stored, read, "RETURNING is the row as stored");
        let mut changed = read.clone();
        changed.amount = Numeric::new("7.005").unwrap();
        let updated = counting.update(&changed).await.unwrap();
        assert_eq!(counting.take(), 1);
        assert_eq!(updated, c.find::<Price>(stored.id).await.unwrap().unwrap());
        let upserted = counting.upsert(&updated).await.unwrap();
        assert_eq!(counting.take(), 1);
        assert_eq!(upserted, c.find::<Price>(stored.id).await.unwrap().unwrap());

        // A model that is all key: the second upsert finds the row there and
        // returns it from the same statement, with no re-read to race a
        // delete.
        let pair = Pair { a: 1, b: 2 };
        assert_eq!(counting.upsert(&pair).await.unwrap(), pair);
        assert_eq!(counting.upsert(&pair).await.unwrap(), pair);
        assert_eq!(counting.take(), 2);
        assert_eq!(Pair::query().count(&c).await.unwrap(), 1);

        // Deletes can return what they removed.
        c.insert(&Pair { a: 3, b: 4 }).await.unwrap();
        let gone = Pair::query()
            .filter(Pair::a.ge(3i64))
            .delete_returning(&c)
            .await
            .unwrap();
        assert_eq!(gone, [Pair { a: 3, b: 4 }]);
        assert_eq!(Pair::query().count(&c).await.unwrap(), 1);
        // The server takes RETURNING * as well; the ORM names its columns,
        // in order, to cast the ones native cannot send as they are.
        assert_eq!(
            c.run("DELETE FROM pairs RETURNING *").await.unwrap().len(),
            1
        );
    })
    .await
}
