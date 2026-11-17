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
