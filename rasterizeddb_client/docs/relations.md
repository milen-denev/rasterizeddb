# Relations

A relation is a field of a `#[derive(Model)]` struct that is not a column:
it holds rows of another model, and is filled by loading it. The derive
gives each one a [`Relation<Self, C>`](crate::orm::Relation) constant named
as the field (`User::posts`), which every call below takes.

```rust,no_run
use rasterizeddb_client::prelude::*;

#[derive(Model, Debug, Clone)]
#[rdb(table = "users")]
struct User {
    #[rdb(primary_key, serial)]
    id: i64,
    name: String,
    #[rdb(has_many(Post, foreign_key = "user_id"))]
    posts: Vec<Post>,
    #[rdb(has_one(Profile, foreign_key = "user_id"))]
    profile: Option<Profile>,
    #[rdb(many_to_many(Tag, through = UserTag, local = "user_id", foreign = "tag_id"))]
    tags: Vec<Tag>,
    #[rdb(count_of = "posts")]
    posts_count: i64,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "posts")]
struct Post {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(index, references = "users(id)", on_delete = "cascade")]
    user_id: i64,
    title: String,
    published: bool,
    #[rdb(belongs_to(User, foreign_key = "user_id"))]
    author: Option<Box<User>>,
    #[rdb(has_many(Comment, foreign_key = "post_id"))]
    comments: Vec<Comment>,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "comments")]
struct Comment {
    #[rdb(primary_key, serial)]
    id: i64,
    #[rdb(references = "posts(id)", on_delete = "cascade")]
    post_id: i64,
    body: String,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "profiles")]
struct Profile {
    #[rdb(primary_key, references = "users(id)", on_delete = "cascade")]
    user_id: i64,
    bio: String,
}

#[derive(Model, Debug, Clone)]
#[rdb(table = "tags")]
struct Tag {
    #[rdb(primary_key, serial)]
    id: i64,
    name: String,
}

/// The join table: a key on both columns, so a link is stored once.
#[derive(Model, Debug, Clone)]
#[rdb(table = "user_tags")]
struct UserTag {
    #[rdb(primary_key, references = "users(id)", on_delete = "cascade")]
    user_id: i64,
    #[rdb(primary_key, references = "tags(id)", on_delete = "cascade")]
    tag_id: i64,
}
# fn main() {}
```

| attribute | field type | matches |
|---|---|---|
| `belongs_to(C, foreign_key = "c_id")` | `Option<C>`, `Option<Box<C>>` | this model's `c_id` against `C`'s primary key (`owner_key = "…"` for another column) |
| `has_one(C, foreign_key = "p_id")` | `Option<C>`, `Option<Box<C>>` | `C`'s `p_id` against this model's primary key (`local_key = "…"`) |
| `has_many(C, foreign_key = "p_id")` | `Vec<C>` | as `has_one`, every row |
| `many_to_many(C, through = J, local = "p_id", foreign = "c_id")` | `Vec<C>` | rows of the join table `J` (a model, or a table name in quotes) linking this model's key (`local_key`) to `C`'s (`related_key`) |
| `count_of = "posts"` | `i64` | holds the count [`with_count`](crate::orm::Select::with_count) loads |

A relation's fields are `Default` until loaded: an empty `Vec`, `None`, 0.
The related model must be `Clone` (a belongs-to or many-to-many row is
shared by several parents) and `Send`.

## Eager loading, without N+1

[`Select::with`](crate::orm::Select::with) loads a relation into every row a
query reads, with **one statement per relation level**, whatever the number
of rows: `SELECT … FROM posts WHERE user_id IN (<every user's id>)`, then
the rows are matched to their parents here. A many-to-many level is two:
the join rows, then the related rows they name. Past
[`RELATION_CHUNK`](crate::orm::RELATION_CHUNK) parents, a level takes one
statement per chunk.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "users")] struct User { #[rdb(primary_key, serial)] id: i64, name: String,
#   #[rdb(has_many(Post, foreign_key = "user_id"))] posts: Vec<Post>,
#   #[rdb(count_of = "posts")] posts_count: i64 }
# #[derive(Model, Debug, Clone)] #[rdb(table = "posts")] struct Post { #[rdb(primary_key, serial)] id: i64, user_id: i64, title: String, published: bool,
#   #[rdb(belongs_to(User, foreign_key = "user_id"))] author: Option<Box<User>>,
#   #[rdb(has_many(Comment, foreign_key = "post_id"))] comments: Vec<Comment> }
# #[derive(Model, Debug, Clone)] #[rdb(table = "comments")] struct Comment { #[rdb(primary_key, serial)] id: i64, post_id: i64, body: String }
# async fn demo<E: Executor>(db: &E) -> rasterizeddb_client::Result<()> {
// Three statements: users, their published posts, those posts' comments.
let users: Vec<User> = User::query()
    .with(
        User::posts
            .filter(Post::published.eq(true))
            .order_by(Post::id.desc())
            .limit(3) // per user
            .with(Post::comments),
    )
    .with_count(User::posts) // one GROUP BY statement
    .all(db)
    .await?;

// Belongs-to the other way round.
let posts: Vec<Post> = Post::query().with(Post::author).all(db).await?;
# let _ = (users, posts); Ok(()) }
```

A per-relation `limit` is applied **per parent, on this side**: the native
protocol refuses the derived table a per-parent `LIMIT` needs in SQL (a
window over a subquery), so every matching related row is read and each
parent keeps its first `n`. Filter and order apply in the statement.

## Lazy loading

For rows already read: [`load`](crate::orm::ModelRelations::load) on one row
(the trait is in the prelude), or [`With::load`](crate::orm::With::load) on a
slice, which loads them all in one statement.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "users")] struct User { #[rdb(primary_key, serial)] id: i64,
#   #[rdb(has_many(Post, foreign_key = "user_id"))] posts: Vec<Post> }
# #[derive(Model, Debug, Clone)] #[rdb(table = "posts")] struct Post { #[rdb(primary_key, serial)] id: i64, user_id: i64 }
# async fn demo(db: &Client) -> rasterizeddb_client::Result<()> {
let mut user: User = db.find(1i64).await?.expect("there");
user.load(User::posts, db).await?;

let mut users: Vec<User> = User::query().all(db).await?;
User::posts.all().load(&mut users, db).await?;
# Ok(()) }
```

## Filtering on a relation

[`where_has`](crate::orm::Select::where_has) and
[`where_doesnt_have`](crate::orm::Select::where_doesnt_have) keep the rows
that have (or have no) related row matching the relation's filters. Each is
one uncorrelated subquery in the same statement:
`WHERE id IN (SELECT user_id FROM posts WHERE user_id IS NOT NULL AND …)`.

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "users")] struct User { #[rdb(primary_key, serial)] id: i64,
#   #[rdb(has_many(Post, foreign_key = "user_id"))] posts: Vec<Post> }
# #[derive(Model, Debug, Clone)] #[rdb(table = "posts")] struct Post { #[rdb(primary_key, serial)] id: i64, user_id: i64, published: bool }
# async fn demo(db: &Client) -> rasterizeddb_client::Result<()> {
let writers = User::query().where_has(User::posts.filter(Post::published.eq(true))).all(db).await?;
let idle = User::query().where_doesnt_have(User::posts).count(db).await?;
# let _ = (writers, idle); Ok(()) }
```

`where_doesnt_have` keeps a row whose key is NULL (it has nothing related),
which a bare `NOT IN` would drop, and the subquery leaves out NULL keys,
which would make a bare `NOT IN` match nothing at all.

The server accepts correlated `EXISTS` too, and joins (`JOIN`, `INNER JOIN`,
`LEFT JOIN`); the ORM uses `IN (SELECT …)` because the builder writes
column names unqualified, which is unambiguous inside a one-table
subquery and could be ambiguous in a join.

## Linking many-to-many rows

[`attach`](crate::orm::Relation::attach),
[`detach`](crate::orm::Relation::detach),
[`detach_all`](crate::orm::Relation::detach_all) and
[`sync`](crate::orm::Relation::sync) write the join table:

```rust,no_run
# use rasterizeddb_client::prelude::*;
# #[derive(Model, Debug, Clone)] #[rdb(table = "users")] struct User { #[rdb(primary_key, serial)] id: i64,
#   #[rdb(many_to_many(Tag, through = "user_tags", local = "user_id", foreign = "tag_id"))] tags: Vec<Tag> }
# #[derive(Model, Debug, Clone)] #[rdb(table = "tags")] struct Tag { #[rdb(primary_key, serial)] id: i64 }
# async fn demo(db: &Client, user: User, rust: Tag, sql: Tag) -> rasterizeddb_client::Result<()> {
User::tags.attach(&user, [&rust, &sql], db).await?;  // INSERT … ON CONFLICT DO NOTHING
User::tags.detach(&user, [&sql], db).await?;         // DELETE … AND tag_id IN (…)
User::tags.sync(&user, [&rust], db).await?;          // exactly these: a DELETE, then an INSERT
# Ok(()) }
```

`attach` relies on a key over the join table's two columns to skip a link
that is already there. `sync` is two statements; run it in a transaction to
make it one change.

## Foreign keys

`#[rdb(references = "users(id)")]` on a column declares a foreign key in the
`CREATE TABLE` the ORM writes (and in a migrator's `ADD COLUMN`), with
`on_delete` / `on_update` = `cascade`, `restrict`, `set null`, `set default`
or `no action`. The server enforces it: an orphan is refused, a cascade
deletes the children. A foreign key is one column (the server refuses
composite ones).

The model sync does not compare foreign keys on columns that already exist:
the native protocol has no catalogue of constraints to read them from. Add
one to an existing column in a migration step with `ALTER TABLE t ADD
CONSTRAINT <name> FOREIGN KEY (col) REFERENCES …` (without `IF NOT EXISTS`,
which this server reads as a constraint named `if`).
