# Administration

[`client.admin()`](crate::Client::admin) turns the server's own statements
into typed calls. Each writes the statement the server's grammar reads, with
every value quoted or checked so that it cannot change the statement, and
reads the answer into a struct. Most need a superuser login (the server's
rule); a unix socket under `--rdb-socket-auth trust` is one.

## Key/value stores

A deployment that adds its S3 store on every start:

```rust,no_run
use rasterizeddb_client::admin::{Addressing, Placement, Store};

# struct S3 { endpoint: String, region: String, bucket: String, access_key_id: String, secret_access_key: String }
# async fn demo(client: &rasterizeddb_client::Client, s3: &S3, prefix: &str) -> rasterizeddb_client::Result<()> {
client
    .admin()
    .add_store(
        &Store::s3("telemetry", &s3.endpoint, &s3.bucket)
            .region(&s3.region)
            .prefix(prefix)
            .addressing(Addressing::Virtual)
            .credentials(&s3.access_key_id, &s3.secret_access_key)
            .if_not_exists(), // a no-op on the same definition; credentials still rotate
    )
    .await?;

// Tables go on it before they are created, or move there later.
client.admin().place_table("events", &Placement::kv("telemetry")).await?;
client.admin().move_table("old_events", &Placement::kv("telemetry")).await?;
# Ok(()) }
```

Credentials go to the server's secrets file, never its store catalogue, and
are redacted from its log; this crate's `Debug` for a
[`Store`](crate::admin::Store) redacts them too. A value no store statement
can carry (both quote kinds, a newline) is refused here, without echoing it.

| call | statement |
|---|---|
| [`add_store`](crate::admin::Admin::add_store) | `STORAGE ADD STORE [IF NOT EXISTS] …` |
| [`alter_store`](crate::admin::Admin::alter_store), [`rotate_store_credentials`](crate::admin::Admin::rotate_store_credentials) | `STORAGE ALTER STORE … SET …` |
| [`drop_store`](crate::admin::Admin::drop_store), [`drop_store_if_exists`](crate::admin::Admin::drop_store_if_exists) | `STORAGE DROP STORE …` |
| [`stores`](crate::admin::Admin::stores), [`store`](crate::admin::Admin::store) | `SHOW STORAGE STORES` |
| [`place_table`](crate::admin::Admin::place_table), [`move_table`](crate::admin::Admin::move_table) | `STORAGE PLACE/MOVE TABLE …` |
| [`storage_locations`](crate::admin::Admin::storage_locations), [`storage_stats`](crate::admin::Admin::storage_stats) | `SHOW STORAGE LOCATIONS/STATS` |

## Indexes and partitions

```rust,no_run
use rasterizeddb_client::admin::{Index, IndexMethod};

# async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
let admin = client.admin();
admin.create_index(&Index::on("users", &["email"]).unique().if_not_exists()).await?;
admin.create_index(&Index::on("users", &["team"]).using(IndexMethod::Hash).if_not_exists()).await?;
admin.create_index(&Index::on("docs", &["body"]).using(IndexMethod::GinPathOps)).await?;
admin.index_checkpoint("users").await?; // seal the indexes so a restart loads them

admin.partition_by_hash("events", &["tenant_id"], Some(64)).await?;
for p in admin.partitions("events").await? {
    println!("{} {}..{} {}", p.pid, p.lo, p.hi, p.total_bytes);
}
# Ok(()) }
```

## Looking around

```rust,no_run
# async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
let admin = client.admin();
for t in admin.tables().await? {
    println!("{}: {} bytes, {} reclaimable", t.name, t.total_bytes, t.reclaimable_bytes);
}
for c in admin.columns("users").await? {
    println!("{} {} nullable={} unique={}", c.name, c.data_type, c.nullable, c.unique);
}
let reclaimed = admin.vacuum(Some("users"), false).await?;
let settings = admin.config().await?;
admin.set_config("vacuum.enabled", "on").await?;
# let _ = (reclaimed, settings); Ok(()) }
```

## Roles, backups, replication

```rust,no_run
use rasterizeddb_client::admin::{RestoreTarget, Role};

# async fn demo(client: &rasterizeddb_client::Client) -> rasterizeddb_client::Result<()> {
let admin = client.admin();
admin.create_role(&Role::user("app").password("s3cret")).await?;
admin.set_role_password("app", Some("rotated")).await?;

let bytes = admin.backup_database().await?;
for b in admin.backups().await? {
    println!("{} {} {}", b.kind, b.id, b.created_at);
}
let report = admin.restore(&RestoreTarget::Latest, "/srv/restore-test").await?;

let peers = admin.replication_peers().await?;
# let _ = (bytes, report, peers); Ok(()) }
```
