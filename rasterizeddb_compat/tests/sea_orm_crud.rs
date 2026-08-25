//! Drives the engine through sea-orm rather than raw SQL.
//!
//! The raw-SQL probes isolate language features; these tests check that the SQL
//! an ORM *actually emits* — always parameterised, always through the extended
//! query protocol — reaches the engine intact and decodes back into typed Rust
//! values.

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use rasterizeddb_compat::{TestServer, entities::prelude::*, entities::*, probes};
use rust_decimal::Decimal;
use sea_orm::*;
use uuid::Uuid;

async fn connect() -> (TestServer, DatabaseConnection) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");
    let db = server.connect_sea_orm().await.expect("sea-orm should connect");
    (server, db)
}

fn ts(s: &str) -> DateTime<FixedOffset> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .map(|d| d.and_utc().fixed_offset())
        .unwrap_or_else(|_| {
            DateTime::parse_from_rfc3339(s).expect("test timestamp should parse")
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn find_by_uuid_primary_key_round_trips_every_column_type() {
    let (_server, db) = connect().await;

    let id: Uuid = rasterizeddb_compat::schema::ids::USER_ALICE.parse().unwrap();
    let alice = User::find_by_id(id)
        .one(&db)
        .await
        .expect("query should succeed")
        .expect("Alice should exist");

    assert_eq!(alice.id, id, "UUID primary key");
    assert_eq!(alice.name, "Alice", "TEXT");
    assert!(alice.billing_setup, "BOOL");
    assert_eq!(alice.vat_rate, Decimal::new(20, 0), "NUMERIC");
    assert_eq!(
        alice.created_at.naive_utc(),
        ts("2026-01-01 08:00:00").naive_utc(),
        "TIMESTAMPTZ"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_compose_the_way_the_orm_builds_them() {
    let (_server, db) = connect().await;

    let acme: Uuid = rasterizeddb_compat::schema::ids::DOMAIN_ACME.parse().unwrap();
    let users = User::find()
        .filter(user::Column::DomainId.eq(acme))
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("query should succeed");

    let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ordering_and_paging_agree_with_the_orm() {
    let (_server, db) = connect().await;

    let page = User::find()
        .order_by_desc(user::Column::Name)
        .limit(2)
        .offset(1)
        .all(&db)
        .await
        .expect("query should succeed");

    let names: Vec<&str> = page.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Carol", "Bob"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_and_read_back_through_the_orm() {
    let (_server, db) = connect().await;

    let id = Uuid::new_v4();
    let domain_id: Uuid = rasterizeddb_compat::schema::ids::DOMAIN_ACME.parse().unwrap();
    let now = ts("2026-08-20 12:00:00");

    let model = user::ActiveModel {
        id: Set(id),
        name: Set("Erin".to_string()),
        email: Set("erin@acme.test".to_string()),
        password: Set("hash".to_string()),
        is_service_account: Set(false),
        is_privilaged: Set(false),
        domain_id: Set(domain_id),
        billing_setup: Set(true),
        is_legal_entity: Set(false),
        legal_entity_name: Set(None),
        country: Set(Some("FR".to_string())),
        vat_rate: Set(Decimal::new(2050, 2)),
        email_verified_at: Set(None),
        username: Set(None),
        approved_at: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    };

    User::insert(model).exec(&db).await.expect("insert should succeed");

    let found = User::find_by_id(id)
        .one(&db)
        .await
        .expect("query should succeed")
        .expect("the inserted user should be found");

    assert_eq!(found.name, "Erin");
    assert_eq!(found.vat_rate, Decimal::new(2050, 2));
    assert_eq!(found.country.as_deref(), Some("FR"));
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamp_range_filters_select_the_right_rows() {
    let (_server, db) = connect().await;

    let cutoff = ts("2026-02-15 00:00:00");
    let users = User::find()
        .filter(user::Column::CreatedAt.gt(cutoff))
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("query should succeed");

    let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Carol", "Dave"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn decimal_comparisons_keep_their_precision() {
    let (_server, db) = connect().await;

    let invoices = Invoice::find()
        .filter(invoice::Column::NetAmount.gt(Decimal::new(20000, 2)))
        .order_by_asc(invoice::Column::InvoiceNumber)
        .all(&db)
        .await
        .expect("query should succeed");

    let numbers: Vec<i64> = invoices.iter().map(|i| i.invoice_number).collect();
    assert_eq!(numbers, vec![1002, 1004, 1005]);
    assert_eq!(invoices[0].net_amount, Decimal::new(25050, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn related_entities_load_through_a_join() {
    let (_server, db) = connect().await;

    // `find_also_related` emits a LEFT JOIN with both entities selected.
    let pairs = User::find()
        .find_also_related(Domain)
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("join query should succeed");

    let rendered: Vec<String> = pairs
        .iter()
        .map(|(u, d)| format!("{}:{}", u.name, d.as_ref().map(|d| d.name.as_str()).unwrap_or("-")))
        .collect();

    assert_eq!(
        rendered,
        vec!["Alice:Acme", "Bob:Acme", "Carol:Globex", "Dave:Globex"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_on_the_joined_relation_is_pushed_down() {
    let (_server, db) = connect().await;

    let users = User::find()
        .inner_join(Domain)
        .filter(domain::Column::Name.eq("Acme"))
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("join query should succeed");

    let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn optional_columns_round_trip_as_none() {
    let (_server, db) = connect().await;

    let dave: Uuid = rasterizeddb_compat::schema::ids::USER_DAVE.parse().unwrap();
    let user = User::find_by_id(dave)
        .one(&db)
        .await
        .expect("query should succeed")
        .expect("Dave should exist");

    assert_eq!(user.country, None, "a NULL column decodes as None");
    assert_eq!(user.legal_entity_name, None);
    assert_eq!(user.email_verified_at, None, "a NULL timestamp");

    let alice: Uuid = rasterizeddb_compat::schema::ids::USER_ALICE.parse().unwrap();
    let alice = User::find_by_id(alice).one(&db).await.unwrap().unwrap();
    assert_eq!(alice.country.as_deref(), Some("BG"), "a present value is still Some");
    assert!(alice.email_verified_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn is_null_filters_the_way_the_orm_builds_them() {
    let (_server, db) = connect().await;

    let unverified = User::find()
        .filter(user::Column::EmailVerifiedAt.is_null())
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("query should succeed");
    let names: Vec<&str> = unverified.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Bob", "Dave"]);

    let verified = User::find()
        .filter(user::Column::EmailVerifiedAt.is_not_null())
        .order_by_asc(user::Column::Name)
        .all(&db)
        .await
        .expect("query should succeed");
    let names: Vec<&str> = verified.iter().map(|u| u.name.as_str()).collect();
    assert_eq!(names, vec!["Alice", "Carol"]);
}

/// The session lookup an application actually performs: live, not revoked.
#[tokio::test(flavor = "multi_thread")]
async fn the_live_session_query_selects_only_the_live_session() {
    let (_server, db) = connect().await;

    let cutoff = ts("2026-06-01 00:00:00");
    let live = UserSession::find()
        .filter(user_session::Column::ExpiresAt.gt(cutoff))
        .filter(user_session::Column::RevokedAt.is_null())
        .all(&db)
        .await
        .expect("query should succeed");

    let ids: Vec<String> = live.iter().map(|s| s.id.to_string()).collect();
    assert_eq!(ids, vec![rasterizeddb_compat::schema::ids::SESSION_LIVE]);
}

#[tokio::test(flavor = "multi_thread")]
async fn setting_a_column_to_null_and_back_round_trips() {
    let (_server, db) = connect().await;

    let alice: Uuid = rasterizeddb_compat::schema::ids::USER_ALICE.parse().unwrap();

    let mut model: user::ActiveModel = User::find_by_id(alice)
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .into();
    model.country = Set(None);
    model.update(&db).await.expect("update to NULL should succeed");

    let cleared = User::find_by_id(alice).one(&db).await.unwrap().unwrap();
    assert_eq!(cleared.country, None, "the column reads back as NULL");

    let mut model: user::ActiveModel = cleared.into();
    model.country = Set(Some("PL".to_string()));
    model.update(&db).await.expect("update back to a value should succeed");

    let restored = User::find_by_id(alice).one(&db).await.unwrap().unwrap();
    assert_eq!(
        restored.country.as_deref(),
        Some("PL"),
        "a column set to NULL must not stay NULL once given a value"
    );
}
