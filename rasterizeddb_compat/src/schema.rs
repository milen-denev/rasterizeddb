//! DDL and seed data for the compatibility schema.
//!
//! Written the way an ORM migration would emit it — Postgres type names,
//! `NOT NULL` spelled out — so applying the schema is itself the first
//! compatibility probe.
//!
//! Deliberately absent: `JSON`/`JSONB` columns and `CREATE INDEX`. RasterizedDB
//! has no JSON type planned and does its own leaf-structure indexing, so
//! probing for them would only measure decisions already made.

/// Every table in dependency order.
pub const TABLES: &[&str] = &[
    "domain",
    "users",
    "roles",
    "user_roles",
    "domain_member",
    "user_session",
    "subscriptions",
    "invoice",
    "attachment",
    "document",
];

/// `CREATE TABLE` for each entry of [`TABLES`], in the same order.
pub const CREATE_TABLES: &[&str] = &[
    r#"CREATE TABLE domain (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        default_subscription UUID
    )"#,
    r#"CREATE TABLE users (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        password TEXT NOT NULL,
        is_service_account BOOL NOT NULL,
        is_privilaged BOOL NOT NULL,
        domain_id UUID NOT NULL,
        billing_setup BOOL NOT NULL,
        is_legal_entity BOOL NOT NULL,
        legal_entity_name TEXT,
        country TEXT,
        vat_rate NUMERIC NOT NULL,
        email_verified_at TIMESTAMPTZ,
        username TEXT,
        approved_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )"#,
    r#"CREATE TABLE roles (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        domain_id UUID,
        date_added TIMESTAMPTZ NOT NULL
    )"#,
    r#"CREATE TABLE user_roles (
        user_id UUID NOT NULL,
        role_id UUID NOT NULL,
        date_added TIMESTAMPTZ NOT NULL
    )"#,
    r#"CREATE TABLE domain_member (
        domain_id UUID NOT NULL,
        user_id UUID NOT NULL
    )"#,
    r#"CREATE TABLE user_session (
        id UUID NOT NULL,
        user_id UUID NOT NULL,
        domain_id UUID NOT NULL,
        ip_address TEXT,
        ip_inet INET,
        user_agent TEXT,
        approx_location TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        last_seen_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        revoked_at TIMESTAMPTZ,
        reverify_required BOOL NOT NULL
    )"#,
    r#"CREATE TABLE subscriptions (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        date_introduced TIMESTAMPTZ NOT NULL,
        last_charge TIMESTAMPTZ,
        user_id UUID NOT NULL,
        group_name TEXT NOT NULL,
        kind SMALLINT NOT NULL,
        allow_new_domains BOOL NOT NULL,
        infinite BOOL NOT NULL,
        status TEXT NOT NULL,
        active_until TIMESTAMPTZ
    )"#,
    r#"CREATE TABLE invoice (
        id UUID NOT NULL,
        invoice_number BIGINT NOT NULL,
        subscription_id UUID NOT NULL,
        domain_id UUID NOT NULL,
        user_id UUID NOT NULL,
        billing_period_start TIMESTAMPTZ NOT NULL,
        billing_period_end TIMESTAMPTZ NOT NULL,
        issued_at TIMESTAMPTZ NOT NULL,
        due_at TIMESTAMPTZ NOT NULL,
        net_amount NUMERIC NOT NULL,
        vat_amount NUMERIC NOT NULL,
        total_amount NUMERIC NOT NULL,
        vat_rate NUMERIC NOT NULL,
        status TEXT NOT NULL,
        stripe_payment_intent_id TEXT
    )"#,
    // Binary and array columns, which the `pms` schema uses sparingly but does
    // use. Kept in their own table so the entity models stay faithful.
    r#"CREATE TABLE attachment (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        content BYTEA,
        tags TEXT[]
    )"#,
    // A JSON column is not stored as JSON: each document is mapped onto a table
    // of its own, one column per key, so that the fields are queryable as their
    // inferred types. This table exercises that mapping end to end.
    r#"CREATE TABLE document (
        id UUID NOT NULL,
        name TEXT NOT NULL,
        payload JSONB
    )"#,
];

/// Stable UUIDs so probes can assert on exact values.
pub mod ids {
    pub const DOMAIN_ACME: &str = "11111111-1111-4111-8111-111111111111";
    pub const DOMAIN_GLOBEX: &str = "22222222-2222-4222-8222-222222222222";

    pub const USER_ALICE: &str = "aaaaaaaa-0000-4000-8000-000000000001";
    pub const USER_BOB: &str = "aaaaaaaa-0000-4000-8000-000000000002";
    pub const USER_CAROL: &str = "aaaaaaaa-0000-4000-8000-000000000003";
    pub const USER_DAVE: &str = "aaaaaaaa-0000-4000-8000-000000000004";

    pub const ROLE_ADMIN: &str = "bbbbbbbb-0000-4000-8000-000000000001";
    pub const ROLE_MEMBER: &str = "bbbbbbbb-0000-4000-8000-000000000002";
    pub const ROLE_AUDITOR: &str = "bbbbbbbb-0000-4000-8000-000000000003";

    pub const SUB_ACME: &str = "cccccccc-0000-4000-8000-000000000001";
    pub const SUB_GLOBEX: &str = "cccccccc-0000-4000-8000-000000000002";

    pub const SESSION_LIVE: &str = "dddddddd-0000-4000-8000-000000000001";
    pub const SESSION_EXPIRED: &str = "dddddddd-0000-4000-8000-000000000002";
    pub const SESSION_REVOKED: &str = "dddddddd-0000-4000-8000-000000000003";
}

/// Seed rows covering both domains, all relation shapes, and a spread of
/// timestamps and amounts wide enough for range and aggregate probes.
pub fn seed_statements() -> Vec<String> {
    use ids::*;
    let mut out = Vec::new();

    out.push(format!(
        "INSERT INTO domain (id, name, default_subscription) VALUES ('{}', 'Acme', '{}')",
        DOMAIN_ACME, SUB_ACME
    ));
    out.push(format!(
        "INSERT INTO domain (id, name, default_subscription) VALUES ('{}', 'Globex', NULL)",
        DOMAIN_GLOBEX
    ));

    // id, name, email, domain, legal_entity_name, country, vat_rate, verified_at, username, created_at
    let users: &[(&str, &str, &str, &str, &str, &str, &str, &str, &str, &str)] = &[
        (USER_ALICE, "Alice", "alice@acme.test", DOMAIN_ACME, "'Acme Ltd'", "'BG'", "20.00", "'2026-01-15 09:00:00'", "NULL", "'2026-01-01 08:00:00'"),
        (USER_BOB, "Bob", "bob@acme.test", DOMAIN_ACME, "NULL", "'BG'", "20.00", "NULL", "'bob'", "'2026-02-01 08:00:00'"),
        (USER_CAROL, "Carol", "carol@globex.test", DOMAIN_GLOBEX, "'Globex GmbH'", "'DE'", "19.00", "'2026-03-10 12:30:00'", "NULL", "'2026-03-01 08:00:00'"),
        (USER_DAVE, "Dave", "dave@globex.test", DOMAIN_GLOBEX, "NULL", "NULL", "0.00", "NULL", "NULL", "'2026-04-01 08:00:00'"),
    ];
    for (id, name, email, domain, legal, country, vat, verified, username, created) in users {
        let is_legal = if *legal == "NULL" { "false" } else { "true" };
        out.push(format!(
            "INSERT INTO users (id, name, email, password, is_service_account, is_privilaged, \
             domain_id, billing_setup, is_legal_entity, legal_entity_name, country, vat_rate, \
             email_verified_at, username, approved_at, created_at, updated_at) VALUES \
             ('{id}', '{name}', '{email}', 'hash', false, false, '{domain}', true, {is_legal}, \
             {legal}, {country}, {vat}, {verified}, {username}, NULL, {created}, {created})"
        ));
    }

    let acme = format!("'{}'", DOMAIN_ACME);
    let roles: &[(&str, &str, &str)] = &[
        (ROLE_ADMIN, "admin", acme.as_str()),
        (ROLE_MEMBER, "member", acme.as_str()),
        (ROLE_AUDITOR, "auditor", "NULL"),
    ];
    for (id, name, domain) in roles {
        out.push(format!(
            "INSERT INTO roles (id, name, domain_id, date_added) VALUES \
             ('{}', '{}', {}, '2026-01-01 00:00:00')",
            id, name, domain
        ));
    }

    for (user, role) in [
        (USER_ALICE, ROLE_ADMIN),
        (USER_ALICE, ROLE_MEMBER),
        (USER_BOB, ROLE_MEMBER),
        (USER_CAROL, ROLE_AUDITOR),
    ] {
        out.push(format!(
            "INSERT INTO user_roles (user_id, role_id, date_added) VALUES \
             ('{}', '{}', '2026-01-02 00:00:00')",
            user, role
        ));
    }

    for (domain, user) in [
        (DOMAIN_ACME, USER_ALICE),
        (DOMAIN_ACME, USER_BOB),
        (DOMAIN_GLOBEX, USER_CAROL),
        (DOMAIN_GLOBEX, USER_DAVE),
    ] {
        out.push(format!(
            "INSERT INTO domain_member (domain_id, user_id) VALUES ('{}', '{}')",
            domain, user
        ));
    }

    // One live session, one expired, one revoked.
    let sessions: &[(&str, &str, &str, &str, &str, &str)] = &[
        (SESSION_LIVE, USER_ALICE, DOMAIN_ACME, "'10.0.0.1'", "'2027-01-01 00:00:00'", "NULL"),
        (SESSION_EXPIRED, USER_ALICE, DOMAIN_ACME, "NULL", "'2026-01-20 00:00:00'", "NULL"),
        (SESSION_REVOKED, USER_BOB, DOMAIN_ACME, "'2001:db8::1'", "'2027-01-01 00:00:00'", "'2026-06-01 00:00:00'"),
    ];
    for (id, user, domain, ip, expires, revoked) in sessions {
        out.push(format!(
            "INSERT INTO user_session (id, user_id, domain_id, ip_address, user_agent, \
             ip_inet, approx_location, created_at, last_seen_at, expires_at, revoked_at, \
             reverify_required) \
             VALUES ('{}', '{}', '{}', {}, 'agent', {}, NULL, '2026-01-10 00:00:00', \
             '2026-01-11 00:00:00', {}, {}, false)",
            id, user, domain, ip, ip, expires, revoked
        ));
    }

    out.push(format!(
        "INSERT INTO subscriptions (id, name, date_introduced, last_charge, user_id, group_name, \
         kind, allow_new_domains, infinite, status, active_until) VALUES \
         ('{}', 'Acme Org', '2026-01-01 00:00:00', '2026-06-01 00:00:00', '{}', 'org', 1, true, \
         false, 'active', '2027-01-01 00:00:00')",
        SUB_ACME, USER_ALICE
    ));
    out.push(format!(
        "INSERT INTO subscriptions (id, name, date_introduced, last_charge, user_id, group_name, \
         kind, allow_new_domains, infinite, status, active_until) VALUES \
         ('{}', 'Globex Single', '2026-03-01 00:00:00', NULL, '{}', 'single', 0, false, false, \
         'active', NULL)",
        SUB_GLOBEX, USER_CAROL
    ));

    for (id, name, content, tags) in [
        ("ffffffff-0000-4000-8000-000000000001", "logo", "'\\xdeadbeef'", "'{alpha,beta}'"),
        ("ffffffff-0000-4000-8000-000000000002", "empty", "NULL", "'{}'"),
        ("ffffffff-0000-4000-8000-000000000003", "odd", "'\\x00ff'", "'{\"has,comma\",NULL}'"),
    ] {
        out.push(format!(
            "INSERT INTO attachment (id, name, content, tags) VALUES ('{}', '{}', {}, {})",
            id, name, content, tags
        ));
    }

    // One document per shape the inference rules have to distinguish.
    for (id, name, payload) in [
        (
            "cccccccc-1111-4000-8000-000000000001",
            "full",
            r#"{"name":"acme","hits":42,"ratio":1.5,"active":true,"seen":"2026-08-20T14:30:00Z","ip":"10.0.0.1","tags":["a","b"]}"#,
        ),
        (
            "cccccccc-1111-4000-8000-000000000002",
            "nested",
            r#"{"outer":"x","inner":{"a":1,"b":"deep"}}"#,
        ),
        ("cccccccc-1111-4000-8000-000000000003", "none", "NULL"),
    ] {
        let literal =
            if payload == "NULL" { "NULL".to_string() } else { format!("'{}'", payload) };
        out.push(format!(
            "INSERT INTO document (id, name, payload) VALUES ('{}', '{}', {})",
            id, name, literal
        ));
    }

    // Invoices spread across months and statuses for aggregate probes.
    let invoices: &[(&str, i64, &str, &str, &str, &str, &str, &str, &str, &str)] = &[
        ("eeeeeeee-0000-4000-8000-000000000001", 1001, SUB_ACME, DOMAIN_ACME, USER_ALICE, "2026-01-01", "100.00", "20.00", "120.00", "paid"),
        ("eeeeeeee-0000-4000-8000-000000000002", 1002, SUB_ACME, DOMAIN_ACME, USER_ALICE, "2026-02-01", "250.50", "50.10", "300.60", "paid"),
        ("eeeeeeee-0000-4000-8000-000000000003", 1003, SUB_ACME, DOMAIN_ACME, USER_ALICE, "2026-03-01", "80.00", "16.00", "96.00", "open"),
        ("eeeeeeee-0000-4000-8000-000000000004", 1004, SUB_GLOBEX, DOMAIN_GLOBEX, USER_CAROL, "2026-03-01", "500.00", "95.00", "595.00", "paid"),
        ("eeeeeeee-0000-4000-8000-000000000005", 1005, SUB_GLOBEX, DOMAIN_GLOBEX, USER_CAROL, "2026-04-01", "1000.00", "190.00", "1190.00", "void"),
    ];
    for (id, number, sub, domain, user, month, net, vat, total, status) in invoices {
        out.push(format!(
            "INSERT INTO invoice (id, invoice_number, subscription_id, domain_id, user_id, \
             billing_period_start, billing_period_end, issued_at, due_at, net_amount, vat_amount, \
             total_amount, vat_rate, status, stripe_payment_intent_id) VALUES \
             ('{id}', {number}, '{sub}', '{domain}', '{user}', '{month} 00:00:00', \
             '{month} 00:00:00', '{month} 00:00:00', '{month} 00:00:00', {net}, {vat}, {total}, \
             20.00, '{status}', NULL)"
        ));
    }

    out
}
