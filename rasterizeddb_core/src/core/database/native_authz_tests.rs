//! Who is allowed to issue what over the native protocol.
//!
//! The exchange that decides *who* a caller is lives in `crate::native_auth`
//! and is tested end to end there. What is tested here is the other half: that
//! having authenticated as an ordinary user does not let you rewrite the role
//! catalogue, and that the gate does not fire on a listener where nobody was
//! asked to authenticate at all — which would leave a fresh database with no
//! way to create its first account.

use super::*;
use std::sync::Arc as StdArc;

use rastcp::session::Principal;

async fn in_memory_db() -> Arc<Database> {
    let dir = std::env::temp_dir().join(format!(
        "rdb_authz_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos()
    ));
    Arc::new(Database::new_with_backend(&dir.to_string_lossy(), true).await)
}

fn text_of(response: &[u8]) -> String {
    String::from_utf8_lossy(&response[1..]).to_string()
}

#[tokio::test]
async fn an_ordinary_user_cannot_create_a_role() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: true,
    };

    let response = process_incoming_queries(
        context.clone(),
        StdArc::new(Principal::new("bob", false)),
        b"CREATE USER mallory PASSWORD 'letmein'".to_vec(),
    )
    .await;

    assert_eq!(response.first(), Some(&3), "must be refused: {}", text_of(&response));
    assert!(text_of(&response).contains("not a superuser"), "{}", text_of(&response));

    // And the refusal has to have been real, not merely reported.
    assert!(
        crate::auth::lookup_role(&context.database, "mallory")
            .await
            .is_none(),
        "the role must not exist"
    );
}

#[tokio::test]
async fn a_superuser_can_create_a_role_and_its_password_is_stored_as_a_verifier() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: true,
    };

    let response = process_incoming_queries(
        context.clone(),
        StdArc::new(Principal::new("root", true)),
        b"CREATE USER alice PASSWORD 'hunter2'".to_vec(),
    )
    .await;
    assert_eq!(response.first(), Some(&0), "{}", text_of(&response));

    let role = crate::auth::lookup_role(&context.database, "alice")
        .await
        .expect("the role should exist");
    assert!(role.can_login);
    assert!(!role.is_superuser);

    match role.verifier.expect("a password should have been stored") {
        crate::auth::Verifier::Scram { .. } => {}
        other => panic!("a password must never be stored as written: {:?}", other),
    }
}

/// On a listener that asks nobody to authenticate, every principal is
/// anonymous. Gating on `superuser` there would refuse the one statement an
/// operator needs to lock the database down.
#[tokio::test]
async fn the_gate_does_not_fire_when_nobody_was_asked_to_authenticate() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: false,
    };

    let response = process_incoming_queries(
        context.clone(),
        StdArc::new(Principal::anonymous()),
        b"CREATE USER first PASSWORD 'bootstrap'".to_vec(),
    )
    .await;

    assert_eq!(response.first(), Some(&0), "{}", text_of(&response));
    assert!(
        crate::auth::lookup_role(&context.database, "first")
            .await
            .is_some()
    );
}

/// Creating that first account is what makes the next start-up demand one.
#[tokio::test]
async fn creating_an_account_is_what_makes_authentication_required_next_time() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: false,
    };

    assert!(
        !crate::auth::has_login_role(&context.database).await,
        "a fresh database has nobody to demand a password from"
    );

    let response = process_incoming_queries(
        context.clone(),
        StdArc::new(Principal::anonymous()),
        b"CREATE USER first PASSWORD 'bootstrap'".to_vec(),
    )
    .await;
    assert_eq!(response.first(), Some(&0), "{}", text_of(&response));

    assert!(
        crate::auth::has_login_role(&context.database).await,
        "once an account holds a password, the listener must start requiring one"
    );
}

/// A role that cannot log in does not count: requiring authentication on its
/// account would lock everyone out.
#[tokio::test]
async fn a_role_that_cannot_log_in_does_not_make_authentication_required() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: false,
    };

    let response = process_incoming_queries(
        context.clone(),
        StdArc::new(Principal::anonymous()),
        b"CREATE ROLE reader NOLOGIN PASSWORD 'x'".to_vec(),
    )
    .await;
    assert_eq!(response.first(), Some(&0), "{}", text_of(&response));

    assert!(!crate::auth::has_login_role(&context.database).await);
}

/// Ordinary statements are unaffected by any of this.
#[tokio::test]
async fn an_ordinary_query_from_an_ordinary_user_still_runs() {
    let context = NativeContext {
        database: in_memory_db().await,
        authentication_required: true,
    };

    let response = process_incoming_queries(
        context,
        StdArc::new(Principal::new("bob", false)),
        b"CREATE TABLE things (id INT)".to_vec(),
    )
    .await;

    assert_ne!(
        response.first(),
        Some(&3),
        "a non-superuser must still be able to work: {}",
        text_of(&response)
    );
}
