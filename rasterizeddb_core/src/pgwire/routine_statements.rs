//! `CREATE FUNCTION`/`PROCEDURE`, `DROP FUNCTION`/`PROCEDURE` and `CALL`,
//! answered above the engine (`core::rql::functions`).

use super::*;
use crate::core::rql::functions::{self, RoutineKind, RoutineStatement};

pub(super) async fn handle_routine_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<RoutineStatement, functions::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let read_only = state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only);
    let tx = state.current_tx();
    match statement {
        RoutineStatement::Create { routine, or_replace } => {
            let verb = if routine.kind == RoutineKind::Function { "CREATE FUNCTION" } else { "CREATE PROCEDURE" };
            if read_only {
                return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
            }
            let existing = match functions::load(database, Some(&routine.name), tx).await {
                Ok(found) => found.into_iter().find(|r| r.arg_types.len() == routine.arg_types.len()),
                Err(e) => return fail_statement(state, stream, "XX000", &e).await,
            };
            if let Some(existing) = &existing {
                if !or_replace {
                    return fail_statement(
                        state,
                        stream,
                        "42723",
                        &format!("{} \"{}\" already exists with same argument types", routine.kind.noun(), routine.name),
                    )
                    .await;
                }
                if existing.kind != routine.kind {
                    return fail_statement(
                        state,
                        stream,
                        "42809",
                        &format!("\"{}\" is a {}", routine.name, existing.kind.noun()),
                    )
                    .await;
                }
                if existing.return_type != routine.return_type {
                    return fail_statement(state, stream, "42P13", "cannot change return type of existing function").await;
                }
            }
            if let Err(e) = functions::store(database, &routine, existing.is_some(), tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            send_command_complete(stream, verb).await
        }
        RoutineStatement::Drop { kind, targets, if_exists } => {
            let verb = if kind == RoutineKind::Function { "DROP FUNCTION" } else { "DROP PROCEDURE" };
            if read_only {
                return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
            }
            for (name, arity) in targets {
                let found = functions::load(database, Some(&name), tx).await.unwrap_or_default();
                let matching: Vec<_> = found
                    .into_iter()
                    .filter(|r| r.kind == kind && arity.is_none_or(|n| r.arg_types.len() == n))
                    .collect();
                if matching.is_empty() {
                    if if_exists {
                        crate::core::notice::push(
                            "NOTICE",
                            "00000",
                            format!("{} {}() does not exist, skipping", kind.noun(), name),
                        );
                        continue;
                    }
                    return fail_statement(state, stream, "42883", &format!("{} {}() does not exist", kind.noun(), name)).await;
                }
                if arity.is_none() && matching.len() > 1 {
                    return fail_statement(
                        state,
                        stream,
                        "42725",
                        &format!("{} name \"{name}\" is not unique (name its argument types)", kind.noun()),
                    )
                    .await;
                }
                for routine in &matching {
                    // A trigger that runs it: PostgreSQL's dependency.
                    if let Some(trigger) = crate::core::rql::triggers::users_of(database, &routine.name, tx).await.first() {
                        return fail_statement(
                            state,
                            stream,
                            "2BP01",
                            &format!(
                                "cannot drop function {}() because other objects depend on it: trigger {} on table {} \
                                 depends on function {}()",
                                routine.name, trigger.name, trigger.table, routine.name
                            ),
                        )
                        .await;
                    }
                    if let Err(e) = functions::remove(database, routine, tx).await {
                        return fail_statement(state, stream, sqlstate_for(&e), &e).await;
                    }
                }
            }
            send_command_complete(stream, verb).await
        }
        RoutineStatement::Call { name, args } => {
            let found = functions::load(database, Some(&name), tx).await.unwrap_or_default();
            let Some(routine) = found.into_iter().find(|r| r.kind == RoutineKind::Procedure && r.arg_types.len() == args.len())
            else {
                return fail_statement(state, stream, "42883", &format!("procedure {name}() does not exist")).await;
            };
            let statements = match functions::procedure_statements(&routine, &args) {
                Ok(statements) => statements,
                Err((code, message)) => return fail_statement(state, stream, code, &message).await,
            };
            // In the caller's transaction, one statement after another; the
            // first failure is the CALL's.
            for sql in statements {
                if let Err(e) = super::view_statements::run_internal(database, state, &sql).await? {
                    return send_error_response(stream, sqlstate_for(&e), &e).await;
                }
            }
            send_command_complete(stream, "CALL").await
        }
    }
}
