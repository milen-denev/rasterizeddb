use super::*;

#[test]
fn parses_branch_statements() {
    let p = |sql: &str| {
        parse_database_statement(sql)
            .expect("claimed")
            .expect("parsed")
    };
    assert_eq!(
        p("CREATE BRANCH dev"),
        DatabaseStatement::CreateBranch {
            name: "dev".into(),
            from: None,
            options: BranchOptions::default(),
            template: false
        }
    );
    assert_eq!(
        p("create branch Dev from \"Prod\" without base"),
        DatabaseStatement::CreateBranch {
            name: "dev".into(),
            from: Some("Prod".into()),
            options: BranchOptions {
                point: BranchPoint::Now,
                with_base: false
            },
            template: false,
        }
    );
    assert_eq!(
        p("CREATE BRANCH dev FROM prod AS OF LSN 42"),
        DatabaseStatement::CreateBranch {
            name: "dev".into(),
            from: Some("prod".into()),
            options: BranchOptions {
                point: BranchPoint::Lsn(42),
                with_base: true
            },
            template: false,
        }
    );
    match p("CREATE BRANCH dev AS OF TIMESTAMP '2026-11-19 10:00:00+00' WITHOUT BASE") {
        DatabaseStatement::CreateBranch { options, .. } => {
            assert_eq!(
                options.point,
                BranchPoint::Time(
                    crate::core::backup::timeutil::parse_rfc3339("2026-11-19T10:00:00Z").unwrap()
                )
            );
            assert!(!options.with_base);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        p("CREATE BRANCH dev AS OF SNAPSHOT 'snap-1'"),
        DatabaseStatement::CreateBranch {
            name: "dev".into(),
            from: None,
            options: BranchOptions {
                point: BranchPoint::Snapshot("snap-1".into()),
                with_base: true
            },
            template: false,
        }
    );
    assert_eq!(p("SHOW BRANCHES;"), DatabaseStatement::ListBranches);
    assert_eq!(
        p("SHOW BRANCH DIFF dev"),
        DatabaseStatement::DiffBranch {
            name: "dev".into(),
            into: None
        }
    );
    assert_eq!(
        p("RESET BRANCH dev"),
        DatabaseStatement::ResetBranch { name: "dev".into() }
    );
    assert_eq!(
        p("PROMOTE BRANCH dev"),
        DatabaseStatement::PromoteBranch { name: "dev".into() }
    );
    assert_eq!(
        p("DROP BRANCH IF EXISTS dev"),
        DatabaseStatement::DropBranch {
            name: "dev".into(),
            if_exists: true
        }
    );
    assert_eq!(
        p("MERGE BRANCH dev INTO prod ON CONFLICT BRANCH WINS ALLOW DROP"),
        DatabaseStatement::MergeBranch {
            name: "dev".into(),
            into: Some("prod".into()),
            policy: ConflictPolicy::BranchWins,
            allow_drop: true,
        }
    );
    assert_eq!(
        p("MERGE BRANCH dev"),
        DatabaseStatement::MergeBranch {
            name: "dev".into(),
            into: None,
            policy: ConflictPolicy::Fail,
            allow_drop: false
        }
    );
    // PostgreSQL's spelling of a copy.
    assert_eq!(
        p("CREATE DATABASE dev WITH TEMPLATE = prod"),
        DatabaseStatement::CreateBranch {
            name: "dev".into(),
            from: Some("prod".into()),
            options: BranchOptions::default(),
            template: true
        }
    );
    assert_eq!(
        p("CREATE DATABASE dev TEMPLATE template0"),
        DatabaseStatement::Create {
            name: "dev".into(),
            if_not_exists: false
        }
    );
    // Not ours.
    assert!(
        parse_database_statement("MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE").is_none()
    );
    assert!(parse_database_statement("RESET search_path").is_none());
    assert!(
        parse_database_statement(
            "MERGE branch USING src ON branch.id = src.id WHEN MATCHED THEN DELETE"
        )
        .is_none()
    );
    for bad in [
        "CREATE BRANCH",
        "MERGE BRANCH dev ON CONFLICT MAYBE",
        "CREATE BRANCH dev AS OF LSN x",
        "SHOW BRANCH dev",
    ] {
        assert!(
            matches!(parse_database_statement(bad), Some(Err(_))),
            "{bad}"
        );
    }
}

/// A PROMOTE stopped after any number of its renames, with or without the
/// step recorded, is finished by the next start and leaves each database
/// under the other's name.
#[test]
fn an_interrupted_promote_is_finished_from_any_point() {
    let meta = |parent: &str| BranchMeta {
        parent: parent.to_string(),
        created_at: 1,
        parent_lsn: 0,
        method: "promote".into(),
        point: "replaced by promote".into(),
        has_base: false,
        bytes: 0,
        merged_at: None,
        base_generation: 0,
    };
    // (renames done before the crash, step recorded)
    for (done, recorded) in [(0u8, 0u8), (1, 0), (1, 1), (2, 1), (2, 2), (3, 2), (3, 3)] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (parent, branch, aside) = (
            root.join("prod"),
            root.join("dev"),
            root.join(".promoting-prod"),
        );
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(&branch).unwrap();
        std::fs::write(parent.join("who"), "parent").unwrap();
        std::fs::write(branch.join("who"), "branch").unwrap();
        meta("prod").write(&branch).unwrap();
        let mut intent = PromoteIntent {
            parent: "prod".into(),
            branch: "dev".into(),
            step: recorded,
            parent_meta: None,
            branch_meta: meta("prod"),
        };
        intent.store(root).unwrap();
        if done >= 1 {
            std::fs::rename(&parent, &aside).unwrap();
        }
        if done >= 2 {
            std::fs::rename(&branch, &parent).unwrap();
        }
        if done >= 3 {
            std::fs::rename(&aside, &branch).unwrap();
        }
        let mut loaded = PromoteIntent::load(root).expect("intent");
        finish_promote(root, &mut loaded).unwrap_or_else(|e| panic!("{done}/{recorded}: {e}"));
        assert_eq!(
            std::fs::read_to_string(parent.join("who")).unwrap(),
            "branch",
            "{done}/{recorded}"
        );
        assert_eq!(
            std::fs::read_to_string(branch.join("who")).unwrap(),
            "parent",
            "{done}/{recorded}"
        );
        assert!(
            !aside.exists() && !root.join(PROMOTE_INTENT).exists(),
            "{done}/{recorded}"
        );
        assert!(
            BranchMeta::read(&parent).is_none(),
            "the promoted copy still says it is a branch"
        );
        assert_eq!(
            BranchMeta::read(&branch).map(|m| m.parent),
            Some("prod".to_string())
        );
    }
}
