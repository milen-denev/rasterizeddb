# Ledger tables and secret vaults: plan

Status: started 2026-11-26 on `wip/ledger`, in its own worktree. It stays
there until the owner decides to merge it. It is not handed to the
integrator.

Two features, one foundation:

- **Ledger tables.** These are append-only tables with a retention lock.
  Each row gets a digest, and the digests are hash-chained across
  transactions. The chain heads are anchored somewhere the database host
  cannot rewrite. Tampering, deletion and rollback can all be detected,
  and nothing can be changed before its time.
- **Secret vaults.** A replacement for Azure Key Vault, AWS Secrets Manager
  and HashiCorp Vault's KV engine. A vault is a ledger table with a fixed
  shape:
  - every value is encrypted under keys held in hardware (HSM or TPM);
  - every change is a new version;
  - every read is written to a tamper-evident audit trail;
  - deletion is soft and purge-protected.

Both are **dual-licensed** and live in the licensed crates
(sharding-plan.md §10, D10/D12). The GPL crates get hook traits only.

## 1. Decisions taken (owner may overrule)

| # | Question | Taken |
|---|---|---|
| Q1 | Modes | `APPEND ONLY` only. Vault versioning is append-only too: each version is a new row, so no `VERSIONED` mode is needed now |
| Q2 | Anchor unreachable | Refuse writes to ledger tables once the unanchored window is used up (`on_anchor_failure = refuse_writes`). `continue_and_alarm` is opt-in |
| Q3 | Encryption | Column-level AEAD (AES-256-GCM, hardware AES via AES-NI or ARMv8-CE). Key providers: `file` (dev/test only), `pkcs11` (HSM, cloud HSM, TPM 2.0 through tpm2-pkcs11). KMS and Vault-transit come later |
| Q4 | Multi-primary | A ledger table or vault has one writer origin. Replicas apply the origin's ledger metadata and verify the chain as they go |
| Q5 | Release | 1.1.0, enterprise build only |
| Q6 | Certificate login (mTLS) | GPL core, as part of the authentication floor like R0. Not licensed. **Confirmed by the owner 2026-11-26** |
| Q7 | Post-quantum | An option (feature `pqc`, in the enterprise default): hybrid ML-KEM-768 + X25519 key wrapping. AES-256 and SHA-256 already keep a ~128-bit margin against Grover's search |

## 2. Crates and features

| Crate | Licence | Feature | Gates |
|---|---|---|---|
| `rasterizeddb_core` | GPL | none (always built) | `core/ledger_hooks.rs`: `TableWritePolicy`, `CommitParticipant`, `ColumnCipher`, `HiddenColumns`, the replication extension payload, runtime-registered `incompat_features` markers. Fail closed: a table with a ledger or vault header flag and no installed hook refuses to open |
| `rasterizeddb_enterprise` | dual | `ledger` | policy sidecar, allowlist guard, retention, legal hold, trusted clock, digest store, transaction chain, `dir:` anchors, `VERIFY LEDGER`, offline verifier |
| | | `ledger_s3` = `ledger` + `rasterizeddb_core/s3_backup` | S3 Object Lock anchors, archiving of digest segments |
| | | `encryption` | AES-256-GCM column cipher, envelope keys, the `KeyProvider` trait, the `file` provider |
| | | `pkcs11` = `encryption` + `dep:cryptoki` | HSM and TPM key providers. The PKCS#11 module is loaded at run time, so there is no C dependency at link time |
| | | `vault` = `ledger` + `encryption` | vaults, `vault_*` functions, read audit, purge protection, secure memory |
| `rasterizeddb_client` | GPL | none | `Model::LEDGER` data constant; update/upsert/restore/drop refused before sending |
| `rasterizeddb_client_enterprise` (new) | dual | `ledger`, `vault` | `LedgerModel`, receipts, proof verification, `VERIFY` reports, ledger migrations, `Vault` client with zeroizing `SecretString` and a locked-memory cache |
| `rdb_studio_enterprise` (new) | dual | — | ledger and vault panels, behind Studio's `enterprise` feature |
| `rasterizeddb_server` | GPL, unpublished | `enterprise` | links the licensed crate and installs its hooks |
| `rasterizeddb_embedded` | GPL, published | none | no dependency on the licensed crate (crates.io must resolve every optional dependency). An application links `rasterizeddb_enterprise` and calls `install_always()` before `Builder::start`; a ledger table opened without it is refused (fail closed) |

All of these are off in the default GPL build, as D12 requires. A GPL build
refuses a directory carrying `ledger_v1`, `encrypted_v1` or `vault_v1`.

## 3. Ledger tables

### 3.1 SQL

```sql
CREATE LEDGER TABLE payments (...)
  NO DELETE UNTIL 7 YEARS AFTER INSERT
  NO DROP UNTIL 90 DAYS IDLE
  [HASHING USING 'sha256']
  [ANCHOR TO 'dir:/mnt/disk2/ledger', 's3://bucket/prefix/'];
ALTER LEDGER TABLE payments SET RETENTION 10 YEARS;      -- increase only
ALTER LEDGER TABLE payments SET LEGAL HOLD 'case' [WHERE ...];
ALTER LEDGER TABLE payments RELEASE LEGAL HOLD 'case';
VERIFY LEDGER [payments] [FULL | SINCE <seq>];
ACKNOWLEDGE LEDGER ROLLBACK '<reason>';
```

The same statements reach the pgwire, MySQL and TDS front ends through the
rewrite layer. Hidden columns: `__ledger_rid` (stable per-table id),
`__ledger_ts` (trusted insert time), `__ledger_salt`.

### 3.2 The guard (allowlist)

The guard is checked on the `Table`/`DbTable` write entry points, so every
front end and the embedded API pass through it.

**Allowed:**
- INSERT, COPY, bulk load
- SELECT
- DELETE of rows past retention with no hold, which leaves a deletion entry
- VACUUM, compaction, MOVE TABLE, REBALANCE and partition moves (P7/P9),
  which copy the hidden columns raw

**Everything else is refused by default**, including statement kinds added
later:
- UPDATE, in-place update, ON CONFLICT DO UPDATE, MERGE
- TRUNCATE, DROP before the drop-idle time
- ALTER, except nullable ADD COLUMN and the ledger ALTERs
- branch RESET/PROMOTE, unless the branch's chain extends main's
- PITR restore, unless acknowledged

Superuser is not exempt. The `table.rs` seam test gets a ledger list: a new
mutating method fails the test until it declares its ledger behaviour.

### 3.3 Trusted time

- `__ledger_ts = max(now, persisted high-water)`.
- Retention uses `min(wall_now, last_anchor_server_time + CLOCK_BOOTTIME elapsed)`.
  Moving the clock forward releases nothing.

### 3.4 Digests, chain and anchors

- **Leaf** = `H(alg ‖ incarnation ‖ rid ‖ ts ‖ salt ‖ canonical stored values)`.
  It covers ciphertext for encrypted columns, so an auditor can verify
  without keys.
- **Transaction entry** = `{seq, txid, commit_ts, role, merkle_root(leaves), prev}`.
  There is one chain per database, and its entries travel in the WAL
  commit group. CommitParticipant's output is a serialisable value, so the
  Raft log can carry it (sharding B3).
- **Digest store** is kept on `--ledger-dir`. Startup is refused if that
  directory is on the data device, unless overridden.
- **Blocks** are closed every N ms or M transactions and signed with
  Ed25519. They are pushed to `dir:` and `s3:` sinks (Object Lock
  COMPLIANCE plus `x-amz-checksum-sha256`). Anchoring is async by default,
  with a bounded window; `sync` is opt-in.
- **VERIFY**, the offline verifier, receipts and Merkle proofs work as in
  the 2026-11-23 plan.

## 4. Secret vaults (Key Vault replacement)

### 4.1 What Key Vault gives and what a vault does

| Key Vault | Vault here |
|---|---|
| Encryption at rest, keys in an HSM | Every value is AES-256-GCM under a per-vault DEK. The DEK is wrapped by a KEK that never leaves the HSM or TPM (`CKA_SENSITIVE`, non-extractable). Wrap and unwrap happen inside the device |
| Access control per secret | `GRANT VAULT READ/WRITE/MANAGE ON VAULT v [SECRETS LIKE 'pattern'] TO role`, checked on every `vault_*` call. A plain `SELECT` returns metadata only: the value column is always NULL |
| Audit of who read what | Every `vault_get` appends `{role, database, protocol, client address, name, version, outcome, ts}` to the vault's audit ledger before the value is returned. If the audit write fails, no value is returned. Denied reads are audited too |
| Version history | A set is an INSERT of `(name, version + 1)`. Every version can be read, and nothing is updated in place |
| Soft delete, purge protection | Delete appends a `deleted` state row. `vault_recover` appends `active`. `vault_purge` works only after the vault's retention. Purge crypto-shreds the value by destroying the version's per-secret wrapped key, and the digest stays |
| Expiry, not-before, enabled | Metadata columns, enforced by `vault_get`. Expiry events are raised through `NOTIFY rdb_vault_events` |
| Certificate login, private access | mTLS client certificates mapped to roles (Q6, GPL), plus `hostssl`-style rules |
| Keys you can use but never see (sign, encrypt) | Phase V5: `KEY` objects whose operations run in the HSM (`vault_sign`, `vault_encrypt`, `vault_decrypt`) |
| Rotation | `vault_rotate_kek` rewraps the DEKs and leaves stored values unchanged. A new DEK epoch applies to new versions |

### 4.2 SQL surface: functions, so sqlx, ORMs and every front end work

```sql
CREATE VAULT app_secrets
  KEY PROVIDER 'pkcs11:module=/usr/lib/x86_64-linux-gnu/libtpm2_pkcs11.so;token=rdb;key=vault-kek'
  RETAIN DELETED 90 DAYS;
SELECT vault_set('app_secrets', 'db-password', $1 [, expires => ..., tags => ...]);  -- returns version
SELECT vault_get('app_secrets', 'db-password' [, version]);                         -- audited
SELECT * FROM vault_list('app_secrets');                                            -- metadata only
SELECT vault_delete(...), vault_recover(...), vault_purge(...), vault_rotate_kek('app_secrets');
SELECT * FROM vault_audit('app_secrets') WHERE ...;                                 -- needs MANAGE
```

### 4.3 Where a secret must never appear

The secret value must stay out of:
- statement text
- the WAL in plaintext
- replication in plaintext
- logs, query stats or the statement cache
- core dumps and swap

What that requires:

- **No literals.** `vault_set` refuses a literal value and accepts only a
  bind parameter. A literal would put the secret into the statement-text
  surfaces: the log, query_stats, the replication stream and the statement
  cache (see the "statement text reaches four surfaces" note).
- **WAL and replication carry ciphertext only.** A vault write travels as a
  replication extension payload (ciphertext plus ledger metadata), not as
  statement text.
- **Secure memory.** DEKs and decrypted values live in pages from
  `memfd_secret(2)` when it is available (Linux 5.14+, removed from the
  kernel's direct map). Otherwise they live in `mlock`ed memory with
  `MADV_DONTDUMP` and `MADV_WIPEONFORK`, and are zeroized on drop. Once a
  vault is open, `PR_SET_DUMPABLE` is set to 0.
- **The result value is sent once and wiped.** The connection buffer is
  zeroized after the send.

### 4.4 Hardware

**KEK providers**, through one `KeyProvider` trait:
- `file`: tests only. It is refused unless `vault.allow_file_key = true`.
- `pkcs11`: YubiHSM 2, Nitrokey HSM, SoftHSM (tests), TPM 2.0 through
  tpm2-pkcs11, AWS CloudHSM, Azure Managed HSM, Thales/Entrust.
  - Mechanisms: `CKM_AES_KEY_WRAP_KWP`, falling back to `CKM_AES_GCM`.
  - The PIN comes from a root-only file or the environment, and is never
    in SQL.

**Later phases:**
- **TPM-sealed:** a KEK sealed to PCRs, so a stolen disk or a different
  boot state cannot unwrap.
- **Confidential VMs:** SEV-SNP or TDX with attestation-gated key release.
- **AWS KMS and Vault transit providers.**

**AES** runs on AES-NI or ARMv8 Crypto Extensions (via `aes-gcm`).

## 4.5 Post-quantum (feature `pqc`)

Quantum computers threaten public-key cryptography, not symmetric ciphers
or hashes. AES-256 (values, keyrings) and SHA-256 (leaves, chain) are
unchanged.

**Built:**
- `KEY PROVIDER 'mlkem:<key file>'` wraps each data key with a hybrid KEM:
  ML-KEM-768 (FIPS 203) and X25519, the wrapping key derived from both
  shared secrets. Breaking it needs breaking both.
- `mlkem:<path>;create` makes the key file (0600).
- `VAULT v ROTATE KEY TO 'mlkem:…'` moves an existing vault onto it
  without re-encrypting a single value.

**Not built yet:**
- (built since) ML-DSA-65 (FIPS 204) signatures on anchors: `RDB_LEDGER_SIGNING_KEY` (seed file), `RDB_LEDGER_VERIFY_KEY` (public key for auditors).
- A SHA-512 chain option.
- Post-quantum TLS (rustls X25519MLKEM768), which belongs to the GPL
  listener.

## 4.6 FIPS 140-3 (feature `fips`, off by default)

In a `fips` build, every cryptographic function the server relies on for
security runs inside **AWS-LC's FIPS module** (`aws-lc-rs` with `fips`).
aws-lc-rs is already rustls's provider here.

**Core (GPL):**
- TLS uses rustls's FIPS provider, so only approved suites and groups are
  offered.
- SCRAM-SHA-256 (SHA-256, HMAC, PBKDF2), S3 SigV4, and the S3 Object Lock
  checksum go through `core::fips`.
- `md5` logins are refused.
- The server runs `aws_lc_rs::try_fips_mode()` (the power-on self-tests) and
  refuses to start if it fails.
- `rasterizeddb_server --features fips` turns all of this on.
- The client's SCRAM: `rasterizeddb_client --features fips` (its TLS follows the process's rustls provider).

**Enterprise:**
- The ledger's leaf and chain hashes, the vault's AES-256-GCM, and its
  random numbers come from the module.
- `mlkem:` providers, `mldsa65` keys and ML-DSA anchor signatures are
  refused. The ML-KEM and ML-DSA code here is RustCrypto, not the validated
  module.
- In FIPS mode, keys belong in a FIPS 140-3 validated HSM through
  `pkcs11:`.

**Build and certification:**
- AWS-LC's FIPS build needs CMake, Go and Perl. This development box has no
  Go, so the `fips` build is checked on CI.
- A certified deployment must pin an AWS-LC FIPS version named on a current
  CMVP certificate. That is a release decision.

**Not covered:**
- Non-security hashes (CRC32C and xxHash for integrity and routing, and the
  `md5()` SQL function) stay as they are.

## 5. Client

`rasterizeddb_client_enterprise`:

```rust
let vault = Vault::new(&client, "app_secrets");
let pw: SecretString = vault.get("db-password").await?;   // zeroized on drop
vault.set("db-password", &new).await?;                    // sent as a bind parameter
let cached = vault.cached(Duration::from_secs(60));       // mlock'ed cache
```

The ledger side covers `LedgerModel`, receipts, proofs and `verify` (the
2026-11-23 plan). The derive emits only the plain-data constant
`Model::LEDGER`.

## 5.5 Status (2026-11-26)

Written on `wip/ledger`, `cargo check` pending, **no tests run yet**:

- **L0:** core hooks; runtime incompat flags; header kind; fail-closed open.
- **L1:** `CREATE LEDGER TABLE`, `CREATE VAULT`, the policy sidecar,
  retention, legal holds, the `__ledger_ts` stamp, and the per-row DELETE
  check.
- **L2:** the chain store, `VERIFY LEDGER` and `SHOW LEDGER`.
- **L3:** `dir:` and `s3:` anchors with Object Lock, sync/async mode, the
  failure policy, and `ACKNOWLEDGE LEDGER ROLLBACK`.
- **E1:** AES-256-GCM envelope encryption and the `file` provider.
- **E2:** the PKCS#11 provider.
- **PQC:** the `mlkem` hybrid provider.
- **V1–V3:** the VAULT statements, the keyring, purge as crypto-shredding,
  the audit trail, secure memory, and vault inserts only through VAULT
  statements.
- **C1:** `Model::LEDGER`, the derive, the client refusals, the migrator
  filter, the error helpers, and `rasterizeddb_client_enterprise` (`Vault`,
  `CachedVault`, `LedgerAdmin`, `LedgerModelExt`).
- **S1:** `rdb_studio_enterprise` and Studio's LEDGER tab.
- **A1:** `--pg-auth cert`, `--pg-client-ca` and `--pg-cert-map`.

**Not built yet:**
- row-level offline verification (the `rdb-ledger-verify` binary checks the chain and anchors)
- (built since) receipts: `SHOW LEDGER RECEIPT <table> <stamp>` and `rasterizeddb_client_enterprise::verify_receipt`, which checks one against a head the caller read from an anchor; the chain hashes payload digests, so a proof is 32 bytes per later entry
- Ed25519 anchor signing (ML-DSA-65 is built, under `pqc`)
- transparent `ENCRYPTED` columns on ordinary ledger tables (vault values
  are encrypted; other columns are not)
- `VERSIONED` mode
- (built since) vault keys: `VAULT v CREATE KEY 'k' TYPE 'aes256-gcm'|'mldsa65'`, `ENCRYPT`/`DECRYPT`/`SIGN`/`VERIFY`/`PUBLIC KEY`; the material is a wrapped data key that never leaves the server. Keys inside the HSM itself (CKM operations on token objects) are not built
- TPM-sealed keys and attested key release
- KMS and Vault-transit providers
- replication single-writer enforcement, and ledger metadata in the
  replication payload
- branch and PITR rollback checks
- partitioned ledger tables
- cert login on the native protocol
- persisting the clock's high-water mark (the chain re-raises it at open)

**Known limits:**
- A plain `SELECT` on a vault shows ciphertext and metadata.
- `VERIFY` reads every row into memory.
- The audit of a GET refused by the privilege check happens in core,
  before the hook, so it is not recorded.

## 5.6 Second round (wip/ledger-2, 2026-11-26)

**Certificate login over the native protocol.**
- `--rdb-auth cert` with `--pg-client-ca` (the one CA for every listener) and `--pg-cert-map`.
- rastcp hands the verified client certificate to the login exchange (`AuthChannel::peer_certificate`, additive).
- The client logs in with `Client::connect_with_certificate(addr, options, user, cert.pem, key.pem)`.
- A unix socket refuses `cert` (it carries no TLS).

**Studio.** The LEDGER page manages everything, through two generic Studio messages (run, input); the licensed crate builds every statement:
- verify (all or one table), receipts, raising retention, legal holds, anchors;
- creating ledger tables and vaults, acknowledging a rollback;
- per secret: reveal (explicit, cleared on reload), versions, set, disable/enable, delete/recover, purge;
- vault-wide: key rotation, audit, keys (create, encrypt, decrypt, sign, verify, public key).

**Offline verifier.** `rdb-ledger-verify --store <served path> --data <copy>` also opens a copy of the data directory and checks every ledger table's rows against the chain.

**VERSIONED ledger tables.** `CREATE LEDGER TABLE … VERSIONED NO DELETE UNTIL …`:
- UPDATE is allowed. Each update chains the new version's digest (op 4). The hidden `__ledger_ts` is the row's identity and is never assigned.
- The table never takes the in-place overwrite, vacuum is off, and maintenance is refused, so every version stays on disk.
- `SHOW LEDGER HISTORY <table> <stamp>` lists them, each marked chained or not.
- Client: `#[rdb(ledger_versioned)]` (`update` is then allowed), `LedgerAdmin::history`.

**Keys inside the HSM.** `VAULT v CREATE KEY 'k' TYPE 'hsm-aes256-gcm' | 'hsm-ecdsa-p256'` on a `pkcs11:` vault generates the key on the token (sensitive, not extractable). ENCRYPT, DECRYPT, SIGN, VERIFY and PUBLIC KEY run on the token. The keyring only records that the key exists. Tested against SoftHSM.

**TPM-sealed keys** (feature `tpm`, opt-in, links libtss2-esys):
- `KEY PROVIDER 'tpm:<file>[;tcti=…][;create]'` seals the vault's key-encryption key to this machine's TPM 2.0 storage hierarchy.
- The file is useless on any other machine. The key is unsealed once at open, over an encrypted HMAC session.
- PCR-bound sealing (to a measured boot state) is not built.
- Tested against swtpm.

## 5.7 Encrypted columns (wip/ledger-3)

Ordinary and ledger tables can store a column encrypted under a vault key. A
statement that would filter, sort, group, join, index or compute on the
ciphertext is refused (0A000) before it runs: a filter over ciphertext would
otherwise return a wrong answer with no error.

```sql
VAULT hr CREATE KEY 'pii' TYPE 'aes256-gcm';           -- or hsm-aes256-gcm on a pkcs11: vault
CREATE TABLE patients (id BIGINT PRIMARY KEY, name TEXT,
                       ssn VARCHAR(11) ENCRYPTED WITH VAULT hr KEY 'pii' NOT NULL);
INSERT INTO patients (id, name, ssn) VALUES (1, 'ann', '123-45-6789');
SELECT id, ssn FROM patients WHERE id = 1;              -- plaintext, for a role with SELECT on hr
SELECT id FROM patients WHERE ssn = '123-45-6789';      -- 0A000: column "patients.ssn" is encrypted …
```

**Storage.**
- Each column gets its own random 256-bit data key, wrapped by the vault key (AAD: the column id). The wrapped key is kept in `encrypted_columns` beside the chain.
- Each value is AES-256-GCM under that data key (AAD: the column id), stored as the text `rdbenc1:<column id>:<base64>`. So a column is TEXT or VARCHAR(n); VARCHAR's limit is checked on the plaintext.
- `ENCRYPTED_VALUES_V1` fences the directory: a build without the hooks refuses to open it, rather than serve ciphertext as values.

**What may name an encrypted column.** An allowlist over the statement's tokens, run twice: on the client's text before any rewrite, and on the text the executor runs. The column may appear:
- whole, as a select-list or `RETURNING` item (`ssn`, `p.ssn`, `ssn AS s`, `*`);
- as a value of `INSERT … VALUES`: a literal, `NULL`, `DEFAULT` or a bound parameter, given by an explicit column list or by position (no list: the table's columns in order, a ledger table's hidden stamp skipped; a shorter tuple leaves it NULL), one row or many;
- as `UPDATE … SET ssn = <literal | NULL | $n>`.

Everything else that names it is refused (0A000):
- `WHERE`, `JOIN … ON`, `ORDER BY` (by name, alias or position), `GROUP BY`, `HAVING`, aggregates;
- `DISTINCT`, set operations, window `PARTITION BY`, functions, casts, concatenation;
- subqueries, CTEs, `INSERT … SELECT`, `CREATE TABLE … AS`, `SELECT … INTO`, `ON CONFLICT`, `COPY`;
- indexes, renaming the table or column, dropping the column, `ALTER TABLE … ADD … ENCRYPTED`.

A declaration takes `NOT NULL` only. A key, unique, check, default or reference would compare or compute on ciphertext.

**Fast paths stay off.** A statement naming such a table never takes a learned or prepared parse, or the native protocol's direct path. It runs on the guarded text path (`column_crypto::touches`), the same rule that holds for temporary tables and the catalogues.

**Who reads plaintext.**
- Values are encrypted and decrypted only at the task's outermost statement, never inside a subquery.
- Plaintext is returned to a role holding `SELECT` on the column's vault (the grant `VAULT … DECRYPT` needs), to a superuser, and to an in-process (embedded) caller.
- Any other role reads the ciphertext. It copies, dumps and restores as text, and Studio shows it as "encrypted".
- Writing needs the same grant: sealing a value uses the vault's key (42501 otherwise).

**Limits.**
- A replica runs the statement as rewritten: it stores the same ciphertext, but it does not hold the column registry, so it serves ciphertext.
- `COPY` into such a table is refused; use `INSERT`.
- No equality search (deterministic encryption) is offered. It would leak which values repeat, and it is not built.

**Studio.**
- The LEDGER page lists the encrypted columns (table, column, vault, key, type).
- Its column editor has an "Encrypted" toggle per column, with a vault and AES key pick.
- The DATA and QUERY grids show a ciphertext cell as "🔒 encrypted (no access to its vault)".

**Code.**
- `core::column_crypto` (GPL): the hooks, the executor and wire call sites, the response cell rewrite.
- `rasterizeddb_enterprise::columns` (licensed): the analysis, the cipher, the registry.
- `vault::key_seal` / `key_open`: data keys wrapped by a vault key, AES or in the HSM.
- Tests: `columns` unit tests (what passes, what is refused, declarations); `tests/encrypted_columns.rs` end to end.

## 6. Phases (implement all first with `cargo check`, then test in one go)

| Phase | Contents |
|---|---|
| L0 | Core hooks, runtime incompat markers, header flag with fail-closed open, enterprise `ledger` feature skeleton, guard on every write entry point |
| L1 | `CREATE LEDGER TABLE` parse and rewrite, policy sidecar, retention, legal hold, hidden columns, trusted clock |
| L2 | Digest store, transaction chain in the WAL commit group, `VERIFY LEDGER`, offline verifier |
| L3 | Anchors (`dir`, S3 Object Lock), signing, sync and async modes, failure policy, receipts and proofs |
| E1 | `encryption`: column cipher, envelope keys, `file` provider, `encrypted_v1` |
| E2 | `pkcs11` provider (SoftHSM in tests, a TPM through tpm2-pkcs11 on real hardware) |
| V1 | Vaults: `CREATE VAULT`, the `vault_*` functions, versioning, metadata-only `SELECT`, grants |
| V2 | Read audit ledger, soft delete, recover, purge (crypto-shred), expiry events |
| V3 | Secure memory, no-literal rule, ciphertext-only WAL and replication payloads, zeroized results |
| C1 | `rasterizeddb_client_enterprise`: `Vault`, `SecretString`, `LedgerModel`, migrations |
| S1 | `rdb_studio_enterprise` panels |
| V5 | Key objects (`vault_sign`/`encrypt`/`decrypt` in the HSM), TPM sealing, KMS/Vault providers |
| A1 | mTLS certificate login (GPL, Q6) |

**Tests (after the phases):**
- refusal matrix: every statement kind on every front end
- tamper matrix: flip a byte in each file kind and VERIFY must name it
- power-loss failpoints around the commit group
- clock-jump tests
- SoftHSM provider tests
- a scan of WAL, snapshots, backups, logs and query stats for plaintext
  secrets

Full suites run on GitHub CI. Locally, only the needed files, each through
`.claude/heavy/rdb-heavy`.
