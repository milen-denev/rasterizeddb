#!/usr/bin/env python3
"""A real cluster on scratch ports: three shards (`--role shard`) and one
gateway, driven through the gateway with psql and checked against what was
written (sharding-plan.md, the cluster test bar).

  cluster_e2e.py <rasterizeddb_core binary> <rasterizeddb_gateway binary> <workdir> [<rdb-shard binary>]

With rdb-shard, a fourth shard joins at the end: half of shard 1's range
moves onto it online (`rdb-shard split --online`) while a writer runs.

Each check prints PASS or FAIL with what it saw; the exit code is the
number of failures. Every process is killed at the end, and the workdir is
left for the logs (`shard<N>.log`, `gateway.log`).
"""
import hashlib, os, signal, socket, ssl, struct, subprocess, sys, time

CORE, GATEWAY, WORK = sys.argv[1], sys.argv[2], sys.argv[3]
RDB_SHARD = sys.argv[4] if len(sys.argv) > 4 else None
BASE = int(os.environ.get("CLUSTER_BASE_PORT", "57100"))
SHARDS = [0, 1, 2]
ALL = SHARDS + [3]                             # 3 joins by a split
PG = {n: BASE + n for n in ALL}                # each shard's own pgwire
RDB = {n: BASE + 10 + n for n in ALL}          # native (rdb-shard's way in)
RSHARD = {n: BASE + 20 + n for n in ALL}       # rshard (the gateway's links)
GW = BASE + 30                                 # the gateway's pgwire
FE_PG, FE_MY, FE_RDB = BASE + 40, BASE + 41, BASE + 42  # the front-end node (B4.7)
CLUSTER = "0123456789abcdef0123456789abcdef"
BLOCKS = 1 << 48
ENV = dict(os.environ, PGPASSWORD="pw", PGSSLMODE="prefer")
procs = {}
failures = 0


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"{cmd}: {r.stderr}")


def certs():
    d = f"{WORK}/tls"
    os.makedirs(d, exist_ok=True)
    sh(f"openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=rdb-test-ca -keyout {d}/ca.key -out {d}/ca.pem")
    for name in [f"shard{n}" for n in ALL] + ["gateway"]:
        ext = f"{d}/{name}.ext"
        with open(ext, "w") as f:
            f.write("subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n"
                    "keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n")
        sh(f"openssl req -newkey rsa:2048 -nodes -subj /CN={name} -keyout {d}/{name}.key -out {d}/{name}.csr")
        sh(f"openssl x509 -req -in {d}/{name}.csr -CA {d}/ca.pem -CAkey {d}/ca.key -CAcreateserial "
           f"-days 2 -extfile {ext} -out {d}/{name}.pem")
    return d


def write_map():
    lines = ["rasterizeddb shard map", "format 1", f"cluster {CLUSTER}", "epoch 1", "route_v 1"]
    lines += [f"shard {n} 127.0.0.1:{RSHARD[n]}" for n in SHARDS]
    lines += [f"range {hex(BLOCKS * n // len(SHARDS))} {n}" for n in SHARDS]
    path = f"{WORK}/cluster.map"
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def start_shard(n, tls, cmap):
    log = open(f"{WORK}/shard{n}.log", "ab")
    procs[f"shard{n}"] = subprocess.Popen(
        [CORE, "--location", f"{WORK}/s{n}", "--protocol", "all", "--pg-port", str(PG[n]), "--pg-password", "pw", "--pg-tls", "prefer",
         "--rdb-port", str(RDB[n]), "--role", "shard", "--cluster-map", cmap, "--shard-id", str(n),
         "--cluster-id", CLUSTER, "--shard-listen", f"127.0.0.1:{RSHARD[n]}",
         "--shard-cert", f"{tls}/shard{n}.pem", "--shard-key", f"{tls}/shard{n}.key",
         "--gateway-ca", f"{tls}/ca.pem", "--peer-ca", f"{tls}/ca.pem", "--log-level", "info"],
        stdout=log, stderr=log)


def start_gateway(tls, cmap):
    conf = f"{WORK}/gateway.conf"
    with open(conf, "w") as f:
        f.write(f"""listen = 127.0.0.1:{GW}
workers = 2
map = {cmap}
home_shard = 0
shard_trust = ca:{tls}/ca.pem
shard_cert = {tls}/gateway.pem
shard_key = {tls}/gateway.key
auth = scram
unrouted = fan_out
two_phase = on
retries = 10
""")
    log = open(f"{WORK}/gateway.log", "ab")
    procs["gateway"] = subprocess.Popen([GATEWAY, "--config", conf], stdout=log, stderr=log)


def psql(port, sql, timeout=60):
    """`sql` in one session, each `;`-separated statement its own query:
    the gateway forwards a multi-statement query whole, to one shard."""
    args = []
    for stmt in [x.strip() for x in sql.split(";") if x.strip()]:
        args += ["-c", stmt]
    try:
        r = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "rasterized", "-d", "postgres",
                            "-At", "-v", "ON_ERROR_STOP=1"] + args, env=ENV, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, f"no answer in {timeout}s: {sql}"
    return r.returncode, (r.stdout.strip() if r.returncode == 0 else r.stderr.strip())


def psql_script(port, script, timeout=60):
    """`script` run by psql -f in one session: for meta-commands (\\bind,
    \\parse, \\bind_named), which send the extended protocol."""
    path = f"{WORK}/script.sql"
    with open(path, "w") as f:
        f.write(script)
    r = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "rasterized", "-d", "postgres",
                        "-At", "-v", "ON_ERROR_STOP=1", "-f", path], env=ENV, capture_output=True, text=True, timeout=timeout)
    return r.returncode, (r.stdout.strip() if r.returncode == 0 else r.stderr.strip())


def psql_script_bg(port, script, name):
    """`script` started by psql -f in the background; `.communicate()` it."""
    path = f"{WORK}/{name}.sql"
    with open(path, "w") as f:
        f.write(script)
    return subprocess.Popen(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "rasterized", "-d", "postgres",
                             "-At", "-f", path], env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def up(port, what, secs=90):
    end = time.time() + secs
    last = ""
    while time.time() < end:
        code, out = psql(port, "SELECT 1", timeout=10)
        if code == 0 and out == "1":
            return
        last = out
        time.sleep(0.5)
    sys.exit(f"{what} did not come up ({last}); see {WORK}")


class MySQL:
    """Just enough of the MySQL client protocol for the front-end node:
    TLS, caching_sha2_password full authentication, COM_QUERY with text
    results (rows as tuples of str/None, or the affected-row count)."""

    def __init__(self, port, user, password, database):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.seq = 0
        greeting = self.read()
        if greeting[0] != 10:
            raise RuntimeError(f"not a MySQL greeting: {greeting[:16]!r}")
        CLIENT_LONG_PASSWORD, CONNECT_WITH_DB, PROTOCOL_41, SSL = 1, 8, 0x200, 0x800
        TRANSACTIONS, SECURE, PLUGIN_AUTH = 0x2000, 0x8000, 0x80000
        caps = CLIENT_LONG_PASSWORD | CONNECT_WITH_DB | PROTOCOL_41 | SSL | TRANSACTIONS | SECURE | PLUGIN_AUTH
        head = struct.pack("<IIB", caps, 1 << 24, 33) + b"\0" * 23
        self.write(head)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.sock = ctx.wrap_socket(self.sock)
        self.write(head + user.encode() + b"\0" + b"\0" + database.encode() + b"\0" + b"caching_sha2_password\0")
        while True:
            p = self.read()
            if p[:2] == b"\x01\x04":          # perform full authentication
                self.write(password.encode() + b"\0")
            elif p[:2] == b"\x01\x03":        # fast auth done; OK follows
                continue
            elif p[0] == 0x00:
                return
            elif p[0] == 0xff:
                raise RuntimeError(f"login refused: {p[9:].decode(errors='replace')}")
            else:
                raise RuntimeError(f"unexpected login packet {p[:16]!r}")

    def read(self):
        h = self.recv(4)
        n = h[0] | h[1] << 8 | h[2] << 16
        self.seq = (h[3] + 1) & 0xff
        return self.recv(n)

    def recv(self, n):
        b = b""
        while len(b) < n:
            c = self.sock.recv(n - len(b))
            if not c:
                raise RuntimeError("the connection closed")
            b += c
        return b

    def write(self, payload):
        self.sock.sendall(struct.pack("<I", len(payload))[:3] + bytes([self.seq]) + payload)
        self.seq = (self.seq + 1) & 0xff

    @staticmethod
    def lenenc(b, at):
        f = b[at]
        if f < 0xfb:
            return f, at + 1
        if f == 0xfb:
            return None, at + 1
        n = {0xfc: 2, 0xfd: 3, 0xfe: 8}[f]
        return int.from_bytes(b[at + 1:at + 1 + n], "little"), at + 1 + n

    def query(self, sql):
        """Rows, or ("ok", affected); raises on an error packet."""
        self.seq = 0
        self.write(b"\x03" + sql.encode())
        p = self.read()
        if p[0] == 0x00:
            return ("ok", self.lenenc(p, 1)[0])
        if p[0] == 0xff:
            raise RuntimeError(p[9:].decode(errors="replace"))
        ncols, _ = self.lenenc(p, 0)
        for _ in range(ncols):
            self.read()
        if self.read()[0] != 0xfe:
            raise RuntimeError("no EOF after the columns")
        rows = []
        while True:
            p = self.read()
            if p[0] == 0xfe and len(p) < 9:
                return rows
            if p[0] == 0xff:
                raise RuntimeError(p[9:].decode(errors="replace"))
            at, row = 0, []
            for _ in range(ncols):
                n, at = self.lenenc(p, at)
                if n is None:
                    row.append(None)
                else:
                    row.append(p[at:at + n].decode())
                    at += n
            rows.append(tuple(row))


def check(name, ok, detail=""):
    global failures
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": {detail}"), flush=True)
    if not ok:
        failures += 1


def on_every_shard(sql):
    for n in SHARDS:
        code, out = psql(PG[n], sql)
        if code != 0 and "already exists" not in out:
            sys.exit(f"shard {n}: {sql}: {out}")


def main():
    os.makedirs(WORK, exist_ok=True)
    tls = certs()
    cmap = write_map()
    for n in SHARDS:
        start_shard(n, tls, cmap)
    for n in SHARDS:
        up(PG[n], f"shard {n}")
    # The schema on every shard, as rdb-shard ddl would apply it.
    on_every_shard("CREATE TABLE orders (id INT PRIMARY KEY, store INT NOT NULL, v INT NOT NULL) PARTITION BY HASH (id)")
    on_every_shard("CREATE TABLE stores (id INT PRIMARY KEY, name TEXT NOT NULL)")
    start_gateway(tls, cmap)
    up(GW, "the gateway")
    if os.environ.get("CLUSTER_SERVE") == "1":
        # Up and left running for poking at by hand, until killed.
        print(f"serving: gateway {GW}, shards {PG}", flush=True)
        while True:
            time.sleep(3600)

    # 1. Writes routed by key: every row lands once, on its owner.
    rows = 300
    for i in range(1, rows + 1, 50):
        values = ", ".join(f"({k}, {k % 5}, {k})" for k in range(i, min(i + 50, rows + 1)))
        # One statement per key: a multi-row INSERT spans shards.
        for k in range(i, min(i + 50, rows + 1)):
            code, out = psql(GW, f"INSERT INTO orders (id, store, v) VALUES ({k}, {k % 5}, {k})")
            if code != 0:
                check(f"insert {k}", False, out)
                break
    counts = []
    for n in SHARDS:
        code, out = psql(PG[n], "SET rdb.shard_part = on; SELECT count(*) FROM orders")
        counts.append(int(out.splitlines()[-1]) if code == 0 else -1)
    check("every row is on exactly one shard", sum(counts) == rows and all(c > 0 for c in counts), f"per shard {counts}")

    # 2. Reads through the gateway.
    check("a keyed read", psql(GW, "SELECT v FROM orders WHERE id = 137") == (0, "137"), psql(GW, "SELECT v FROM orders WHERE id = 137"))
    check("count(*) fans out", psql(GW, "SELECT count(*) FROM orders") == (0, str(rows)), psql(GW, "SELECT count(*) FROM orders"))
    want_sum = rows * (rows + 1) // 2
    check("sum on the coordinator", psql(GW, "SELECT sum(v) FROM orders") == (0, str(want_sum)), psql(GW, "SELECT sum(v) FROM orders"))
    code, out = psql(GW, "SELECT id FROM orders ORDER BY id DESC LIMIT 3")
    check("top-N across shards", (code, out.split()) == (0, ["300", "299", "298"]), out)
    code, out = psql(GW, "SELECT DISTINCT store FROM orders ORDER BY store")
    check("DISTINCT across shards", (code, out.split()) == (0, ["0", "1", "2", "3", "4"]), out)

    # 3. A join with an unsharded table: gathered onto the home shard.
    for s in range(5):
        psql(GW, f"INSERT INTO stores (id, name) VALUES ({s}, 'store{s}')")
    code, out = psql(GW, "SELECT count(*) FROM orders o JOIN stores s ON s.id = o.store WHERE s.name = 'store3'")
    check("a gathered join", (code, out) == (0, str(rows // 5)), out)
    code, out = psql(GW, "SELECT max(r) FROM (SELECT row_number() OVER (ORDER BY id) AS r FROM orders) w")
    check("a window over a sharded table", (code, out) == (0, str(rows)), out)

    # 4. Two-phase commit across shards: both or neither.
    # Keys 1001 and 1002 land on different shards in this map; the
    # check below holds whether they do or not.
    a, b = 1001, 1002
    code, out = psql(GW, f"BEGIN; INSERT INTO orders (id, store, v) VALUES ({a}, 1, 1); "
                         f"INSERT INTO orders (id, store, v) VALUES ({b}, 1, 1); COMMIT")
    got = psql(GW, f"SELECT count(*) FROM orders WHERE id IN ({a}, {b})")
    check("a two-shard transaction commits both", code == 0 and got == (0, "2"), f"{out} / {got}")
    code, out = psql(GW, f"BEGIN; UPDATE orders SET v = -7 WHERE id = {a}; UPDATE orders SET v = -7 WHERE id = {b}; ROLLBACK")
    got = psql(GW, f"SELECT count(*) FROM orders WHERE v = -7")
    per = [psql(PG[n], "SET rdb.shard_part = on; SELECT id, v FROM orders WHERE v = -7") for n in SHARDS]
    check("a two-shard rollback changes neither", got == (0, "0"), f"{out} / {got} / per shard {per}")

    # 4b. Statements without a route key under two-phase commit.
    check("a keyed IN list across shards", psql(GW, f"SELECT count(*) FROM orders WHERE id IN ({a}, {b}, 7)") == (0, "3"),
          psql(GW, f"SELECT count(*) FROM orders WHERE id IN ({a}, {b}, 7)"))
    code, out = psql(GW, f"UPDATE orders SET v = v + 1000 WHERE id >= {a}")
    check("a keyless UPDATE on every shard", (code, out) == (0, "UPDATE 2"), out)
    check("  ...changed both rows", psql(GW, "SELECT sum(v) FROM orders WHERE id >= 1001") == (0, "2002"),
          psql(GW, "SELECT sum(v) FROM orders WHERE id >= 1001"))
    code, out = psql(GW, "BEGIN; UPDATE orders SET v = 0 WHERE id >= 1001; ROLLBACK")
    check("a keyless UPDATE rolled back", psql(GW, "SELECT sum(v) FROM orders WHERE id >= 1001") == (0, "2002"), out)
    code, out = psql(GW, "BEGIN; SELECT count(*) FROM orders; COMMIT")
    check("a coordinated count inside a transaction", code == 0 and out.splitlines()[1] == str(rows + 2), out)
    code, out = psql(GW, "BEGIN; INSERT INTO orders (id, store, v) VALUES (2000, 1, 1); "
                         "SELECT id FROM orders WHERE id >= 1001 ORDER BY id; ROLLBACK")
    check("a fan-out read in a transaction sees its own write", code == 0 and "2000" in out.split(), out)
    check("  ...and the rollback undid it", psql(GW, "SELECT count(*) FROM orders WHERE id = 2000") == (0, "0"),
          psql(GW, "SELECT count(*) FROM orders WHERE id = 2000"))
    code, out = psql(GW, f"DELETE FROM orders WHERE id >= {a}")
    check("a keyless DELETE on every shard", (code, out) == (0, "DELETE 2"), out)
    for k in (a, b):
        psql(GW, f"INSERT INTO orders (id, store, v) VALUES ({k}, 1, 1)")

    # 4c. A multi-row INSERT across shards: split by row, all or nothing.
    code, out = psql(GW, "INSERT INTO orders (id, store, v) VALUES (3001, 1, 1), (3002, 2, 2), (3003, 3, 3), (3004, 4, 4)")
    check("a multi-row INSERT across shards", (code, out) == (0, "INSERT 0 4"), out)
    code, out = psql(GW, "INSERT INTO orders (id, store, v) VALUES (3005, 1, 5), (3006, 2, 6) RETURNING id")
    check("  ...with RETURNING", code == 0 and sorted(out.split()[:2]) == ["3005", "3006"], out)
    code, out = psql(GW, "INSERT INTO orders (id, store, v) VALUES (3007, 1, 7), (3008, 2, 8), (3001, 9, 9)")
    got = psql(GW, "SELECT count(*) FROM orders WHERE id IN (3007, 3008)")
    check("  ...a duplicate in one row inserts none", code != 0 and got == (0, "0"), f"{out} / {got}")
    psql(GW, "DELETE FROM orders WHERE id >= 3001")

    # 4d. The same, prepared: parameters bound, each row routed by its own.
    code, out = psql_script(GW, "INSERT INTO orders (id, store, v) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) RETURNING id, v "
                                "\\bind 4001 1 11 4002 2 12 4003 3 13 \\g\n")
    check("a prepared multi-row INSERT across shards", code == 0 and sorted(l for l in out.split("\n") if "|" in l) == ["4001|11", "4002|12", "4003|13"]
          and out.split("\n")[-1] == "INSERT 0 3", out)
    code, out = psql_script(GW, "INSERT INTO orders (id, store, v) VALUES ($1, 1, $2), ($3, 2, $4) \\parse ins2\n"
                                "\\bind_named ins2 4004 14 4005 15 \\g\n"
                                "\\bind_named ins2 4006 16 4007 17 \\g\n")
    got = psql(GW, "SELECT count(*) FROM orders WHERE id BETWEEN 4004 AND 4007")
    check("  ...a named one, run twice", code == 0 and got == (0, "4"), f"{out} / {got}")
    code, out = psql_script(GW, "INSERT INTO orders (id, store, v) VALUES ($1, 1, 1), ($2, 1, 1), ($3, 1, 1) \\bind 4008 4009 4001 \\g\n")
    got = psql(GW, "SELECT count(*) FROM orders WHERE id IN (4008, 4009)")
    check("  ...a duplicate in one row inserts none", code != 0 and got == (0, "0"), f"{out} / {got}")
    psql(GW, "DELETE FROM orders WHERE id >= 4001")

    # 4f. Keyless statements over the extended protocol, as drivers send them.
    code, out = psql_script(GW, "SELECT count(*) FROM orders \\bind \\g\n"
                                "SELECT sum(v) FROM orders WHERE store = $1 \\bind 3 \\g\n"
                                "SELECT store, count(*) FROM orders GROUP BY store ORDER BY store \\bind \\g\n"
                                "SELECT id FROM orders WHERE v > $1 \\parse big\n"
                                "\\bind_named big 297 \\g\n")
    lines = [l for l in out.split("\n") if l]
    want_sum = sum(k for k in range(1, rows + 1) if k % 5 == 3)
    ok = (code == 0 and lines[0] == str(rows + 2) and lines[1] == str(want_sum) and lines[2:7] == [f"{s}|{rows // 5 + (2 if s == 1 else 0)}" for s in range(5)]
          and sorted(lines[7:]) == ["298", "299", "300"])
    check("keyless reads over the extended protocol", ok, out)
    code, out = psql_script(GW, "UPDATE orders SET v = v + 0 WHERE store = $1 \\bind 4 \\g\n")
    check("a keyless write over the extended protocol", (code, out) == (0, f"UPDATE {rows // 5}"), out)
    check("a plain DISTINCT across shards", sorted(psql(GW, "SELECT DISTINCT store FROM orders")[1].split()) == ["0", "1", "2", "3", "4"],
          psql(GW, "SELECT DISTINCT store FROM orders"))

    # 4e. An aggregate in a transaction that already wrote on other shards
    # sees those writes.
    code, out = psql(GW, "BEGIN; INSERT INTO orders (id, store, v) VALUES (5001, 1, 100000); "
                         "INSERT INTO orders (id, store, v) VALUES (5002, 2, 100000); "
                         "SELECT count(*), max(v) FROM orders; ROLLBACK")
    want = f"{rows + 4}|100000"
    check("an aggregate in a transaction sees its own writes on every shard", code == 0 and want in out.split(), out)
    check("  ...and the rollback undid them", psql(GW, "SELECT count(*) FROM orders WHERE id > 5000") == (0, "0"),
          psql(GW, "SELECT count(*) FROM orders WHERE id > 5000"))

    # 4g. LISTEN on one session, NOTIFY from another: the gateway's hub
    # carries it whatever shard the NOTIFY ran on.
    listener = psql_script_bg(GW, "LISTEN jobs;\n\\! sleep 3\nSELECT 1;\n", "listen")
    time.sleep(1)
    psql(GW, "NOTIFY jobs, 'hello'")
    psql(GW, f"BEGIN; UPDATE orders SET v = v WHERE id = {b}; NOTIFY jobs, 'at commit'; COMMIT")
    out, _ = listener.communicate(timeout=60)
    check("LISTEN/NOTIFY through the gateway", '"hello"' in out and '"at commit"' in out, out)

    # 4h. Two transactions locking two shards' rows in opposite orders: the
    # cluster's deadlock detector picks one, the other finishes.
    s1 = psql_script_bg(GW, f"BEGIN;\nUPDATE orders SET v = v WHERE id = {a};\n\\! sleep 2\n"
                            f"UPDATE orders SET v = v WHERE id = {b};\nCOMMIT;\n", "dl1")
    s2 = psql_script_bg(GW, f"BEGIN;\nUPDATE orders SET v = v WHERE id = {b};\n\\! sleep 2\n"
                            f"UPDATE orders SET v = v WHERE id = {a};\nCOMMIT;\n", "dl2")
    started = time.time()
    o1, _ = s1.communicate(timeout=120)
    o2, _ = s2.communicate(timeout=120)
    took = time.time() - started
    victims = [o for o in (o1, o2) if "40P01" in o or "deadlock" in o]
    winners = [o for o in (o1, o2) if o.strip().endswith("COMMIT") and "ERROR" not in o]
    check("a cross-shard deadlock is broken", len(victims) == 1 and len(winners) == 1 and took < 60,
          f"{took:.1f}s\n--- 1:\n{o1}\n--- 2:\n{o2}")

    # 5. The DDL fence: a fenced table refuses with a retryable 40001.
    on_every_shard("SET CONFIG shard.ddl_fence = 'orders'")
    code, out = psql(GW, "SELECT v FROM orders WHERE id = 5")
    check("a fenced table refuses retryably", code != 0 and "schema changed" in out, out)
    on_every_shard("SET CONFIG shard.ddl_fence = ''")
    check("the fence lifts", psql(GW, "SELECT v FROM orders WHERE id = 5") == (0, "5"), psql(GW, "SELECT v FROM orders WHERE id = 5"))

    # 6. A consistent fan-out.
    code, out = psql(GW, "SET rdb.consistent_fanout = on; SELECT count(*) FROM orders")
    check("a consistent fan-out", code == 0 and out.splitlines()[-1] == str(rows + 2), out)

    # 7. A shard killed and back: its keys fail cleanly, then work again.
    victim = 1
    procs[f"shard{victim}"].send_signal(signal.SIGKILL)
    procs[f"shard{victim}"].wait()
    failed = [k for k in range(1, 40) if psql(GW, f"SELECT v FROM orders WHERE id = {k}", timeout=20)[0] != 0]
    check("a dead shard's keys fail, the others still answer", 0 < len(failed) < 39, f"{len(failed)} of 39 failed")
    start_shard(victim, tls, cmap)
    up(PG[victim], f"shard {victim} again")
    time.sleep(2)
    check("after the restart every row is there", psql(GW, "SELECT count(*) FROM orders") == (0, str(rows + 2)),
          psql(GW, "SELECT count(*) FROM orders"))

    frontend(tls)
    if RDB_SHARD:
        split_online(tls, cmap)
    if os.environ.get("CLUSTER_SERVE") == "after":
        print(f"serving after the checks: gateway {GW}, shards {PG}", flush=True)
        while True:
            time.sleep(3600)


def split_online(tls, cmap):
    """Shard 3 joins; the upper half of shard 1's range moves to it online
    while a writer keeps inserting through the gateway."""
    start_shard(3, tls, cmap)
    up(PG[3], "shard 3")
    on_every_shard_of([3], "CREATE TABLE orders (id INT PRIMARY KEY, store INT NOT NULL, v INT NOT NULL) PARTITION BY HASH (id)")
    on_every_shard_of([3], "CREATE TABLE stores (id INT PRIMARY KEY, name TEXT NOT NULL)")
    before = int(psql(GW, "SELECT count(*) FROM orders")[1])
    writer = psql_script_bg(GW, "".join(f"INSERT INTO orders (id, store, v) VALUES ({k}, 1, 1);\n" for k in range(6000, 6200)), "writer")
    args = [RDB_SHARD, "split", "--map", cmap, "--shard", "1", "--new-id", "3", "--new-addr", f"127.0.0.1:{RSHARD[3]}",
            "--new-native", f"127.0.0.1:{RDB[3]}", "--online", "--out", f"{WORK}/split.map", "--state-dir", f"{WORK}/split",
            "--table", "orders:id:int", "--user", "rasterized", "--database", "postgres"]
    for n in SHARDS:
        args += ["--native", f"{n}=127.0.0.1:{RDB[n]}"]
    r = subprocess.run(args, env=dict(ENV, RDB_PASSWORD="pw"), capture_output=True, text=True, timeout=600)
    check("rdb-shard split --online", r.returncode == 0, r.stdout + r.stderr)
    wout, _ = writer.communicate(timeout=300)
    failed = [l for l in wout.splitlines() if "ERROR" in l]
    # A writer may see a retryable refusal at the hand-off; every other
    # insert lands.
    got = psql(GW, "SELECT count(*) FROM orders WHERE id >= 6000")
    landed = int(got[1]) if got[0] == 0 else -1
    if landed < 0:
        check("  ...a count after the split", False, got)
    check("  ...the writer kept writing", landed + len(failed) == 200 and len(failed) <= 5, f"{landed} landed, {failed[:3]}")
    code, out = psql(PG[3], "SET rdb.shard_part = on; SELECT count(*) FROM orders")
    on3 = int(out.splitlines()[-1]) if code == 0 else -1
    check("  ...shard 3 holds rows", on3 > 0, out)
    total = psql(GW, "SELECT count(*) FROM orders")
    check("  ...every row once, through the gateway", total == (0, str(before + landed)), f"{total}, want {before + landed}")
    keyed = [k for k in range(1, 300, 7) if psql(GW, f"SELECT v FROM orders WHERE id = {k}") != (0, str(k))]
    check("  ...keyed reads after the split", not keyed, f"wrong for {keyed[:5]}")


def frontend(tls):
    """A front-end node (B4.7): MySQL clients on it, their statements run on
    the cluster behind the gateway, logged in as their own role."""
    log = open(f"{WORK}/frontend.log", "ab")
    procs["frontend"] = subprocess.Popen(
        [CORE, "--location", f"{WORK}/fe", "--protocol", "pgwire", "--pg-port", str(FE_PG), "--pg-password", "pw",
         "--pg-tls", "prefer", "--pg-cert", f"{tls}/gateway.pem", "--pg-key", f"{tls}/gateway.key",
         "--rdb-port", str(FE_RDB), "--mysql-port", str(FE_MY), "--mysql-tls", "prefer",
         "--frontend-upstream", f"127.0.0.1:{GW}", "--log-level", "info"],
        stdout=log, stderr=log)
    up(FE_PG, "the front-end node")
    # Its copy of the schema, as `rdb-shard ddl --frontend` keeps it.
    for ddl in ["CREATE TABLE orders (id INT PRIMARY KEY, store INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE stores (id INT PRIMARY KEY, name TEXT NOT NULL)"]:
        psql(FE_PG, ddl)
    try:
        my = MySQL(FE_MY, "rasterized", "pw", "postgres")
    except Exception as e:
        check("a MySQL login on the front-end node", False, repr(e))
        return
    check("a MySQL login on the front-end node", True)
    try:
        got = my.query("SELECT v FROM orders WHERE id = 42")
        check("  ...a keyed read reaches the cluster", got == [("42",)], got)
        got = my.query("SELECT count(*) FROM orders")
        want = psql(GW, "SELECT count(*) FROM orders")[1]
        check("  ...a count over every shard", got == [(want,)], f"{got} vs {want}")
        got = my.query("INSERT INTO orders (id, store, v) VALUES (7001, 1, 1), (7002, 2, 2), (7003, 3, 3)")
        check("  ...a multi-row INSERT across shards", got == ("ok", 3), got)
        check("  ...seen through the gateway", psql(GW, "SELECT count(*) FROM orders WHERE id > 7000") == (0, "3"),
              psql(GW, "SELECT count(*) FROM orders WHERE id > 7000"))
        my.query("DELETE FROM orders WHERE id > 7000")
    except Exception as e:
        check("  ...statements through the front-end node", False, repr(e))
    try:
        MySQL(FE_MY, "rasterized", "wrong", "postgres")
        check("  ...a wrong password is refused", False, "it logged in")
    except Exception as e:
        check("  ...a wrong password is refused", "refused" in str(e) or "password" in str(e).lower(), repr(e))


def on_every_shard_of(ids, sql):
    for n in ids:
        code, out = psql(PG[n], sql)
        if code != 0 and "already exists" not in out:
            sys.exit(f"shard {n}: {sql}: {out}")


try:
    main()
except SystemExit as e:
    print(f"stopped: {e}", flush=True)
    failures += 1
except Exception:
    import traceback
    traceback.print_exc()
    failures += 1
finally:
    for p in procs.values():
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait()
    print(f"{failures} failure(s); logs in {WORK}")
    sys.exit(min(failures, 100))
