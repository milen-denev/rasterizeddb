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
import base64, hashlib, hmac, os, signal, socket, ssl, struct, subprocess, sys, time

CORE, GATEWAY, WORK = sys.argv[1], sys.argv[2], sys.argv[3]
RDB_SHARD = sys.argv[4] if len(sys.argv) > 4 else None
BASE = int(os.environ.get("CLUSTER_BASE_PORT", "57100"))
SHARDS = [0, 1, 2]
ALL = SHARDS + [3]                             # 3 joins by a split
PG = {n: BASE + n for n in ALL}                # each shard's own pgwire
RDB = {n: BASE + 10 + n for n in ALL}          # native (rdb-shard's way in)
RSHARD = {n: BASE + 20 + n for n in ALL}       # rshard (the gateway's links)
GW = BASE + 30                                 # the gateway's pgwire
GW2 = BASE + 31                                # a second gateway, two_phase off
FE_PG, FE_MY, FE_RDB, FE_MS = BASE + 40, BASE + 41, BASE + 42, BASE + 43  # the front-end node (B4.7)
CLUSTER = "0123456789abcdef0123456789abcdef"
BLOCKS = 1 << 48
ENV = dict(os.environ, PGPASSWORD="pw", PGSSLMODE="prefer")
# Every link compressed (gateway to shards, shard to shard), with a low
# threshold so nearly every frame is.
COMPRESS = os.environ.get("CLUSTER_COMPRESSION") == "1"
# The enterprise gateway in place of the GPL one, with per-client statistics
# (and their history in an embedded RasterizedDB).
ENTERPRISE = os.environ.get("CLUSTER_ENTERPRISE")
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
         "--gateway-ca", f"{tls}/ca.pem", "--peer-ca", f"{tls}/ca.pem", "--log-level", "info"]
        + (["--shard-peer-compression", "zstd:3"] if COMPRESS else []),
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
""" + ("shard_compression = zstd:3\nshard_compression_min_bytes = 64\n" if COMPRESS else ""))
    log = open(f"{WORK}/gateway.log", "ab")
    if ENTERPRISE:
        cmd = [ENTERPRISE, "--config", conf, "--client-stats-roles", "rasterized", "--client-stats-dir", f"{WORK}/stats",
               "--client-stats-flush-secs", "2"]
    else:
        cmd = [GATEWAY, "--config", conf]
    procs["gateway"] = subprocess.Popen(cmd, stdout=log, stderr=log)


def start_gateway_plain(tls, cmap):
    """A second gateway on the same shards, without two-phase commit."""
    conf = f"{WORK}/gateway2.conf"
    with open(conf, "w") as f:
        f.write(f"""listen = 127.0.0.1:{GW2}
workers = 1
map = {cmap}
home_shard = 0
gateway_name = plain_gateway
shard_trust = ca:{tls}/ca.pem
shard_cert = {tls}/gateway.pem
shard_key = {tls}/gateway.key
auth = scram
unrouted = fan_out
""")
    log = open(f"{WORK}/gateway2.log", "ab")
    procs["gateway2"] = subprocess.Popen([GATEWAY, "--config", conf], stdout=log, stderr=log)


def without_two_phase(tls, cmap):
    start_gateway_plain(tls, cmap)
    up(GW2, "the plain gateway")
    code, out = psql_script(GW2, "SET rdb.unrouted_writes = each_shard;\n"
                                 "UPDATE orders SET v = v + 0 WHERE store = $1 \\bind 4 \\g\n")
    check("without two_phase: a prepared keyless write, shard by shard", code == 0 and out.splitlines()[-1] == "UPDATE 60", out)
    code, out = psql(GW2, "SET rdb.unrouted_writes = each_shard; UPDATE orders SET v = v WHERE id > 297 RETURNING id")
    check("  ...its RETURNING rows from every shard", code == 0 and sorted(out.splitlines()[1:6]) == ["1001", "1002", "298", "299", "300"], out)
    code, out = psql(GW2, "BEGIN; SELECT count(*) FROM orders o JOIN stores s ON s.id = o.store WHERE s.name = 'store3'; ROLLBACK")
    check("  ...a gathered join in a block", code == 0 and "60" in out.split(), out)
    # A block that wrote on some shard: the join runs on the home shard (its
    # other table is there), the block's own row counted.
    code, out = psql(GW2, "BEGIN; INSERT INTO orders (id, store, v) VALUES (6002, 3, 1); "
                          "SELECT count(*) FROM orders o JOIN stores s ON s.id = o.store WHERE s.name = 'store3'; ROLLBACK")
    check("  ...after a write in the block, its own row counted", code == 0 and "61" in out.split(), out)
    # A table that lives on the home shard, read in a block that wrote
    # elsewhere: read there, not as the empty copy on the block's shard.
    want = psql(GW2, "SELECT count(*) FROM stores")[1]
    found = None
    for k in range(6003, 6010):
        code, out = psql(GW2, f"BEGIN; INSERT INTO orders (id, store, v) VALUES ({k}, 1, 1); SELECT count(*) FROM stores; ROLLBACK")
        found = (code, out)
        if code != 0 or want not in out.split():
            break
    check("  ...a home table read in a block that wrote on any shard", found[0] == 0 and want in found[1].split(), f"{found} want {want}")
    code, out = psql(GW2, "BEGIN; INSERT INTO orders (id, store, v) VALUES (6010, 1, 1); INSERT INTO orders (id, store, v) VALUES (6011, 1, 1); "
                          "INSERT INTO orders (id, store, v) VALUES (6012, 1, 1); INSERT INTO orders (id, store, v) VALUES (6013, 1, 1); ROLLBACK")
    check("  ...a second shard's write in the block is refused by name", code != 0 and "two_phase" in out, out)
    with open(f"{WORK}/plain.tsv", "w") as f:
        f.write("".join(f"{k}\t1\t1\n" for k in range(9500, 9520)))
    code, out = psql_script(GW2, f"\\copy orders (id, store, v) FROM '{WORK}/plain.tsv'\n")
    check("  ...a COPY across shards is refused without the opt-in", code != 0 and "split_bulk_writes" in out, out)
    code, out = psql_script(GW2, f"SET rdb.split_bulk_writes = on;\n\\copy orders (id, store, v) FROM '{WORK}/plain.tsv'\n")
    check("  ...and split with SET rdb.split_bulk_writes = on", code == 0 and out.splitlines()[-1] == "COPY 20", out)
    psql(GW, "DELETE FROM orders WHERE id >= 9500")


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


class PgClient:
    """A PostgreSQL client of just one feature psql lacks: the extended
    protocol with binary results, as Npgsql and sqlx ask for them."""

    def __init__(self, port, user, password, database):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        body = struct.pack("!I", 196608) + b"user\0" + user.encode() + b"\0database\0" + database.encode() + b"\0\0"
        self.sock.sendall(struct.pack("!I", len(body) + 4) + body)
        nonce = base64.b64encode(os.urandom(18)).decode()
        first_bare = f"n=,r={nonce}"
        salted = None
        while True:
            t, b = self.read()
            if t == b"R":
                kind = struct.unpack("!I", b[:4])[0]
                if kind == 10:      # SASL: SCRAM-SHA-256
                    init = ("n,," + first_bare).encode()
                    msg = b"SCRAM-SHA-256\0" + struct.pack("!I", len(init)) + init
                    self.send(b"p", msg)
                elif kind == 11:    # SASL continue
                    server_first = b[4:].decode()
                    f = dict(x.split("=", 1) for x in server_first.split(","))
                    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(f["s"]), int(f["i"]))
                    client_key = hmac.new(salted, b"Client Key", "sha256").digest()
                    without_proof = f"c=biws,r={f['r']}"
                    auth = f"{first_bare},{server_first},{without_proof}".encode()
                    sig = hmac.new(hashlib.sha256(client_key).digest(), auth, "sha256").digest()
                    proof = bytes(a ^ b for a, b in zip(client_key, sig))
                    self.send(b"p", f"{without_proof},p={base64.b64encode(proof).decode()}".encode())
                elif kind in (0, 12):
                    continue
                else:
                    raise RuntimeError(f"auth kind {kind}")
            elif t == b"E":
                raise RuntimeError(self.error(b))
            elif t == b"Z":
                return

    def send(self, t, body):
        self.sock.sendall(t + struct.pack("!I", len(body) + 4) + body)

    def read(self):
        h = b""
        while len(h) < 5:
            c = self.sock.recv(5 - len(h))
            if not c:
                raise RuntimeError("the connection closed")
            h += c
        n = struct.unpack("!I", h[1:5])[0] - 4
        b = b""
        while len(b) < n:
            c = self.sock.recv(n - len(b))
            if not c:
                raise RuntimeError("the connection closed")
            b += c
        return h[:1], b

    @staticmethod
    def error(b):
        fields = dict((f[:1], f[1:].decode(errors="replace")) for f in b.split(b"\0") if f)
        return f"{fields.get(b'C', '')}: {fields.get(b'M', '')}"

    def execute(self, sql, params, binary=True):
        """Parse/Bind/Describe/Execute/Sync with text parameters; rows as
        lists of raw cell bytes (None for NULL), in binary when `binary`."""
        self.send(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))
        bind = b"\0\0" + struct.pack("!h", 0) + struct.pack("!h", len(params))
        for p in params:
            v = str(p).encode()
            bind += struct.pack("!i", len(v)) + v
        bind += struct.pack("!hh", 1, 1 if binary else 0)
        self.send(b"B", bind)
        self.send(b"D", b"P\0")
        self.send(b"E", b"\0" + struct.pack("!i", 0))
        self.send(b"S", b"")
        rows, error, described = [], None, None
        while True:
            t, b = self.read()
            if t == b"T":
                described = struct.unpack("!h", b[:2])[0]
            elif t == b"D":
                n = struct.unpack("!h", b[:2])[0]
                at, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", b[at:at + 4])[0]
                    at += 4
                    row.append(None if ln < 0 else b[at:at + ln])
                    at += max(ln, 0)
                rows.append(row)
            elif t == b"E":
                error = self.error(b)
            elif t == b"Z":
                break
        if error:
            raise RuntimeError(error)
        return described, rows


class TDS:
    """Just enough of SQL Server's TDS 7.4 for the front-end node: an
    unencrypted PRELOGIN, LOGIN7, SQL batches; rows as lists of ints, strs,
    bytes or None, and the last DONE's row count."""

    def __init__(self, port, user, password, database):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        # PRELOGIN: VERSION, ENCRYPTION = not supported.
        opts = bytes([0x00]) + struct.pack(">HH", 11, 6) + bytes([0x01]) + struct.pack(">HH", 17, 1) + b"\xff"
        self.send(0x12, opts + bytes([16, 0, 0, 0, 0, 0]) + b"\x02")
        self.recv()
        u = lambda t: t.encode("utf-16-le")
        obfuscated = bytes(((b << 4 | b >> 4) & 0xFF) ^ 0xA5 for b in u(password))
        fields = [u("rdb-test"), u(user), obfuscated, u("cluster_e2e"), u("127.0.0.1"), b"", u("python"), b"", u(database)]
        off, data, table = 94, b"", b""
        for i, f in enumerate(fields):
            table += struct.pack("<HH", off + len(data), len(f) // 2)
            data += f
        table += b"\0" * 6 + struct.pack("<HHHHHHI", off + len(data), 0, off + len(data), 0, off + len(data), 0, 0)
        head = struct.pack("<IIIIII", 0, 0x74000004, 4096, 7, os.getpid(), 0) + bytes([0xE0, 0x03, 0, 0]) + struct.pack("<iI", 0, 0x0409)
        login = head + table + data
        login = struct.pack("<I", len(login)) + login[4:]
        self.send(0x10, login)
        _, _, error = self.tokens(self.recv())
        if error:
            raise RuntimeError(f"login refused: {error}")

    def send(self, kind, payload):
        self.sock.sendall(struct.pack(">BBHHBB", kind, 1, len(payload) + 8, 0, 1, 0) + payload)

    def recv(self):
        out = b""
        while True:
            h = self.exact(8)
            out += self.exact(struct.unpack(">H", h[2:4])[0] - 8)
            if h[1] & 1:
                return out

    def exact(self, n):
        b = b""
        while len(b) < n:
            c = self.sock.recv(n - len(b))
            if not c:
                raise RuntimeError("the connection closed")
            b += c
        return b

    def tokens(self, b):
        at, cols, rows, count, error = 0, [], [], None, None
        while at < len(b):
            t = b[at]
            at += 1
            if t == 0x81:                       # COLMETADATA
                n = struct.unpack("<H", b[at:at + 2])[0]
                at += 2
                cols = []
                for _ in range(0 if n == 0xFFFF else n):
                    at += 6
                    ty = b[at]
                    at += 1
                    size = None
                    if ty in (0x68, 0x26, 0x6d, 0x24, 0x2a, 0x2b, 0x29):
                        size = b[at]
                        at += 1
                    elif ty in (0x6a, 0x6c):
                        size = b[at]
                        at += 3
                    elif ty == 0xe7:
                        size = struct.unpack("<H", b[at:at + 2])[0]
                        at += 7
                    elif ty == 0xa5:
                        size = struct.unpack("<H", b[at:at + 2])[0]
                        at += 2
                    elif ty != 0x28:
                        raise RuntimeError(f"TDS type {ty:#x} is not read by this client")
                    nl = b[at]
                    at += 1 + 2 * nl
                    cols.append((ty, size))
            elif t == 0xD1:                     # ROW
                row = []
                for ty, size in cols:
                    if ty in (0xe7, 0xa5) and size == 0xFFFF:   # PLP
                        total = struct.unpack("<Q", b[at:at + 8])[0]
                        at += 8
                        if total == 0xFFFFFFFFFFFFFFFF:
                            row.append(None)
                            continue
                        v = b""
                        while True:
                            cl = struct.unpack("<I", b[at:at + 4])[0]
                            at += 4
                            if cl == 0:
                                break
                            v += b[at:at + cl]
                            at += cl
                    elif ty in (0xe7, 0xa5):
                        ln = struct.unpack("<H", b[at:at + 2])[0]
                        at += 2
                        if ln == 0xFFFF:
                            row.append(None)
                            continue
                        v = b[at:at + ln]
                        at += ln
                    else:
                        ln = b[at]
                        at += 1
                        if ln == 0:
                            row.append(None)
                            continue
                        v = b[at:at + ln]
                        at += ln
                    if ty == 0x26:
                        row.append(int.from_bytes(v, "little", signed=True))
                    elif ty == 0xe7:
                        row.append(v.decode("utf-16-le"))
                    else:
                        row.append(v)
                rows.append(row)
            elif t in (0xFD, 0xFE, 0xFF):       # DONE, DONEPROC, DONEINPROC
                status = struct.unpack("<H", b[at:at + 2])[0]
                if status & 0x10:
                    count = struct.unpack("<Q", b[at + 4:at + 12])[0]
                at += 12
            elif t == 0xAA:                     # ERROR
                ln = struct.unpack("<H", b[at:at + 2])[0]
                ml = struct.unpack("<H", b[at + 8:at + 10])[0]
                error = b[at + 10:at + 10 + 2 * ml].decode("utf-16-le")
                at += 2 + ln
            elif t in (0xAB, 0xAD, 0xE3, 0xA9):  # INFO, LOGINACK, ENVCHANGE, ORDER
                at += 2 + struct.unpack("<H", b[at:at + 2])[0]
            elif t == 0x79:                     # RETURNSTATUS
                at += 4
            else:
                raise RuntimeError(f"TDS token {t:#x} is not read by this client")
        return rows, count, error

    def query(self, sql):
        headers = struct.pack("<IIHQI", 22, 18, 2, 0, 1)
        self.send(0x01, headers + sql.encode("utf-16-le"))
        rows, count, error = self.tokens(self.recv())
        if error:
            raise RuntimeError(error)
        return rows if rows else ("ok", count)


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
    code, out = psql_script(GW, "SELECT count(*) FROM orders o JOIN stores s ON s.id = o.store WHERE s.name = $1 \\bind store3 \\g\n")
    check("a gathered join, prepared", (code, out) == (0, str(rows // 5)), out)
    code, out = psql(GW, "BEGIN; INSERT INTO orders (id, store, v) VALUES (6001, 3, 1); "
                         "SELECT count(*) FROM orders o JOIN stores s ON s.id = o.store WHERE s.name = 'store3'; ROLLBACK")
    check("a gathered join in a transaction sees its own write", code == 0 and str(rows // 5 + 1) in out.split(), out)
    check("  ...and the rollback undid it", psql(GW, "SELECT count(*) FROM orders WHERE id = 6001") == (0, "0"),
          psql(GW, "SELECT count(*) FROM orders WHERE id = 6001"))
    code, out = psql(GW, "SELECT max(r) FROM (SELECT row_number() OVER (ORDER BY id) AS r FROM orders) w")
    check("a window over a sharded table", (code, out) == (0, str(rows)), out)

    # 3b. The node's SHOWs about tables and storage, for the whole cluster.
    code, out = psql(GW, "SHOW TABLES")
    slots = [int(l.split("|")[2]) for l in out.splitlines() if l.split("|")[0] == "orders"] if code == 0 else []
    check("SHOW TABLES counts a partitioned table's rows on every shard", slots and slots[0] >= rows, out)
    code, out = psql(GW, "SHOW STORAGE LOCATIONS")
    shards_seen = sorted({l.split("|")[-1] for l in out.splitlines()}) if code == 0 else []
    want_shards = sorted(psql(GW, "SELECT 1")[0] == 0 and [l.split("|")[0] for l in psql(GW, "SHOW SHARDS")[1].splitlines()] or [])
    check("SHOW STORAGE LOCATIONS lists every shard's", shards_seen == want_shards, f"{out} want {want_shards}")
    code, out = psql(GW, "SHOW STORAGE STATS")
    check("SHOW STORAGE STATS answers for the cluster", code == 0 and f"shards|{len(want_shards)}" in out, out)

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

    # 4i. A prepared top-N and DISTINCT over every shard: text and binary.
    code, out = psql_script(GW, "SELECT id, v FROM orders WHERE v > $1 ORDER BY v DESC, id LIMIT $2 OFFSET $3 \\bind 0 3 1 \\g\n"
                                "SELECT DISTINCT store FROM orders WHERE id < $1 \\bind 1000 \\g\n")
    lines = [l for l in out.split("\n") if l]
    check("a prepared top-N across shards (text)", code == 0 and lines[:3] == ["299|299", "298|298", "297|297"], out)
    code, out = psql(GW, "SELECT id FROM orders ORDER BY v DESC, id LIMIT 2")
    check("a top-N by a column the select list does not show", (code, out.split()) == (0, ["300", "299"]), out)
    check("a prepared DISTINCT across shards (text)", code == 0 and sorted(lines[3:]) == ["0", "1", "2", "3", "4"], out)
    try:
        pg = PgClient(GW, "rasterized", "pw", "postgres")
        cols, got = pg.execute("SELECT id, v FROM orders ORDER BY id DESC LIMIT $1", [2])
        ok = cols == 2 and [[struct.unpack("!i", c)[0] for c in r] for r in got] == [[b, 1], [a, 1]]
        check("a prepared top-N across shards (binary results)", ok, f"{cols} {got}")
        cols, got = pg.execute("SELECT DISTINCT store FROM orders", [], binary=True)
        check("a prepared DISTINCT across shards (binary results)",
              sorted(struct.unpack("!i", r[0])[0] for r in got) == [0, 1, 2, 3, 4], got)
        cols, got = pg.execute("SELECT count(*) FROM orders", [], binary=True)
        check("a prepared count across shards (binary results)", struct.unpack("!q", got[0][0])[0] == rows + 2, got)
    except Exception as e:
        check("prepared statements with binary results", False, repr(e))
    code, out = psql_script(GW, "BEGIN;\nINSERT INTO orders (id, store, v) VALUES (5001, 1, 100000);\n"
                                "INSERT INTO orders (id, store, v) VALUES (5002, 2, 99999);\n"
                                "SELECT id FROM orders ORDER BY v DESC LIMIT $1 \\bind 2 \\g\nROLLBACK;\n")
    check("a prepared top-N in a transaction that wrote on two shards", code == 0 and "5001\n5002" in out, out)

    # 4e. An aggregate in a transaction that already wrote on other shards
    # sees those writes.
    code, out = psql(GW, "BEGIN; INSERT INTO orders (id, store, v) VALUES (5001, 1, 100000); "
                         "INSERT INTO orders (id, store, v) VALUES (5002, 2, 100000); "
                         "SELECT count(*), max(v) FROM orders; ROLLBACK")
    want = f"{rows + 4}|100000"
    check("an aggregate in a transaction sees its own writes on every shard", code == 0 and want in out.split(), out)
    check("  ...and the rollback undid them", psql(GW, "SELECT count(*) FROM orders WHERE id > 5000") == (0, "0"),
          psql(GW, "SELECT count(*) FROM orders WHERE id > 5000"))

    # 4j. COPY: into the unsharded table (one shard), and into the sharded
    # one, split by row, all or nothing.
    with open(f"{WORK}/stores.tsv", "w") as f:
        f.write("10\tten\n11\televen\n")
    code, out = psql_script(GW, f"\\copy stores (id, name) FROM '{WORK}/stores.tsv'\n")
    check("a COPY into an unsharded table", (code, out) == (0, "COPY 2"), out)
    with open(f"{WORK}/orders.tsv", "w") as f:
        f.write("".join(f"{k}\t{k % 5}\t{k}\n" for k in range(8001, 9001)))
    code, out = psql_script(GW, f"\\copy orders (id, store, v) FROM '{WORK}/orders.tsv'\n")
    per = [int(psql(PG[n], "SET rdb.shard_part = on; SELECT count(*) FROM orders WHERE id > 8000")[1].splitlines()[-1]) for n in SHARDS]
    check("a COPY into a sharded table, split by row", (code, out) == (0, "COPY 1000") and sum(per) == 1000 and all(per), f"{out} {per}")
    with open(f"{WORK}/orders.csv", "w") as f:
        f.write('id,store,v\n9001,1,"7"\n9002,2,"8"\n9003,"3",9\n')
    code, out = psql_script(GW, f"\\copy orders (id, store, v) FROM '{WORK}/orders.csv' WITH (FORMAT csv, HEADER true)\n")
    got = psql(GW, "SELECT sum(v) FROM orders WHERE id > 9000")
    check("  ...CSV with a header and quoted fields", (code, out) == (0, "COPY 3") and got == (0, "24"), f"{out} / {got}")
    with open(f"{WORK}/dup.tsv", "w") as f:
        f.write("".join(f"{k}\t1\t1\n" for k in range(9100, 9150)) + "8001\t1\t1\n")
    code, out = psql_script(GW, f"\\copy orders (id, store, v) FROM '{WORK}/dup.tsv'\n")
    got = psql(GW, "SELECT count(*) FROM orders WHERE id BETWEEN 9100 AND 9149")
    check("  ...a duplicate key in one row loads nothing", code != 0 and got == (0, "0"), f"{out} / {got}")
    psql(GW, "DELETE FROM orders WHERE id > 8000")
    psql(GW, "DELETE FROM stores WHERE id >= 10")
    # A reference table: every shard's copy takes every row.
    on_every_shard("CREATE TABLE ref_copy (id INT PRIMARY KEY, name TEXT NOT NULL)")
    on_every_shard("SHARD REFERENCE TABLE ref_copy")
    with open(f"{WORK}/ref.tsv", "w") as f:
        f.write("".join(f"{k}\tr{k}\n" for k in range(1, 41)))
    code, out = psql_script(GW, f"\\copy ref_copy (id, name) FROM '{WORK}/ref.tsv'\n")
    per = [psql(PG[n], "SELECT count(*) FROM ref_copy")[1] for n in SHARDS]
    check("a COPY into a reference table loads every shard's copy", (code, out) == (0, "COPY 40") and all(p == "40" for p in per), f"{out} {per}")

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

    # 4k. The same cluster through a gateway without two-phase commit.
    without_two_phase(tls, cmap)

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

    # 7b. A prepared transaction's NOTIFY survives its shard's restart and is
    # sent at COMMIT PREPARED.
    code, out = psql(PG[2], "BEGIN; NOTIFY jobs, 'survived'; PREPARE TRANSACTION 'rdbtest:notify:1'")
    if code != 0:
        check("a prepared NOTIFY across a restart", False, out)
    else:
        procs["shard2"].send_signal(signal.SIGKILL)
        procs["shard2"].wait()
        start_shard(2, tls, cmap)
        up(PG[2], "shard 2 again")
        listener = psql_script_bg(PG[2], "LISTEN jobs;\n\\! sleep 3\nSELECT 1;\n", "listen2")
        time.sleep(1)
        done = psql(PG[2], "COMMIT PREPARED 'rdbtest:notify:1'")
        out, _ = listener.communicate(timeout=60)
        check("a prepared NOTIFY across a restart", done[0] == 0 and '"survived"' in out, f"{done} / {out}")

    if COMPRESS:
        rows_ = dict(l.split("|", 1) for l in psql(GW, "SHOW GATEWAY")[1].splitlines() if "|" in l)
        check("the links to the shards are compressed",
              int(rows_.get("shard_frames_compressed_in", "0")) > 0 and int(rows_.get("shard_compression_saved_bytes", "0")) > 0,
              {k: v for k, v in rows_.items() if "compress" in k})
    if ENTERPRISE:
        client_stats()
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
         "--mssql-port", str(FE_MS), "--mssql-tls", "prefer",
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
    frontend_tds()


def frontend_tds():
    """The same front-end node, as SQL Server clients reach it."""
    try:
        ms = TDS(FE_MS, "rasterized", "pw", "postgres")
    except Exception as e:
        check("a SQL Server login on the front-end node", False, repr(e))
        return
    check("a SQL Server login on the front-end node", True)
    try:
        got = ms.query("SELECT v FROM orders WHERE id = 42")
        check("  ...a keyed read reaches the cluster (TDS)", got == [[42]], got)
        got = ms.query("SELECT TOP 3 id FROM orders ORDER BY id DESC")
        want = [[int(x)] for x in psql(GW, "SELECT id FROM orders ORDER BY id DESC LIMIT 3")[1].split()]
        check("  ...a TOP across shards (TDS)", got == want, f"{got} vs {want}")
        got = ms.query("INSERT INTO orders (id, store, v) VALUES (7101, 1, 1), (7102, 2, 2), (7103, 3, 3)")
        check("  ...a multi-row INSERT across shards (TDS)", got == ("ok", 3), got)
        check("  ...seen through the gateway (TDS)", psql(GW, "SELECT count(*) FROM orders WHERE id > 7100") == (0, "3"),
              psql(GW, "SELECT count(*) FROM orders WHERE id > 7100"))
        ms.query("DELETE FROM orders WHERE id > 7100")
    except Exception as e:
        check("  ...statements through the front-end node (TDS)", False, repr(e))
    try:
        TDS(FE_MS, "rasterized", "wrong", "postgres")
        check("  ...a wrong password is refused (TDS)", False, "it logged in")
    except Exception as e:
        check("  ...a wrong password is refused (TDS)", "refused" in str(e) or "failed" in str(e).lower(), repr(e))


def client_stats():
    """The enterprise gateway's per-client statistics, live and kept."""
    code, out = psql(GW, "SHOW CLIENT STATS")
    rows = [l.split("|") for l in out.splitlines()] if code == 0 else []
    mine = [r for r in rows if r[1] == "rasterized" and r[2] == "127.0.0.1"]
    ok = code == 0 and len(mine) >= 1 and sum(int(r[6]) for r in mine) > 300 and sum(int(r[15]) for r in mine) > 0
    check("SHOW CLIENT STATS counts this client's statements and shard calls", ok, out[:600])
    if ok and COMPRESS:
        plain = sum(int(r[16]) + int(r[18]) for r in mine)
        wire = sum(int(r[17]) + int(r[19]) for r in mine)
        check("  ...shard traffic after compression is smaller", 0 < wire < plain, f"plain {plain}, wire {wire}")
    if ok:
        errors = sum(int(r[7]) for r in mine)
        check("  ...and its errors", errors > 0, f"{errors} errors")
    time.sleep(5)
    code, out = psql(GW, "SHOW CLIENT STATS HISTORY 10")
    check("SHOW CLIENT STATS HISTORY reads the embedded store", code == 0 and len(out.splitlines()) >= 1, out[:400])
    # A role not listed may not read them.
    # Roles are made on every shard (the gateway runs no role statement).
    on_every_shard_of(SHARDS, "CREATE ROLE peeker LOGIN PASSWORD 'pw'")
    r = subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(GW), "-U", "peeker", "-d", "postgres", "-At", "-c", "SHOW CLIENT STATS"],
                       env=ENV, capture_output=True, text=True, timeout=30)
    check("  ...not for a role --client-stats-roles does not list", r.returncode != 0 and "client-stats-roles" in r.stderr, r.stderr[:300])


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
