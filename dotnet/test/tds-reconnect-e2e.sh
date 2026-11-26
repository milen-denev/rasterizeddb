#!/bin/bash
# A server restart under Microsoft.Data.SqlClient, end to end: a connection that was open (or pooled)
# when the server went away is used again once it is back. SqlClient reconnects such a connection by
# itself, but only for a server that acknowledged TDS session recovery; this checks that it does, that
# the session's database and SET options come back, and that a connection holding what cannot come
# back (a transaction, a #table) reports the break instead of carrying on without it.
#
#   RDB_SERVER_BIN=/path/to/rasterizeddb_core dotnet/test/tds-reconnect-e2e.sh [work directory]
#
# Needs Microsoft.Data.SqlClient (from nuget.org, or the local package cache).
set -u
WORK="${1:-$(mktemp -d)}"
: "${RDB_SERVER_BIN:?set RDB_SERVER_BIN to the server binary}"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 MSBUILDDISABLENODEREUSE=1 DOTNET_CLI_USE_MSBUILD_SERVER=0
BUILD=(-m:1 -nodeReuse:false -p:UseSharedCompilation=false -v q)

rm -rf "$WORK/app" "$WORK/db"
mkdir -p "$WORK/app" "$WORK/db"
# The program starts and stops the server; whatever it leaves running is stopped here.
trap '[ -f "$WORK/server.pid" ] && kill "$(cat "$WORK/server.pid")" 2>/dev/null' EXIT

cd "$WORK/app" || exit 1
cat >App.csproj <<'EOF'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Data.SqlClient" Version="6.1.2" />
  </ItemGroup>
</Project>
EOF
cat >Program.cs <<'EOF'
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Data.SqlClient;

var bin = args[0];
var work = args[1];
var port = FreePort();
// A unix socket's path has a short limit, which a work directory may pass.
var sockets = Directory.CreateTempSubdirectory("rdbtds.").FullName;
Process? server = null;
var failures = 0;

void Start()
{
    var info = new ProcessStartInfo(bin)
    {
        RedirectStandardOutput = true,
        RedirectStandardError = true,
    };
    foreach (var a in new[]
    {
        "--location", Path.Combine(work, "db"), "--rdb-tcp", "off", "--rdb-socket", Path.Combine(sockets, "s"), "--rdb-socket-auth", "trust",
        "--log-level", "warn", "--mssql-port", port.ToString(), "--mssql-auth", "trust",
    })
    {
        info.ArgumentList.Add(a);
    }

    server = Process.Start(info)!;
    server.OutputDataReceived += (_, _) => { };
    server.ErrorDataReceived += (_, e) => { if (e.Data is not null) File.AppendAllText(Path.Combine(work, "server.log"), e.Data + "\n"); };
    server.BeginOutputReadLine();
    server.BeginErrorReadLine();
    File.WriteAllText(Path.Combine(work, "server.pid"), server.Id.ToString());
    for (var i = 0; i < 300; i++)
    {
        try
        {
            using var probe = new TcpClient();
            probe.Connect("127.0.0.1", port);
            return;
        }
        catch (SocketException)
        {
            if (server.HasExited) throw new InvalidOperationException("the server exited: see server.log");
            Thread.Sleep(100);
        }
    }

    throw new InvalidOperationException("the server did not start");
}

// `graceful`: SIGTERM, on which the server closes its connections; else SIGKILL.
void Stop(bool graceful)
{
    if (graceful)
    {
        Process.Start("kill", server!.Id.ToString())!.WaitForExit();
        if (!server.WaitForExit(30_000)) server.Kill();
    }
    else
    {
        server!.Kill();
    }

    server.WaitForExit();
}

void Restart(bool graceful)
{
    Stop(graceful);
    Start();
    // SqlClient looks at a connection's socket only when it has been quiet for five seconds.
    Thread.Sleep(6_000);
}

void Check(string what, bool ok, string detail = "")
{
    Console.WriteLine($"{(ok ? "ok  " : "FAIL")} {what}{(detail.Length > 0 ? ": " + detail : "")}");
    if (!ok) failures++;
}

object? Scalar(SqlConnection c, string sql, SqlTransaction? tx = null)
{
    using var command = new SqlCommand(sql, c, tx);
    return command.ExecuteScalar();
}

int Run(SqlConnection c, string sql, SqlTransaction? tx = null)
{
    using var command = new SqlCommand(sql, c, tx);
    return command.ExecuteNonQuery();
}

var connectionString = $"Server=127.0.0.1,{port};User Id=rasterized;Password=unused;Encrypt=false;TrustServerCertificate=true;Connect Timeout=15";
try
{
    Start();
    using (var setup = new SqlConnection(connectionString))
    {
        setup.Open();
        Run(setup, "CREATE TABLE [Item] ([Id] int PRIMARY KEY, [Name] nvarchar(20) NOT NULL)");
        Run(setup, "INSERT INTO [Item] VALUES (1, N'one'), (2, N'two')");
        Run(setup, "CREATE DATABASE [Other]");
    }

    // 1. A pooled connection, the server stopped in order and by force.
    foreach (var graceful in new[] { true, false })
    {
        Restart(graceful);
        try
        {
            using var c = new SqlConnection(connectionString);
            c.Open();
            Check($"a pooled connection after a {(graceful ? "shutdown" : "kill")}", (int)Scalar(c, "SELECT COUNT(*) FROM [Item]")! == 2);
        }
        catch (SqlException e)
        {
            Check($"a pooled connection after a {(graceful ? "shutdown" : "kill")}", false, $"number {e.Number} class {e.Class}: {e.Message}");
        }
    }

    // 2. An open connection: its database and its SET options are the session's again.
    using (var c = new SqlConnection(connectionString))
    {
        c.Open();
        var before = c.ClientConnectionId;
        c.ChangeDatabase("Other");
        Run(c, "CREATE TABLE [Here] ([Id] int PRIMARY KEY)");
        Run(c, "SET NOCOUNT ON");
        Check("NOCOUNT before", Run(c, "INSERT INTO [Here] VALUES (1)") == -1);
        Restart(true);
        try
        {
            Check("an open connection carries on", (int)Scalar(c, "SELECT COUNT(*) FROM [Here]")! == 1);
            Check("in the database it was in", (string)Scalar(c, "SELECT DB_NAME()")! == "Other", (string)Scalar(c, "SELECT DB_NAME()")!);
            Check("with NOCOUNT still on", Run(c, "INSERT INTO [Here] VALUES (2)") == -1);
            Check("over a new connection", c.ClientConnectionId != before);
            Run(c, "SET NOCOUNT OFF");
            Check("NOCOUNT off again", Run(c, "INSERT INTO [Here] VALUES (3)") == 1);
        }
        catch (SqlException e)
        {
            Check("an open connection carries on", false, $"number {e.Number} class {e.Class}: {e.Message}");
        }
    }

    // 3. What cannot be made again: the break is reported, the statement not run.
    using (var c = new SqlConnection(connectionString))
    {
        c.Open();
        var tx = c.BeginTransaction();
        Run(c, "INSERT INTO [Item] VALUES (3, N'three')", tx);
        Restart(true);
        try
        {
            Run(c, "INSERT INTO [Item] VALUES (4, N'four')", tx);
            Check("a transaction is not carried on without", false, "the statement ran");
        }
        catch (Exception e) when (e is SqlException or InvalidOperationException)
        {
            Check("a transaction is not carried on without", true);
        }
    }

    using (var c = new SqlConnection(connectionString))
    {
        c.Open();
        Run(c, "CREATE TABLE #scratch ([Id] int)");
        Restart(true);
        try
        {
            Run(c, "INSERT INTO [Item] VALUES (5, N'five')");
            Check("a #table's session is not carried on without", false, "the statement ran");
        }
        catch (Exception e) when (e is SqlException or InvalidOperationException)
        {
            Check("a #table's session is not carried on without", true);
        }
    }

    using (var c = new SqlConnection(connectionString))
    {
        c.Open();
        Check("neither insert happened", (int)Scalar(c, "SELECT COUNT(*) FROM [Item]")! == 2, Scalar(c, "SELECT COUNT(*) FROM [Item]")!.ToString()!);
        // Once the transaction is over, the session can be made again.
        var tx = c.BeginTransaction();
        Run(c, "INSERT INTO [Item] VALUES (6, N'six')", tx);
        tx.Commit();
        Restart(true);
        try
        {
            Check("after a commit the connection carries on", (int)Scalar(c, "SELECT COUNT(*) FROM [Item]")! == 3);
        }
        catch (SqlException e)
        {
            Check("after a commit the connection carries on", false, $"number {e.Number} class {e.Class}: {e.Message}");
        }
    }
}
finally
{
    try { server?.Kill(); } catch (InvalidOperationException) { }
    try { Directory.Delete(sockets, true); } catch (IOException) { }
}

Console.WriteLine(failures == 0 ? "PASS" : $"FAIL: {failures}");
return failures == 0 ? 0 : 1;

static int FreePort()
{
    var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
    listener.Start();
    var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}
EOF
dotnet build "${BUILD[@]}" 2>&1 | grep -E "error|Warn.*NU" | head -20
dotnet run --no-build -- "$RDB_SERVER_BIN" "$WORK"
