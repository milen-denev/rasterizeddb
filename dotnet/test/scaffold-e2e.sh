#!/bin/bash
# `dotnet ef dbcontext scaffold` end to end, against a real server: a throwaway project creates a few
# tables, the tool reverse-engineers them through the provider's design-time services, and the code it
# wrote is compiled and used to read the rows back.
#
#   RDB_SERVER_BIN=/path/to/rasterizeddb_core dotnet/test/scaffold-e2e.sh [work directory]
#
# Needs the dotnet-ef tool (installed into the work directory's own tool manifest, from nuget.org).
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${1:-$(mktemp -d)}"
: "${RDB_SERVER_BIN:?set RDB_SERVER_BIN to the server binary}"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 MSBUILDDISABLENODEREUSE=1 DOTNET_CLI_USE_MSBUILD_SERVER=0
BUILD=(-m:1 -nodeReuse:false -p:UseSharedCompilation=false -v q)

rm -rf "$WORK/app" "$WORK/db"
mkdir -p "$WORK/app" "$WORK/db"
# A unix socket's path has a short limit, which a work directory may pass.
SOCKETS="$(mktemp -d /tmp/rdbsc.XXXXXX)"
SOCKET="$SOCKETS/s"
"$RDB_SERVER_BIN" --location "$WORK/db" --rdb-socket "$SOCKET" --socket-mode 600 --log-level warn \
    --rdb-tcp off --rdb-socket-auth trust >"$WORK/server.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null; wait $SERVER 2>/dev/null; rm -rf "$SOCKETS"' EXIT
for _ in $(seq 100); do [ -S "$SOCKET" ] && break; sleep 0.1; done
[ -S "$SOCKET" ] || { echo "FAIL: the server did not start"; cat "$WORK/server.log"; exit 1; }
CONNECTION="Socket=$SOCKET"

cd "$WORK/app" || exit 1
cat >App.csproj <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>App</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="$HERE/src/RasterizedDb.EntityFrameworkCore/RasterizedDb.EntityFrameworkCore.csproj" />
    <PackageReference Include="Microsoft.EntityFrameworkCore.Design" Version="10.0.5" />
  </ItemGroup>
</Project>
EOF
cat >Program.cs <<'EOF'
using RasterizedDb.Data;

if (args.Length == 2 && args[0] == "seed")
{
    await using var connection = new RasterizedDbConnection(args[1]);
    await connection.OpenAsync();
    foreach (var sql in new[]
    {
        "CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, email TEXT, joined TIMESTAMPTZ NOT NULL)",
        "CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers (id) ON DELETE CASCADE, " +
        "total NUMERIC(10,2) NOT NULL, placed_on DATE, tags TEXT[], reference UUID NOT NULL, paid BOOLEAN NOT NULL)",
        "CREATE INDEX orders_customer ON orders (customer_id)",
        "INSERT INTO customers (name, email, joined) VALUES ('Ada', 'ada@example.com', '2024-01-02 03:04:05+00'), ('Bo', NULL, '2024-02-03 00:00:00+00')",
        "INSERT INTO orders (customer_id, total, placed_on, tags, reference, paid) VALUES " +
        "(1, 19.90, '2024-03-01', ARRAY['new','gift'], '11111111-2222-3333-4444-555555555555', true), " +
        "(1, 5.00, NULL, NULL, '11111111-2222-3333-4444-666666666666', false), " +
        "(2, 120.50, '2024-03-02', ARRAY['bulk'], '11111111-2222-3333-4444-777777777777', true)",
    })
    {
        await using var command = new RasterizedDbCommand(sql, connection) { ResultFormat = RasterizedDbResultFormat.Text };
        await command.ExecuteNonQueryAsync();
    }

    Console.WriteLine("seeded");
    return 0;
}

return await App.Check.Run(args);
EOF
cat >Check.cs <<'EOF'
namespace App;

public static partial class Check
{
    public static partial Task<int> Run(string[] args);
}
EOF
cat >CheckStub.cs <<'EOF'
namespace App;

public static partial class Check
{
    public static partial Task<int> Run(string[] args) => Task.FromResult(2);
}
EOF

step() { echo "== $1"; }
step "tool"
dotnet new tool-manifest --force >/dev/null && dotnet tool install dotnet-ef --version 10.0.5 2>&1 | tail -1
step "build"
dotnet build "${BUILD[@]}" 2>&1 | grep -E "error|Error\(s\)" | sort -u | head -20
step "seed"
dotnet run --no-build -- seed "$CONNECTION" || { echo "FAIL: seed"; exit 1; }
step "scaffold"
dotnet ef dbcontext scaffold "$CONNECTION" RasterizedDb.EntityFrameworkCore --no-build \
    --output-dir Models --context ShopContext --no-onconfiguring 2>&1 | tail -15
ls Models 2>/dev/null || { echo "FAIL: nothing was scaffolded"; exit 1; }

# The scaffolded context and entities, used.
rm CheckStub.cs
cat >CheckReal.cs <<'EOF'
using App.Models;
using Microsoft.EntityFrameworkCore;

namespace App;

public static partial class Check
{
    public static async partial Task<int> Run(string[] args)
    {
        var options = new DbContextOptionsBuilder<ShopContext>().UseRasterizedDb(args[1]).Options;
        await using var context = new ShopContext(options);
        var customers = await context.Customers.Include(c => c.Orders).OrderBy(c => c.Id).ToListAsync();
        var ada = customers[0];
        var problems = new List<string>();
        void Expect(bool ok, string what)
        {
            if (!ok)
            {
                problems.Add(what);
            }
        }

        Expect(customers.Count == 2, "two customers");
        Expect(ada.Name == "Ada" && ada.Email == "ada@example.com", "Ada's columns");
        Expect(customers[1].Email is null, "a NULL email");
        Expect(ada.Orders.Count == 2, "Ada's two orders, by the scaffolded navigation");
        Expect(ada.Orders.Sum(o => o.Total) == 24.90m, "numeric totals");
        Expect(ada.Orders.Any(o => o.Tags is { } tags && tags.SequenceEqual(new[] { "new", "gift" })), "an array column");
        Expect(ada.Orders.Any(o => o.PlacedOn == new DateOnly(2024, 3, 1)), "a date column");
        Expect(ada.Orders.Any(o => o.Reference == Guid.Parse("11111111-2222-3333-4444-555555555555")), "a uuid column");
        Expect(await context.Orders.CountAsync(o => o.Paid && o.Total > 10) == 2, "a filter on scaffolded columns");

        // Written through the scaffolded model: the key comes back from its sequence.
        var order = new Order { CustomerId = 2, Total = 1.5m, Reference = Guid.NewGuid(), Paid = false };
        context.Orders.Add(order);
        await context.SaveChangesAsync();
        Expect(order.Id > 3, "a generated key");

        foreach (var problem in problems)
        {
            Console.WriteLine($"WRONG: {problem}");
        }

        Console.WriteLine(problems.Count == 0 ? "scaffolded model reads and writes" : "FAIL");
        return problems.Count == 0 ? 0 : 1;
    }
}
EOF
step "build scaffolded"
dotnet build "${BUILD[@]}" 2>&1 | grep -E "error|Error\(s\)" | sort -u | head -30
step "check"
dotnet run --no-build -- check "$CONNECTION"
STATUS=$?
step "scaffolded files"
ls Models
grep -n -E "public .* (Id|Total|Tags|PlacedOn|Reference|Orders|Customer) " Models/*.cs | head -20
grep -n -E "HasColumnName|HasColumnType|ToTable|HasForeignKey|HasIndex|UseHiLo|ValueGenerated" Models/ShopContext.cs | head -30
[ $STATUS -eq 0 ] && echo "PASS: dotnet ef dbcontext scaffold end to end" || echo "FAIL: exit $STATUS"
exit $STATUS
