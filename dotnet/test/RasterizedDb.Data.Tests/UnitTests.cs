using System.Text;
using RasterizedDb.Data.Internal;

namespace RasterizedDb.Data.Tests;

public class SqlParserTests
{
    private static string Text(ParsedSql parsed) => Encoding.UTF8.GetString(parsed.Utf8);

    [Fact]
    public void Positional_placeholders_are_found_outside_strings_names_and_comments()
    {
        var parsed = ParsedSql.Parse("SELECT '$1', \"$2\", $1 -- $3\n, $2 /* $4 /* nested */ $5 */ FROM t WHERE a = $$ $6 $$ AND b = $tag$ $7 $tag$");
        Assert.Equal(2, parsed.Spans.Length);
        Assert.Equal(2, parsed.ParameterCount);
        Assert.Equal(0, parsed.Spans[0].Parameter);
        Assert.Equal(1, parsed.Spans[1].Parameter);
        Assert.False(parsed.MultipleStatements);
    }

    [Fact]
    public void Named_placeholders_become_numbered_ones()
    {
        var parsed = ParsedSql.Parse("SELECT * FROM t WHERE a = @first AND b = @second AND c = @first AND d @> e AND f = 'x@y'");
        Assert.Equal("SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1 AND d @> e AND f = 'x@y'", Text(parsed));
        Assert.Equal(["first", "second"], parsed.Names);
        Assert.Equal(3, parsed.Spans.Length);
        foreach (var span in parsed.Spans)
        {
            Assert.Equal((byte)'$', parsed.Utf8[span.Start]);
        }
    }

    [Fact]
    public void An_escape_string_keeps_its_backslashed_quote()
    {
        var parsed = ParsedSql.Parse(@"SELECT E'it\'s $1', $1");
        Assert.Single(parsed.Spans);
    }

    [Theory]
    [InlineData("SELECT 1; SELECT 2", true)]
    [InlineData("SELECT 1;", false)]
    [InlineData("SELECT 1; -- done\n ", false)]
    [InlineData("SELECT ';' ; /* ; */", false)]
    [InlineData("DO $$ BEGIN PERFORM 1; END $$", false)]
    public void A_second_statement_is_seen(string sql, bool multiple) => Assert.Equal(multiple, ParsedSql.Parse(sql).MultipleStatements);

    [Theory]
    [InlineData("BEGIN", TransactionVerb.Begin)]
    [InlineData("  start transaction", TransactionVerb.Begin)]
    [InlineData("/* x */ COMMIT", TransactionVerb.End)]
    [InlineData("ROLLBACK", TransactionVerb.End)]
    [InlineData("ROLLBACK TO SAVEPOINT a", TransactionVerb.Savepoint)]
    [InlineData("rollback transaction to a", TransactionVerb.Savepoint)]
    [InlineData("SAVEPOINT a", TransactionVerb.Savepoint)]
    [InlineData("SELECT 1", TransactionVerb.None)]
    [InlineData("ENDPOINT", TransactionVerb.None)]
    public void Transaction_control_is_recognised_by_its_first_words(string sql, object verb) => Assert.Equal((TransactionVerb)verb, ParsedSql.Parse(sql).Verb);

    [Fact]
    public void Mixed_placeholder_forms_are_refused() => Assert.Throws<ArgumentException>(() => ParsedSql.Parse("SELECT $1, @a"));

    [Fact]
    public void Multibyte_text_keeps_byte_offsets_right()
    {
        var parsed = ParsedSql.Parse("SELECT 'héllo wörld', $1, 'ü'");
        var span = parsed.Spans[0];
        Assert.Equal("$1", Encoding.UTF8.GetString(parsed.Utf8, span.Start, span.End - span.Start));
    }
}

public class LiteralTests
{
    private static string Literal(object? value)
    {
        using var writer = new ByteWriter();
        LiteralWriter.WriteObject(writer, value);
        return Encoding.UTF8.GetString(writer.WrittenSpan);
    }

    private static string Typed<T>(T value)
    {
        using var writer = new ByteWriter();
        LiteralWriter.Write(writer, value);
        return Encoding.UTF8.GetString(writer.WrittenSpan);
    }

    [Fact]
    public void Values_are_written_as_the_literals_the_server_reads()
    {
        Assert.Equal("NULL", Literal(null));
        Assert.Equal("NULL", Literal(DBNull.Value));
        Assert.Equal("true", Literal(true));
        Assert.Equal("42", Literal(42));
        Assert.Equal("(-42)", Literal(-42));
        Assert.Equal("(-9223372036854775808)", Literal(long.MinValue));
        Assert.Equal("18446744073709551615", Literal(ulong.MaxValue));
        Assert.Equal("'it''s'", Literal("it's"));
        Assert.Equal("''", Literal(""));
        Assert.Equal("'\\x00ff10'", Literal(new byte[] { 0, 255, 16 }));
        Assert.Equal("12.50", Literal(12.50m));
        Assert.Equal("(-0.5)", Literal(-0.5m));
        Assert.Equal("'6f9619ff-8b86-d011-b42d-00c04fc964ff'", Literal(Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff")));
    }

    [Fact]
    public void A_negative_number_cannot_start_a_comment() => Assert.Equal("(-1)", Typed(-1));

    [Fact]
    public void Floats_keep_their_shortest_digits_and_stay_floats()
    {
        Assert.Equal("0.1", Literal(0.1));
        Assert.Equal("1.0", Literal(1.0));
        Assert.Equal("(-2.5)", Literal(-2.5));
        Assert.Equal("1e21", Literal(1e21));
        Assert.Equal("1e-7", Literal(1e-7));
        Assert.Equal("1.7976931348623157e308", Literal(double.MaxValue));
        Assert.Equal("'NaN'", Literal(double.NaN));
        Assert.Equal("'-Infinity'", Literal(double.NegativeInfinity));
        Assert.Equal("0.1::real", Literal(0.1f));
        Assert.Equal("(-1.5)::real", Literal(-1.5f));
    }

    [Fact]
    public void Times_are_written_in_utc_or_with_no_zone()
    {
        Assert.Equal("'2024-02-29 13:14:15.123456+00'", Literal(new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc).AddTicks(1234560)));
        Assert.Equal("'2024-02-29 13:14:15'", Literal(new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Unspecified)));
        Assert.Equal("'2024-02-29 11:14:15.5+00'", Literal(new DateTimeOffset(2024, 2, 29, 13, 14, 15, 500, TimeSpan.FromHours(2))));
        Assert.Equal("'2024-02-29'", Literal(new DateOnly(2024, 2, 29)));
        Assert.Equal("'01:02:03.000004'", Literal(new TimeOnly(1, 2, 3).Add(TimeSpan.FromTicks(40))));
        Assert.Equal("'2 days 3000000 microseconds'", Literal(new TimeSpan(2, 0, 0, 3)));
        Assert.Equal("'0 microseconds'", Literal(TimeSpan.Zero));
        Assert.Equal("'14 mons -3 days 5 microseconds'", Literal(new RasterizedDbInterval(14, -3, 5)));
    }

    [Fact]
    public void Arrays_are_one_quoted_literal()
    {
        Assert.Equal("'{1,2,-3}'", Literal(new[] { 1, 2, -3 }));
        Assert.Equal("'{\"a b\",\"it''s\",\"q\\\"q\",NULL}'", Literal(new string?[] { "a b", "it's", "q\"q", null }));
        Assert.Equal("'{t,f}'", Literal(new List<bool> { true, false }));
        Assert.Equal("'{{1,2},{3,4}}'", Literal(new[] { new[] { 1, 2 }, new[] { 3, 4 } }));
        Assert.Equal("'{}'", Literal(Array.Empty<long>()));
    }

    [Fact]
    public void A_nul_is_refused() => Assert.Throws<ArgumentException>(() => Literal("a\0b"));

    [Fact]
    public void Identifiers_are_quoted_only_when_they_must_be()
    {
        Assert.Equal("users", LiteralWriter.QuoteIdentifier("users"));
        Assert.Equal("\"order\"", LiteralWriter.QuoteIdentifier("order"));
        Assert.Equal("\"Users\"", LiteralWriter.QuoteIdentifier("Users"));
        Assert.Equal("\"a\"\"b\"", LiteralWriter.QuoteIdentifier("a\"b"));
    }
}

public class ValueReaderTests
{
    private static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public void Native_cells_decode_by_their_tag()
    {
        Assert.Equal(-7, ValueReader.Read<int>(Tag.I32, BitConverter.GetBytes(-7)));
        Assert.Equal(-7L, ValueReader.Read<long>(Tag.I32, BitConverter.GetBytes(-7)));
        Assert.Equal(long.MinValue, ValueReader.Read<long>(Tag.I64, BitConverter.GetBytes(long.MinValue)));
        Assert.True(ValueReader.Read<bool>(Tag.Bool, [1]));
        Assert.Equal(1.5, ValueReader.Read<double>(Tag.F64, BitConverter.GetBytes(1.5)));
        Assert.Equal(1.5f, ValueReader.Read<float>(Tag.F32, BitConverter.GetBytes(1.5f)));
        Assert.Equal("héllo", ValueReader.Read<string>(Tag.String, Utf8("héllo")));
        Assert.Throws<OverflowException>(() => ValueReader.Read<int>(Tag.I64, BitConverter.GetBytes(long.MaxValue)));
        Assert.Throws<InvalidCastException>(() => ValueReader.Read<int>(Tag.Uuid, new byte[16]));
    }

    [Fact]
    public void A_numeric_reads_with_its_scale_and_not_without()
    {
        var cell = new byte[17];
        BitConverter.TryWriteBytes(cell, (Int128)(-1250));
        cell[16] = 2;
        Assert.Equal(-12.50m, ValueReader.Read<decimal>(Tag.Decimal, cell));
        Assert.Equal("-12.50", ValueReader.Read<string>(Tag.Decimal, cell));
        Assert.Throws<InvalidCastException>(() => ValueReader.Read<int>(Tag.Decimal, cell));
        cell[16] = 0;
        Assert.Equal(-1250, ValueReader.Read<int>(Tag.Decimal, cell));
        var error = Assert.Throws<InvalidCastException>(() => ValueReader.Read<decimal>(Tag.Decimal, new byte[16]));
        Assert.Contains("::text", error.Message);
    }

    [Fact]
    public void Dates_and_times_decode()
    {
        var micros = (new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc).Ticks - DateTime.UnixEpoch.Ticks) / 10 + 123456;
        var instant = ValueReader.Read<DateTime>(Tag.DateTime, BitConverter.GetBytes(micros));
        Assert.Equal(DateTimeKind.Utc, instant.Kind);
        Assert.Equal(new DateTime(2024, 2, 29, 13, 14, 15, DateTimeKind.Utc).AddTicks(1234560), instant);
        Assert.Equal(DateTimeKind.Unspecified, ValueReader.Read<DateTime>(Tag.Timestamp, BitConverter.GetBytes(micros)).Kind);

        byte[] date = [0, 0, 0, 1]; // big endian: one day after 2000-01-01
        Assert.Equal(new DateOnly(2000, 1, 2), ValueReader.Read<DateOnly>(Tag.Date, date));

        var uuid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        Assert.Equal(uuid, ValueReader.Read<Guid>(Tag.Uuid, Convert.FromHexString("00112233445566778899aabbccddeeff")));
    }

    [Fact]
    public void Every_type_reads_from_text()
    {
        Assert.Equal(42, ValueReader.Read<int>(Tag.PgText, Utf8("42")));
        Assert.True(ValueReader.Read<bool>(Tag.PgText, Utf8("t")));
        Assert.Equal(12.5m, ValueReader.Read<decimal>(Tag.PgText, Utf8("12.50")));
        Assert.Equal(double.NegativeInfinity, ValueReader.Read<double>(Tag.PgText, Utf8("-Infinity")));
        Assert.Equal(new byte[] { 0, 255 }, ValueReader.Read<byte[]>(Tag.PgText, Utf8("\\x00ff")));
        Assert.Equal(new DateOnly(2024, 2, 29), ValueReader.Read<DateOnly>(Tag.PgText, Utf8("2024-02-29")));
        Assert.Equal(new TimeOnly(1, 2, 3, 400), ValueReader.Read<TimeOnly>(Tag.PgText, Utf8("01:02:03.4")));
        var stamp = ValueReader.Read<DateTime>(Tag.PgText, Utf8("2024-02-29 13:14:15.5+02"));
        Assert.Equal(new DateTime(2024, 2, 29, 11, 14, 15, 500, DateTimeKind.Utc), stamp);
        Assert.Equal(DateTimeKind.Unspecified, ValueReader.Read<DateTime>(Tag.PgText, Utf8("2024-02-29 13:14:15")).Kind);
        Assert.Equal(new RasterizedDbInterval(14, 3, 14_706_500_000), ValueReader.Read<RasterizedDbInterval>(Tag.PgText, Utf8("1 year 2 mons 3 days 04:05:06.5")));
        Assert.Equal(new RasterizedDbInterval(14, -3, 5), ValueReader.Read<RasterizedDbInterval>(Tag.PgText, Utf8("14 mons -3 days 5 microseconds")));
        Assert.Equal(7, ValueReader.Read<int?>(Tag.PgText, Utf8("7")));
        Assert.Equal(DayOfWeek.Tuesday, ValueReader.Read<DayOfWeek>(Tag.PgText, Utf8("2")));
    }

    [Fact]
    public void Arrays_read_from_a_literal()
    {
        Assert.Equal([1, 2, 3], ValueReader.Read<int[]>(Tag.PgText, Utf8("{1,2,3}")));
        Assert.Equal([1, null, 3], ValueReader.Read<int?[]>(Tag.PgText, Utf8("{1,NULL,3}")));
        Assert.Equal(["a b", "q\"q", "NULL", null], ValueReader.Read<string?[]>(Tag.PgText, Utf8("{\"a b\",\"q\\\"q\",\"NULL\",NULL}")));
        Assert.Equal(new List<long> { 5, 6 }, ValueReader.Read<List<long>>(Tag.PgText, Utf8("{5,6}")));
        Assert.Empty(ValueReader.Read<int[]>(Tag.PgText, Utf8("{}")));
        Assert.Throws<InvalidCastException>(() => ValueReader.Read<int[]>(Tag.PgText, Utf8("{1,NULL}")));
    }
}

public class ConnectionStringTests
{
    [Fact]
    public void Aliases_settle_on_one_keyword()
    {
        var builder = new RasterizedDbConnectionStringBuilder("Server=db;User Id=app;PWD=x;Initial Catalog=shop;Max Pool Size=7");
        Assert.Equal("db", builder.Host);
        Assert.Equal("app", builder.Username);
        Assert.Equal("shop", builder.Database);
        Assert.Equal(7, builder.MaxPoolSize);
        Assert.Equal(RasterizedDbConnectionStringBuilder.DefaultPort, builder.Port);
        Assert.Throws<ArgumentException>(() => new RasterizedDbConnectionStringBuilder("Hots=db"));
    }

    [Fact]
    public void A_login_is_inferred_and_checked()
    {
        Assert.Equal(RasterizedDbLogin.Password, new ConnectionSettings("Host=db;Username=a;Password=b").Login);
        Assert.Equal(RasterizedDbLogin.Trust, new ConnectionSettings("Socket=/tmp/s.sock").Login);
        var role = new ConnectionSettings("Socket=/tmp/s.sock;Role=app;Database=shop");
        Assert.Equal("\u0001ROLE;app\u001fshop", Encoding.UTF8.GetString(role.SelectionRequest!));
        Assert.Equal("\u0001DATABASE;shop", Encoding.UTF8.GetString(new ConnectionSettings("Socket=/tmp/s.sock;Database=shop").SelectionRequest!));
        Assert.Throws<ArgumentException>(() => new ConnectionSettings("Host=db;Login=Password;Username=a"));
        Assert.Throws<ArgumentException>(() => new ConnectionSettings("Database=x"));
        Assert.DoesNotContain("secret", new ConnectionSettings("Host=db;Username=a;Password=secret").PublicConnectionString);
        Assert.Equal(("h", 7000), (new ConnectionSettings("Host=h:7000").Host!, new ConnectionSettings("Host=h:7000").Port));
        Assert.Equal("/run/s.sock", new ConnectionSettings("Host=unix:/run/s.sock").SocketPath);
    }

    [Fact]
    public void A_server_error_gives_up_its_sqlstate()
    {
        var error = RasterizedDbException.FromServer("duplicate key value violates unique constraint \"t_pkey\" (SQLSTATE 23505)");
        Assert.Equal("23505", error.SqlState);
        Assert.Equal("duplicate key value violates unique constraint \"t_pkey\"", error.Message);
        Assert.True(error.IsUniqueViolation);
        Assert.Null(RasterizedDbException.FromServer("no code here").SqlState);
        Assert.True(RasterizedDbException.FromServer("could not serialize (SQLSTATE 40001)").IsTransient);
    }
}
