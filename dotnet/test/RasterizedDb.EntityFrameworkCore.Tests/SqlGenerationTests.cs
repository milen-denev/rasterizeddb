using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;

namespace RasterizedDb.EntityFrameworkCore.Tests;

/// <summary>What the provider generates, checked without a server.</summary>
public class SqlGenerationTests(ITestOutputHelper output)
{
    private static BloggingContext Context() =>
        new(new DbContextOptionsBuilder<BloggingContext>().UseRasterizedDb("Socket=/nonexistent/rdb.sock").Options);

    private string Sql<T>(IQueryable<T> query)
    {
        var sql = query.ToQueryString().Replace("\r", "");
        output.WriteLine(sql);
        output.WriteLine("");
        if (Environment.GetEnvironmentVariable("RDB_SQL_DUMP") is { Length: > 0 } dump)
        {
            File.AppendAllText(dump, sql + "\n\n");
        }

        // Table aliases are quoted like every identifier; the assertions read easier without that.
        return System.Text.RegularExpressions.Regex.Replace(sql, "\"([a-z][0-9]*)\"", "$1");
    }

    [Fact]
    public void Paging_is_limit_and_offset()
    {
        using var context = Context();
        var sql = Sql(context.Blogs.Where(b => b.IsPublic).OrderBy(b => b.Id).Skip(5).Take(10));
        Assert.Contains("FROM \"Blogs\" AS b", sql);
        Assert.Contains("WHERE b.\"IsPublic\"", sql);
        Assert.Contains("LIMIT @", sql);
        Assert.Contains("OFFSET @", sql);
        Assert.DoesNotContain("TOP", sql);
    }

    [Fact]
    public void Strings_translate_to_postgresql_functions()
    {
        using var context = Context();
        var needle = "x";
        Assert.Contains("LIKE 'Blog%'", Sql(context.Blogs.Where(b => b.Title.StartsWith("Blog"))));
        Assert.Contains("LIKE '%50\\%%' ESCAPE '\\'", Sql(context.Blogs.Where(b => b.Title.Contains("50%"))));
        Assert.Contains("strpos(b.\"Title\", @", Sql(context.Blogs.Where(b => b.Title.Contains(needle))));
        Assert.Contains("upper(b.\"Title\")", Sql(context.Blogs.Where(b => b.Title.ToUpper() == "A")));
        Assert.Contains(" || ", Sql(context.Blogs.Select(b => b.Title + "!" + b.Subtitle)));
        Assert.Contains("length(b.\"Title\")", Sql(context.Blogs.Where(b => b.Title.Length > 3)));
        Assert.Contains("lower(b.\"Title\") LIKE lower(", Sql(context.Blogs.Where(b => EF.Functions.ILike(b.Title, "a%"))));
    }

    [Fact]
    public void Constants_are_postgresql_literals()
    {
        using var context = Context();
        Assert.Contains("TIMESTAMPTZ '2024-01-02 03:04:05+00'", Sql(context.Blogs.Where(b => b.CreatedAt > new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc))));
        Assert.Contains("DATE '2024-01-02'", Sql(context.Posts.Where(p => p.PublishedOn == new DateOnly(2024, 1, 2))));
        Assert.Contains("NOT (b.\"IsPublic\")", Sql(context.Blogs.Where(b => !b.IsPublic)));
        Assert.Contains("date_part('year', b.\"CreatedAt\")", Sql(context.Blogs.Where(b => b.CreatedAt.Year == 2024)));
    }

    [Fact]
    public void Joins_groups_and_subqueries_generate()
    {
        using var context = Context();
        Assert.Contains("LEFT JOIN \"Posts\" AS p", Sql(context.Blogs.Include(b => b.Posts)));
        Assert.Contains("GROUP BY p.\"BlogId\"", Sql(context.Posts.GroupBy(p => p.BlogId).Select(g => new { g.Key, Count = g.Count() })));
        Assert.Contains("SELECT count(*)", Sql(context.Blogs.Select(b => new { b.Title, Posts = b.Posts.Count })), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EXISTS (", Sql(context.Blogs.Where(b => b.Posts.Any(p => p.Score > 1))));
        var titles = new[] { "a", "b" };
        Assert.Contains(" IN (", Sql(context.Blogs.Where(b => titles.Contains(b.Title))));
        Assert.Contains("ROW_NUMBER() OVER", Sql(context.Blogs.SelectMany(b => b.Posts.OrderBy(p => p.Id).Take(2), (b, p) => new { b.Title, p.Content })));
    }

    [Fact]
    public void A_create_script_is_postgresql_ddl()
    {
        using var context = Context();
        var script = context.Database.GenerateCreateScript();
        output.WriteLine(script);
        Assert.Contains("\"Id\" serial NOT NULL", script);
        Assert.Contains("\"Id\" bigserial NOT NULL", script);
        Assert.Contains("\"Title\" character varying(200) NOT NULL", script);
        Assert.Contains("\"Rating\" numeric(10,2) NOT NULL", script);
        Assert.Contains("\"CreatedAt\" timestamp with time zone NOT NULL", script);
        Assert.Contains("\"Token\" uuid NOT NULL", script);
        Assert.Contains("\"Attachment\" bytea NULL", script);
        Assert.Contains("\"PublishedOn\" date NULL", script);
        Assert.Contains("\"Score\" double precision NOT NULL", script);
        Assert.Contains("\"IsPublic\" boolean NOT NULL", script);
        Assert.Contains("REFERENCES \"Blogs\" (\"Id\")", script);
    }
}
