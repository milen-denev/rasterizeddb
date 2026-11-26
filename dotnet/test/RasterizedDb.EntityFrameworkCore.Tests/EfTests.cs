using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using RasterizedDb.Data;
using RasterizedDb.Data.Tests;

namespace RasterizedDb.EntityFrameworkCore.Tests;

public class Blog
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public string? Subtitle { get; set; }
    public decimal Rating { get; set; }
    public bool IsPublic { get; set; }
    public DateTime CreatedAt { get; set; }
    public int Version { get; set; }
    public string[] Tags { get; set; } = [];
    public List<int> Scores { get; set; } = [];
    public BlogDetails Details { get; set; } = new();
    public List<Post> Posts { get; set; } = [];
}

public class BlogDetails
{
    public string Author { get; set; } = "";
    public int Stars { get; set; }
    public bool Verified { get; set; }
    public Address Address { get; set; } = new();
    public List<Link> Links { get; set; } = [];
}

public class Address
{
    public string City { get; set; } = "";
    public string? Zip { get; set; }
}

public class Link
{
    public string Url { get; set; } = "";
    public int Clicks { get; set; }
}

public class Post
{
    public long Id { get; set; }
    public int BlogId { get; set; }
    public Blog Blog { get; set; } = null!;
    public string Content { get; set; } = "";
    public Guid Token { get; set; }
    public DateOnly? PublishedOn { get; set; }
    public double Score { get; set; }
    public byte[]? Attachment { get; set; }
    public PostKind Kind { get; set; }
}

public enum PostKind
{
    Note,
    Article,
}

public class BloggingContext(DbContextOptions<BloggingContext> options) : DbContext(options)
{
    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Blog>(b =>
        {
            b.Property(x => x.Title).HasMaxLength(200);
            b.Property(x => x.Rating).HasPrecision(10, 2);
            b.Property(x => x.Version).IsConcurrencyToken();
            b.OwnsOne(x => x.Details, d =>
            {
                d.ToJson();
                d.OwnsOne(x => x.Address);
                d.OwnsMany(x => x.Links);
            });
        });
        modelBuilder.Entity<Post>(p => p.HasOne(x => x.Blog).WithMany(x => x.Posts).HasForeignKey(x => x.BlogId));
    }
}

/// <summary>The provider against a real server: created once, then read and written by every test.</summary>
public sealed class BloggingFixture : IDisposable
{
    private readonly ServerFixture _servers = new();
    private readonly object _gate = new();
    private bool _created;

    public List<string> Sql { get; } = [];

    public BloggingContext? Context()
    {
        if (_servers.Trust() is not { } connectionString)
        {
            return null;
        }

        var options = new DbContextOptionsBuilder<BloggingContext>()
            .UseRasterizedDb(connectionString)
            .LogTo(
                message =>
                {
                    lock (Sql)
                    {
                        Sql.Add(message);
                    }
                },
                [Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.CommandExecuting])
            .EnableSensitiveDataLogging()
            .Options;
        var context = new BloggingContext(options);
        lock (_gate)
        {
            if (!_created)
            {
                Assert.True(context.Database.EnsureCreated());
                Seed(context);
                _created = true;
            }
        }

        return context;
    }

    private static void Seed(BloggingContext context)
    {
        var day = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        for (var i = 1; i <= 5; i++)
        {
            var blog = new Blog
            {
                Title = $"Blog {i}",
                Subtitle = i % 2 == 0 ? null : $"about {i}",
                Rating = i * 1.25m,
                IsPublic = i != 3,
                CreatedAt = day.AddDays(i),
                Tags = i % 2 == 0 ? ["even", $"n{i}"] : ["odd", $"n{i}", "it's"],
                Scores = Enumerable.Range(1, i).ToList(),
                Details = new BlogDetails
                {
                    Author = $"author {i}",
                    Stars = i * 10,
                    Verified = i > 3,
                    Address = new Address { City = i < 3 ? "Sofia" : "Plovdiv", Zip = i == 1 ? null : $"{1000 + i}" },
                    Links = Enumerable.Range(1, i % 3).Select(n => new Link { Url = $"https://example.com/{i}/{n}", Clicks = n }).ToList(),
                },
            };
            for (var p = 1; p <= i; p++)
            {
                blog.Posts.Add(new Post
                {
                    Content = $"post {p} of blog {i}",
                    Token = Guid.NewGuid(),
                    PublishedOn = p % 2 == 0 ? null : new DateOnly(2024, i, p),
                    Score = p / 2.0,
                    Attachment = p == 1 ? [1, 2, 3] : null,
                    Kind = p % 2 == 0 ? PostKind.Article : PostKind.Note,
                });
            }

            context.Blogs.Add(blog);
        }

        Assert.Equal(20, context.SaveChanges());
        context.ChangeTracker.Clear();
    }

    public void Dispose() => _servers.Dispose();
}

public class EfTests(BloggingFixture fixture) : IClassFixture<BloggingFixture>
{
    [Fact]
    public async Task Generated_keys_come_back_and_rows_round_trip()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var blogs = await context.Blogs.AsNoTracking().OrderBy(b => b.Id).Take(5).ToListAsync();
        Assert.Equal(5, blogs.Count);
        Assert.True(blogs[0].Id > 0);
        Assert.True(blogs.Zip(blogs.Skip(1)).All(pair => pair.First.Id < pair.Second.Id));
        Assert.Equal("Blog 1", blogs[0].Title);
        Assert.Equal(1.25m, blogs[0].Rating);
        Assert.Null(blogs[1].Subtitle);
        Assert.Equal(new DateTime(2024, 1, 2, 12, 0, 0, DateTimeKind.Utc), blogs[0].CreatedAt);

        var post = await context.Posts.AsNoTracking().OrderBy(p => p.Id).FirstAsync();
        Assert.Equal(new byte[] { 1, 2, 3 }, post.Attachment);
        Assert.NotEqual(Guid.Empty, post.Token);
        Assert.Equal(new DateOnly(2024, 1, 1), post.PublishedOn);
        Assert.Equal(PostKind.Note, post.Kind);
    }

    [Fact]
    public async Task Filters_ordering_and_paging_translate()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var minimum = 2.0m;
        var titles = await context.Blogs
            .Where(b => b.IsPublic && b.Rating > minimum && b.Title.StartsWith("Blog"))
            .OrderByDescending(b => b.Rating)
            .Skip(1)
            .Take(2)
            .Select(b => b.Title)
            .ToListAsync();
        Assert.Equal(["Blog 4", "Blog 2"], titles);

        Assert.Equal(4, await context.Blogs.CountAsync(b => b.IsPublic));
        Assert.True(await context.Blogs.AnyAsync(b => b.Subtitle == null));
        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Subtitle == null));
        var ids = new[] { "Blog 1", "Blog 5", "nope" };
        Assert.Equal(2, await context.Blogs.CountAsync(b => ids.Contains(b.Title)));
        Assert.Equal(18.75m, await context.Blogs.SumAsync(b => b.Rating));
        Assert.Equal(6.25m, await context.Blogs.MaxAsync(b => b.Rating));
        var needle = "of blog 5";
        Assert.Equal(5, await context.Posts.CountAsync(p => p.Content.Contains(needle)));
        Assert.Equal(5, await context.Posts.CountAsync(p => p.Content.EndsWith("blog 5")));
        Assert.Equal(1, await context.Blogs.CountAsync(b => b.Title.ToUpper() == "BLOG 3"));
        Assert.Equal(5, await context.Blogs.CountAsync(b => EF.Functions.ILike(b.Title, "blog%")));
        Assert.Equal(7, await context.Posts.CountAsync(p => p.Kind == PostKind.Article));
    }

    [Fact]
    public async Task Joins_includes_and_projections_load()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var blog = await context.Blogs.AsNoTracking().Include(b => b.Posts).SingleAsync(b => b.Title == "Blog 3");
        Assert.Equal(3, blog.Posts.Count);
        Assert.All(blog.Posts, p => Assert.Equal(blog.Id, p.BlogId));

        var counts = await context.Blogs
            .OrderBy(b => b.Id)
            .Select(b => new { b.Title, Posts = b.Posts.Count })
            .ToListAsync();
        Assert.Equal([1, 2, 3, 4, 5], counts.Select(c => c.Posts));

        var joined = await context.Posts
            .Where(p => p.Blog.IsPublic)
            .Select(p => new { p.Content, p.Blog.Title })
            .OrderBy(x => x.Content)
            .FirstAsync();
        Assert.Equal("Blog 1", joined.Title);

        var grouped = await context.Posts
            .GroupBy(p => p.BlogId)
            .Select(g => new { BlogId = g.Key, Count = g.Count(), Best = g.Max(p => p.Score) })
            .OrderByDescending(g => g.Count)
            .FirstAsync();
        Assert.Equal(5, grouped.Count);
        Assert.Equal(2.5, grouped.Best);
    }

    [Fact]
    public async Task Updates_deletes_and_concurrency_conflicts()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var blog = new Blog { Title = "to change", Rating = 1, CreatedAt = DateTime.UtcNow, IsPublic = true };
        blog.Posts.Add(new Post { Content = "first", Token = Guid.NewGuid() });
        context.Blogs.Add(blog);
        Assert.Equal(2, await context.SaveChangesAsync());
        Assert.True(blog.Id > 0);
        Assert.True(blog.Posts[0].Id > 0);

        blog.Title = "changed";
        blog.Version = 1;
        Assert.Equal(1, await context.SaveChangesAsync());

        await using (var other = fixture.Context()!)
        {
            var same = await other.Blogs.SingleAsync(b => b.Id == blog.Id);
            Assert.Equal("changed", same.Title);
            same.Version = 2;
            await other.SaveChangesAsync();
        }

        blog.Title = "stale";
        blog.Version = 3;
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => context.SaveChangesAsync());

        context.ChangeTracker.Clear();
        var fresh = await context.Blogs.Include(b => b.Posts).SingleAsync(b => b.Id == blog.Id);
        context.RemoveRange(fresh.Posts);
        context.Remove(fresh);
        Assert.Equal(2, await context.SaveChangesAsync());
        Assert.False(await context.Blogs.AnyAsync(b => b.Id == blog.Id));
    }

    [Fact]
    public async Task A_transaction_rolls_back_what_it_wrote()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            context.Blogs.Add(new Blog { Title = "never committed", CreatedAt = DateTime.UtcNow });
            await context.SaveChangesAsync();
            Assert.True(await context.Blogs.AnyAsync(b => b.Title == "never committed"));
            await transaction.RollbackAsync();
        }

        context.ChangeTracker.Clear();
        Assert.False(await context.Blogs.AnyAsync(b => b.Title == "never committed"));
    }

    [Fact]
    public async Task Raw_sql_reads_by_column_name_and_writes()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var title = "Blog 2";
        var blogs = await context.Blogs
            .FromSql($"""SELECT "Version", "Title", "Id", "Subtitle", "Rating", "IsPublic", "CreatedAt", "Tags", "Scores", "Details" FROM "Blogs" WHERE "Title" = {title}""")
            .AsNoTracking()
            .ToListAsync();
        Assert.Equal("Blog 2", Assert.Single(blogs).Title);
        Assert.Equal(2.5m, blogs[0].Rating);

        var titles = await context.Database.SqlQuery<string>($"""SELECT "Title" AS "Value" FROM "Blogs" WHERE "Rating" > {5m}""").ToListAsync();
        Assert.Equal(["Blog 5"], titles);

        var marker = Guid.NewGuid().ToString("N");
        var none = "{}";
        Assert.Equal(1, await context.Database.ExecuteSqlAsync($"""INSERT INTO "Blogs" ("Title", "Rating", "IsPublic", "CreatedAt", "Version", "Tags", "Scores", "Details") VALUES ({marker}, 0, FALSE, now(), 0, {none}, {none}, {none})"""));
        Assert.Equal(1, await context.Blogs.Where(b => b.Title == marker).ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Bulk_update_and_delete_run_as_one_statement()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var marker = "bulk " + Guid.NewGuid().ToString("N");
        context.Blogs.AddRange(Enumerable.Range(0, 3).Select(i => new Blog { Title = marker, Rating = i, CreatedAt = DateTime.UtcNow }));
        await context.SaveChangesAsync();
        Assert.Equal(3, await context.Blogs.Where(b => b.Title == marker).ExecuteUpdateAsync(s => s.SetProperty(b => b.IsPublic, true)));
        Assert.Equal(3, await context.Blogs.CountAsync(b => b.Title == marker && b.IsPublic));
        Assert.Equal(3, await context.Blogs.Where(b => b.Title == marker).ExecuteDeleteAsync());
    }

    [Fact]
    public async Task Array_columns_round_trip_and_filter()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var first = await context.Blogs.AsNoTracking().SingleAsync(b => b.Title == "Blog 1");
        Assert.Equal(["odd", "n1", "it's"], first.Tags);
        Assert.Equal([1], first.Scores);

        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Tags.Contains("even") && b.Title.StartsWith("Blog ")));
        var tag = "n3";
        Assert.Equal("Blog 3", (await context.Blogs.SingleAsync(b => b.Tags.Contains(tag))).Title);
        Assert.Equal(3, await context.Blogs.CountAsync(b => b.Tags.Length == 3 && b.Title.StartsWith("Blog ")));
        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Scores.Count >= 4 && b.Title.StartsWith("Blog ")));
        Assert.Equal(3, await context.Blogs.CountAsync(b => b.Scores.Contains(3) && b.Title.StartsWith("Blog ")));

        var blog = new Blog { Title = "arrays", CreatedAt = DateTime.UtcNow, Tags = ["a"], Scores = [5] };
        context.Blogs.Add(blog);
        await context.SaveChangesAsync();
        blog.Tags = ["a", "b \"quoted\"", ""];
        blog.Scores.Add(6);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
        var again = await context.Blogs.SingleAsync(b => b.Id == blog.Id);
        Assert.Equal(["a", "b \"quoted\"", ""], again.Tags);
        Assert.Equal([5, 6], again.Scores);
        context.Remove(again);
        await context.SaveChangesAsync();
    }

    [Fact]
    public async Task Json_columns_load_filter_and_update_in_place()
    {
        await using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var second = await context.Blogs.AsNoTracking().SingleAsync(b => b.Title == "Blog 2");
        Assert.Equal("author 2", second.Details.Author);
        Assert.Equal(20, second.Details.Stars);
        Assert.Equal("Sofia", second.Details.Address.City);
        Assert.Equal("1002", second.Details.Address.Zip);
        Assert.Equal(2, second.Details.Links.Count);
        Assert.Equal("https://example.com/2/2", second.Details.Links[1].Url);

        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Details.Stars > 30 && b.Title.StartsWith("Blog ")));
        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Details.Address.City == "Sofia" && b.Title.StartsWith("Blog ")));
        Assert.Equal(2, await context.Blogs.CountAsync(b => b.Details.Verified && b.Title.StartsWith("Blog ")));
        var authors = await context.Blogs.Where(b => b.Title.StartsWith("Blog ")).OrderBy(b => b.Details.Stars).Select(b => b.Details.Author).ToListAsync();
        Assert.Equal("author 1", authors[0]);
        Assert.Equal(5, authors.Count);

        var blog = new Blog
        {
            Title = "json",
            CreatedAt = DateTime.UtcNow,
            Details = new BlogDetails { Author = "o'neil", Stars = 1, Address = new Address { City = "Varna" }, Links = [new Link { Url = "u", Clicks = 1 }] },
        };
        context.Blogs.Add(blog);
        await context.SaveChangesAsync();

        // One scalar inside the document, then a nested one, then the collection.
        blog.Details.Stars = 2;
        await context.SaveChangesAsync();
        blog.Details.Address.City = "Burgas \"quoted\"";
        await context.SaveChangesAsync();
        blog.Details.Links.Add(new Link { Url = "v", Clicks = 7 });
        blog.Details.Author = "changed";
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();
        var again = await context.Blogs.SingleAsync(b => b.Id == blog.Id);
        Assert.Equal(2, again.Details.Stars);
        Assert.Equal("Burgas \"quoted\"", again.Details.Address.City);
        Assert.Equal("changed", again.Details.Author);
        Assert.Equal([1, 7], again.Details.Links.Select(l => l.Clicks));
        context.Remove(again);
        await context.SaveChangesAsync();
    }

    [Fact]
    public void An_existing_database_scaffolds()
    {
        using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var factory = new RasterizedDb.EntityFrameworkCore.Scaffolding.RasterizedDbDatabaseModelFactory();
        var model = factory.Create(context.Database.GetConnectionString()!, new Microsoft.EntityFrameworkCore.Scaffolding.DatabaseModelFactoryOptions());
        var blogs = model.Tables.Single(t => t.Name == "Blogs");
        var posts = model.Tables.Single(t => t.Name == "Posts");
        Assert.Equal(["Id"], blogs.PrimaryKey!.Columns.Select(c => c.Name));
        Assert.Equal(Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd, blogs.Columns.Single(c => c.Name == "Id").ValueGenerated);
        Assert.Equal("character varying(200)", blogs.Columns.Single(c => c.Name == "Title").StoreType);
        Assert.Equal("numeric(10,2)", blogs.Columns.Single(c => c.Name == "Rating").StoreType);
        Assert.True(blogs.Columns.Single(c => c.Name == "Subtitle").IsNullable);
        Assert.False(blogs.Columns.Single(c => c.Name == "Title").IsNullable);
        Assert.Equal("jsonb", blogs.Columns.Single(c => c.Name == "Details").StoreType);
        Assert.Equal("text[]", blogs.Columns.Single(c => c.Name == "Tags").StoreType);
        Assert.Equal("uuid", posts.Columns.Single(c => c.Name == "Token").StoreType);
        var foreignKey = Assert.Single(posts.ForeignKeys);
        Assert.Equal("Blogs", foreignKey.PrincipalTable.Name);
        Assert.Equal(["BlogId"], foreignKey.Columns.Select(c => c.Name));
        Assert.Equal(["Id"], foreignKey.PrincipalColumns.Select(c => c.Name));
        Assert.Equal(Microsoft.EntityFrameworkCore.Migrations.ReferentialAction.Cascade, foreignKey.OnDelete);
        Assert.Contains(posts.Indexes, i => i.Columns.Select(c => c.Name).SequenceEqual(["BlogId"]));

        var only = factory.Create(context.Database.GetConnectionString()!, new Microsoft.EntityFrameworkCore.Scaffolding.DatabaseModelFactoryOptions(["Posts"]));
        Assert.Equal(["Posts"], only.Tables.Select(t => t.Name));
    }

    [Fact]
    public void Migrations_sql_is_postgresql_shaped()
    {
        using var context = fixture.Context();
        if (context is null)
        {
            return;
        }

        var script = context.Database.GenerateCreateScript();
        Assert.Contains("\"Id\" serial NOT NULL", script);
        Assert.Contains("\"Id\" bigserial NOT NULL", script);
        Assert.Contains("\"Title\" character varying(200) NOT NULL", script);
        Assert.Contains("\"Rating\" numeric(10,2) NOT NULL", script);
        Assert.Contains("\"CreatedAt\" timestamp with time zone NOT NULL", script);
        Assert.Contains("\"Token\" uuid NOT NULL", script);
        Assert.Contains("\"Attachment\" bytea NULL", script);
        Assert.Contains("REFERENCES \"Blogs\" (\"Id\")", script);

        var history = context.GetService<IHistoryRepository>();
        Assert.False(history.Exists());
        Assert.Contains("CREATE TABLE IF NOT EXISTS \"__EFMigrationsHistory\"", history.GetCreateIfNotExistsScript());
    }
}
