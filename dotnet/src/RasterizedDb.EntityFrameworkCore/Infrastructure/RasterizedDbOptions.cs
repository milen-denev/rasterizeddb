using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using RasterizedDb.Data;
using RasterizedDb.EntityFrameworkCore.Infrastructure;
using RasterizedDb.EntityFrameworkCore.Storage;

namespace RasterizedDb.EntityFrameworkCore.Infrastructure
{
    /// <summary>What <c>UseRasterizedDb</c> configured.</summary>
    public class RasterizedDbOptionsExtension : RelationalOptionsExtension
    {
        private DbContextOptionsExtensionInfo? _info;

        public RasterizedDbOptionsExtension()
        {
        }

        protected RasterizedDbOptionsExtension(RasterizedDbOptionsExtension copyFrom)
            : base(copyFrom)
        {
            DataSource = copyFrom.DataSource;
        }

        /// <summary>The data source connections come from, when one was given in place of a connection string.</summary>
        public virtual RasterizedDbDataSource? DataSource { get; private set; }

        public virtual RasterizedDbOptionsExtension WithDataSource(RasterizedDbDataSource? dataSource)
        {
            var clone = (RasterizedDbOptionsExtension)Clone();
            clone.DataSource = dataSource;
            return clone;
        }

        protected override RelationalOptionsExtension Clone() => new RasterizedDbOptionsExtension(this);

        public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

        public override void ApplyServices(IServiceCollection services) => services.AddEntityFrameworkRasterizedDb();

        private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : RelationalExtensionInfo(extension)
        {
            private new RasterizedDbOptionsExtension Extension => (RasterizedDbOptionsExtension)base.Extension;

            public override bool IsDatabaseProvider => true;

            public override string LogFragment => "using RasterizedDB ";

            public override int GetServiceProviderHashCode() =>
                HashCode.Combine(base.GetServiceProviderHashCode(), Extension.DataSource);

            public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) =>
                other is ExtensionInfo info && base.ShouldUseSameServiceProvider(other) && ReferenceEquals(Extension.DataSource, info.Extension.DataSource);

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) =>
                debugInfo["RasterizedDb:DataSource"] = (Extension.DataSource?.GetHashCode() ?? 0).ToString(CultureInfo.InvariantCulture);
        }
    }

    /// <summary>The provider's own options, inside <c>UseRasterizedDb(…, o => …)</c>.</summary>
    public class RasterizedDbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : RelationalDbContextOptionsBuilder<RasterizedDbDbContextOptionsBuilder, RasterizedDbOptionsExtension>(optionsBuilder)
    {
        /// <summary>
        /// Runs a unit of work again when it fails with a serialization failure (40001) or a deadlock
        /// (40P01), which is all that is safe to repeat: a statement whose connection failed is never
        /// sent twice, because whether it ran is unknown.
        /// </summary>
        public virtual RasterizedDbDbContextOptionsBuilder EnableRetryOnFailure(int maxRetryCount = 2, TimeSpan? maxRetryDelay = null) =>
            ExecutionStrategy(dependencies => new RasterizedDbRetryingExecutionStrategy(dependencies, maxRetryCount, maxRetryDelay ?? TimeSpan.FromSeconds(5)));
    }
}

namespace Microsoft.EntityFrameworkCore
{
    /// <summary><c>UseRasterizedDb</c>: RasterizedDB over its native protocol as a context's database.</summary>
    public static class RasterizedDbDbContextOptionsExtensions
    {
        /// <summary>
        /// Uses the RasterizedDB server a connection string names
        /// (<c>Host=…;Port=61170;Username=…;Password=…;Database=…</c>, or <c>Socket=/run/rdb/rdb.sock</c>;
        /// see <see cref="RasterizedDbConnectionStringBuilder"/>).
        /// </summary>
        public static DbContextOptionsBuilder UseRasterizedDb(
            this DbContextOptionsBuilder optionsBuilder,
            string? connectionString,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null)
        {
            var extension = (RasterizedDbOptionsExtension)GetOrCreate(optionsBuilder).WithConnectionString(connectionString);
            return Apply(optionsBuilder, extension, optionsAction);
        }

        /// <summary>Uses a connection the caller made (and, unless <paramref name="contextOwnsConnection"/>, disposes of).</summary>
        public static DbContextOptionsBuilder UseRasterizedDb(
            this DbContextOptionsBuilder optionsBuilder,
            DbConnection connection,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null,
            bool contextOwnsConnection = false)
        {
            var extension = (RasterizedDbOptionsExtension)GetOrCreate(optionsBuilder).WithConnection(connection, contextOwnsConnection);
            return Apply(optionsBuilder, extension, optionsAction);
        }

        /// <summary>Uses a data source: one pool, made once, for every context.</summary>
        public static DbContextOptionsBuilder UseRasterizedDb(
            this DbContextOptionsBuilder optionsBuilder,
            RasterizedDbDataSource dataSource,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null)
        {
            var extension = GetOrCreate(optionsBuilder).WithDataSource(dataSource);
            return Apply(optionsBuilder, extension, optionsAction);
        }

        public static DbContextOptionsBuilder<TContext> UseRasterizedDb<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            string? connectionString,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null)
            where TContext : DbContext =>
            (DbContextOptionsBuilder<TContext>)UseRasterizedDb((DbContextOptionsBuilder)optionsBuilder, connectionString, optionsAction);

        public static DbContextOptionsBuilder<TContext> UseRasterizedDb<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            DbConnection connection,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null,
            bool contextOwnsConnection = false)
            where TContext : DbContext =>
            (DbContextOptionsBuilder<TContext>)UseRasterizedDb((DbContextOptionsBuilder)optionsBuilder, connection, optionsAction, contextOwnsConnection);

        public static DbContextOptionsBuilder<TContext> UseRasterizedDb<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            RasterizedDbDataSource dataSource,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction = null)
            where TContext : DbContext =>
            (DbContextOptionsBuilder<TContext>)UseRasterizedDb((DbContextOptionsBuilder)optionsBuilder, dataSource, optionsAction);

        private static RasterizedDbOptionsExtension GetOrCreate(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.Options.FindExtension<RasterizedDbOptionsExtension>() ?? new RasterizedDbOptionsExtension();

        private static DbContextOptionsBuilder Apply(
            DbContextOptionsBuilder optionsBuilder,
            RasterizedDbOptionsExtension extension,
            Action<RasterizedDbDbContextOptionsBuilder>? optionsAction)
        {
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
            optionsAction?.Invoke(new RasterizedDbDbContextOptionsBuilder(optionsBuilder));
            return optionsBuilder;
        }
    }
}
