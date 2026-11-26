using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using RasterizedDb.EntityFrameworkCore.Infrastructure;
using RasterizedDb.EntityFrameworkCore.Migrations;
using RasterizedDb.EntityFrameworkCore.Query;
using RasterizedDb.EntityFrameworkCore.Storage;
using RasterizedDb.EntityFrameworkCore.Update;

namespace Microsoft.Extensions.DependencyInjection;

public static class RasterizedDbServiceCollectionExtensions
{
    /// <summary>
    /// Registers the provider's services. <c>UseRasterizedDb</c> calls this; call it yourself only when
    /// building an internal service provider by hand.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkRasterizedDb(this IServiceCollection serviceCollection)
    {
        new EntityFrameworkRelationalServicesBuilder(serviceCollection)
            .TryAdd<LoggingDefinitions, RasterizedDbLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<RasterizedDbOptionsExtension>>()
            .TryAdd<IProviderConventionSetBuilder, RasterizedDbConventionSetBuilder>()
            .TryAdd<IRelationalTypeMappingSource, RasterizedDbTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, RelationalSqlGenerationHelper>()
            .TryAdd<IRelationalAnnotationProvider, RasterizedDbAnnotationProvider>()
            .TryAdd<IRelationalConnection, RasterizedDbRelationalConnection>()
            .TryAdd<IRelationalCommandBuilderFactory, RasterizedDbCommandBuilderFactory>()
            .TryAdd<IRawSqlCommandBuilder, RasterizedDbRawSqlCommandBuilder>()
            .TryAdd<IRelationalDatabaseCreator, RasterizedDbDatabaseCreator>()
            .TryAdd<IHistoryRepository, RasterizedDbHistoryRepository>()
            .TryAdd<IMigrationsSqlGenerator, RasterizedDbMigrationsSqlGenerator>()
            .TryAdd<IUpdateSqlGenerator, RasterizedDbUpdateSqlGenerator>()
            .TryAdd<IModificationCommandBatchFactory, RasterizedDbModificationCommandBatchFactory>()
            .TryAdd<IQuerySqlGeneratorFactory, RasterizedDbQuerySqlGeneratorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, RasterizedDbMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, RasterizedDbMemberTranslatorProvider>()
            .TryAddCoreServices();
        return serviceCollection;
    }
}
