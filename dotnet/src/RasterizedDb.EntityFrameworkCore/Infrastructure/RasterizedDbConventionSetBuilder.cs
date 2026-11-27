using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace RasterizedDb.EntityFrameworkCore.Infrastructure;

/// <summary>The relational conventions; RasterizedDB adds none of its own.</summary>
public class RasterizedDbConventionSetBuilder(ProviderConventionSetBuilderDependencies dependencies, RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
}
