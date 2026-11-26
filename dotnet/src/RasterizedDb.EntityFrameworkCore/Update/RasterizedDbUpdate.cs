using Microsoft.EntityFrameworkCore.Update;

namespace RasterizedDb.EntityFrameworkCore.Update;

/// <summary>
/// INSERT, UPDATE and DELETE with <c>RETURNING</c>: generated values come back from the statement that
/// wrote them, and an UPDATE or DELETE returns a row per row it changed, which is how a concurrency
/// conflict is seen.
/// </summary>
public class RasterizedDbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : UpdateSqlGenerator(dependencies)
{
}

/// <summary>
/// One statement per request: the native protocol runs one statement per request, so each command of a
/// <c>SaveChanges</c> is its own round trip, inside one transaction when there are several.
/// </summary>
public class RasterizedDbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies) : IModificationCommandBatchFactory
{
    public virtual ModificationCommandBatch Create() => new SingularModificationCommandBatch(dependencies);
}
