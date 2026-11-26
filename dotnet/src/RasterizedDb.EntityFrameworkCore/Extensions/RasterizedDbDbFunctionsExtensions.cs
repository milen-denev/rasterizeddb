namespace Microsoft.EntityFrameworkCore;

/// <summary>Functions for LINQ queries against RasterizedDB.</summary>
public static class RasterizedDbDbFunctionsExtensions
{
    /// <summary>A case-insensitive <c>LIKE</c>.</summary>
    public static bool ILike(this DbFunctions _, string matchExpression, string pattern) =>
        throw new InvalidOperationException("ILike is for LINQ queries; it has no client implementation");
}
