using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using RasterizedDb.EntityFrameworkCore.Migrations;

namespace RasterizedDb.EntityFrameworkCore.ValueGeneration
{
    /// <summary>
    /// The blocks of key values taken so far, per sequence and database: shared by every context of the
    /// application, so a block is used up before another is asked for.
    /// </summary>
    public class RasterizedDbHiLoStates
    {
        private readonly ConcurrentDictionary<(string Connection, string Sequence), HiLoValueGeneratorState> _states = new();

        public virtual HiLoValueGeneratorState For(string connectionString, string sequence, int blockSize) =>
            _states.GetOrAdd((connectionString, sequence), _ => new HiLoValueGeneratorState(blockSize));
    }

    /// <summary>
    /// Keys from a sequence a block at a time: one <c>nextval</c> answers the start of a block of as many
    /// values as the sequence increments by, and the rest of the block is handed out without a round trip.
    /// </summary>
    public class RasterizedDbHiLoValueGenerator<TValue>(
        HiLoValueGeneratorState state,
        IRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IRelationalCommandDiagnosticsLogger logger,
        string nextValueSql)
        : HiLoValueGenerator<TValue>(state)
    {
        private RelationalCommandParameterObject Parameters => new(connection, null, null, null, logger);

        protected override long GetNewLowValue() =>
            Convert.ToInt64(rawSqlCommandBuilder.Build(nextValueSql).ExecuteScalar(Parameters), System.Globalization.CultureInfo.InvariantCulture);

        protected override async Task<long> GetNewLowValueAsync(CancellationToken cancellationToken = default) =>
            Convert.ToInt64(
                await rawSqlCommandBuilder.Build(nextValueSql).ExecuteScalarAsync(Parameters, cancellationToken).ConfigureAwait(false),
                System.Globalization.CultureInfo.InvariantCulture);

        public override bool GeneratesTemporaryValues => false;
    }

    /// <summary>Gives a property configured with <c>UseHiLo</c> its generator; everything else is EF Core's choice.</summary>
    public class RasterizedDbValueGeneratorSelector(
        ValueGeneratorSelectorDependencies dependencies,
        RasterizedDbHiLoStates states,
        IRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IRelationalCommandDiagnosticsLogger logger,
        ISqlGenerationHelper sqlGenerationHelper)
        : RelationalValueGeneratorSelector(dependencies)
    {
        public override bool TrySelect(IProperty property, ITypeBase typeBase, out ValueGenerator? valueGenerator)
        {
            if (property.GetValueGeneratorFactory() is null && property[RasterizedDbAnnotationNames.HiLoSequence] is string name)
            {
                var schema = property[RasterizedDbAnnotationNames.HiLoSequenceSchema] as string;
                var blockSize = property.DeclaringType.Model.FindSequence(name, schema)?.IncrementBy ?? RasterizedDbHiLoExtensions.DefaultBlockSize;
                var type = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
                if (type != typeof(int) && type != typeof(long) && type != typeof(short))
                {
                    throw new InvalidOperationException(
                        $"{typeBase.DisplayName()}.{property.Name} uses UseHiLo, which makes int, long and short keys; it is a {type.Name}");
                }

                var delimited = sqlGenerationHelper.DelimitIdentifier(name, schema);
                valueGenerator = (ValueGenerator)Activator.CreateInstance(
                    typeof(RasterizedDbHiLoValueGenerator<>).MakeGenericType(type),
                    states.For(connection.ConnectionString ?? string.Empty, delimited, blockSize),
                    connection,
                    rawSqlCommandBuilder,
                    logger,
                    $"SELECT nextval('{delimited.Replace("'", "''")}')")!;
                return true;
            }

            return base.TrySelect(property, typeBase, out valueGenerator);
        }
    }
}

namespace Microsoft.EntityFrameworkCore
{
    using RasterizedDb.EntityFrameworkCore.Migrations;

    public static class RasterizedDbHiLoExtensions
    {
        public const string DefaultSequenceName = "ef_hilo_sequence";

        /// <summary>How many keys one round trip reserves, unless the sequence is configured otherwise.</summary>
        public const int DefaultBlockSize = 10;

        /// <summary>
        /// Takes the property's values from a sequence a block at a time (the HiLo pattern): the key is
        /// known before the row is saved, and one round trip serves a block of inserts. The sequence is
        /// added to the model, so migrations and <c>EnsureCreated</c> create it; configure its block
        /// size with <c>modelBuilder.HasSequence(name).IncrementsBy(n)</c>.
        /// </summary>
        public static PropertyBuilder UseHiLo(this PropertyBuilder propertyBuilder, string? name = null, string? schema = null)
        {
            name ??= DefaultSequenceName;
            var property = propertyBuilder.Metadata;
            var model = property.DeclaringType.Model;
            if (model.FindSequence(name, schema) is null)
            {
                model.AddSequence(name, schema).IncrementBy = DefaultBlockSize;
            }

            property.SetAnnotation(RasterizedDbAnnotationNames.HiLoSequence, name);
            property.SetAnnotation(RasterizedDbAnnotationNames.HiLoSequenceSchema, schema);
            property.ValueGenerated = ValueGenerated.OnAdd;
            return propertyBuilder;
        }

        public static PropertyBuilder<TProperty> UseHiLo<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, string? name = null, string? schema = null) =>
            (PropertyBuilder<TProperty>)UseHiLo((PropertyBuilder)propertyBuilder, name, schema);
    }
}
