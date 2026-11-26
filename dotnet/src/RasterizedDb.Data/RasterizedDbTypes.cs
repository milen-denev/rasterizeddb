using System.Data.Common;

namespace RasterizedDb.Data;

/// <summary>
/// An <c>interval</c> as PostgreSQL and RasterizedDB hold it: months, days and microseconds kept
/// apart, because a month is not a fixed number of days nor a day a fixed number of hours.
/// </summary>
public readonly record struct RasterizedDbInterval(int Months, int Days, long Microseconds)
{
    private const long MicrosPerDay = 86_400_000_000;

    /// <summary>A <see cref="TimeSpan"/> as days and the rest of a day.</summary>
    public static RasterizedDbInterval FromTimeSpan(TimeSpan value)
    {
        var micros = value.Ticks / 10;
        return new RasterizedDbInterval(0, (int)(micros / MicrosPerDay), micros % MicrosPerDay);
    }

    /// <summary>
    /// This interval as a <see cref="TimeSpan"/>. An interval holding months has no fixed length and
    /// throws: read it as a <see cref="RasterizedDbInterval"/>.
    /// </summary>
    public TimeSpan ToTimeSpan()
    {
        if (Months != 0)
        {
            throw new InvalidCastException(
                "an interval holding months has no fixed length, so it is not a TimeSpan; read it as RasterizedDbInterval");
        }

        return new TimeSpan(checked((Days * MicrosPerDay + Microseconds) * 10));
    }

    public override string ToString()
    {
        using var w = new Internal.ByteWriter(48);
        Internal.LiteralWriter.WriteIntervalBody(w, this);
        return System.Text.Encoding.ASCII.GetString(w.WrittenSpan);
    }
}

/// <summary>How a command's rows are asked for and read.</summary>
public enum RasterizedDbResultFormat
{
    /// <summary>
    /// The connection's setting (<c>Result Format</c> in the connection string, <see cref="Text"/> unless set).
    /// </summary>
    Default = 0,

    /// <summary>
    /// The answer PostgreSQL's simple query gives: column names and types, every cell as text, any
    /// number of statements per command. The complete ADO.NET surface (names, schema, several result
    /// sets), at the cost of the server's PostgreSQL path and of parsing text.
    /// </summary>
    Text = 1,

    /// <summary>
    /// The native answer: the engine's binary cells, one statement per command, and no column names
    /// (a column is read by its position in the select list). The fastest path, and the one the
    /// Entity Framework Core provider uses for the SQL it generates.
    /// </summary>
    Binary = 2,
}

/// <summary>How a connection logs in.</summary>
public enum RasterizedDbLogin
{
    /// <summary>Chosen from the connection string: a password login when one is given, otherwise trust.</summary>
    Auto = 0,

    /// <summary>No exchange: for a server that asks for none (a unix socket under <c>--rdb-socket-auth trust</c>).</summary>
    Trust = 1,

    /// <summary>
    /// The operating-system user this process runs as, which the server reads from the unix socket
    /// itself (<c>--rdb-socket-auth peer</c>). <c>Username</c>, when set, names the role instead.
    /// </summary>
    Peer = 2,

    /// <summary>SCRAM-SHA-256, bound to the connection it runs on.</summary>
    Password = 3,
}

/// <summary>Whether a TCP connection believes the server it reached.</summary>
public enum RasterizedDbTrust
{
    /// <summary>
    /// Encrypted, and the server's certificate is not checked. Suits the self-signed certificate a
    /// server makes for itself on a network you control; anyone in the path can impersonate the server.
    /// </summary>
    AcceptAny = 0,

    /// <summary>The operating system's trusted roots, and the host name.</summary>
    WebPki = 1,

    /// <summary>A private certificate authority (<c>Root Certificate</c>: a PEM file).</summary>
    RootCa = 2,

    /// <summary>Exactly the certificate whose SHA-256 fingerprint is <c>Certificate Fingerprint</c>.</summary>
    Pinned = 3,
}

/// <summary>Which TLS implementation a TCP connection runs on.</summary>
public enum RasterizedDbTlsEngine
{
    /// <summary>
    /// The platform's (<see cref="System.Net.Security.SslStream"/>) wherever the login needs no channel
    /// binding or the binding can be read from it, the managed one otherwise.
    /// </summary>
    Auto = 0,

    /// <summary>Always <see cref="System.Net.Security.SslStream"/>; a password login fails where its binding cannot be read.</summary>
    Platform = 1,

    /// <summary>Always the managed implementation (BouncyCastle).</summary>
    Managed = 2,
}

/// <summary>What the server says about a statement without failing it.</summary>
public sealed class RasterizedDbNotice(string severity, string sqlState, string message)
{
    /// <summary><c>NOTICE</c>, <c>WARNING</c>, <c>INFO</c>, <c>LOG</c> or <c>DEBUG</c>.</summary>
    public string Severity { get; } = severity;

    /// <summary>The five-character SQLSTATE: <c>00000</c> for a plain notice, <c>01000</c> for a warning, …</summary>
    public string SqlState { get; } = sqlState;

    public string Message { get; } = message;

    public override string ToString() => $"{Severity}: {Message} (SQLSTATE {SqlState})";
}

public sealed class RasterizedDbNoticeEventArgs(RasterizedDbNotice notice) : EventArgs
{
    public RasterizedDbNotice Notice { get; } = notice;
}

/// <summary>What went wrong, coarsely.</summary>
public enum RasterizedDbErrorKind
{
    /// <summary>The server refused or failed the statement; <see cref="DbException.SqlState"/> holds its code when it sent one.</summary>
    Server,

    /// <summary>The connection failed: whether a statement in flight ran is unknown.</summary>
    Transport,

    /// <summary>The server's answer was not one this driver reads.</summary>
    Protocol,

    /// <summary>The login was refused, or could not be carried out.</summary>
    Authentication,

    /// <summary>No answer came within the timeout.</summary>
    Timeout,

    /// <summary>The connection of an open transaction was lost; the server rolled it back unless <see cref="RasterizedDbException.CommitOutcomeUnknown"/>.</summary>
    TransactionLost,

    /// <summary>The statement cannot be sent in the form asked for (several statements in the binary format, a server too old for a request).</summary>
    Unsupported,
}

/// <summary>An error from the server or from the connection to it.</summary>
public sealed class RasterizedDbException : DbException
{
    private readonly string? _sqlState;

    internal RasterizedDbException(RasterizedDbErrorKind kind, string message, string? sqlState = null, Exception? inner = null, bool commitOutcomeUnknown = false)
        : base(message, inner)
    {
        Kind = kind;
        _sqlState = sqlState;
        CommitOutcomeUnknown = commitOutcomeUnknown;
    }

    public RasterizedDbErrorKind Kind { get; }

    /// <summary>The five-character SQLSTATE the server sent, when it sent one.</summary>
    public override string? SqlState => _sqlState;

    /// <summary>
    /// A COMMIT whose connection failed: the transaction may or may not have committed. Never retry it
    /// blindly.
    /// </summary>
    public bool CommitOutcomeUnknown { get; }

    /// <summary>A serialization failure (40001) or a deadlock (40P01): the transaction can be run again.</summary>
    public override bool IsTransient => _sqlState is "40001" or "40P01";

    /// <summary>The server stopped the statement at its statement timeout (57014); the connection goes on.</summary>
    public bool IsStatementTimeout => _sqlState == "57014";

    public bool IsUniqueViolation =>
        _sqlState == "23505"
        || (Kind == RasterizedDbErrorKind.Server
            && Message.Contains("duplicate", StringComparison.OrdinalIgnoreCase)
            && (Message.Contains("unique", StringComparison.OrdinalIgnoreCase) || Message.Contains("primary key", StringComparison.OrdinalIgnoreCase)));

    /// <summary>
    /// A server error as the native answer carries it: the message, then <c> (SQLSTATE xxxxx)</c> when
    /// the server knew a code.
    /// </summary>
    internal static RasterizedDbException FromServer(string message)
    {
        string? code = null;
        if (message.Length > 16 && message[^1] == ')')
        {
            var at = message.LastIndexOf("(SQLSTATE ", StringComparison.Ordinal);
            if (at >= 0 && message.Length - at == 16)
            {
                var candidate = message.AsSpan(at + 10, 5);
                var ok = true;
                foreach (var c in candidate)
                {
                    ok &= char.IsAsciiLetterOrDigit(c);
                }

                if (ok)
                {
                    code = candidate.ToString();
                    message = message[..at].TrimEnd();
                }
            }
        }

        return new RasterizedDbException(RasterizedDbErrorKind.Server, message, code);
    }
}
