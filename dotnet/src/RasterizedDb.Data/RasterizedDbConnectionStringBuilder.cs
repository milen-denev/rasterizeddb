using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace RasterizedDb.Data;

/// <summary>
/// A RasterizedDB connection string.
/// </summary>
/// <remarks>
/// <code>
/// Host=db.example.com;Port=61170;Username=app;Password=secret;Database=shop
/// Socket=/run/rdb/rdb.sock;Role=app
/// </code>
/// <list type="table">
/// <item><term>Host, Port</term><description>The native listener (<c>--rdb-addr</c>, <c>--rdb-port</c>; 61170 unless set). Over TCP the transport is TLS 1.3.</description></item>
/// <item><term>Socket</term><description>A unix socket's path (<c>--rdb-socket</c>) instead of Host. No TLS, and none needed.</description></item>
/// <item><term>Database</term><description>The database; the server's default one when left out.</description></item>
/// <item><term>Username, Password</term><description>A SCRAM-SHA-256 login.</description></item>
/// <item><term>Login</term><description><c>Auto</c> (password when one is given, else trust), <c>Trust</c>, <c>Peer</c>, <c>Password</c>.</description></item>
/// <item><term>Role</term><description>On a trust connection, the role its statements run as.</description></item>
/// <item><term>Trust</term><description><c>AcceptAny</c>, <c>WebPki</c>, <c>RootCa</c> (with <c>Root Certificate</c>), <c>Pinned</c> (with <c>Certificate Fingerprint</c>).</description></item>
/// <item><term>Result Format</term><description><c>Text</c> (names, schema, several statements) or <c>Binary</c> (native cells by position).</description></item>
/// <item><term>Timeout, Command Timeout</term><description>Seconds to connect, and for a statement's answer (30; 0 waits indefinitely).</description></item>
/// <item><term>Pooling, Maximum Pool Size, Minimum Pool Size, Connection Idle Lifetime, Keepalive</term><description>The pool.</description></item>
/// <item><term>Max Message Size</term><description>The largest answer accepted, in bytes (8 MiB).</description></item>
/// </list>
/// </remarks>
public sealed class RasterizedDbConnectionStringBuilder : DbConnectionStringBuilder
{
    public const int DefaultPort = 61170;

    public RasterizedDbConnectionStringBuilder()
    {
    }

    public RasterizedDbConnectionStringBuilder(string? connectionString)
    {
        ConnectionString = connectionString ?? string.Empty;
    }

    // One canonical keyword per setting; the others are accepted on input and rewritten.
    private static readonly Dictionary<string, string> Canonical = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Host"] = "Host", ["Server"] = "Host", ["Data Source"] = "Host", ["DataSource"] = "Host", ["Address"] = "Host",
        ["Port"] = "Port",
        ["Socket"] = "Socket", ["Unix Socket"] = "Socket", ["UnixSocket"] = "Socket",
        ["Database"] = "Database", ["Initial Catalog"] = "Database", ["DB"] = "Database",
        ["Username"] = "Username", ["User Name"] = "Username", ["User"] = "Username", ["User Id"] = "Username", ["UserId"] = "Username", ["UID"] = "Username",
        ["Password"] = "Password", ["PWD"] = "Password",
        ["Login"] = "Login",
        ["Role"] = "Role",
        ["Trust"] = "Trust", ["Server Trust"] = "Trust",
        ["Root Certificate"] = "Root Certificate", ["RootCertificate"] = "Root Certificate",
        ["Certificate Fingerprint"] = "Certificate Fingerprint", ["CertificateFingerprint"] = "Certificate Fingerprint",
        ["Tls Engine"] = "Tls Engine", ["TlsEngine"] = "Tls Engine",
        ["Result Format"] = "Result Format", ["ResultFormat"] = "Result Format",
        ["Timeout"] = "Timeout", ["Connect Timeout"] = "Timeout", ["Connection Timeout"] = "Timeout",
        ["Command Timeout"] = "Command Timeout", ["CommandTimeout"] = "Command Timeout",
        ["Pooling"] = "Pooling",
        ["Maximum Pool Size"] = "Maximum Pool Size", ["Max Pool Size"] = "Maximum Pool Size", ["MaxPoolSize"] = "Maximum Pool Size",
        ["Minimum Pool Size"] = "Minimum Pool Size", ["Min Pool Size"] = "Minimum Pool Size", ["MinPoolSize"] = "Minimum Pool Size",
        ["Connection Idle Lifetime"] = "Connection Idle Lifetime", ["ConnectionIdleLifetime"] = "Connection Idle Lifetime",
        ["Keepalive"] = "Keepalive", ["Keep Alive"] = "Keepalive",
        ["Idle Reconnect"] = "Idle Reconnect", ["IdleReconnect"] = "Idle Reconnect",
        ["Max Message Size"] = "Max Message Size", ["MaxMessageSize"] = "Max Message Size",
        ["Notices"] = "Notices",
        ["Application Name"] = "Application Name", ["ApplicationName"] = "Application Name",
    };

    [AllowNull]
    public override object this[string keyword]
    {
        get => base[Key(keyword)];
        set
        {
            if (value is null)
            {
                base.Remove(Key(keyword));
            }
            else
            {
                base[Key(keyword)] = value;
            }
        }
    }

    private static string Key(string keyword) =>
        Canonical.TryGetValue(keyword, out var canonical)
            ? canonical
            : throw new ArgumentException($"\"{keyword}\" is not a RasterizedDB connection string keyword", nameof(keyword));

    public override bool ContainsKey(string keyword) => Canonical.TryGetValue(keyword, out var canonical) && base.ContainsKey(canonical);

    public override bool Remove(string keyword) => Canonical.TryGetValue(keyword, out var canonical) && base.Remove(canonical);

    public override bool TryGetValue(string keyword, [NotNullWhen(true)] out object? value)
    {
        if (Canonical.TryGetValue(keyword, out var canonical))
        {
            return base.TryGetValue(canonical, out value);
        }

        value = null;
        return false;
    }

    private string? Text(string key) => base.TryGetValue(key, out var value) ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;

    private int Int(string key, int fallback) =>
        base.TryGetValue(key, out var value) ? Convert.ToInt32(value, CultureInfo.InvariantCulture) : fallback;

    private bool Bool(string key, bool fallback)
    {
        if (!base.TryGetValue(key, out var value))
        {
            return fallback;
        }

        return value switch
        {
            bool b => b,
            string s when s.Equals("yes", StringComparison.OrdinalIgnoreCase) || s == "1" || s.Equals("on", StringComparison.OrdinalIgnoreCase) => true,
            string s when s.Equals("no", StringComparison.OrdinalIgnoreCase) || s == "0" || s.Equals("off", StringComparison.OrdinalIgnoreCase) => false,
            _ => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
        };
    }

    private T Enumeration<T>(string key, T fallback) where T : struct, Enum
    {
        if (!base.TryGetValue(key, out var value))
        {
            return fallback;
        }

        return value is T typed ? typed : Enum.Parse<T>(Convert.ToString(value, CultureInfo.InvariantCulture)!.Replace(" ", "").Replace("-", ""), ignoreCase: true);
    }

    private void Set(string key, object? value)
    {
        if (value is null)
        {
            base.Remove(key);
        }
        else
        {
            base[key] = value is Enum ? value.ToString()! : value;
        }
    }

    /// <summary>The server's host name or address. <c>unix:/path</c> names a unix socket, as <see cref="Socket"/> does.</summary>
    public string? Host { get => Text("Host"); set => Set("Host", value); }

    public int Port { get => Int("Port", DefaultPort); set => Set("Port", value); }

    /// <summary>The path of the server's unix socket (<c>--rdb-socket</c>).</summary>
    public string? Socket { get => Text("Socket"); set => Set("Socket", value); }

    public string? Database { get => Text("Database"); set => Set("Database", value); }

    public string? Username { get => Text("Username"); set => Set("Username", value); }

    public string? Password { get => Text("Password"); set => Set("Password", value); }

    public RasterizedDbLogin Login { get => Enumeration("Login", RasterizedDbLogin.Auto); set => Set("Login", value); }

    /// <summary>
    /// On a trust connection, the role every statement runs as. A trust connection that names none is
    /// no administrator in a database with roles (RasterizedDB 1.0.2 and later).
    /// </summary>
    public string? Role { get => Text("Role"); set => Set("Role", value); }

    public RasterizedDbTrust Trust { get => Enumeration("Trust", RasterizedDbTrust.AcceptAny); set => Set("Trust", value); }

    /// <summary>A PEM file holding the certificate authority that <see cref="RasterizedDbTrust.RootCa"/> trusts.</summary>
    public string? RootCertificate { get => Text("Root Certificate"); set => Set("Root Certificate", value); }

    /// <summary>The server certificate's SHA-256 fingerprint in hex (<c>openssl x509 -fingerprint -sha256</c>), for <see cref="RasterizedDbTrust.Pinned"/>. Several are separated by commas.</summary>
    public string? CertificateFingerprint { get => Text("Certificate Fingerprint"); set => Set("Certificate Fingerprint", value); }

    public RasterizedDbTlsEngine TlsEngine { get => Enumeration("Tls Engine", RasterizedDbTlsEngine.Auto); set => Set("Tls Engine", value); }

    public RasterizedDbResultFormat ResultFormat { get => Enumeration("Result Format", RasterizedDbResultFormat.Text); set => Set("Result Format", value); }

    /// <summary>Seconds to open and log in a connection (15).</summary>
    public int Timeout { get => Int("Timeout", 15); set => Set("Timeout", value); }

    /// <summary>
    /// Seconds a statement may wait for its answer (30; 0 waits indefinitely). The server stops a
    /// statement at its own statement timeout and answers with SQLSTATE 57014; so that answer is the one
    /// a caller gets, the wait here is this plus two seconds.
    /// </summary>
    public int CommandTimeout { get => Int("Command Timeout", 30); set => Set("Command Timeout", value); }

    public bool Pooling { get => Bool("Pooling", true); set => Set("Pooling", value); }

    public int MaxPoolSize { get => Int("Maximum Pool Size", 100); set => Set("Maximum Pool Size", value); }

    public int MinPoolSize { get => Int("Minimum Pool Size", 0); set => Set("Minimum Pool Size", value); }

    /// <summary>Seconds an unused pooled connection is kept before it is closed (300).</summary>
    public int ConnectionIdleLifetime { get => Int("Connection Idle Lifetime", 300); set => Set("Connection Idle Lifetime", value); }

    /// <summary>
    /// Seconds between the pings that keep an unused pooled connection open (10; 0 sends none). The
    /// server closes a native connection that has been quiet for its request timeout, 30 s.
    /// </summary>
    public int Keepalive { get => Int("Keepalive", 10); set => Set("Keepalive", value); }

    /// <summary>
    /// A connection quiet for this many seconds is reopened before a statement is sent on it (20; 0
    /// never does), so no statement is sent on a connection the server may be closing.
    /// </summary>
    public int IdleReconnect { get => Int("Idle Reconnect", 20); set => Set("Idle Reconnect", value); }

    /// <summary>The largest answer accepted, in bytes (8 MiB). One past it fails whole and costs the connection.</summary>
    public int MaxMessageSize { get => Int("Max Message Size", 8 * 1024 * 1024); set => Set("Max Message Size", value); }

    /// <summary>Whether connections ask the server for notices (true).</summary>
    public bool Notices { get => Bool("Notices", true); set => Set("Notices", value); }

    public string? ApplicationName { get => Text("Application Name"); set => Set("Application Name", value); }
}
