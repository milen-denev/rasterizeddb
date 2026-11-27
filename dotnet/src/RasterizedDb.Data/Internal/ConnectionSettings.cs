using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>A connection string read once: what every connection of a data source is opened with.</summary>
internal sealed class ConnectionSettings
{
    public readonly string ConnectionString;
    /// <summary>The connection string without its password, for <c>DbConnection.ConnectionString</c> after opening.</summary>
    public readonly string PublicConnectionString;
    public readonly string? Host;
    public readonly int Port;
    public readonly string? SocketPath;
    public readonly string? Database;
    public readonly string? Username;
    public readonly string? Password;
    public readonly RasterizedDbLogin Login;
    public readonly string? Role;
    public readonly RasterizedDbTrust Trust;
    public readonly X509Certificate2Collection? RootCertificates;
    public readonly byte[][] Fingerprints;
    public readonly X509Certificate2? ClientCertificate;
    public readonly RasterizedDbTlsEngine TlsEngine;
    public readonly RasterizedDbResultFormat ResultFormat;
    public readonly int ConnectTimeoutMs;
    public readonly int CommandTimeoutSeconds;
    public readonly bool Pooling;
    public readonly int MaxPoolSize;
    public readonly int MinPoolSize;
    public readonly long IdleLifetimeMs;
    public readonly long KeepaliveMs;
    public readonly long IdleReconnectMs;
    public readonly int MaxMessageSize;
    public readonly bool Notices;

    /// <summary>The setup request that names the database or role of a connection that runs no exchange, if any.</summary>
    public readonly byte[]? SelectionRequest;

    public bool IsUnix => SocketPath is not null;

    public string DataSourceName => SocketPath ?? $"{Host}:{Port}";

    public ConnectionSettings(string connectionString)
    {
        var b = new RasterizedDbConnectionStringBuilder(connectionString);
        ConnectionString = connectionString;

        var host = b.Host;
        var socket = b.Socket;
        if (socket is null && host is not null && host.StartsWith("unix:", StringComparison.Ordinal))
        {
            socket = host[5..];
            host = null;
        }

        if (socket is null && host is not null && host.StartsWith('/'))
        {
            socket = host;
            host = null;
        }

        if (socket is { Length: 0 })
        {
            throw new ArgumentException("a unix socket address needs a path");
        }

        Port = b.Port;
        if (socket is null)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("the connection string names no server: set Host (and Port), or Socket for a unix socket");
            }

            // host:port in Host, as the client crate's addresses are written.
            var colon = host.LastIndexOf(':');
            if (colon > 0 && host.IndexOf(':') == colon && int.TryParse(host.AsSpan(colon + 1), out var port) && !b.ContainsKey("Port"))
            {
                Port = port;
                host = host[..colon];
            }
        }

        Host = host;
        SocketPath = socket;
        Database = NullIfEmpty(b.Database);
        Username = NullIfEmpty(b.Username);
        Password = b.Password;
        Role = NullIfEmpty(b.Role);
        Trust = b.Trust;
        TlsEngine = b.TlsEngine;
        ResultFormat = b.ResultFormat == RasterizedDbResultFormat.Default ? RasterizedDbResultFormat.Text : b.ResultFormat;
        ConnectTimeoutMs = b.Timeout <= 0 ? Timeout.Infinite : checked(b.Timeout * 1000);
        CommandTimeoutSeconds = Math.Max(0, b.CommandTimeout);
        Pooling = b.Pooling;
        MaxPoolSize = Math.Max(1, b.MaxPoolSize);
        MinPoolSize = Math.Clamp(b.MinPoolSize, 0, MaxPoolSize);
        IdleLifetimeMs = Math.Max(1, b.ConnectionIdleLifetime) * 1000L;
        KeepaliveMs = Math.Max(0, b.Keepalive) * 1000L;
        IdleReconnectMs = Math.Max(0, b.IdleReconnect) * 1000L;
        MaxMessageSize = Math.Max(1024, b.MaxMessageSize);
        Notices = b.Notices;

        Login = b.Login;
        if (Login == RasterizedDbLogin.Auto)
        {
            Login = Password is not null ? RasterizedDbLogin.Password : RasterizedDbLogin.Trust;
        }

        switch (Login)
        {
            case RasterizedDbLogin.Password when Username is null || Password is null:
                throw new ArgumentException("a password login needs Username and Password");
            case RasterizedDbLogin.Peer when Username is null:
                Username = Environment.UserName;
                if (string.IsNullOrEmpty(Username))
                {
                    throw new ArgumentException("this process's user has no name; set Username for the peer login");
                }

                break;
            case RasterizedDbLogin.Trust when Role is null && Username is not null && Password is null:
                // "Username=app" with nothing to prove it by, on a server that asks for nothing: the role.
                Role = Username;
                break;
        }

        if (Role is not null && Login != RasterizedDbLogin.Trust)
        {
            throw new ArgumentException("Role names the role of a trust connection; a password or peer login names its own with Username");
        }

        if (IsUnix)
        {
            if (b.ContainsKey("Trust") && Trust != RasterizedDbTrust.AcceptAny)
            {
                throw new ArgumentException("a unix socket carries no TLS: a server certificate cannot be checked over one");
            }
        }
        else
        {
            switch (Trust)
            {
                case RasterizedDbTrust.RootCa:
                {
                    var path = b.RootCertificate ?? throw new ArgumentException("Trust=RootCa needs Root Certificate, a PEM file");
                    var collection = new X509Certificate2Collection();
                    collection.ImportFromPemFile(path);
                    if (collection.Count == 0)
                    {
                        throw new ArgumentException($"{path} holds no certificates");
                    }

                    RootCertificates = collection;
                    break;
                }
                case RasterizedDbTrust.Pinned:
                {
                    var text = b.CertificateFingerprint ?? throw new ArgumentException("Trust=Pinned needs Certificate Fingerprint");
                    var pins = text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    Fingerprints = new byte[pins.Length][];
                    for (var i = 0; i < pins.Length; i++)
                    {
                        Fingerprints[i] = ParseFingerprint(pins[i]);
                    }

                    if (Fingerprints.Length == 0)
                    {
                        throw new ArgumentException("pinned trust needs at least one fingerprint");
                    }

                    break;
                }
            }
        }

        Fingerprints ??= [];
        if (b.ClientCertificate is { Length: > 0 } certificatePath)
        {
            if (IsUnix)
            {
                throw new ArgumentException("a unix socket carries no TLS: a client certificate cannot be presented over one");
            }

            ClientCertificate = X509Certificate2.CreateFromPemFile(certificatePath, string.IsNullOrEmpty(b.ClientKey) ? null : b.ClientKey);
        }

        if (Login == RasterizedDbLogin.Trust)
        {
            if (Role is not null)
            {
                CheckName(Role, "role");
                var request = new List<byte>("\u0001ROLE;"u8.ToArray());
                request.AddRange(Encoding.UTF8.GetBytes(Role));
                if (Database is not null)
                {
                    request.Add(0x1f);
                    request.AddRange(Encoding.UTF8.GetBytes(Database));
                }

                SelectionRequest = [.. request];
            }
            else if (Database is not null)
            {
                SelectionRequest = [.. "\u0001DATABASE;"u8, .. Encoding.UTF8.GetBytes(Database)];
            }
        }

        var shown = new RasterizedDbConnectionStringBuilder(connectionString);
        shown.Remove("Password");
        PublicConnectionString = shown.ConnectionString;
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    private static void CheckName(string name, string what)
    {
        if (name.AsSpan().IndexOfAny('\0', '\u001f') >= 0)
        {
            throw new ArgumentException($"the {what} name holds a control character");
        }
    }

    /// <summary>
    /// A SHA-256 fingerprint written as hex, tolerating the <c>:</c> and spaces <c>openssl x509
    /// -fingerprint</c> puts in, its case, and the <c>SHA256=</c> some tools print in front.
    /// </summary>
    internal static byte[] ParseFingerprint(string text)
    {
        var cleaned = new StringBuilder(64);
        foreach (var c in text)
        {
            if (!char.IsWhiteSpace(c) && c != ':')
            {
                cleaned.Append(c);
            }
        }

        var hex = cleaned.ToString();
        if (hex.StartsWith("SHA256", StringComparison.OrdinalIgnoreCase))
        {
            hex = hex[6..].TrimStart('=');
        }

        if (hex.Length != 64)
        {
            throw new ArgumentException($"a SHA-256 fingerprint is 64 hex characters; got {hex.Length}");
        }

        try
        {
            return Convert.FromHexString(hex);
        }
        catch (FormatException)
        {
            throw new ArgumentException("a fingerprint must be hexadecimal");
        }
    }
}
