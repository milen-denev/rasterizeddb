using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace RasterizedDb.Data.Tests;

/// <summary>
/// Real servers for the live tests, started from the binary <c>RDB_SERVER_BIN</c> names. Without it
/// each live test says so and passes, so the suite runs anywhere; with it, the tests are what keeps the
/// driver's copy of the wire format in step with the server.
/// </summary>
public sealed class ServerFixture : IDisposable
{
    private readonly List<(Process Process, string Directory)> _servers = [];
    private readonly object _gate = new();
    private string? _trust;
    private (string Socket, int Port)? _scram;

    public const string User = "alice";
    public const string Password = "hunter2";

    public static string? ServerBinary => Environment.GetEnvironmentVariable("RDB_SERVER_BIN") is { Length: > 0 } path ? path : null;

    /// <summary>A connection string to a server whose socket asks for no login; null when there is no server binary.</summary>
    public string? Trust(string extra = "")
    {
        if (ServerBinary is null)
        {
            return null;
        }

        lock (_gate)
        {
            _trust ??= Start("trust", ["--rdb-tcp", "off", "--rdb-socket-auth", "trust"], null);
        }

        return $"Socket={_trust};{extra}";
    }

    /// <summary>A server that asks for a password on its socket and on TCP, with the bootstrap role alice.</summary>
    public (string Socket, string Tcp)? Scram(string extra = "")
    {
        if (ServerBinary is null)
        {
            return null;
        }

        lock (_gate)
        {
            if (_scram is null)
            {
                var port = FreePort();
                var socket = Start(
                    "scram",
                    ["--rdb-socket-auth", "scram-sha-256", "--rdb-addr", "127.0.0.1", "--rdb-port", port.ToString(), "--rdb-user", User],
                    Password);
                _scram = (socket, port);
            }
        }

        var login = $"Username={User};Password={Password};{extra}";
        return ($"Socket={_scram.Value.Socket};{login}", $"Host=127.0.0.1;Port={_scram.Value.Port};{login}");
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private string Start(string name, string[] arguments, string? password)
    {
        // Under /tmp: a unix socket's path must fit in 107 bytes.
        var directory = Path.Combine("/tmp", $"rdbnet_{name}_{Environment.ProcessId}");
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }

        Directory.CreateDirectory(Path.Combine(directory, "db"));
        var socket = Path.Combine(directory, "s.sock");
        var start = new ProcessStartInfo(ServerBinary!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = false,
            UseShellExecute = false,
        };
        foreach (var argument in (string[])["--location", Path.Combine(directory, "db"), "--rdb-socket", socket, "--socket-mode", "600", "--log-level", "warn", .. arguments])
        {
            start.ArgumentList.Add(argument);
        }

        if (password is not null)
        {
            start.Environment["RASTERIZEDDB_PASSWORD"] = password;
        }

        var process = Process.Start(start) ?? throw new InvalidOperationException("the server did not start");
        process.OutputDataReceived += static (_, _) => { };
        process.BeginOutputReadLine();
        _servers.Add((process, directory));
        for (var i = 0; i < 600; i++)
        {
            if (process.HasExited)
            {
                throw new InvalidOperationException($"the server exited with code {process.ExitCode} before opening {socket}");
            }

            if (File.Exists(socket))
            {
                try
                {
                    using var probe = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                    probe.Connect(new UnixDomainSocketEndPoint(socket));
                    return socket;
                }
                catch (SocketException)
                {
                    // Not listening yet.
                }
            }

            Thread.Sleep(100);
        }

        throw new TimeoutException($"the server did not open {socket} within 60 s");
    }

    public void Dispose()
    {
        RasterizedDbDataSource.ClearAllPools();
        foreach (var (process, directory) in _servers)
        {
            try
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit(10_000);
            }
            catch (Exception)
            {
                // Already gone.
            }

            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception)
            {
                // Best effort.
            }
        }
    }
}

[CollectionDefinition("server")]
public sealed class ServerCollection : ICollectionFixture<ServerFixture>;
