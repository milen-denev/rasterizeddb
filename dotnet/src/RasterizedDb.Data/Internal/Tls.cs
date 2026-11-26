using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Tls;
using Org.BouncyCastle.Tls.Crypto.Impl.BC;

namespace RasterizedDb.Data.Internal;

/// <summary>
/// The TLS a TCP connection runs on: TLS 1.3, naming the protocol in ALPN (<c>rastcp/1</c>), with the
/// server's certificate checked as the connection string's <c>Trust</c> says.
/// </summary>
/// <remarks>
/// A password login signs over the connection's <c>tls-exporter</c> channel binding (RFC 9266), which
/// <see cref="SslStream"/> has no API for. Where the platform's TLS can be asked for it directly
/// (OpenSSL, on Linux) the connection runs on <see cref="SslStream"/> all the same; elsewhere it runs on
/// a managed TLS implementation that exposes the exporter. A login that needs no binding always runs on
/// <see cref="SslStream"/>.
/// </remarks>
internal static class Tls
{
    public const int BindingLength = 32;
    private const string Alpn = "rastcp/1";
    private const string ExporterLabel = "EXPORTER-Channel-Binding";

    /// <summary>What an exchange on a unix socket binds itself to, in place of the exporter a socket does not have.</summary>
    public static ReadOnlySpan<byte> UnixBinding => "rastcp unix socket: no exporter."u8;

    public static async ValueTask<(Stream Stream, byte[]? Binding)> Open(Socket socket, ConnectionSettings settings, bool needBinding, bool async, CancellationToken cancellation)
    {
        var engine = settings.TlsEngine;
        if (engine == RasterizedDbTlsEngine.Auto)
        {
            engine = needBinding && !PlatformExporter.IsSupported ? RasterizedDbTlsEngine.Managed : RasterizedDbTlsEngine.Platform;
        }

        if (engine == RasterizedDbTlsEngine.Managed)
        {
            var managed = new ManagedTlsStream(new NetworkStream(socket, ownsSocket: true), settings);
            try
            {
                await managed.Handshake(async, cancellation).ConfigureAwait(false);
            }
            catch
            {
                managed.Dispose();
                throw;
            }

            return (managed, managed.ChannelBinding);
        }

        var ssl = new SslStream(new NetworkStream(socket, ownsSocket: true), leaveInnerStreamOpen: false);
        try
        {
            var options = new SslClientAuthenticationOptions
            {
                TargetHost = settings.Host,
                ApplicationProtocols = [new SslApplicationProtocol(Alpn)],
                EnabledSslProtocols = SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                RemoteCertificateValidationCallback = (_, certificate, chain, errors) => Check(settings, certificate, chain, errors),
            };
            if (settings.Trust == RasterizedDbTrust.RootCa)
            {
                options.CertificateChainPolicy = RootPolicy(settings);
            }

            if (async)
            {
                await ssl.AuthenticateAsClientAsync(options, cancellation).ConfigureAwait(false);
            }
            else
            {
                ssl.AuthenticateAsClient(options);
            }

            if (ssl.NegotiatedApplicationProtocol != new SslApplicationProtocol(Alpn))
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "the server did not agree to speak rastcp/1: it is not a RasterizedDB native listener");
            }

            byte[]? binding = null;
            if (needBinding)
            {
                binding = PlatformExporter.Export(ssl)
                    ?? throw new RasterizedDbException(
                        RasterizedDbErrorKind.Authentication,
                        "this platform's TLS does not give out the channel binding a password login signs over; set Tls Engine=Managed in the connection string");
            }

            return (ssl, binding);
        }
        catch
        {
            ssl.Dispose();
            throw;
        }
    }

    private static X509ChainPolicy RootPolicy(ConnectionSettings settings)
    {
        var policy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
        };
        policy.CustomTrustStore.AddRange(settings.RootCertificates!);
        return policy;
    }

    private static bool Check(ConnectionSettings settings, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
    {
        switch (settings.Trust)
        {
            case RasterizedDbTrust.AcceptAny:
                return true;
            case RasterizedDbTrust.Pinned:
                return certificate is not null && IsPinned(settings, certificate.GetRawCertData());
            default:
                return errors == SslPolicyErrors.None;
        }
    }

    /// <summary>
    /// Whether a certificate is one of the pinned ones, by SHA-256 over its DER encoding. Every pin is
    /// compared, in constant time: a timing difference would let a caller learn a pin a byte at a time.
    /// </summary>
    internal static bool IsPinned(ConnectionSettings settings, ReadOnlySpan<byte> der)
    {
        Span<byte> presented = stackalloc byte[32];
        SHA256.HashData(der, presented);
        var matched = false;
        foreach (var pin in settings.Fingerprints)
        {
            matched |= CryptographicOperations.FixedTimeEquals(presented, pin);
        }

        return matched;
    }

    /// <summary>The chain and name checks the managed TLS leaves to its caller.</summary>
    internal static void CheckManaged(ConnectionSettings settings, byte[][] chainDer)
    {
        if (chainDer.Length == 0)
        {
            throw new TlsFatalAlert(AlertDescription.bad_certificate);
        }

        switch (settings.Trust)
        {
            case RasterizedDbTrust.AcceptAny:
                return;
            case RasterizedDbTrust.Pinned:
                if (!IsPinned(settings, chainDer[0]))
                {
                    throw new TlsFatalAlert(AlertDescription.bad_certificate, "the server's certificate is not among the pinned ones");
                }

                return;
        }

#if NET9_0_OR_GREATER
        using var leaf = X509CertificateLoader.LoadCertificate(chainDer[0]);
#else
        using var leaf = new X509Certificate2(chainDer[0]);
#endif
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        if (settings.Trust == RasterizedDbTrust.RootCa)
        {
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.AddRange(settings.RootCertificates!);
        }

        for (var i = 1; i < chainDer.Length; i++)
        {
#if NET9_0_OR_GREATER
            chain.ChainPolicy.ExtraStore.Add(X509CertificateLoader.LoadCertificate(chainDer[i]));
#else
            chain.ChainPolicy.ExtraStore.Add(new X509Certificate2(chainDer[i]));
#endif
        }

        if (!chain.Build(leaf))
        {
            throw new TlsFatalAlert(AlertDescription.bad_certificate, "the server's certificate chain is not trusted");
        }

        if (!leaf.MatchesHostname(settings.Host!))
        {
            throw new TlsFatalAlert(AlertDescription.bad_certificate, $"the server's certificate is not for {settings.Host}");
        }
    }

    /// <summary>
    /// Reads the RFC 5705 exporter from the OpenSSL session under an <see cref="SslStream"/>. The handle
    /// is reached through the runtime's internals, each step checked by name and type, so a runtime laid
    /// out differently answers "not supported" and the managed TLS is used instead.
    /// </summary>
    private static class PlatformExporter
    {
        private static readonly FieldInfo? ContextField;
        private static readonly PropertyInfo? HandleProperty;
        private static readonly nint ExportFunction;

        public static bool IsSupported => ExportFunction != 0;

        static PlatformExporter()
        {
            if (!OperatingSystem.IsLinux() && !OperatingSystem.IsFreeBSD())
            {
                return;
            }

            try
            {
                var field = typeof(SslStream).GetField("_securityContext", BindingFlags.Instance | BindingFlags.NonPublic);
                var contextType = field?.FieldType.Assembly.GetType("System.Net.Security.SafeDeleteSslContext");
                var property = contextType?.GetProperty("SslContext", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (field is null || property is null || !typeof(SafeHandle).IsAssignableFrom(property.PropertyType)
                    || property.PropertyType.Name != "SafeSslHandle")
                {
                    return;
                }

                // The library the runtime itself loaded: the handle is only meaningful to that one.
                foreach (var name in (ReadOnlySpan<string>)["libssl.so.3", "libssl.so.1.1"])
                {
                    if (NativeLibrary.TryLoad(name, out var library) && NativeLibrary.TryGetExport(library, "SSL_export_keying_material", out var function))
                    {
                        ContextField = field;
                        HandleProperty = property;
                        ExportFunction = function;
                        return;
                    }
                }
            }
            catch (Exception)
            {
                // Not supported here.
            }
        }

        public static unsafe byte[]? Export(SslStream stream)
        {
            if (ExportFunction == 0)
            {
                return null;
            }

            try
            {
                var context = ContextField!.GetValue(stream);
                if (context is null || !HandleProperty!.DeclaringType!.IsInstanceOfType(context) || HandleProperty.GetValue(context) is not SafeHandle handle || handle.IsInvalid)
                {
                    return null;
                }

                var output = new byte[BindingLength];
                var label = System.Text.Encoding.ASCII.GetBytes(ExporterLabel);
                var added = false;
                try
                {
                    handle.DangerousAddRef(ref added);
                    int result;
                    fixed (byte* outPtr = output)
                    fixed (byte* labelPtr = label)
                    {
                        // int SSL_export_keying_material(SSL*, unsigned char* out, size_t olen, const char* label,
                        //     size_t llen, const unsigned char* context, size_t contextlen, int use_context)
                        byte empty = 0;
                        result = ((delegate* unmanaged[Cdecl]<nint, byte*, nuint, byte*, nuint, byte*, nuint, int, int>)ExportFunction)(
                            handle.DangerousGetHandle(), outPtr, (nuint)output.Length, labelPtr, (nuint)label.Length, &empty, 0, 1);
                    }

                    return result == 1 ? output : null;
                }
                finally
                {
                    if (added)
                    {
                        handle.DangerousRelease();
                    }
                }
            }
            catch (Exception)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// TLS 1.3 by BouncyCastle's non-blocking engine, driven over the socket here so reads and writes
    /// stay asynchronous.
    /// </summary>
    private sealed class ManagedTlsStream(Stream inner, ConnectionSettings settings) : Stream
    {
        private readonly TlsClientProtocol _protocol = new();
        private readonly byte[] _net = new byte[16 * 1024 + 512];
        private byte[] _out = new byte[16 * 1024 + 512];
        private Client? _client;

        public byte[]? ChannelBinding => _client?.Binding;

        public async ValueTask Handshake(bool async, CancellationToken cancellation)
        {
            _client = new Client(settings);
            _protocol.Connect(_client);
            while (true)
            {
                await Flush(async, cancellation).ConfigureAwait(false);
                if (!_protocol.IsHandshaking)
                {
                    break;
                }

                await Fill(async, cancellation).ConfigureAwait(false);
            }

            if (_client.Binding is null)
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Authentication, "this connection exports no channel binding");
            }

            if (_client.Protocol != Alpn)
            {
                throw new RasterizedDbException(RasterizedDbErrorKind.Protocol, "the server did not agree to speak rastcp/1: it is not a RasterizedDB native listener");
            }
        }

        private async ValueTask Flush(bool async, CancellationToken cancellation)
        {
            int available;
            while ((available = _protocol.GetAvailableOutputBytes()) > 0)
            {
                if (_out.Length < available)
                {
                    _out = new byte[available];
                }

                var n = _protocol.ReadOutput(_out, 0, available);
                if (async)
                {
                    await inner.WriteAsync(_out.AsMemory(0, n), cancellation).ConfigureAwait(false);
                }
                else
                {
                    inner.Write(_out, 0, n);
                }
            }
        }

        private async ValueTask Fill(bool async, CancellationToken cancellation)
        {
            var n = async ? await inner.ReadAsync(_net, cancellation).ConfigureAwait(false) : inner.Read(_net, 0, _net.Length);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }

            _protocol.OfferInput(_net, 0, n);
        }

        private async ValueTask<int> ReadCore(Memory<byte> buffer, bool async, CancellationToken cancellation)
        {
            if (buffer.IsEmpty)
            {
                return 0;
            }

            while (true)
            {
                var available = _protocol.GetAvailableInputBytes();
                if (available > 0)
                {
                    var take = Math.Min(available, buffer.Length);
                    if (MemoryMarshal.TryGetArray<byte>(buffer, out var segment))
                    {
                        return _protocol.ReadInput(segment.Array!, segment.Offset, take);
                    }

                    var copy = new byte[take];
                    var read = _protocol.ReadInput(copy, 0, take);
                    copy.AsSpan(0, read).CopyTo(buffer.Span);
                    return read;
                }

                if (_protocol.IsClosed)
                {
                    return 0;
                }

                var n = async ? await inner.ReadAsync(_net, cancellation).ConfigureAwait(false) : inner.Read(_net, 0, _net.Length);
                if (n == 0)
                {
                    return 0;
                }

                _protocol.OfferInput(_net, 0, n);
                // A key update or a session ticket may want an answer.
                await Flush(async, cancellation).ConfigureAwait(false);
            }
        }

        private async ValueTask WriteCore(ReadOnlyMemory<byte> buffer, bool async, CancellationToken cancellation)
        {
            if (MemoryMarshal.TryGetArray(buffer, out var segment))
            {
                _protocol.WriteApplicationData(segment.Array!, segment.Offset, segment.Count);
            }
            else
            {
                var copy = buffer.ToArray();
                _protocol.WriteApplicationData(copy, 0, copy.Length);
            }

            await Flush(async, cancellation).ConfigureAwait(false);
        }

        public override int Read(byte[] buffer, int offset, int count) => ReadCore(buffer.AsMemory(offset, count), false, default).GetAwaiter().GetResult();

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => ReadCore(buffer, true, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadCore(buffer.AsMemory(offset, count), true, cancellationToken).AsTask();

        public override void Write(byte[] buffer, int offset, int count) => WriteCore(buffer.AsMemory(offset, count), false, default).GetAwaiter().GetResult();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => WriteCore(buffer, true, cancellationToken);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            WriteCore(buffer.AsMemory(offset, count), true, cancellationToken).AsTask();

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                try
                {
                    _protocol.Close();
                }
                catch (Exception)
                {
                    // The connection is going away either way.
                }

                inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        private sealed class Client(ConnectionSettings settings) : DefaultTlsClient(new BcTlsCrypto())
        {
            public byte[]? Binding;
            public string? Protocol;

            protected override ProtocolVersion[] GetSupportedVersions() => ProtocolVersion.TLSv13.Only();

            protected override IList<ProtocolName> GetProtocolNames() => [ProtocolName.AsUtf8Encoding(Alpn)];

            protected override IList<ServerName>? GetSniServerNames() =>
                IPAddress.TryParse(settings.Host, out _) ? null : [new ServerName(NameType.host_name, System.Text.Encoding.ASCII.GetBytes(settings.Host!))];

            public override TlsAuthentication GetAuthentication() => new Authentication(settings);

            public override void NotifyHandshakeComplete()
            {
                base.NotifyHandshakeComplete();
                Protocol = m_context.SecurityParameters.ApplicationProtocol?.GetUtf8Decoding();
                Binding = m_context.ExportKeyingMaterial(ExporterLabel, [], BindingLength);
            }
        }

        private sealed class Authentication(ConnectionSettings settings) : TlsAuthentication
        {
            public void NotifyServerCertificate(TlsServerCertificate serverCertificate)
            {
                var list = serverCertificate.Certificate.GetCertificateList();
                var der = new byte[list.Length][];
                for (var i = 0; i < list.Length; i++)
                {
                    der[i] = list[i].GetEncoded();
                }

                CheckManaged(settings, der);
            }

            public TlsCredentials? GetClientCredentials(Org.BouncyCastle.Tls.CertificateRequest certificateRequest) => null;
        }
    }
}
