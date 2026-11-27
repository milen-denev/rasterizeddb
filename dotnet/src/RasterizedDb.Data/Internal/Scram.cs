using System.Security.Cryptography;
using System.Text;

namespace RasterizedDb.Data.Internal;

/// <summary>
/// The client side of SCRAM-SHA-256 (RFC 5802, RFC 7677), bound to the connection it runs on (RFC 9266
/// <c>tls-exporter</c>, or rastcp's unix-socket binding). The same exchange as the client crate's
/// <c>scram.rs</c> and the server's <c>auth::ScramClient</c>.
/// </summary>
internal sealed class ScramClient
{
    private const int MinIterations = 4096;
    private const int MaxIterations = 1_000_000;
    private const int NonceLength = 18;
    private const string Gs2Header = "p=tls-exporter,,";

    private readonly string _password;
    private readonly byte[] _binding;
    private readonly string _clientFirstBare;
    private readonly string _clientNonce;
    private byte[]? _serverSignature;

    public string ClientFirst { get; }

    public ScramClient(string user, string password, ReadOnlySpan<byte> binding)
    {
        _password = password;
        _binding = binding.ToArray();
        _clientNonce = MakeNonce();
        _clientFirstBare = $"n={user.Replace("=", "=3D").Replace(",", "=2C")},r={_clientNonce}";
        ClientFirst = Gs2Header + _clientFirstBare;
    }

    /// <summary>The client-final message for the server's first one.</summary>
    public string Respond(string serverFirst)
    {
        var nonce = Field(serverFirst, 'r') ?? throw Fail("SCRAM server-first has no nonce");
        // The server's nonce must extend the client's: otherwise a replayed server-first from an
        // earlier exchange would be accepted.
        if (!nonce.StartsWith(_clientNonce, StringComparison.Ordinal) || nonce.Length == _clientNonce.Length)
        {
            throw Fail("SCRAM server-first does not extend the client nonce");
        }

        byte[] salt;
        try
        {
            salt = Convert.FromBase64String(Field(serverFirst, 's') ?? throw Fail("SCRAM server-first has no salt"));
        }
        catch (FormatException)
        {
            throw Fail("SCRAM salt is not valid base64");
        }

        if (!int.TryParse(Field(serverFirst, 'i') ?? throw Fail("SCRAM server-first has no iteration count"), out var iterations))
        {
            throw Fail("SCRAM iteration count is not a number");
        }

        if (iterations < MinIterations)
        {
            throw Fail($"SCRAM iteration count {iterations} is below the {MinIterations} this client accepts");
        }

        if (iterations > MaxIterations)
        {
            throw Fail($"SCRAM iteration count {iterations} is above the {MaxIterations} this client accepts");
        }

        Span<byte> salted = stackalloc byte[32];
        Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(_password), salt, salted, iterations, HashAlgorithmName.SHA256);
        Span<byte> clientKey = stackalloc byte[32];
        Span<byte> storedKey = stackalloc byte[32];
        Span<byte> serverKey = stackalloc byte[32];
        HMACSHA256.HashData(salted, "Client Key"u8, clientKey);
        SHA256.HashData(clientKey, storedKey);
        HMACSHA256.HashData(salted, "Server Key"u8, serverKey);

        var channel = new byte[Gs2Header.Length + _binding.Length];
        Encoding.ASCII.GetBytes(Gs2Header, channel);
        _binding.CopyTo(channel, Gs2Header.Length);
        var withoutProof = $"c={Convert.ToBase64String(channel)},r={nonce}";
        var authMessage = Encoding.UTF8.GetBytes($"{_clientFirstBare},{serverFirst},{withoutProof}");

        Span<byte> proof = stackalloc byte[32];
        HMACSHA256.HashData(storedKey, authMessage, proof);
        for (var i = 0; i < proof.Length; i++)
        {
            proof[i] ^= clientKey[i];
        }

        _serverSignature = HMACSHA256.HashData(serverKey, authMessage);
        CryptographicOperations.ZeroMemory(salted);
        CryptographicOperations.ZeroMemory(clientKey);
        return $"{withoutProof},p={Convert.ToBase64String(proof)}";
    }

    /// <summary>
    /// Checks the server's signature. Not optional: without it this side has proved itself to whoever
    /// answered.
    /// </summary>
    public void Verify(string serverFinal)
    {
        var expected = _serverSignature ?? throw Fail("the exchange has not reached the server-final message");
        byte[] signature;
        try
        {
            signature = Convert.FromBase64String(Field(serverFinal, 'v') ?? throw Fail("SCRAM server-final has no signature"));
        }
        catch (FormatException)
        {
            throw Fail("SCRAM server signature is not valid base64");
        }

        if (!CryptographicOperations.FixedTimeEquals(signature, expected))
        {
            throw Fail("the server did not prove it knows this password");
        }
    }

    private static RasterizedDbException Fail(string message) => new(RasterizedDbErrorKind.Authentication, message);

    private static string MakeNonce()
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        // GetString samples without bias.
        return RandomNumberGenerator.GetString(alphabet, NonceLength);
    }

    private static string? Field(string message, char key)
    {
        foreach (var part in message.Split(','))
        {
            if (part.Length >= 2 && part[0] == key && part[1] == '=')
            {
                return part[2..];
            }
        }

        return null;
    }
}
