using System.Data.Common;
using System.Globalization;

namespace RasterizedDb.Messaging;

/// <summary>Why the broker refused a request.</summary>
public enum BrokerErrorCode : ushort
{
    Unknown = 1,
    Protocol = 2,
    AuthRequired = 3,
    AuthFailed = 4,
    Permission = 5,
    NoTopic = 6,
    TopicExists = 7,
    NoPartition = 8,
    BadBatch = 9,
    TooLarge = 10,
    OffsetOutOfRange = 11,
    NoSubscription = 12,
    NoExchange = 13,
    /// <summary>Declared again with different settings.</summary>
    Mismatch = 14,
    Unroutable = 15,
    Storage = 16,
    ShuttingDown = 17,
    Invalid = 18,
    Fenced = 19,
    OutOfOrder = 20,
    Unsupported = 21,
    /// <summary>A queue at its maximum length that rejects on overflow.</summary>
    Full = 22,
}

/// <summary>An error from the broker (<see cref="Code"/> is set) or from the connection to it (it is null).</summary>
public sealed class BrokerException : Exception
{
    public BrokerException(string message, BrokerErrorCode? code = null, Exception? inner = null)
        : base(code is null ? message : $"{message} ({code})", inner)
    {
        Code = code;
    }

    public BrokerErrorCode? Code { get; }

    /// <summary>The connection is gone: every pending request and every subscription on it has ended.</summary>
    public bool IsConnectionLost => Code is null;
}

/// <summary>How a topic's appends are made durable.</summary>
public enum BrokerDurability : byte
{
    /// <summary>Acknowledged once on disk, by a group commit.</summary>
    Sync = 0,
    Async = 1,
    Memory = 2,
}

/// <summary>What a producer waits for.</summary>
public enum BrokerAck : byte
{
    /// <summary>Nothing: the request is sent and forgotten.</summary>
    None = 0,
    Written = 1,
    Durable = 2,
}

public enum BrokerSubscriptionMode : byte
{
    /// <summary>One consumer per partition, in order.</summary>
    Ordered = 0,
    /// <summary>A work queue: each message goes to one member, acknowledged one by one.</summary>
    Shared = 1,
    /// <summary>Every subscriber gets every message.</summary>
    Broadcast = 2,
}

public enum BrokerExchangeKind : byte
{
    Direct = 0,
    Fanout = 1,
    Topic = 2,
    Headers = 3,
    Mqtt = 4,
}

/// <summary>Where a new subscription starts reading.</summary>
public readonly record struct BrokerStart
{
    internal byte Kind { get; private init; }

    internal long Value { get; private init; }

    public static BrokerStart Earliest => new() { Kind = 0 };

    public static BrokerStart Latest => new() { Kind = 1 };

    public static BrokerStart Offset(ulong offset) => new() { Kind = 2, Value = unchecked((long)offset) };

    public static BrokerStart Time(DateTime utc) => new() { Kind = 3, Value = (utc.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) / 10 };
}

public sealed record BrokerTopicConfig
{
    public uint Partitions { get; init; } = 1;
    public BrokerDurability Durability { get; init; } = BrokerDurability.Sync;
    /// <summary>How long messages are kept; null keeps them whatever their age.</summary>
    public TimeSpan? Retention { get; init; } = TimeSpan.FromDays(7);
    /// <summary>How many bytes are kept; -1 for no limit.</summary>
    public long RetentionBytes { get; init; } = -1;
    public ulong SegmentBytes { get; init; } = 256UL << 20;
    public uint MaxBatchBytes { get; init; } = 16 << 20;
    public ulong MemoryBytes { get; init; } = 64UL << 20;
}

public sealed record BrokerQueueConfig
{
    public BrokerDurability Durability { get; init; } = BrokerDurability.Sync;
    /// <summary>How long a delivered message may stay unacknowledged before it is delivered again.</summary>
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Deliveries after which a message is dead-lettered; 0 for no limit.</summary>
    public uint MaxDeliveries { get; init; }
    /// <summary>The queue that takes messages past <see cref="MaxDeliveries"/>; empty for none.</summary>
    public string DeadLetter { get; init; } = "";
    public TimeSpan? MessageTtl { get; init; }
    /// <summary>The most messages the queue holds; -1 for no limit.</summary>
    public long MaxLength { get; init; } = -1;
}

public sealed record BrokerSubscriptionConfig
{
    public BrokerSubscriptionMode Mode { get; init; } = BrokerSubscriptionMode.Shared;
    public BrokerStart Start { get; init; } = BrokerStart.Earliest;
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public uint MaxDeliveries { get; init; }
    public string DeadLetter { get; init; } = "";
}

/// <summary>A topic or queue by the id the broker gave its name.</summary>
public sealed record BrokerTopic(string Name, uint Id, uint Partitions, BrokerDurability Durability, bool IsQueue);

/// <summary>A topic's settings and depth, for monitoring.</summary>
public sealed record BrokerTopicInfo(uint Id, string Name, BrokerTopicConfig Config, bool IsQueue, ulong Messages, ulong Bytes);

public readonly record struct BrokerProduceResult(uint Partition, ulong BaseOffset, bool Duplicate);

/// <summary>A header of a message: a name and bytes.</summary>
public readonly record struct BrokerHeader(string Name, ReadOnlyMemory<byte> Value)
{
    public BrokerHeader(string name, string value)
        : this(name, System.Text.Encoding.UTF8.GetBytes(value))
    {
    }

    public string ValueAsString => System.Text.Encoding.UTF8.GetString(Value.Span);
}

/// <summary>
/// A message as stored. Its bytes are slices of the frame it arrived in, which lives as long as any
/// message of that frame is referenced.
/// </summary>
public sealed class BrokerMessage
{
    internal BrokerMessage(uint partition, ulong offset, DateTime time, bool hasKey, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte> headers, int headerCount)
    {
        Partition = partition;
        Offset = offset;
        Time = time;
        HasKey = hasKey;
        Key = key;
        Value = value;
        _headers = headers;
        _headerCount = headerCount;
    }

    private readonly ReadOnlyMemory<byte> _headers;
    private readonly int _headerCount;
    private BrokerHeader[]? _parsed;

    public uint Partition { get; }

    public ulong Offset { get; }

    /// <summary>When the broker stored it (or the producer's own time, if it set one), in UTC.</summary>
    public DateTime Time { get; }

    public bool HasKey { get; }

    public ReadOnlyMemory<byte> Key { get; }

    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>
    /// The message's headers. A dead-lettered message carries <c>x-death-reason</c>,
    /// <c>x-death-topic</c> and <c>x-death-subscription</c>.
    /// </summary>
    public IReadOnlyList<BrokerHeader> Headers => _parsed ??= BrokerBatch.ParseHeaders(_headers, _headerCount);

    /// <summary>The value of the first header named <paramref name="name"/>, as text; null when there is none.</summary>
    public string? Header(string name)
    {
        foreach (var header in Headers)
        {
            if (header.Name == name)
            {
                return header.ValueAsString;
            }
        }

        return null;
    }
}

/// <summary>
/// How to reach a broker: <c>Host=mq.example.com;Port=7171;Username=app;Password=…</c>, or
/// <c>Socket=/run/rdb/mq.sock</c>.
/// </summary>
/// <remarks>
/// Over plain TCP the password crosses the network as it is: use <c>Tls=true</c> against a listener
/// that has TLS, a unix socket, or a network you trust.
/// </remarks>
public sealed class RasterizedDbBrokerOptions
{
    public const int DefaultPort = 7171;

    public string? Host { get; set; }
    public int Port { get; set; } = DefaultPort;
    /// <summary>A unix socket's path, instead of <see cref="Host"/>.</summary>
    public string? Socket { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string ClientName { get; set; } = "RasterizedDb.Messaging";
    /// <summary>How long a request waits for its reply.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
    public bool Tls { get; set; }
    /// <summary>With <see cref="Tls"/>: accept any server certificate. Only for a network you trust.</summary>
    public bool TrustServerCertificate { get; set; }

    public static RasterizedDbBrokerOptions Parse(string connectionString)
    {
        var options = new RasterizedDbBrokerOptions();
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        foreach (string key in builder.Keys)
        {
            var value = Convert.ToString(builder[key], CultureInfo.InvariantCulture) ?? "";
            switch (key.Replace(" ", "").ToLowerInvariant())
            {
                case "host" or "server":
                    options.Host = value;
                    break;
                case "port":
                    options.Port = int.Parse(value, CultureInfo.InvariantCulture);
                    break;
                case "socket" or "unixsocket":
                    options.Socket = value;
                    break;
                case "username" or "user" or "userid" or "uid":
                    options.Username = value;
                    break;
                case "password" or "pwd":
                    options.Password = value;
                    break;
                case "clientname" or "applicationname":
                    options.ClientName = value;
                    break;
                case "timeout" or "commandtimeout":
                    options.Timeout = TimeSpan.FromSeconds(double.Parse(value, CultureInfo.InvariantCulture));
                    break;
                case "tls" or "ssl":
                    options.Tls = bool.Parse(value);
                    break;
                case "trustservercertificate":
                    options.TrustServerCertificate = bool.Parse(value);
                    break;
                default:
                    throw new ArgumentException($"\"{key}\" is not a RasterizedDB broker connection string keyword");
            }
        }

        if (options.Socket is null && options.Host is { } host && host.StartsWith("unix:", StringComparison.Ordinal))
        {
            options.Socket = host[5..];
            options.Host = null;
        }

        if (options.Socket is null && string.IsNullOrWhiteSpace(options.Host))
        {
            throw new ArgumentException("the connection string names no broker: set Host (and Port), or Socket");
        }

        return options;
    }
}
