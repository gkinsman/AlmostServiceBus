using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AlmostServiceBus.Tests.Shared;

// The SQL filter/action compatibility table (tests/shared/sql-compat-cases.json): each case is a set of rules,
// one message, and the outcome real Azure Service Bus produced. Shared by the broker-level unit runner and the
// conformance suite; deliberately free of SDK and emulator types.

public sealed class SqlCompatTable
{
    public List<SqlCompatCase> Cases { get; set; } = [];
}

public sealed class SqlCompatCase
{
    public string Id { get; set; } = "";
    public string Area { get; set; } = "";
    public List<SqlCompatRule> Rules { get; set; } = [];
    public SqlCompatSubscription Subscription { get; set; } = new();
    public SqlCompatMessage Message { get; set; } = new();
    public SqlCompatExpected Expected { get; set; } = new();

    /// <summary><c>azure</c> when <see cref="Expected"/> was recorded from real Azure.</summary>
    public string Verified { get; set; } = "doc";

    public string? Note { get; set; }

    /// <summary>Set for a case the emulator can't model; <see cref="Note"/> says why.</summary>
    public bool? SkipEmulator { get; set; }

    /// <summary>
    /// The filter actually installed: every filter is guarded by <c>user.caseId</c> so that, when all cases share one
    /// topic, only the case's own message can match. <c>TRUE AND x</c> keeps x's result, unknown included.
    /// </summary>
    public string GuardedFilter(SqlCompatRule rule) =>
        rule.Filter is null ? $"user.caseId = '{Id}'" : $"user.caseId = '{Id}' AND ({rule.Filter})";

    public override string ToString() => Id;
}

public sealed class SqlCompatRule
{
    /// <summary>The SQL filter; null means the guard alone (or a correlation filter when <see cref="Correlation"/>).</summary>
    public string? Filter { get; set; }

    public Dictionary<string, TypedValue>? FilterParameters { get; set; }
    public string? Action { get; set; }
    public Dictionary<string, TypedValue>? ActionParameters { get; set; }

    /// <summary>A correlation filter on <c>caseId</c> instead of a SQL filter.</summary>
    public bool? Correlation { get; set; }
}

public sealed class SqlCompatSubscription
{
    public bool DeadLetterOnFilterExceptions { get; set; } = true;
}

public sealed class SqlCompatMessage
{
    /// <summary>
    /// System properties the sender sets: Label, To, ReplyTo, CorrelationId, ContentType, ReplyToSessionId,
    /// SessionId, PartitionKey, MessageId, TimeToLive (TimeSpan "c" format).
    /// </summary>
    public Dictionary<string, string?> Sys { get; set; } = [];

    public Dictionary<string, TypedValue> User { get; set; } = [];
}

public sealed class SqlCompatExpected
{
    /// <summary>Match, NoMatch, Rejected or DeadLettered (any dead-lettered copy).</summary>
    public string Outcome { get; set; } = "";

    public List<SqlCompatCopy> Copies { get; set; } = [];
    public List<SqlCompatDeadLetter> DeadLettered { get; set; } = [];
    public SqlCompatError? Error { get; set; }
}

public sealed class SqlCompatCopy
{
    /// <summary>Every application property except <c>caseId</c>.</summary>
    public Dictionary<string, TypedValue> User { get; set; } = [];

    /// <summary>Every non-null system property among <see cref="SqlCompatCases.ObservedSys"/>.</summary>
    public Dictionary<string, string?> Sys { get; set; } = [];
}

public sealed class SqlCompatDeadLetter
{
    public string? Reason { get; set; }
    public string? Description { get; set; }

    /// <summary>Application properties except <c>caseId</c> and the DeadLetter* ones.</summary>
    public Dictionary<string, TypedValue> User { get; set; } = [];
}

public sealed class SqlCompatError
{
    public string Type { get; set; } = "";

    /// <summary>The service's message without its TrackingId tail; null when the SDK rejected it client-side.</summary>
    public string? Message { get; set; }
}

/// <summary>A CLR value with its type, so the JSON table can carry ints, longs, Guids, TimeSpans, nulls…</summary>
public sealed class TypedValue : IEquatable<TypedValue>
{
    public string Type { get; set; } = "null";
    public string? Value { get; set; }

    public static TypedValue From(object? value)
    {
        var inv = CultureInfo.InvariantCulture;
        var (type, text) = value switch
        {
            null => ("null", (string?)null),
            string s => ("string", s),
            int i => ("int", i.ToString(inv)),
            long l => ("long", l.ToString(inv)),
            uint u => ("uint", u.ToString(inv)),
            ulong u => ("ulong", u.ToString(inv)),
            short s => ("short", s.ToString(inv)),
            ushort s => ("ushort", s.ToString(inv)),
            byte b => ("byte", b.ToString(inv)),
            sbyte b => ("sbyte", b.ToString(inv)),
            char c => ("char", c.ToString()),
            float f => ("float", f.ToString("R", inv)),
            double d => ("double", d.ToString("R", inv)),
            decimal m => ("decimal", m.ToString(inv)),
            bool b => ("bool", b ? "true" : "false"),
            DateTime dt => ("datetime", dt.ToUniversalTime().ToString("o", inv)),
            DateTimeOffset dto => ("datetimeoffset", dto.ToUniversalTime().ToString("o", inv)),
            Guid g => ("guid", g.ToString("D")),
            TimeSpan ts => ("timespan", ts.ToString("c", inv)),
            Uri u => ("uri", u.OriginalString),
            _ => throw new ArgumentException($"Unsupported value type {value.GetType()}", nameof(value))
        };
        return new TypedValue { Type = type, Value = text };
    }

    public object? ToClr()
    {
        var inv = CultureInfo.InvariantCulture;
        var v = Value!;
        return Type switch
        {
            "null" => null,
            "string" => v,
            "int" => int.Parse(v, inv),
            "long" => long.Parse(v, inv),
            "uint" => uint.Parse(v, inv),
            "ulong" => ulong.Parse(v, inv),
            "short" => short.Parse(v, inv),
            "ushort" => ushort.Parse(v, inv),
            "byte" => byte.Parse(v, inv),
            "sbyte" => sbyte.Parse(v, inv),
            "char" => v[0],
            "float" => float.Parse(v, inv),
            "double" => double.Parse(v, inv),
            "decimal" => decimal.Parse(v, inv),
            "bool" => v == "true",
            "datetime" => DateTime.Parse(v, inv, DateTimeStyles.RoundtripKind).ToUniversalTime(),
            "datetimeoffset" => DateTimeOffset.Parse(v, inv),
            "guid" => Guid.Parse(v),
            "timespan" => TimeSpan.ParseExact(v, "c", inv),
            "uri" => new Uri(v, UriKind.RelativeOrAbsolute),
            _ => throw new InvalidOperationException($"Unknown typed value '{Type}'")
        };
    }

    public bool Equals(TypedValue? other) => other is not null && Type == other.Type && Value == other.Value;
    public override bool Equals(object? obj) => Equals(obj as TypedValue);
    public override int GetHashCode() => HashCode.Combine(Type, Value);
    public override string ToString() => Type == "null" ? "null" : $"{Value} ({Type})";
}

public static class SqlCompatCases
{
    public const string FileName = "sql-compat-cases.json";

    /// <summary>The system properties compared on delivered copies.</summary>
    public static readonly string[] ObservedSys =
    [
        "Subject", "To", "CorrelationId", "MessageId", "ReplyTo", "ReplyToSessionId", "SessionId", "ContentType",
        "PartitionKey", "TimeToLive", "ScheduledEnqueueTime"
    ];

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly Lazy<IReadOnlyList<SqlCompatCase>> Cached = new(() =>
        JsonSerializer.Deserialize<SqlCompatTable>(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, FileName)), JsonOptions)!.Cases);

    /// <summary>The table copied next to the test assembly.</summary>
    public static IReadOnlyList<SqlCompatCase> Load() => Cached.Value;

    public static SqlCompatCase Get(string id) => Load().Single(c => c.Id == id);

    /// <summary>The table's path in the source tree (for record mode).</summary>
    public static string SourcePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "AlmostServiceBus.sln")))
            dir = dir.Parent;
        return dir is null
            ? throw new InvalidOperationException("Can't find AlmostServiceBus.sln above " + AppContext.BaseDirectory)
            : Path.Combine(dir.FullName, "tests", "shared", FileName);
    }

    public static void Save(IReadOnlyList<SqlCompatCase> cases, string? path = null) =>
        File.WriteAllText(path ?? SourcePath(),
            JsonSerializer.Serialize(new SqlCompatTable { Cases = [.. cases] }, JsonOptions).ReplaceLineEndings("\n") + "\n");
}
