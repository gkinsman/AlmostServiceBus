using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Amqp.Types;

namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// Value semantics of Service Bus SQL, as observed on Azure: CLR values compared and combined with C# operator
/// rules, C# <c>null</c> equality, and a separate <see cref="Missing"/> state for absent user properties that
/// makes any comparison unknown. Type errors throw <see cref="SqlEvaluationException"/> with Azure's wording.
/// </summary>
internal static class SqlValues
{
    /// <summary>An absent user property: comparisons and arithmetic involving it are unknown.</summary>
    public static readonly object Missing = new MissingValue();

    private const string TimeSpanDescriptor = "com.microsoft:timespan";
    private const string UriDescriptor = "com.microsoft:uri";
    private const string DateTimeOffsetDescriptor = "com.microsoft:datetime-offset";

    private const string GuidFormatMessage =
        "Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).";

    private sealed class MissingValue
    {
        public override string ToString() => "<missing>";
    }

    // ── AMQP mapping ─────────────────────────────────────────────────────────

    /// <summary>Decodes an application property value as received over AMQP into the CLR value Azure evaluates.</summary>
    public static object? FromMessage(object? raw) => raw switch
    {
        DescribedValue { Descriptor: var d, Value: long ticks } when Is(d, TimeSpanDescriptor) => new TimeSpan(ticks),
        DescribedValue { Descriptor: var d, Value: string uri } when Is(d, UriDescriptor) => new Uri(uri, UriKind.RelativeOrAbsolute),
        DescribedValue { Descriptor: var d, Value: long ticks } when Is(d, DateTimeOffsetDescriptor) => new DateTimeOffset(ticks, TimeSpan.Zero),
        Symbol symbol => (string)symbol,
        DateTime { Kind: not DateTimeKind.Utc } dt => dt.ToUniversalTime(),
        _ => raw
    };

    /// <summary>Encodes a value written by an action so it serialises the way the .NET SDK encodes it.</summary>
    public static object? ToMessage(object? value) => value switch
    {
        TimeSpan ts => new DescribedValue(new Symbol(TimeSpanDescriptor), ts.Ticks),
        Uri uri => new DescribedValue(new Symbol(UriDescriptor), uri.OriginalString),
        DateTimeOffset dto => new DescribedValue(new Symbol(DateTimeOffsetDescriptor), dto.UtcTicks),
        _ => value
    };

    private static bool Is(object descriptor, string name) => descriptor is Symbol s && (string)s == name;

    // ── Comparison ───────────────────────────────────────────────────────────

    /// <summary><c>=</c>: <c>true</c>/<c>false</c>, or <see cref="Missing"/>.</summary>
    public static object Equal(object? a, object? b) => Compare(SqlBinaryOp.Equal, a, b);

    /// <summary>A comparison operator: <c>true</c>/<c>false</c>, or <see cref="Missing"/>.</summary>
    public static object Compare(SqlBinaryOp op, object? a, object? b)
    {
        if (a == Missing || b == Missing)
            return Missing;

        if (a is null || b is null)
        {
            return op switch
            {
                SqlBinaryOp.Equal => a is null && b is null,
                SqlBinaryOp.NotEqual => !(a is null && b is null),
                _ => false // C# lifted ordering operators are false when either side is null.
            };
        }

        var ordering = op is not (SqlBinaryOp.Equal or SqlBinaryOp.NotEqual);
        int result;
        if (Numeric.TryPromote(a, b, out var x, out var y))
        {
            result = ((IComparable)x).CompareTo(y);
        }
        else if (!ordering && TryEquality(a, b, out var equal))
        {
            result = equal ? 0 : 1;
        }
        else if (ordering && a.GetType() == b.GetType() && a is DateTime or TimeSpan or DateTimeOffset or Enum)
        {
            result = ((IComparable)a).CompareTo(b);
        }
        else
        {
            throw NotApplicable(op, a, b);
        }

        return op switch
        {
            SqlBinaryOp.Equal => result == 0,
            SqlBinaryOp.NotEqual => result != 0,
            SqlBinaryOp.Less => result < 0,
            SqlBinaryOp.LessOrEqual => result <= 0,
            SqlBinaryOp.Greater => result > 0,
            _ => result >= 0
        };
    }

    private static bool TryEquality(object a, object b, out bool equal)
    {
        switch (a, b)
        {
            case (string s, string t):
                equal = string.Equals(s, t, StringComparison.Ordinal);
                return true;
            case (bool p, bool q):
                equal = p == q;
                return true;
            case (Guid g, string s):
                equal = g == ParseGuid(s);
                return true;
            case (string s, Guid g):
                equal = g == ParseGuid(s);
                return true;
        }

        if (a.GetType() == b.GetType() && a is Guid or DateTime or TimeSpan or DateTimeOffset or Uri or Enum)
        {
            equal = a.Equals(b);
            return true;
        }

        equal = false;
        return false;
    }

    // ── Arithmetic ───────────────────────────────────────────────────────────

    public static object? Arithmetic(SqlBinaryOp op, object? a, object? b)
    {
        if (a == Missing || b == Missing)
            return Missing;
        if (a is null || b is null)
            return null;

        if (op == SqlBinaryOp.Add && (a is string || b is string))
            return ToText(a) + ToText(b);

        if (!Numeric.TryPromote(a, b, out var x, out var y))
            throw NotApplicable(op, a, b);

        try
        {
            return (x, y) switch
            {
                (int p, int q) => checked(op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                }),
                (uint p, uint q) => checked(op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                }),
                (long p, long q) => checked(op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                }),
                (ulong p, ulong q) => checked(op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                }),
                (float p, float q) => op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                },
                (double p, double q) => op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => p % q
                },
                (decimal p, decimal q) => op switch
                {
                    SqlBinaryOp.Add => p + q, SqlBinaryOp.Subtract => p - q, SqlBinaryOp.Multiply => p * q,
                    SqlBinaryOp.Divide => p / q, _ => (object)(p % q)
                },
                _ => throw NotApplicable(op, a, b)
            };
        }
        catch (DivideByZeroException)
        {
            throw new SqlEvaluationException("Attempted to divide by zero.");
        }
        catch (OverflowException)
        {
            throw new SqlEvaluationException("Arithmetic operation resulted in an overflow.");
        }
    }

    public static object? Negate(object? value)
    {
        if (value == Missing || value is null)
            return value;

        try
        {
            return value switch
            {
                sbyte or short or int or byte or ushort or char => checked(-Convert.ToInt32(value, CultureInfo.InvariantCulture)),
                uint u => -(long)u,
                long l => checked(-l),
                float f => -f,
                double d => -d,
                decimal m => -m,
                _ => throw new SqlEvaluationException(
                    $"Operator '-' cannot be applied to operand of type '{TypeName(value.GetType())}'")
            };
        }
        catch (OverflowException)
        {
            throw new SqlEvaluationException("Arithmetic operation resulted in an overflow.");
        }
    }

    // ── Conversion ───────────────────────────────────────────────────────────

    /// <summary>Converts <paramref name="value"/> to <paramref name="target"/> like <see cref="Convert.ChangeType(object, Type)"/>.</summary>
    public static object? ConvertTo(Type target, object? value)
    {
        if (value is null || value == Missing)
            return null;
        if (value.GetType() == target)
            return value;

        try
        {
            if (target == typeof(string)) return ToText(value);
            if (target == typeof(Guid)) return value is string s ? ParseGuid(s) : throw InvalidCast(value, target);
            if (target == typeof(TimeSpan))
                return value is string t ? TimeSpan.Parse(t, CultureInfo.InvariantCulture) : throw InvalidCast(value, target);
            if (target == typeof(Uri))
                return value is string u ? new Uri(u, UriKind.RelativeOrAbsolute) : throw InvalidCast(value, target);
            if (target == typeof(DateTimeOffset))
                return value is string o
                    ? DateTimeOffset.Parse(o, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)
                    : throw InvalidCast(value, target);
            if (target == typeof(DateTime) && value is string d)
                return DateTime.Parse(d, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

            return Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }
        catch (SqlEvaluationException)
        {
            throw;
        }
        catch (FormatException) when (Numeric.IsNumericType(target))
        {
            // .NET Framework's wording, which is what Azure reports.
            throw new SqlEvaluationException("Input string was not in a correct format.");
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            throw new SqlEvaluationException(ex.Message);
        }
    }

    private static SqlEvaluationException InvalidCast(object value, Type target) =>
        new($"Invalid cast from '{value.GetType().FullName}' to '{target.FullName}'.");

    private static Guid ParseGuid(string text) =>
        Guid.TryParse(text, out var guid) ? guid : throw new SqlEvaluationException(GuidFormatMessage);

    // ── LIKE ─────────────────────────────────────────────────────────────────

    private static readonly ConcurrentDictionary<(string Pattern, char? Escape), Regex> LikeCache = new();
    private const int LikeCacheLimit = 1024;

    /// <summary><c>true</c>/<c>false</c>, or <c>null</c> (unknown) when any argument is <see cref="Missing"/>.</summary>
    public static bool? Like(object? value, object? pattern, object? escape)
    {
        if (value == Missing || pattern == Missing || escape == Missing)
            return null;
        if (value is null || pattern is null)
            return false;

        var escapeText = escape is null ? null : ToText(escape);
        char? escapeChar = string.IsNullOrEmpty(escapeText) ? null : escapeText[0];
        var key = (ToText(pattern), escapeChar);

        if (!LikeCache.TryGetValue(key, out var regex))
        {
            if (LikeCache.Count >= LikeCacheLimit) LikeCache.Clear();
            regex = LikeCache.GetOrAdd(key, k => BuildLikeRegex(k.Pattern, k.Escape));
        }

        return regex.IsMatch(ToText(value));
    }

    // '%' and '_' do not cross a newline on Azure, so they map to '.' without Singleline. A trailing escape
    // character is ignored.
    private static Regex BuildLikeRegex(string pattern, char? escape)
    {
        var sb = new StringBuilder(@"\A");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == escape)
            {
                if (++i < pattern.Length)
                    sb.Append(Regex.Escape(pattern[i].ToString()));
            }
            else if (c == '%') sb.Append(".*");
            else if (c == '_') sb.Append('.');
            else sb.Append(Regex.Escape(c.ToString()));
        }

        return new Regex(sb.Append(@"\z").ToString(), RegexOptions.CultureInvariant);
    }

    // ── Text and type names ──────────────────────────────────────────────────

    /// <summary>Invariant <c>ToString()</c>, used by <c>LIKE</c>, string concatenation and string conversion.</summary>
    public static string ToText(object value) => value switch
    {
        string s => s,
        bool b => b ? "True" : "False",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? ""
    };

    private static SqlEvaluationException NotApplicable(SqlBinaryOp op, object a, object b) =>
        new($"Operator '{OperatorText(op)}' cannot be applied to operands of type '{TypeName(a.GetType())}' and '{TypeName(b.GetType())}'");

    private static string OperatorText(SqlBinaryOp op) => op switch
    {
        SqlBinaryOp.Equal => "==",
        SqlBinaryOp.NotEqual => "!=",
        SqlBinaryOp.Less => "<",
        SqlBinaryOp.LessOrEqual => "<=",
        SqlBinaryOp.Greater => ">",
        SqlBinaryOp.GreaterOrEqual => ">=",
        SqlBinaryOp.Add => "+",
        SqlBinaryOp.Subtract => "-",
        SqlBinaryOp.Multiply => "*",
        SqlBinaryOp.Divide => "/",
        _ => "%"
    };

    // C# keyword names for primitives, full names otherwise; the message state enum keeps Azure's own name.
    private static string TypeName(Type type) => type switch
    {
        _ when type == typeof(string) => "string",
        _ when type == typeof(bool) => "bool",
        _ when type == typeof(char) => "char",
        _ when type == typeof(sbyte) => "sbyte",
        _ when type == typeof(byte) => "byte",
        _ when type == typeof(short) => "short",
        _ when type == typeof(ushort) => "ushort",
        _ when type == typeof(int) => "int",
        _ when type == typeof(uint) => "uint",
        _ when type == typeof(long) => "long",
        _ when type == typeof(ulong) => "ulong",
        _ when type == typeof(float) => "float",
        _ when type == typeof(double) => "double",
        _ when type == typeof(decimal) => "decimal",
        _ when type == typeof(MessageState) => "Microsoft.ServiceBus.Messaging.MessageState",
        _ => type.FullName ?? type.Name
    };

    /// <summary>C# binary numeric promotion.</summary>
    private static class Numeric
    {
        private enum Kind
        {
            Int,
            UInt,
            Long,
            ULong,
            Float,
            Double,
            Decimal
        }

        public static bool IsNumericType(Type t) => t == typeof(sbyte) || t == typeof(byte) || t == typeof(short)
            || t == typeof(ushort) || t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong)
            || t == typeof(float) || t == typeof(double) || t == typeof(decimal);

        public static bool TryPromote(object a, object b, out object x, out object y)
        {
            x = y = 0;
            if (!TryClassify(a, out var ka, out var signedA) || !TryClassify(b, out var kb, out var signedB))
                return false;

            Kind kind;
            if (ka == Kind.Decimal || kb == Kind.Decimal)
            {
                if (ka is Kind.Float or Kind.Double || kb is Kind.Float or Kind.Double) return false;
                kind = Kind.Decimal;
            }
            else if (ka == Kind.Double || kb == Kind.Double) kind = Kind.Double;
            else if (ka == Kind.Float || kb == Kind.Float) kind = Kind.Float;
            else if (ka == Kind.ULong || kb == Kind.ULong)
            {
                if (signedA || signedB) return false;
                kind = Kind.ULong;
            }
            else if (ka == Kind.Long || kb == Kind.Long) kind = Kind.Long;
            else if (ka == Kind.UInt || kb == Kind.UInt) kind = signedA || signedB ? Kind.Long : Kind.UInt;
            else kind = Kind.Int;

            x = To(kind, a);
            y = To(kind, b);
            return true;
        }

        private static bool TryClassify(object value, out Kind kind, out bool signed)
        {
            (kind, signed) = value switch
            {
                sbyte or short or int => (Kind.Int, true),
                byte or ushort or char => (Kind.Int, false),
                uint => (Kind.UInt, false),
                long => (Kind.Long, true),
                ulong => (Kind.ULong, false),
                float => (Kind.Float, true),
                double => (Kind.Double, true),
                decimal => (Kind.Decimal, true),
                _ => ((Kind)(-1), false)
            };
            return (int)kind >= 0 && value is not char;
        }

        private static object To(Kind kind, object value) => kind switch
        {
            Kind.Int => Convert.ToInt32(value, CultureInfo.InvariantCulture),
            Kind.UInt => Convert.ToUInt32(value, CultureInfo.InvariantCulture),
            Kind.Long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            Kind.ULong => Convert.ToUInt64(value, CultureInfo.InvariantCulture),
            Kind.Float => Convert.ToSingle(value, CultureInfo.InvariantCulture),
            Kind.Double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
            _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture)
        };
    }
}
