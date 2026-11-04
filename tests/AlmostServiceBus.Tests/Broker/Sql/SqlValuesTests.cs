using System.Globalization;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;
using Amqp.Types;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>Value semantics, with expectations taken from the real-Azure probe (decisions log Q4–Q6).</summary>
public class SqlValuesTests
{
    private static readonly Guid G = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");

    private static string Error(Action act) => Assert.Throws<SqlEvaluationException>(act).Message;

    // ── Equality and ordering across CLR types ───────────────────────────────

    public static TheoryData<object, object, bool> EqualPairs => new()
    {
        { 27, 27L, true },
        { 27L, 27L, true },
        { 7, 7.0d, true },
        { (byte)7, 7L, true },
        { (short)7, 7L, true },
        { (sbyte)7, 7L, true },
        { 7u, 7L, true },
        { 1.5f, 1.5d, true },
        { 0.1f, 0.1d, false },
        { "red", "red", true },
        { "red", "RED", false },
        { "red ", "red", false },
        { true, true, true },
        { false, true, false },
        { G, G.ToString(), true },
        { G, G, true },
        { new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), true },
        { TimeSpan.FromHours(1), TimeSpan.FromHours(1), true },
    };

    [Theory]
    [MemberData(nameof(EqualPairs))]
    public void Equal_FollowsCSharpRules(object a, object b, bool expected) => Assert.Equal(expected, SqlValues.Equal(a, b));

    public static TheoryData<object, object, string> MismatchedPairs => new()
    {
        { 1.5m, 1.5d, "Operator '==' cannot be applied to operands of type 'decimal' and 'double'" },
        { 7ul, 7L, "Operator '==' cannot be applied to operands of type 'ulong' and 'long'" },
        { 'x', "x", "Operator '==' cannot be applied to operands of type 'char' and 'string'" },
        { "27", 27L, "Operator '==' cannot be applied to operands of type 'string' and 'long'" },
        { 27, "27", "Operator '==' cannot be applied to operands of type 'int' and 'string'" },
        { true, 1L, "Operator '==' cannot be applied to operands of type 'bool' and 'long'" },
        { true, "true", "Operator '==' cannot be applied to operands of type 'bool' and 'string'" },
        { new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), "2024-01-02", "Operator '==' cannot be applied to operands of type 'System.DateTime' and 'string'" },
        { TimeSpan.FromHours(1), "01:00:00", "Operator '==' cannot be applied to operands of type 'System.TimeSpan' and 'string'" },
        { new Uri("http://a/"), "http://a/", "Operator '==' cannot be applied to operands of type 'System.Uri' and 'string'" },
        { new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero), "x", "Operator '==' cannot be applied to operands of type 'System.DateTimeOffset' and 'string'" },
        { MessageState.Active, 0L, "Operator '==' cannot be applied to operands of type 'Microsoft.ServiceBus.Messaging.MessageState' and 'long'" },
    };

    [Theory]
    [MemberData(nameof(MismatchedPairs))]
    public void Equal_TypeMismatch_ThrowsAzureMessage(object a, object b, string message) =>
        Assert.Equal(message, Error(() => SqlValues.Equal(a, b)));

    [Fact]
    public void Guid_EqualsMalformedString_ThrowsGuidFormatMessage() =>
        Assert.Equal("Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).",
            Error(() => SqlValues.Equal(G, "not-a-guid")));

    [Fact]
    public void Guid_NotEqualString_Works() => Assert.Equal(false, SqlValues.Compare(SqlBinaryOp.NotEqual, G, G.ToString()));

    [Theory]
    [InlineData(SqlBinaryOp.Less, "Operator '<' cannot be applied to operands of type 'string' and 'string'")]
    [InlineData(SqlBinaryOp.Greater, "Operator '>' cannot be applied to operands of type 'string' and 'string'")]
    public void Strings_AreNotOrdered(SqlBinaryOp op, string message) =>
        Assert.Equal(message, Error(() => SqlValues.Compare(op, "a", "b")));

    [Fact]
    public void Bools_AreNotOrdered() =>
        Assert.Equal("Operator '<' cannot be applied to operands of type 'bool' and 'bool'",
            Error(() => SqlValues.Compare(SqlBinaryOp.Less, false, true)));

    [Fact]
    public void Guid_GreaterThanString_Throws() =>
        Assert.Equal("Operator '>' cannot be applied to operands of type 'System.Guid' and 'string'",
            Error(() => SqlValues.Compare(SqlBinaryOp.Greater, G, G.ToString())));

    [Fact]
    public void Ordering_Numbers()
    {
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.Greater, 7u, -1L));
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.Less, 7, 7.5d));
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.GreaterOrEqual, 1.5d, 1L));
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.LessOrEqual, 5L, 5));
    }

    [Fact]
    public void Ordering_DatesAndTimeSpans()
    {
        var earlier = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var later = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.Greater, later, earlier));
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.Greater, TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)));
    }

    // ── NULL and Missing ─────────────────────────────────────────────────────

    [Fact]
    public void Null_UsesCSharpEquality()
    {
        Assert.Equal(true, SqlValues.Equal(null, null));
        Assert.Equal(false, SqlValues.Equal("a", null));
        Assert.Equal(false, SqlValues.Equal(null, "a"));
        Assert.Equal(true, SqlValues.Compare(SqlBinaryOp.NotEqual, null, "x"));
        Assert.Equal(false, SqlValues.Compare(SqlBinaryOp.NotEqual, null, null));
        Assert.Equal(false, SqlValues.Compare(SqlBinaryOp.Less, null, 1L));
    }

    [Fact]
    public void Missing_PropagatesThroughComparisonsWithoutTypeChecks()
    {
        Assert.Same(SqlValues.Missing, SqlValues.Equal(SqlValues.Missing, null));
        Assert.Same(SqlValues.Missing, SqlValues.Equal(SqlValues.Missing, "a"));
        Assert.Same(SqlValues.Missing, SqlValues.Compare(SqlBinaryOp.Greater, SqlValues.Missing, "a"));
        Assert.Same(SqlValues.Missing, SqlValues.Arithmetic(SqlBinaryOp.Add, SqlValues.Missing, 1L));
        Assert.Same(SqlValues.Missing, SqlValues.Negate(SqlValues.Missing));
    }

    // ── Arithmetic ───────────────────────────────────────────────────────────

    [Fact]
    public void Arithmetic_IntegerSemantics()
    {
        Assert.Equal(3L, SqlValues.Arithmetic(SqlBinaryOp.Divide, 7L, 2L));
        Assert.Equal(3L, SqlValues.Arithmetic(SqlBinaryOp.Divide, 7, 2L));
        Assert.Equal(-3L, SqlValues.Arithmetic(SqlBinaryOp.Divide, -7, 2L));
        Assert.Equal(-1L, SqlValues.Arithmetic(SqlBinaryOp.Modulo, -7, 3L));
        Assert.Equal(4294967294L, SqlValues.Arithmetic(SqlBinaryOp.Multiply, 2, 2147483647L));
        Assert.Equal(6L, SqlValues.Arithmetic(SqlBinaryOp.Multiply, 3, 2L));
        Assert.Equal(7.5d, SqlValues.Arithmetic(SqlBinaryOp.Add, 7, 0.5d));
        Assert.Equal(1.5d, SqlValues.Arithmetic(SqlBinaryOp.Modulo, 3.5d, 2L));
    }

    [Fact]
    public void Arithmetic_IntegerDivideByZero_Throws() =>
        Assert.Equal("Attempted to divide by zero.", Error(() => SqlValues.Arithmetic(SqlBinaryOp.Divide, 1, 0L)));

    [Fact]
    public void Arithmetic_IntegerModuloByZero_Throws() =>
        Assert.Equal("Attempted to divide by zero.", Error(() => SqlValues.Arithmetic(SqlBinaryOp.Modulo, 1, 0L)));

    [Fact]
    public void Arithmetic_DoubleDivideByZero_IsInfinity() =>
        Assert.Equal(double.PositiveInfinity, SqlValues.Arithmetic(SqlBinaryOp.Divide, 1.5d, 0L));

    [Fact]
    public void Arithmetic_LongOverflow_Throws() =>
        Assert.Equal("Arithmetic operation resulted in an overflow.",
            Error(() => SqlValues.Arithmetic(SqlBinaryOp.Multiply, 2L, long.MaxValue)));

    [Fact]
    public void Arithmetic_StringConcatenation()
    {
        Assert.Equal("redx", SqlValues.Arithmetic(SqlBinaryOp.Add, "red", "x"));
        Assert.Equal("red1", SqlValues.Arithmetic(SqlBinaryOp.Add, "red", 1L));
    }

    [Fact]
    public void Arithmetic_StringConcatenation_UsesInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal("red1.5", SqlValues.Arithmetic(SqlBinaryOp.Add, "red", 1.5d));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void Arithmetic_NonNumeric_Throws() =>
        Assert.Equal("Operator '-' cannot be applied to operands of type 'string' and 'long'",
            Error(() => SqlValues.Arithmetic(SqlBinaryOp.Subtract, "red", 1L)));

    [Fact]
    public void Arithmetic_Null_IsNull() => Assert.Null(SqlValues.Arithmetic(SqlBinaryOp.Add, null, 1L));

    [Fact]
    public void Negate_Numbers()
    {
        Assert.Equal(-5, SqlValues.Negate(5));
        Assert.Equal(-1.5d, SqlValues.Negate(1.5d));
    }

    // ── Conversions (SET on an existing property) ────────────────────────────

    [Fact]
    public void ConvertTo_FollowsConvertChangeType()
    {
        Assert.Equal(2, SqlValues.ConvertTo(typeof(int), 1.5d));
        Assert.Equal("5", SqlValues.ConvertTo(typeof(string), 5L));
        Assert.Equal(false, SqlValues.ConvertTo(typeof(bool), "false"));
        Assert.Equal(2.0d, SqlValues.ConvertTo(typeof(double), 2L));
        Assert.Equal(TimeSpan.FromHours(2), SqlValues.ConvertTo(typeof(TimeSpan), "02:00:00"));
        Assert.Equal("5", SqlValues.ConvertTo(typeof(string), 5L));
        Assert.Equal(G.ToString(), SqlValues.ConvertTo(typeof(string), G));
    }

    [Theory]
    [InlineData(typeof(int), 9999999999L, "Value was either too large or too small for an Int32.")]
    [InlineData(typeof(int), "text", "Input string was not in a correct format.")]
    [InlineData(typeof(TimeSpan), 5L, "Invalid cast from 'System.Int64' to 'System.TimeSpan'.")]
    [InlineData(typeof(Guid), "abc", "Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).")]
    public void ConvertTo_Failures_ThrowAzureMessages(Type target, object value, string message) =>
        Assert.Equal(message, Error(() => SqlValues.ConvertTo(target, value)));

    // ── LIKE ─────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("red", "r%", null, true)]
    [InlineData("red", "r_d", null, true)]
    [InlineData("red", "R%", null, false)]
    [InlineData("red", "r.d", null, false)]
    [InlineData("red", "[r]ed", null, false)]
    [InlineData("a\nb", "a%", null, false)]
    [InlineData("50%", "50!%", "!", true)]
    [InlineData("500", "50!%", "!", false)]
    [InlineData("50%", "50!%", "!x", true)]
    [InlineData("50x", "50%", "", true)]
    [InlineData("a", "a!", "!", true)]
    [InlineData("a_b", "a!_b", "!", true)]
    [InlineData("axb", "a!_b", "!", false)]
    [InlineData("50%", "%!%%", "!", true)]
    public void Like_MatchesAsAzure(string value, string pattern, string? escape, bool expected) =>
        Assert.Equal(expected, SqlValues.Like(value, pattern, escape));

    [Fact]
    public void Like_NonStringValues_UseInvariantToString()
    {
        Assert.Equal(true, SqlValues.Like(true, "True", null));
        Assert.Equal(true, SqlValues.Like(27, "2%", null));
        Assert.Equal(true, SqlValues.Like(G, "6f96%", null));

        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal(true, SqlValues.Like(1.5d, "1.5", null));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void Like_NullAndMissing()
    {
        Assert.Equal(false, SqlValues.Like(null, "x", null));
        Assert.Null(SqlValues.Like(SqlValues.Missing, "x", null));
        Assert.Null(SqlValues.Like("red", SqlValues.Missing, null));
    }

    // ── AMQP described values sent by the .NET SDK ───────────────────────────

    [Fact]
    public void FromMessage_DecodesSdkDescribedTypes()
    {
        Assert.Equal(TimeSpan.FromHours(1),
            SqlValues.FromMessage(new DescribedValue(new Symbol("com.microsoft:timespan"), TimeSpan.FromHours(1).Ticks)));
        Assert.Equal(new Uri("http://a/"),
            SqlValues.FromMessage(new DescribedValue(new Symbol("com.microsoft:uri"), "http://a/")));
        var dto = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        Assert.Equal(dto,
            SqlValues.FromMessage(new DescribedValue(new Symbol("com.microsoft:datetime-offset"), dto.UtcTicks)));
        Assert.Equal("abc", SqlValues.FromMessage(new Symbol("abc")));
        Assert.Equal(27, SqlValues.FromMessage(27));
    }

    [Fact]
    public void ToMessage_EncodesSdkDescribedTypes()
    {
        var ts = Assert.IsType<DescribedValue>(SqlValues.ToMessage(TimeSpan.FromHours(1)));
        Assert.Equal("com.microsoft:timespan", ts.Descriptor.ToString());
        Assert.Equal(TimeSpan.FromHours(1).Ticks, ts.Value);
        Assert.Equal(TimeSpan.FromHours(1), SqlValues.FromMessage(ts));

        var uri = Assert.IsType<DescribedValue>(SqlValues.ToMessage(new Uri("http://a/")));
        Assert.Equal(new Uri("http://a/"), SqlValues.FromMessage(uri));

        Assert.Equal(5L, SqlValues.ToMessage(5L));
    }
}
