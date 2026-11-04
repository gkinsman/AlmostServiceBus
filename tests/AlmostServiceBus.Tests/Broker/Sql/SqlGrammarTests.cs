using System.Globalization;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

public class SqlGrammarTests
{
    [Theory]
    // precedence
    [InlineData("a = 1 OR b = 2 AND c = 3", "(Or (Equal user:a 1L) (And (Equal user:b 2L) (Equal user:c 3L)))")]
    [InlineData("NOT a = 1 AND b = 2", "(And (Not (Equal user:a 1L)) (Equal user:b 2L))")]
    [InlineData("1 + 2 * 3 = 7", "(Equal (Add 1L (Multiply 2L 3L)) 7L)")]
    [InlineData("-a * 2 = 1", "(Equal (Multiply (Negate user:a) 2L) 1L)")]
    [InlineData("NOT NOT a = 1", "(Not (Not (Equal user:a 1L)))")]
    [InlineData("a - b - c = 0", "(Equal (Subtract (Subtract user:a user:b) user:c) 0L)")]
    [InlineData("(a = 1 OR b = 2) AND c = 3", "(And (Or (Equal user:a 1L) (Equal user:b 2L)) (Equal user:c 3L))")]
    [InlineData("(a + 1) * 2 = 4", "(Equal (Multiply (Add user:a 1L) 2L) 4L)")]
    [InlineData("((((color = 'red'))))", "(Equal user:color 'red')")]
    [InlineData("color = 'red' -- comment", "(Equal user:color (Subtract 'red' (Negate user:comment)))")]
    // identifiers and scopes
    [InlineData("sys.Label = 'L'", "(Equal sys:Label 'L')")]
    [InlineData("SYS.label = 'L'", "(Equal sys:label 'L')")]
    [InlineData("User.Color = 'red'", "(Equal user:Color 'red')")]
    [InlineData("user.[a-b] = 1", "(Equal user:a-b 1L)")]
    [InlineData("user.\"a\"\"b\" = 1", "(Equal user:a\"b 1L)")]
    [InlineData("sys.[Label] = 'L'", "(Equal sys:Label 'L')")]
    [InlineData("[a]]b] = 1", "(Equal user:a]b 1L)")]
    [InlineData("[] = 'x'", "(Equal user: 'x')")]
    [InlineData("café = 'x'", "(Equal user:café 'x')")]
    [InlineData("sys = 1", "(Equal user:sys 1L)")]
    [InlineData("user = 1", "(Equal user:user 1L)")]
    [InlineData("[in] = 'x'", "(Equal user:in 'x')")]
    [InlineData("\"AND\" = 1", "(Equal user:AND 1L)")]
    [InlineData("[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'", "(Like user:NServiceBus.EnclosedMessageTypes '%Ns.MyEvent%')")]
    // literals
    [InlineData("name = 'O''Brien'", "(Equal user:name 'O''Brien')")]
    [InlineData("n = 4.2", "(Equal user:n 4.2D)")]
    [InlineData("n = 1e1", "(Equal user:n 10D)")]
    [InlineData("n = 1.5E-3", "(Equal user:n 0.0015D)")]
    [InlineData("b = TRUE", "(Equal user:b true)")]
    [InlineData("b = false", "(Equal user:b false)")]
    [InlineData("NULL = NULL", "(Equal null null)")]
    [InlineData("n = -1", "(Equal user:n (Negate 1L))")]
    [InlineData("+n = 5", "(Equal (Plus user:n) 5L)")]
    // operators
    [InlineData("a <> 1", "(NotEqual user:a 1L)")]
    [InlineData("a != 1", "(NotEqual user:a 1L)")]
    [InlineData("a < 1 AND a <= 1 AND a > 1 AND a >= 1",
        "(And (And (And (Less user:a 1L) (LessOrEqual user:a 1L)) (Greater user:a 1L)) (GreaterOrEqual user:a 1L))")]
    [InlineData("n % 3 = 1 AND n / 2 = 3", "(And (Equal (Modulo user:n 3L) 1L) (Equal (Divide user:n 2L) 3L))")]
    [InlineData("code LIKE '50!%' ESCAPE '!'", "(Like user:code '50!%' '!')")]
    [InlineData("color LIKE pat ESCAPE esc", "(Like user:color user:pat user:esc)")]
    [InlineData("color NOT LIKE 'b%'", "(NotLike user:color 'b%')")]
    [InlineData("n IN (1 + 1, m, 3)", "(In user:n [(Add 1L 1L), user:m, 3L])")]
    [InlineData("color NOT IN ('x', NULL)", "(NotIn user:color ['x', null])")]
    [InlineData("color IS NULL", "(IsNull user:color)")]
    [InlineData("sys.To IS NOT NULL", "(IsNotNull sys:To)")]
    [InlineData("EXISTS(sys.Label)", "(Exists sys:Label)")]
    [InlineData("NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'",
        "(Or (Not (Exists user:ignore)) (NotLike user:ignore 'true'))")]
    [InlineData("newid() = newid()", "(Equal (Call newid []) (Call newid []))")]
    [InlineData("PROPERTY(which) = 'red'", "(Equal (Call property [user:which]) 'red')")]
    [InlineData("color = @p", "(Equal user:color @p)")]
    [InlineData("color IN (@a, @b)", "(In user:color [@a, @b])")]
    // keywords in mixed case
    [InlineData("a in ('x') and not b is null", "(And (In user:a ['x']) (Not (IsNull user:b)))")]
    public void ParseFilter_BuildsExpectedAst(string text, string expected) =>
        Assert.Equal(expected, SqlAstPrinter.Print(SqlGrammar.ParseFilter(text)));

    [Theory]
    [InlineData("SET a = 1;", "(Set user:a 1L)")]
    [InlineData("SET user.b = user.n * 2; REMOVE sys.Label", "(Set user:b (Multiply user:n 2L)); (Remove sys:Label)")]
    [InlineData("set [a b] = @v", "(Set user:a b @v)")]
    [InlineData("SET user.x = color + '!'", "(Set user:x (Add user:color '!'))")]
    [InlineData("SET user.id = newid()", "(Set user:id (Call newid []))")]
    [InlineData("remove color", "(Remove user:color)")]
    [InlineData("SET sys.TimeToLive = '02:00:00'", "(Set sys:TimeToLive '02:00:00')")]
    public void ParseAction_BuildsExpectedStatements(string text, string expected) =>
        Assert.Equal(expected, SqlAstPrinter.Print(SqlGrammar.ParseAction(text)));

    [Fact]
    public void ParseFilter_DeepNesting_DoesNotOverflow()
    {
        var text = new string('(', 500) + "a = 1" + new string(')', 500);
        Assert.Equal("(Equal user:a 1L)", SqlAstPrinter.Print(SqlGrammar.ParseFilter(text)));
    }

    [Fact]
    public void ParseFilter_DeepNesting_OnSmallStackThread_DoesNotOverflow()
    {
        // Thread-pool and test-host threads can have much smaller stacks than the main thread.
        var text = new string('(', 500) + "a = 1" + new string(')', 500);
        string? printed = null;
        var thread = new Thread(() => printed = SqlAstPrinter.Print(SqlGrammar.ParseFilter(text)), 256 * 1024);
        thread.Start();
        thread.Join();
        Assert.Equal("(Equal user:a 1L)", printed);
    }

    [Fact]
    public void ParseAction_DeepNesting_DoesNotOverflow()
    {
        var text = "SET a = " + new string('(', 500) + "1" + new string(')', 500);
        Assert.Equal("(Set user:a 1L)", SqlAstPrinter.Print(SqlGrammar.ParseAction(text)));
    }

    [Fact]
    public void ParseFilter_OverMaxLength_IsRejected()
    {
        var text = "a = '" + new string('x', 1019) + "'";
        Assert.Equal(1025, text.Length);

        var ex = Assert.Throws<SqlSyntaxException>(() => SqlGrammar.ParseFilter(text));
        Assert.Contains("1024", ex.Message);
    }

    [Fact]
    public void ParseAction_OverMaxLength_IsRejected()
    {
        var text = "SET a = '" + new string('x', 1015) + "'";
        Assert.Throws<SqlSyntaxException>(() => SqlGrammar.ParseAction(text));
    }

    [Fact]
    public void ParseFilter_UnderGermanCulture_ParsesDecimalLiteralInvariantly()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal("(Equal user:n 1.5D)", SqlAstPrinter.Print(SqlGrammar.ParseFilter("n = 1.5")));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
