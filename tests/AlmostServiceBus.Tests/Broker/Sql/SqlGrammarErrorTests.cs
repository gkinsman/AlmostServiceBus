using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>
/// Syntax errors, with the details text real Azure returned for the same expression
/// (the recorded cases in tests/shared/sql-compat-cases.json). Columns are recomputed for the unwrapped expression; for a STRING
/// token Azure reports the column of its last character.
/// </summary>
public class SqlGrammarErrorTests
{
    private const int Eof = -1;

    [Theory]
    [InlineData("b", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("TRUE", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("NULL", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("NOT FALSE", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("missing + 1 IS NULL", 12, "IS", "syntax error, unexpected IS")]
    [InlineData("newid() IS NOT NULL", 8, "IS", "syntax error, unexpected IS")]
    [InlineData("a.b = 'x'", 2, "b", "The specified scope (a) is not supported.")]
    [InlineData("user.a.b = 'x'", 6, ".", "syntax error, unexpected '.'")]
    [InlineData("color IN ()", 10, ")", "syntax error, unexpected ')'")]
    [InlineData("color === 'red'", 7, "=", "syntax error, unexpected EQ")]
    [InlineData("color == 'red'", 7, "=", "syntax error, unexpected EQ")]
    [InlineData("in = 1", 0, "in", "syntax error, unexpected IN")]
    [InlineData("color NOT 'red'", 14, "'", "syntax error, unexpected STRING, expecting LIKE, or IN")]
    [InlineData("color IS 'red'", 13, "'", "syntax error, unexpected STRING, expecting NULL, or NOT")]
    [InlineData("EXISTS('x')", 9, "'", "syntax error, unexpected STRING, expecting IDENTIFIER, or DELIMITED_IDENTIFIER")]
    [InlineData("EXISTS(n + 1)", 9, "+", "syntax error, unexpected '+', expecting ')'")]
    [InlineData("color /* c */ = 'red'", 7, "*", "syntax error, unexpected '*'")]
    [InlineData("1abc = 'x'", 1, "abc", "syntax error, unexpected IDENTIFIER")]
    [InlineData("n = 0x10", 5, "x10", "syntax error, unexpected IDENTIFIER")]
    [InlineData("n = 99999999999999999999", 4, "99999999999999999999", "Value was either too large or too small for an Int64.")]
    [InlineData("color = N'red'", 13, "'", "syntax error, unexpected STRING")]
    [InlineData(".5 = 0.5", 0, ".", "syntax error, unexpected '.'")]
    [InlineData("n = 1.", 5, ".", "syntax error, unexpected '.'")]
    [InlineData("n = 1e", 5, "e", "syntax error, unexpected IDENTIFIER")]
    [InlineData("(color = 'red'", Eof, "", "syntax error, unexpected EOF, expecting ')', or AND, or OR")]
    [InlineData("color = 'red' blue", 14, "blue", "syntax error, unexpected IDENTIFIER")]
    [InlineData("(color = 'red' blue)", 15, "blue", "syntax error, unexpected IDENTIFIER")]
    [InlineData("color = 'red' AND", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("color = 'red';", 13, ";", "Unrecognized character. ';'")]
    [InlineData(" ", Eof, "", "syntax error, unexpected EOF")]
    [InlineData("", Eof, "", "syntax error, unexpected EOF")]
    public void ParseFilter_Rejects(string text, int column, string token, string details) =>
        AssertSyntaxError(() => SqlGrammar.ParseFilter(text), column, token, details);

    [Theory]
    [InlineData("SET user.a", Eof, "", "syntax error, unexpected EOF, expecting '='")]
    [InlineData("UPDATE user.a = 1", 0, "UPDATE", "syntax error, unexpected IDENTIFIER, expecting SET, or REMOVE")]
    [InlineData(";", 0, ";", "syntax error, unexpected ';', expecting SET, or REMOVE")]
    [InlineData("SET user.a = 1;;", 15, ";", "syntax error, unexpected ';', expecting SET, or REMOVE, or EOF")]
    [InlineData("; SET user.a = 1", 0, ";", "syntax error, unexpected ';', expecting SET, or REMOVE")]
    [InlineData("SET user.x = 1 = 1", 15, "=", "syntax error, unexpected '='")]
    [InlineData("SET user.x = color IN ('red')", 19, "IN", "syntax error, unexpected IN")]
    [InlineData("SET user.x = color LIKE 'r%'", 19, "LIKE", "syntax error, unexpected LIKE")]
    [InlineData("SET", Eof, "", "syntax error, unexpected EOF, expecting IDENTIFIER, or DELIMITED_IDENTIFIER")]
    [InlineData(" ", Eof, "", "syntax error, unexpected EOF, expecting SET, or REMOVE")]
    public void ParseAction_Rejects(string text, int column, string token, string details) =>
        AssertSyntaxError(() => SqlGrammar.ParseAction(text), column, token, details);

    [Fact]
    public void ParseFilter_ChecksFunctionArityWhileParsing() =>
        // Azure reports the arity error before it reaches the IS (which a function result can't take).
        Assert.Equal(
            "The 'newid' function expects 0 argument(s), but 1 argument(s) was/were specified.\r\nParameter name: arguments",
            Assert.Throws<SqlSyntaxException>(() => SqlGrammar.ParseFilter("newid(1) IS NOT NULL")).Message);

    private static void AssertSyntaxError(Action parse, int column, string token, string details)
    {
        var ex = Assert.Throws<SqlSyntaxException>(parse);

        Assert.Equal(details, ex.Details);
        Assert.Equal(token, ex.TokenInError);
        if (column == Eof)
        {
            Assert.Equal(2, ex.Line);
            Assert.Equal(0, ex.Column);
        }
        else
        {
            Assert.Equal(1, ex.Line);
            Assert.Equal(column, ex.Column);
        }
    }
}
