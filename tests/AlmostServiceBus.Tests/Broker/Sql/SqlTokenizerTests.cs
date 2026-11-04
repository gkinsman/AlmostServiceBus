using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

public class SqlTokenizerTests
{
    private static SqlToken[] Kinds(string text) => SqlTokenizer.Tokenize(text).Select(t => t.Kind).ToArray();

    [Fact]
    public void Tokenize_ScopedDelimitedIdentifierAndInList()
    {
        Assert.Equal(
            [SqlToken.Identifier, SqlToken.Dot, SqlToken.BracketedIdentifier, SqlToken.Identifier, SqlToken.LParen,
             SqlToken.String, SqlToken.Comma, SqlToken.Float, SqlToken.RParen],
            Kinds("user.[message-type] IN ('a''b', 1.5e3)"));
    }

    [Fact]
    public void Tokenize_QuotedIdentifierWithEscapedQuote_IsOneToken()
    {
        var tokens = SqlTokenizer.Tokenize("\"a\"\"b\"").ToArray();

        Assert.Single(tokens);
        Assert.Equal(SqlToken.QuotedIdentifier, tokens[0].Kind);
        Assert.Equal("\"a\"\"b\"", tokens[0].ToStringValue());
    }

    [Fact]
    public void Tokenize_UnicodeIdentifier() => Assert.Equal([SqlToken.Identifier], Kinds("café"));

    [Fact]
    public void Tokenize_Parameter() => Assert.Equal([SqlToken.Parameter], Kinds("@p1"));

    [Fact]
    public void Tokenize_ComparisonOperators() =>
        Assert.Equal(
            [SqlToken.NotEqual, SqlToken.NotEqual, SqlToken.LessOrEqual, SqlToken.GreaterOrEqual, SqlToken.Equal, SqlToken.Less, SqlToken.Greater],
            Kinds("<> != <= >= = < >"));

    [Fact]
    public void Tokenize_Numbers() =>
        Assert.Equal([SqlToken.Integer, SqlToken.Decimal, SqlToken.Float, SqlToken.Float], Kinds("42 4.2 1e1 1.5E-3"));

    [Fact]
    public void Tokenize_DoubleDashIsTwoMinuses_NotAComment() =>
        Assert.Equal([SqlToken.Minus, SqlToken.Minus, SqlToken.Identifier], Kinds("--x"));

    [Fact]
    public void Tokenize_Punctuation() =>
        Assert.Equal(
            [SqlToken.LParen, SqlToken.RParen, SqlToken.Comma, SqlToken.Semicolon, SqlToken.Plus, SqlToken.Minus, SqlToken.Star, SqlToken.Slash, SqlToken.Percent, SqlToken.Dot],
            Kinds("( ) , ; + - * / % ."));

    [Theory]
    [InlineData("a = #", 1, 4, "#", "Unrecognized character. '#'")]
    [InlineData("a$b", 1, 1, "$", "Unrecognized character. '$'")]
    [InlineData("a = 'red", 2, 0, "", "The string (red) was not terminated properly.")]
    [InlineData("a = \"red", 2, 0, "", "The delimited identifier (red) was not terminated properly.")]
    [InlineData("a = [red", 2, 0, "", "The delimited identifier (red) was not terminated properly.")]
    [InlineData("x = 1\n  #", 2, 2, "#", "Unrecognized character. '#'")]
    public void Tokenize_InvalidInput_ThrowsAzureShapedError(string text, int line, int column, string token, string details)
    {
        var ex = Assert.Throws<SqlSyntaxException>(() => SqlTokenizer.Tokenize(text));

        Assert.Equal(line, ex.Line);
        Assert.Equal(column, ex.Column);
        Assert.Equal(token, ex.TokenInError);
        Assert.Equal(details, ex.Details);
        Assert.Equal(
            $"There was an error parsing the SQL expression. [Token line={line}, column={column}, Token in error={token}, Additional details={details}]",
            ex.Message);
    }

    [Theory]
    [InlineData("n = .5")]
    [InlineData("n = 1.")]
    [InlineData("n = 1e")]
    public void Tokenize_MalformedNumbers_AreRejectedByTokenizerOrGrammar(string text)
    {
        // `.5` and `1.` tokenize as a number and a stray Dot, and `1e` as a number followed by an
        // identifier, so the grammar rejects them (see SqlGrammarErrorTests). The tokenizer must not
        // fold them into a single numeric token.
        var kinds = Kinds(text);
        Assert.DoesNotContain(SqlToken.Decimal, kinds);
        Assert.DoesNotContain(SqlToken.Float, kinds);
    }
}
