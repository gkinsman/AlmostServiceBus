using System.Globalization;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;

namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// Superpower grammars for Service Bus SQL: <see cref="ParseFilter"/> parses a predicate, <see cref="ParseAction"/>
/// a list of <c>SET</c>/<c>REMOVE</c> statements over scalar expressions. Both reject input the way Azure does,
/// including its "syntax error, unexpected X, expecting Y" details.
/// </summary>
public static class SqlGrammar
{
    public const int MaxExpressionLength = 1024;

    // Bison (which Azure's parser is built with) lists the expected tokens only when there are at most four.
    private const int MaxListedExpectations = 4;

    private static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND", "OR", "NOT", "IN", "LIKE", "ESCAPE", "IS", "NULL", "EXISTS", "TRUE", "FALSE", "SET", "REMOVE"
    };

    private enum Mode
    {
        Filter,
        Action
    }

    /// <exception cref="SqlSyntaxException">The text is not a valid Service Bus SQL filter.</exception>
    public static SqlExpr ParseFilter(string text)
    {
        var tokens = Prepare(text);

        // The filter lexer has no ';' token: Azure reports it as an unrecognised character.
        foreach (var token in tokens)
        {
            if (token.Kind == SqlToken.Semicolon)
                throw SqlSyntaxException.Syntax(text, token.Position.Absolute, ";", "Unrecognized character. ';'");
        }

        var result = WithStackFor(tokens, () => Or(tokens));
        if (!result.HasValue)
            throw Error(text, result.Remainder, result.Expectations, Mode.Filter);
        if (!result.Remainder.IsAtEnd)
            throw Error(text, result.Remainder, null, Mode.Filter);
        return result.Value;
    }

    /// <exception cref="SqlSyntaxException">The text is not a valid Service Bus SQL rule action.</exception>
    public static IReadOnlyList<SqlStatement> ParseAction(string text)
    {
        var tokens = Prepare(text);
        return WithStackFor(tokens, () => ParseStatements(text, tokens));
    }

    private static List<SqlStatement> ParseStatements(string text, TokenList<SqlToken> tokens)
    {
        var statements = new List<SqlStatement>();

        var first = Statement(tokens);
        if (!first.HasValue)
            throw Error(text, first.Remainder, first.Expectations, Mode.Action);
        statements.Add(first.Value);

        var rest = first.Remainder;
        while (!rest.IsAtEnd)
        {
            var separator = rest.ConsumeToken();
            if (separator.Value.Kind != SqlToken.Semicolon)
                throw Error(text, rest, null, Mode.Action);

            rest = separator.Remainder;
            if (rest.IsAtEnd)
                break;

            var next = Statement(rest);
            if (!next.HasValue)
            {
                var expected = next.Remainder.Position == rest.Position
                    ? [.. next.Expectations ?? [], "EOF"]
                    : next.Expectations;
                throw Error(text, next.Remainder, expected, Mode.Action);
            }

            statements.Add(next.Value);
            rest = next.Remainder;
        }

        return statements;
    }

    // Superpower spends a dozen or so frames per nesting level; a 1024-character expression can nest about 500
    // deep, which overflows a thread-pool stack. Deeply nested input is parsed on a thread with a larger stack.
    private const int InlineNestingLimit = 64;
    private const int LargeStackBytes = 64 * 1024 * 1024;

    private static T WithStackFor<T>(TokenList<SqlToken> tokens, Func<T> parse)
    {
        var depth = 0;
        var maxDepth = 0;
        foreach (var token in tokens)
        {
            if (token.Kind == SqlToken.LParen) maxDepth = Math.Max(maxDepth, ++depth);
            else if (token.Kind == SqlToken.RParen) depth--;
        }

        if (maxDepth <= InlineNestingLimit)
            return parse();

        T result = default!;
        System.Runtime.ExceptionServices.ExceptionDispatchInfo? error = null;
        var thread = new Thread(() =>
        {
            try { result = parse(); }
            catch (Exception ex) { error = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex); }
        }, LargeStackBytes);
        thread.Start();
        thread.Join();
        error?.Throw();
        return result;
    }

    private static TokenList<SqlToken> Prepare(string text)
    {
        if (text.Length > MaxExpressionLength)
            throw SqlSyntaxException.Semantic(
                $"The SQL expression exceeds the maximum length of {MaxExpressionLength} characters.");
        return SqlTokenizer.Tokenize(text);
    }

    // ── Terminals ────────────────────────────────────────────────────────────

    private static TokenListParser<SqlToken, Token<SqlToken>> Tok(SqlToken kind, string name) =>
        Token.EqualTo(kind).Named(name);

    private static TokenListParser<SqlToken, Token<SqlToken>> Kw(string keyword) =>
        Token.EqualToValueIgnoreCase(SqlToken.Identifier, keyword).Named(keyword);

    private static readonly TokenListParser<SqlToken, Token<SqlToken>> LParen = Tok(SqlToken.LParen, "'('");
    private static readonly TokenListParser<SqlToken, Token<SqlToken>> RParen = Tok(SqlToken.RParen, "')'");

    // One part of a property name: a non-reserved regular identifier, or a "quoted" / [bracketed] one.
    private static readonly TokenListParser<SqlToken, (Token<SqlToken> Token, string Name)> NamePart = input =>
    {
        var next = input.ConsumeToken();
        if (next.HasValue)
        {
            var token = next.Value;
            var name = token.Kind switch
            {
                SqlToken.Identifier when !Reserved.Contains(token.ToStringValue()) => token.ToStringValue(),
                SqlToken.QuotedIdentifier => Unescape(token.ToStringValue(), "\"\"", "\""),
                SqlToken.BracketedIdentifier => Unescape(token.ToStringValue(), "]]", "]"),
                _ => null
            };
            if (name is not null)
                return TokenListParserResult.Value((token, name), input, next.Remainder);
        }

        return TokenListParserResult.Empty<SqlToken, (Token<SqlToken>, string)>(input,
            ["IDENTIFIER, or DELIMITED_IDENTIFIER"]);
    };

    // [sys. | user.] name. An unprefixed regular name followed by '.' is an unsupported scope.
    private static readonly TokenListParser<SqlToken, SqlPropertyRef> Property = input =>
    {
        var first = NamePart(input);
        if (!first.HasValue)
            return Fail<SqlPropertyRef>(first.Remainder, first.Expectations);

        var (token, name) = first.Value;
        var dot = first.Remainder.ConsumeToken();
        if (token.Kind != SqlToken.Identifier || !dot.HasValue || dot.Value.Kind != SqlToken.Dot)
            return TokenListParserResult.Value(new SqlPropertyRef(SqlScope.User, name), input, first.Remainder);

        var isSystem = name.Equals("sys", StringComparison.OrdinalIgnoreCase);
        if (!isSystem && !name.Equals("user", StringComparison.OrdinalIgnoreCase))
        {
            var source = token.Span.Source!;
            var after = dot.Remainder.ConsumeToken();
            throw after.HasValue
                ? SqlSyntaxException.Syntax(source, after.Value.Position.Absolute, after.Value.ToStringValue(),
                    $"The specified scope ({name}) is not supported.")
                : SqlSyntaxException.Syntax(source, source.Length, "", $"The specified scope ({name}) is not supported.");
        }

        var second = NamePart(dot.Remainder);
        if (!second.HasValue)
            return Fail<SqlPropertyRef>(second.Remainder, second.Expectations);
        return TokenListParserResult.Value(
            new SqlPropertyRef(isSystem ? SqlScope.System : SqlScope.User, second.Value.Name), input, second.Remainder);
    };

    private static readonly TokenListParser<SqlToken, SqlExpr> Literal = input =>
    {
        var next = input.ConsumeToken();
        if (next.HasValue)
        {
            var token = next.Value;
            var text = token.ToStringValue();
            SqlExpr? literal = token.Kind switch
            {
                SqlToken.String => new SqlLiteral(Unescape(text, "''", "'")),
                SqlToken.Integer => new SqlLiteral(ParseInteger(token)),
                SqlToken.Decimal or SqlToken.Float => new SqlLiteral(double.Parse(text, CultureInfo.InvariantCulture)),
                SqlToken.Identifier when Is(text, "TRUE") => new SqlLiteral(true),
                SqlToken.Identifier when Is(text, "FALSE") => new SqlLiteral(false),
                SqlToken.Identifier when Is(text, "NULL") => new SqlLiteral(null),
                _ => null
            };
            if (literal is not null)
                return TokenListParserResult.Value(literal, input, next.Remainder);
        }

        return TokenListParserResult.Empty<SqlToken, SqlExpr>(input, ["STRING", "NUMBER", "TRUE", "FALSE", "NULL"]);
    };

    private static readonly TokenListParser<SqlToken, SqlExpr> Parameter =
        Tok(SqlToken.Parameter, "PARAMETER").Select(t => (SqlExpr)new SqlParameterRef(t.ToStringValue()));

    // name '(' [scalar (',' scalar)*] ')' — committed once a regular identifier is followed by '('.
    private static readonly TokenListParser<SqlToken, SqlExpr> FunctionCall = input =>
    {
        var name = input.ConsumeToken();
        if (!name.HasValue || name.Value.Kind != SqlToken.Identifier || Reserved.Contains(name.Value.ToStringValue()))
            return TokenListParserResult.Empty<SqlToken, SqlExpr>(input, ["IDENTIFIER"]);
        var open = name.Remainder.ConsumeToken();
        if (!open.HasValue || open.Value.Kind != SqlToken.LParen)
            return TokenListParserResult.Empty<SqlToken, SqlExpr>(input, ["IDENTIFIER"]);

        var args = new List<SqlExpr>();
        var rest = open.Remainder;
        if (!RParen(rest).HasValue)
        {
            while (true)
            {
                var arg = Additive!(rest);
                if (!arg.HasValue)
                    return Fail<SqlExpr>(arg.Remainder, arg.Expectations);
                args.Add(arg.Value);
                rest = arg.Remainder;

                var comma = Tok(SqlToken.Comma, "','")(rest);
                if (!comma.HasValue) break;
                rest = comma.Remainder;
            }
        }

        var close = RParen(rest);
        if (!close.HasValue)
            return Fail<SqlExpr>(close.Remainder, close.Expectations);

        // Azure resolves the function as soon as the call is parsed, before any later syntax error.
        var function = name.Value.ToStringValue().ToLowerInvariant();
        SqlBinder.CheckFunction(function, args.Count);
        return TokenListParserResult.Value<SqlToken, SqlExpr>(new SqlFunctionCall(function, args), input, close.Remainder);
    };

    // ── Scalar expressions ───────────────────────────────────────────────────

    private static readonly TokenListParser<SqlToken, SqlExpr> ParenthesizedScalar =
        from open in LParen
        from inner in Parse.Ref(() => Additive!)
        from close in RParen
        select inner;

    private static readonly TokenListParser<SqlToken, SqlExpr> Primary =
        Literal
            .Or(Parameter)
            .Or(FunctionCall)
            .Or(Property.Select(p => (SqlExpr)p))
            .Or(ParenthesizedScalar);

    private static readonly TokenListParser<SqlToken, SqlExpr> Unary =
        Tok(SqlToken.Minus, "'-'").IgnoreThen(Parse.Ref(() => Unary!)).Select(e => (SqlExpr)new SqlUnary(SqlUnaryOp.Negate, e))
            .Or(Tok(SqlToken.Plus, "'+'").IgnoreThen(Parse.Ref(() => Unary!)).Select(e => (SqlExpr)new SqlUnary(SqlUnaryOp.Plus, e)))
            .Or(Primary);

    private static readonly TokenListParser<SqlToken, SqlBinaryOp> MultiplicativeOp =
        Tok(SqlToken.Star, "'*'").Value(SqlBinaryOp.Multiply)
            .Or(Tok(SqlToken.Slash, "'/'").Value(SqlBinaryOp.Divide))
            .Or(Tok(SqlToken.Percent, "'%'").Value(SqlBinaryOp.Modulo));

    private static readonly TokenListParser<SqlToken, SqlBinaryOp> AdditiveOp =
        Tok(SqlToken.Plus, "'+'").Value(SqlBinaryOp.Add)
            .Or(Tok(SqlToken.Minus, "'-'").Value(SqlBinaryOp.Subtract));

    private static readonly TokenListParser<SqlToken, SqlExpr> Multiplicative =
        Parse.Chain(MultiplicativeOp, Unary, (op, l, r) => new SqlBinary(op, l, r));

    private static readonly TokenListParser<SqlToken, SqlExpr> Additive =
        Parse.Chain(AdditiveOp, Multiplicative, (op, l, r) => new SqlBinary(op, l, r));

    // ── Predicates ───────────────────────────────────────────────────────────

    private static readonly TokenListParser<SqlToken, SqlBinaryOp> ComparisonOp =
        Tok(SqlToken.Equal, "'='").Value(SqlBinaryOp.Equal)
            .Or(Tok(SqlToken.NotEqual, "'<>'").Value(SqlBinaryOp.NotEqual))
            .Or(Tok(SqlToken.Less, "'<'").Value(SqlBinaryOp.Less))
            .Or(Tok(SqlToken.LessOrEqual, "'<='").Value(SqlBinaryOp.LessOrEqual))
            .Or(Tok(SqlToken.Greater, "'>'").Value(SqlBinaryOp.Greater))
            .Or(Tok(SqlToken.GreaterOrEqual, "'>='").Value(SqlBinaryOp.GreaterOrEqual));

    private static TokenListParser<SqlToken, SqlExpr> LikeTail(SqlExpr left, bool negated) =>
        from like in Kw("LIKE")
        from pattern in Additive
        from escape in Kw("ESCAPE").IgnoreThen(Additive).Select(e => (SqlExpr?)e).OptionalOrDefault()
        select (SqlExpr)new SqlLike(left, pattern, escape, negated);

    private static TokenListParser<SqlToken, SqlExpr> InTail(SqlExpr left, bool negated) =>
        from @in in Kw("IN")
        from open in LParen
        from items in Additive.AtLeastOnceDelimitedBy(Tok(SqlToken.Comma, "','"))
        from close in RParen
        select (SqlExpr)new SqlIn(left, items, negated);

    private static TokenListParser<SqlToken, SqlExpr> IsNullTail(SqlPropertyRef property) =>
        from @is in Kw("IS")
        from negated in Kw("NULL").Value(false).Or(Kw("NOT").IgnoreThen(Kw("NULL")).Value(true))
        select (SqlExpr)new SqlIsNull(property, negated);

    // scalar followed by a comparison, [NOT] LIKE, [NOT] IN, or (on a property) IS [NOT] NULL.
    // A bare scalar is not a predicate.
    private static readonly TokenListParser<SqlToken, SqlExpr> Comparison = input =>
    {
        var left = Additive(input);
        if (!left.HasValue)
            return left;

        var l = left.Value;
        var tail = ComparisonOp.Then(op => Additive.Select(r => (SqlExpr)new SqlBinary(op, l, r)))
            .Or(Kw("NOT").IgnoreThen(LikeTail(l, negated: true).Or(InTail(l, negated: true))))
            .Or(LikeTail(l, negated: false))
            .Or(InTail(l, negated: false));
        if (l is SqlPropertyRef property)
            tail = tail.Or(IsNullTail(property));

        var result = tail(left.Remainder);
        return result.HasValue
            ? TokenListParserResult.Value(result.Value, input, result.Remainder)
            : result;
    };

    private static readonly TokenListParser<SqlToken, SqlExpr> Exists =
        from exists in Kw("EXISTS")
        from open in LParen
        from property in Property
        from close in RParen
        select (SqlExpr)new SqlExists(property);

    // A parenthesised predicate. After it, Azure expects ')' or a continuation of the predicate.
    private const string GroupedPredicateFollow = "')', or AND, or OR";

    private static readonly TokenListParser<SqlToken, SqlExpr> GroupedPredicate =
        from open in LParen
        from inner in Parse.Ref(() => Or!)
        from close in Tok(SqlToken.RParen, GroupedPredicateFollow)
        select inner;

    private static readonly TokenListParser<SqlToken, SqlExpr> PrimaryPredicate =
        Exists.Or(Furthest(GroupedPredicate, Comparison));

    private static readonly TokenListParser<SqlToken, SqlExpr> Not =
        Kw("NOT").IgnoreThen(Parse.Ref(() => Not!)).Select(e => (SqlExpr)new SqlUnary(SqlUnaryOp.Not, e))
            .Or(PrimaryPredicate);

    private static readonly TokenListParser<SqlToken, SqlExpr> And =
        Parse.Chain(Kw("AND"), Not, (_, l, r) => new SqlBinary(SqlBinaryOp.And, l, r));

    private static readonly TokenListParser<SqlToken, SqlExpr> Or =
        Parse.Chain(Kw("OR"), And, (_, l, r) => new SqlBinary(SqlBinaryOp.Or, l, r));

    // ── Actions ──────────────────────────────────────────────────────────────

    private static readonly TokenListParser<SqlToken, SqlStatement> Statement =
        (from set in Kw("SET")
         from target in Property
         from eq in Tok(SqlToken.Equal, "'='")
         from value in Additive
         select (SqlStatement)new SqlSet(target, value))
        .Or(from remove in Kw("REMOVE")
            from target in Property
            select (SqlStatement)new SqlRemove(target));

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Tries both alternatives from the same input and keeps the success, or else the failure that got further
    /// (merging expectations on a tie). Disambiguates '(' between a grouped predicate and a parenthesised scalar.
    /// </summary>
    private static TokenListParser<SqlToken, SqlExpr> Furthest(TokenListParser<SqlToken, SqlExpr> first,
        TokenListParser<SqlToken, SqlExpr> second) => input =>
    {
        var a = first(input);
        if (a.HasValue) return a;
        var b = second(input);
        if (b.HasValue) return b;

        if (a.Remainder.Position != b.Remainder.Position)
            return a.Remainder.Position > b.Remainder.Position ? a : b;
        return TokenListParserResult.Empty<SqlToken, SqlExpr>(a.Remainder,
            [.. (a.Expectations ?? []).Concat(b.Expectations ?? []).Distinct()]);
    };

    private static TokenListParserResult<SqlToken, T> Fail<T>(TokenList<SqlToken> at, string[]? expectations) =>
        TokenListParserResult.Empty<SqlToken, T>(at, expectations ?? []);

    private static long ParseInteger(Token<SqlToken> token)
    {
        var text = token.ToStringValue();
        if (long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            return value;
        throw SqlSyntaxException.Syntax(token.Span.Source!, token.Position.Absolute, text,
            "Value was either too large or too small for an Int64.");
    }

    private static bool Is(string text, string keyword) => text.Equals(keyword, StringComparison.OrdinalIgnoreCase);

    private static string Unescape(string delimited, string doubled, string single) =>
        delimited[1..^1].Replace(doubled, single, StringComparison.Ordinal);

    private static SqlSyntaxException Error(string text, TokenList<SqlToken> at, string[]? expectations, Mode mode)
    {
        var next = at.ConsumeToken();
        // Azure reports a string literal at its closing quote, every other token at its first character.
        var position = !next.HasValue ? text.Length
            : next.Value.Kind == SqlToken.String ? next.Value.Position.Absolute + next.Value.Span.Length - 1
            : next.Value.Position.Absolute;
        var tokenText = !next.HasValue ? ""
            : next.Value.Kind == SqlToken.String ? "'"
            : next.Value.ToStringValue();
        var unexpected = next.HasValue ? TokenClass(next.Value, mode) : "EOF";

        var details = $"syntax error, unexpected {unexpected}";
        var expected = (expectations ?? []).Distinct().ToList();
        // Azure lists what may follow a parenthesised predicate only when the input ends there.
        if (next.HasValue)
            expected.Remove(GroupedPredicateFollow);
        var count = expected.Sum(e => e.Split(", or ").Length);
        if (count is > 0 and <= MaxListedExpectations)
            details += ", expecting " + string.Join(", or ", expected);

        return SqlSyntaxException.Syntax(text, position, tokenText, details, expected);
    }

    // The token class names Azure's parser reports in "unexpected X".
    private static string TokenClass(Token<SqlToken> token, Mode mode) => token.Kind switch
    {
        SqlToken.Identifier when Reserved.Contains(token.ToStringValue()) && (mode == Mode.Action
            || !Is(token.ToStringValue(), "SET") && !Is(token.ToStringValue(), "REMOVE"))
            => token.ToStringValue().ToUpperInvariant(),
        SqlToken.Identifier => "IDENTIFIER",
        SqlToken.QuotedIdentifier or SqlToken.BracketedIdentifier => "DELIMITED_IDENTIFIER",
        SqlToken.String => "STRING",
        SqlToken.Integer => "INTEGER",
        SqlToken.Decimal => "DECIMAL",
        SqlToken.Float => "DOUBLE",
        SqlToken.Parameter => "PARAMETER",
        SqlToken.Equal => mode == Mode.Filter ? "EQ" : "'='",
        SqlToken.NotEqual => "NE",
        SqlToken.Less => "LT",
        SqlToken.LessOrEqual => "LE",
        SqlToken.Greater => "GT",
        SqlToken.GreaterOrEqual => "GE",
        _ => $"'{token.ToStringValue()}'"
    };
}
