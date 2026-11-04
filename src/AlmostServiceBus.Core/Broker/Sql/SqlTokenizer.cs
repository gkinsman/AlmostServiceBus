using Superpower;
using Superpower.Model;

namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// Splits a Service Bus SQL expression into tokens. A hand-rolled <see cref="Tokenizer{TKind}"/> rather than
/// a <c>TokenizerBuilder</c> so that lexical errors carry Azure's wording ("Unrecognized character",
/// "The string (…) was not terminated properly").
/// </summary>
public static class SqlTokenizer
{
    private static readonly Lexer Instance = new();

    /// <exception cref="SqlSyntaxException">The text contains a character or literal Service Bus can't lex.</exception>
    public static TokenList<SqlToken> Tokenize(string text) => Instance.Tokenize(text);

    private sealed class Lexer : Tokenizer<SqlToken>
    {
        protected override IEnumerable<Result<SqlToken>> Tokenize(TextSpan span)
        {
            var text = span.Source!;
            var i = span.Position.Absolute;
            while (i < text.Length)
            {
                var c = text[i];
                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }

                var start = i;
                SqlToken kind;
                if (char.IsLetter(c) || c == '_')
                {
                    i = ScanWord(text, i + 1);
                    kind = SqlToken.Identifier;
                }
                else if (c == '@')
                {
                    i = ScanWord(text, i + 1);
                    if (i == start + 1)
                        throw SqlSyntaxException.Syntax(text, start, "@", "Unrecognized character. '@'");
                    kind = SqlToken.Parameter;
                }
                else if (char.IsAsciiDigit(c))
                {
                    (i, kind) = ScanNumber(text, i);
                }
                else if (c == '\'')
                {
                    i = ScanDelimited(text, i, '\'', "string");
                    kind = SqlToken.String;
                }
                else if (c == '"')
                {
                    i = ScanDelimited(text, i, '"', "delimited identifier");
                    kind = SqlToken.QuotedIdentifier;
                }
                else if (c == '[')
                {
                    i = ScanDelimited(text, i, ']', "delimited identifier");
                    kind = SqlToken.BracketedIdentifier;
                }
                else
                {
                    (i, kind) = ScanSymbol(text, i);
                }

                yield return Result.Value(kind, Span(span, start, i), Span(span, i, text.Length));
            }
        }

        private static TextSpan Span(TextSpan whole, int from, int to)
        {
            var text = whole.Source!;
            return new TextSpan(text, PositionAt(text, from), to - from);
        }

        private static Position PositionAt(string text, int absolute)
        {
            var line = 1;
            var column = 1;
            for (var k = 0; k < absolute; k++)
            {
                if (text[k] == '\n')
                {
                    line++;
                    column = 1;
                }
                else
                {
                    column++;
                }
            }

            return new Position(absolute, line, column);
        }

        private static int ScanWord(string text, int i)
        {
            while (i < text.Length && (char.IsLetterOrDigit(text[i]) || text[i] == '_')) i++;
            return i;
        }

        // Digits [. digits] [e|E [+|-] digits]. `1.` and `.5` are not numbers: the dot is left for the
        // grammar to reject, as Azure does. An exponent marker without digits is left as an identifier.
        private static (int End, SqlToken Kind) ScanNumber(string text, int i)
        {
            var kind = SqlToken.Integer;
            while (i < text.Length && char.IsAsciiDigit(text[i])) i++;

            if (i + 1 < text.Length && text[i] == '.' && char.IsAsciiDigit(text[i + 1]))
            {
                kind = SqlToken.Decimal;
                i += 2;
                while (i < text.Length && char.IsAsciiDigit(text[i])) i++;
            }

            if (i < text.Length && text[i] is 'e' or 'E')
            {
                var j = i + 1;
                if (j < text.Length && text[j] is '+' or '-') j++;
                if (j < text.Length && char.IsAsciiDigit(text[j]))
                {
                    kind = SqlToken.Float;
                    i = j;
                    while (i < text.Length && char.IsAsciiDigit(text[i])) i++;
                }
            }

            return (i, kind);
        }

        // Returns the index after the closing delimiter; a doubled closer is an escaped one.
        private static int ScanDelimited(string text, int i, char close, string what)
        {
            var open = i++;
            while (i < text.Length)
            {
                if (text[i] == close)
                {
                    if (i + 1 < text.Length && text[i + 1] == close)
                    {
                        i += 2;
                        continue;
                    }

                    return i + 1;
                }

                i++;
            }

            throw SqlSyntaxException.Syntax(text, text.Length, "",
                $"The {what} ({text[(open + 1)..]}) was not terminated properly.");
        }

        private static (int End, SqlToken Kind) ScanSymbol(string text, int i)
        {
            var next = i + 1 < text.Length ? text[i + 1] : '\0';
            switch (text[i])
            {
                case '<' when next == '>': return (i + 2, SqlToken.NotEqual);
                case '!' when next == '=': return (i + 2, SqlToken.NotEqual);
                case '<' when next == '=': return (i + 2, SqlToken.LessOrEqual);
                case '>' when next == '=': return (i + 2, SqlToken.GreaterOrEqual);
                case '<': return (i + 1, SqlToken.Less);
                case '>': return (i + 1, SqlToken.Greater);
                case '=': return (i + 1, SqlToken.Equal);
                case '+': return (i + 1, SqlToken.Plus);
                case '-': return (i + 1, SqlToken.Minus);
                case '*': return (i + 1, SqlToken.Star);
                case '/': return (i + 1, SqlToken.Slash);
                case '%': return (i + 1, SqlToken.Percent);
                case '(': return (i + 1, SqlToken.LParen);
                case ')': return (i + 1, SqlToken.RParen);
                case ',': return (i + 1, SqlToken.Comma);
                case ';': return (i + 1, SqlToken.Semicolon);
                case '.': return (i + 1, SqlToken.Dot);
                default:
                    throw SqlSyntaxException.Syntax(text, i, text[i].ToString(), $"Unrecognized character. '{text[i]}'");
            }
        }
    }
}
