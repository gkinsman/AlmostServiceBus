using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Thrown when a SQL filter expression is not valid Service Bus filter syntax, or uses a
/// construct the emulator does not support. Rules are rejected when created rather than
/// silently matching everything (or nothing).
/// </summary>
public sealed class SqlFilterException(string message) : FormatException(message);

/// <summary>
/// A parsed Service Bus SQL filter expression.
/// </summary>
/// <remarks>
/// Implements the <see href="https://learn.microsoft.com/azure/service-bus-messaging/service-bus-messaging-sql-filter">SQL filter grammar</see>:
/// <c>AND</c>/<c>OR</c>/<c>NOT</c>, comparisons, <c>[NOT] LIKE ... [ESCAPE ...]</c>, <c>[NOT] IN (...)</c>,
/// <c>IS [NOT] NULL</c>, <c>EXISTS(...)</c>, arithmetic, <c>sys.</c>/<c>user.</c> scopes and
/// regular, <c>"double-quoted"</c> and <c>[bracketed]</c> identifiers. Evaluation uses SQL
/// three-valued logic: a missing property is <c>NULL</c>, any comparison with it is unknown,
/// and only an expression that is definitely true matches.
/// </remarks>
public sealed class SqlFilterExpression
{
    private readonly Node _root;

    private SqlFilterExpression(string text, Node root)
    {
        Text = text;
        _root = root;
    }

    /// <summary>The expression text this was parsed from.</summary>
    public string Text { get; }

    /// <summary>Parses <paramref name="expression"/>; throws <see cref="SqlFilterException"/> if it is invalid.</summary>
    public static SqlFilterExpression Parse(string expression)
    {
        var parser = new Parser(Lexer.Tokenize(expression));
        return new SqlFilterExpression(expression, parser.ParseFilter());
    }

    /// <summary>Returns <see langword="true"/> only when the expression is definitely true for the message.</summary>
    public bool Matches(BrokeredMessage message) => _root.Evaluate(message) is true;

    // ── Lexer ────────────────────────────────────────────────────────────────

    private enum TokenKind { Identifier, DelimitedIdentifier, String, Number, Symbol, End }

    private readonly record struct Token(TokenKind Kind, string Text, object? Value, int Position)
    {
        public bool IsKeyword(string keyword) =>
            Kind == TokenKind.Identifier && Text.Equals(keyword, StringComparison.OrdinalIgnoreCase);

        public bool IsSymbol(string symbol) => Kind == TokenKind.Symbol && Text == symbol;

        public override string ToString() => Kind == TokenKind.End ? "end of expression" : $"'{Text}'";
    }

    private static class Lexer
    {
        public static List<Token> Tokenize(string text)
        {
            var tokens = new List<Token>();
            var i = 0;
            while (i < text.Length)
            {
                var c = text[i];
                if (char.IsWhiteSpace(c)) { i++; continue; }

                var start = i;
                if (char.IsLetter(c) || c == '_')
                {
                    while (i < text.Length && (char.IsLetterOrDigit(text[i]) || text[i] == '_')) i++;
                    tokens.Add(new Token(TokenKind.Identifier, text[start..i], null, start));
                }
                else if (char.IsDigit(c) || (c == '.' && i + 1 < text.Length && char.IsDigit(text[i + 1])))
                {
                    tokens.Add(ReadNumber(text, ref i));
                }
                else if (c == '\'')
                {
                    var value = ReadDelimited(text, ref i, '\'', "string literal");
                    tokens.Add(new Token(TokenKind.String, text[start..i], value, start));
                }
                else if (c == '"')
                {
                    var name = ReadDelimited(text, ref i, '"', "quoted identifier");
                    tokens.Add(new Token(TokenKind.DelimitedIdentifier, text[start..i], name, start));
                }
                else if (c == '[')
                {
                    var name = ReadDelimited(text, ref i, ']', "bracketed identifier");
                    tokens.Add(new Token(TokenKind.DelimitedIdentifier, text[start..i], name, start));
                }
                else
                {
                    var two = i + 1 < text.Length ? text.Substring(i, 2) : null;
                    if (two is "<>" or "!=" or "<=" or ">=")
                    {
                        tokens.Add(new Token(TokenKind.Symbol, two, null, start));
                        i += 2;
                    }
                    else if ("=<>+-*/%(),.".Contains(c))
                    {
                        tokens.Add(new Token(TokenKind.Symbol, c.ToString(), null, start));
                        i++;
                    }
                    else
                    {
                        throw new SqlFilterException($"Unexpected character '{c}' at position {start}.");
                    }
                }
            }

            tokens.Add(new Token(TokenKind.End, string.Empty, null, text.Length));
            return tokens;
        }

        // Reads text up to the closing delimiter; a doubled closing delimiter is an escaped one.
        private static string ReadDelimited(string text, ref int i, char close, string what)
        {
            var start = i++;
            var sb = new StringBuilder();
            while (i < text.Length)
            {
                if (text[i] == close)
                {
                    if (i + 1 < text.Length && text[i + 1] == close)
                    {
                        sb.Append(close);
                        i += 2;
                        continue;
                    }

                    i++;
                    if (close != '\'' && sb.Length == 0)
                        throw new SqlFilterException($"Empty {what} at position {start}.");
                    return sb.ToString();
                }

                sb.Append(text[i++]);
            }

            throw new SqlFilterException($"Unterminated {what} starting at position {start}.");
        }

        private static Token ReadNumber(string text, ref int i)
        {
            var start = i;
            var isDecimal = false;
            var isFloat = false;
            while (i < text.Length && char.IsDigit(text[i])) i++;
            if (i < text.Length && text[i] == '.')
            {
                isDecimal = true;
                i++;
                while (i < text.Length && char.IsDigit(text[i])) i++;
            }

            if (i < text.Length && (text[i] is 'e' or 'E'))
            {
                var j = i + 1;
                if (j < text.Length && text[j] is '+' or '-') j++;
                if (j < text.Length && char.IsDigit(text[j]))
                {
                    isFloat = true;
                    i = j;
                    while (i < text.Length && char.IsDigit(text[i])) i++;
                }
            }

            if (i < text.Length && (char.IsLetter(text[i]) || text[i] == '_'))
                throw new SqlFilterException($"Invalid numeric literal at position {start}.");

            var literal = text[start..i];
            object value = isFloat
                ? double.Parse(literal, CultureInfo.InvariantCulture)
                : isDecimal || !long.TryParse(literal, NumberStyles.None, CultureInfo.InvariantCulture, out var l)
                    ? decimal.Parse(literal, CultureInfo.InvariantCulture)
                    : l;
            return new Token(TokenKind.Number, literal, value, start);
        }
    }

    // ── Parser ───────────────────────────────────────────────────────────────

    private static readonly HashSet<string> ReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND", "OR", "NOT", "IN", "LIKE", "ESCAPE", "IS", "NULL", "EXISTS", "TRUE", "FALSE"
    };

    private sealed class Parser(List<Token> tokens)
    {
        private int _pos;

        private Token Current => tokens[_pos];

        private Token Next() => tokens[_pos++];

        private bool AcceptKeyword(string keyword)
        {
            if (!Current.IsKeyword(keyword)) return false;
            _pos++;
            return true;
        }

        private bool AcceptSymbol(string symbol)
        {
            if (!Current.IsSymbol(symbol)) return false;
            _pos++;
            return true;
        }

        private void ExpectSymbol(string symbol)
        {
            if (!AcceptSymbol(symbol))
                throw Error($"Expected '{symbol}'");
        }

        private SqlFilterException Error(string what) =>
            new($"{what} but found {Current} at position {Current.Position}.");

        public Node ParseFilter()
        {
            if (Current.Kind == TokenKind.End)
                throw new SqlFilterException("The SQL filter expression is empty.");

            var node = ParseOr();
            if (Current.Kind != TokenKind.End)
                throw Error("Expected end of expression");
            return node;
        }

        private Node ParseOr()
        {
            var left = ParseAnd();
            while (AcceptKeyword("OR"))
                left = new OrNode(left, ParseAnd());
            return left;
        }

        private Node ParseAnd()
        {
            var left = ParseNot();
            while (AcceptKeyword("AND"))
                left = new AndNode(left, ParseNot());
            return left;
        }

        private Node ParseNot() =>
            AcceptKeyword("NOT") ? new NotNode(ParseNot()) : ParsePredicate();

        private Node ParsePredicate()
        {
            if (AcceptKeyword("EXISTS"))
            {
                ExpectSymbol("(");
                var property = ParseProperty();
                ExpectSymbol(")");
                return new ExistsNode(property);
            }

            var left = ParseAdditive();

            if (Current.Kind == TokenKind.Symbol && Current.Text is "=" or "<>" or "!=" or "<" or "<=" or ">" or ">=")
            {
                var op = Next().Text;
                return new ComparisonNode(op, left, ParseAdditive());
            }

            if (AcceptKeyword("IS"))
            {
                var negated = AcceptKeyword("NOT");
                if (!AcceptKeyword("NULL"))
                    throw Error("Expected NULL");
                return new IsNullNode(left, negated);
            }

            var not = AcceptKeyword("NOT");
            if (AcceptKeyword("LIKE"))
                return Negate(ParseLikeTail(left), not);
            if (AcceptKeyword("IN"))
                return Negate(ParseInTail(left), not);
            if (not)
                throw Error("Expected LIKE or IN after NOT");

            return left;
        }

        private static Node Negate(Node node, bool negate) => negate ? new NotNode(node) : node;

        private Node ParseLikeTail(Node left)
        {
            if (Current.Kind != TokenKind.String)
                throw Error("Expected a string pattern after LIKE");
            var pattern = (string)Next().Value!;

            char? escape = null;
            if (AcceptKeyword("ESCAPE"))
            {
                if (Current.Kind != TokenKind.String || ((string)Current.Value!).Length != 1)
                    throw Error("Expected a single-character string after ESCAPE");
                escape = ((string)Next().Value!)[0];
            }

            return new LikeNode(left, BuildLikeRegex(pattern, escape));
        }

        private Node ParseInTail(Node left)
        {
            ExpectSymbol("(");
            var items = new List<Node> { ParseAdditive() };
            while (AcceptSymbol(","))
                items.Add(ParseAdditive());
            ExpectSymbol(")");
            return new InNode(left, items);
        }

        private Node ParseAdditive()
        {
            var left = ParseMultiplicative();
            while (Current.IsSymbol("+") || Current.IsSymbol("-"))
                left = new ArithmeticNode(Next().Text[0], left, ParseMultiplicative());
            return left;
        }

        private Node ParseMultiplicative()
        {
            var left = ParseUnary();
            while (Current.IsSymbol("*") || Current.IsSymbol("/") || Current.IsSymbol("%"))
                left = new ArithmeticNode(Next().Text[0], left, ParseUnary());
            return left;
        }

        private Node ParseUnary()
        {
            if (AcceptSymbol("-")) return new NegateNode(ParseUnary());
            if (AcceptSymbol("+")) return ParseUnary();
            return ParsePrimary();
        }

        private Node ParsePrimary()
        {
            var token = Current;
            switch (token.Kind)
            {
                case TokenKind.String:
                case TokenKind.Number:
                    _pos++;
                    return new LiteralNode(token.Value);

                case TokenKind.Symbol when token.Text == "(":
                    _pos++;
                    var inner = ParseOr();
                    ExpectSymbol(")");
                    return inner;

                case TokenKind.Identifier when token.IsKeyword("NULL"):
                    _pos++;
                    return new LiteralNode(null);

                case TokenKind.Identifier when token.IsKeyword("TRUE"):
                    _pos++;
                    return new LiteralNode(true);

                case TokenKind.Identifier when token.IsKeyword("FALSE"):
                    _pos++;
                    return new LiteralNode(false);

                case TokenKind.Identifier when tokens[_pos + 1].IsSymbol("("):
                    throw new SqlFilterException(
                        $"The function '{token.Text}' at position {token.Position} is not supported by the emulator.");

                case TokenKind.Identifier or TokenKind.DelimitedIdentifier:
                    return ParseProperty();

                default:
                    throw Error("Expected a value, property or '('");
            }
        }

        private PropertyNode ParseProperty()
        {
            var first = ReadIdentifierPart();

            if (first.Kind == TokenKind.Identifier && Current.IsSymbol(".") &&
                (first.Text.Equals("sys", StringComparison.OrdinalIgnoreCase) ||
                 first.Text.Equals("user", StringComparison.OrdinalIgnoreCase)))
            {
                _pos++;
                var name = ReadDottedName(ReadIdentifierPart());
                return first.Text.Equals("sys", StringComparison.OrdinalIgnoreCase)
                    ? PropertyNode.System(name, first.Position)
                    : PropertyNode.User(name);
            }

            // Unscoped names are user properties.
            return PropertyNode.User(ReadDottedName(first));
        }

        private string ReadDottedName(Token first)
        {
            var name = new StringBuilder(IdentifierName(first));
            while (AcceptSymbol("."))
                name.Append('.').Append(IdentifierName(ReadIdentifierPart()));
            return name.ToString();
        }

        private Token ReadIdentifierPart()
        {
            var token = Current;
            if (token.Kind == TokenKind.DelimitedIdentifier ||
                (token.Kind == TokenKind.Identifier && !ReservedWords.Contains(token.Text)))
            {
                _pos++;
                return token;
            }

            throw Error("Expected a property name");
        }

        private static string IdentifierName(Token token) =>
            token.Kind == TokenKind.DelimitedIdentifier ? (string)token.Value! : token.Text;
    }

    private static Regex BuildLikeRegex(string pattern, char? escape)
    {
        var sb = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (escape is { } e && c == e)
            {
                if (++i >= pattern.Length)
                    throw new SqlFilterException("A LIKE pattern cannot end with its escape character.");
                sb.Append(Regex.Escape(pattern[i].ToString()));
            }
            else if (c == '%') sb.Append(".*");
            else if (c == '_') sb.Append('.');
            else sb.Append(Regex.Escape(c.ToString()));
        }

        return new Regex(sb.Append('$').ToString(), RegexOptions.IgnoreCase | RegexOptions.Singleline);
    }

    // ── Evaluation ───────────────────────────────────────────────────────────
    // Every node yields a value; null means SQL NULL / unknown.

    private abstract class Node
    {
        public abstract object? Evaluate(BrokeredMessage message);
    }

    private sealed class LiteralNode(object? value) : Node
    {
        public override object? Evaluate(BrokeredMessage message) => value;
    }

    private sealed class PropertyNode : Node
    {
        private readonly Func<BrokeredMessage, (bool Exists, object? Value)> _resolve;

        private PropertyNode(Func<BrokeredMessage, (bool, object?)> resolve) => _resolve = resolve;

        public static PropertyNode User(string name) =>
            new(m => m.ApplicationProperties.TryGetValue(name, out var v) ? (true, v) : (false, null));

        public static PropertyNode System(string name, int position)
        {
            Func<BrokeredMessage, object?> getter = name.ToLowerInvariant() switch
            {
                "messageid" => m => m.MessageId,
                "correlationid" => m => m.CorrelationId,
                "to" => m => m.To,
                "replyto" => m => m.ReplyTo,
                "label" or "subject" => m => m.Subject,
                "sessionid" => m => m.SessionId,
                "replytosessionid" => m => m.ReplyToSessionId,
                "contenttype" => m => m.ContentType,
                "partitionkey" => m => m.PartitionKey,
                "sequencenumber" => m => m.SequenceNumber,
                "deliverycount" => m => m.DeliveryCount,
                "enqueuedtimeutc" => m => m.EnqueuedTimeUtc.UtcDateTime,
                "scheduledenqueuetimeutc" => m => m.ScheduledEnqueueTimeUtc?.UtcDateTime,
                "timetolive" => m => m.TimeToLive,
                "size" => m => (long)m.Body.Length,
                _ => throw new SqlFilterException($"'sys.{name}' at position {position} is not a known system property.")
            };
            return new PropertyNode(m => getter(m) is { } v ? (true, v) : (false, null));
        }

        public bool Exists(BrokeredMessage message) => _resolve(message).Exists;

        public override object? Evaluate(BrokeredMessage message) => _resolve(message).Value;
    }

    private sealed class ExistsNode(PropertyNode property) : Node
    {
        public override object? Evaluate(BrokeredMessage message) => property.Exists(message);
    }

    private sealed class IsNullNode(Node operand, bool negated) : Node
    {
        public override object? Evaluate(BrokeredMessage message) => (operand.Evaluate(message) is null) != negated;
    }

    private sealed class NotNode(Node operand) : Node
    {
        public override object? Evaluate(BrokeredMessage message) =>
            operand.Evaluate(message) is bool b ? !b : null;
    }

    private sealed class AndNode(Node left, Node right) : Node
    {
        public override object? Evaluate(BrokeredMessage message)
        {
            var l = left.Evaluate(message) as bool?;
            if (l == false) return false;
            var r = right.Evaluate(message) as bool?;
            if (r == false) return false;
            return l == true && r == true ? true : null;
        }
    }

    private sealed class OrNode(Node left, Node right) : Node
    {
        public override object? Evaluate(BrokeredMessage message)
        {
            var l = left.Evaluate(message) as bool?;
            if (l == true) return true;
            var r = right.Evaluate(message) as bool?;
            if (r == true) return true;
            return l == false && r == false ? false : null;
        }
    }

    private sealed class ComparisonNode(string op, Node left, Node right) : Node
    {
        public override object? Evaluate(BrokeredMessage message)
        {
            var result = Values.Compare(left.Evaluate(message), right.Evaluate(message), ordered: op is not ("=" or "<>" or "!="));
            if (result is not { } c) return null;
            return op switch
            {
                "=" => c == 0,
                "<>" or "!=" => c != 0,
                "<" => c < 0,
                "<=" => c <= 0,
                ">" => c > 0,
                _ => c >= 0
            };
        }
    }

    private sealed class InNode(Node operand, List<Node> items) : Node
    {
        public override object? Evaluate(BrokeredMessage message)
        {
            var value = operand.Evaluate(message);
            if (value is null) return null;

            var sawUnknown = false;
            foreach (var item in items)
            {
                var c = Values.Compare(value, item.Evaluate(message), ordered: false);
                if (c == 0) return true;
                if (c is null) sawUnknown = true;
            }

            return sawUnknown ? null : false;
        }
    }

    private sealed class LikeNode(Node operand, Regex pattern) : Node
    {
        public override object? Evaluate(BrokeredMessage message) =>
            operand.Evaluate(message) is { } value ? pattern.IsMatch(Values.ToText(value)) : null;
    }

    private sealed class NegateNode(Node operand) : Node
    {
        public override object? Evaluate(BrokeredMessage message) =>
            Values.Arithmetic('*', operand.Evaluate(message), -1L);
    }

    private sealed class ArithmeticNode(char op, Node left, Node right) : Node
    {
        public override object? Evaluate(BrokeredMessage message) =>
            Values.Arithmetic(op, left.Evaluate(message), right.Evaluate(message));
    }

    private static class Values
    {
        /// <summary>
        /// Compares two values, returning null when either is NULL or the types cannot be compared.
        /// A numeric string compares against a number (e.g. a property "27" equals 27).
        /// </summary>
        public static int? Compare(object? a, object? b, bool ordered)
        {
            if (a is null || b is null) return null;

            if (IsNumeric(a) || IsNumeric(b))
            {
                if (!TryNumber(a, out var x) || !TryNumber(b, out var y)) return null;
                return x is double || y is double
                    ? Convert.ToDouble(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToDouble(y, CultureInfo.InvariantCulture))
                    : ((decimal)x).CompareTo((decimal)y);
            }

            if (a is string || b is string)
            {
                if (a is not string && b is not string) return null;
                return string.CompareOrdinal(ToText(a), ToText(b));
            }

            if (a is bool ba && b is bool bb)
                return ordered ? null : ba.CompareTo(bb);

            if (a.GetType() == b.GetType() && a is IComparable comparable)
                return comparable.CompareTo(b);

            return null;
        }

        public static object? Arithmetic(char op, object? a, object? b)
        {
            if (!TryNumber(a, out var x) || !TryNumber(b, out var y)) return null;

            if (x is double || y is double)
            {
                var dx = Convert.ToDouble(x, CultureInfo.InvariantCulture);
                var dy = Convert.ToDouble(y, CultureInfo.InvariantCulture);
                return op switch
                {
                    '+' => dx + dy,
                    '-' => dx - dy,
                    '*' => dx * dy,
                    '/' => dy == 0 ? null : dx / dy,
                    _ => dy == 0 ? null : dx % dy
                };
            }

            var mx = (decimal)x;
            var my = (decimal)y;
            try
            {
                return op switch
                {
                    '+' => mx + my,
                    '-' => mx - my,
                    '*' => mx * my,
                    '/' => my == 0 ? null : mx / my,
                    _ => my == 0 ? null : mx % my
                };
            }
            catch (OverflowException)
            {
                return null;
            }
        }

        public static string ToText(object value) => value switch
        {
            string s => s,
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };

        private static bool IsNumeric(object value) => value is sbyte or byte or short or ushort or int or uint
            or long or ulong or float or double or decimal;

        // Normalises a value to decimal, or double for floating-point values.
        private static bool TryNumber(object? value, out object number)
        {
            switch (value)
            {
                case float or double:
                    number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                    return true;
                case sbyte or byte or short or ushort or int or uint or long or ulong or decimal:
                    number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                    return true;
                case string s when decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d):
                    number = d;
                    return true;
                default:
                    number = 0m;
                    return false;
            }
        }
    }
}
