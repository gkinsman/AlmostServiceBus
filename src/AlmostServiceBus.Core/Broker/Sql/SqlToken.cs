namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// Token kinds of the Service Bus SQL filter/action language. Keywords are not separate kinds:
/// they arrive as <see cref="Identifier"/> and the grammar matches them case-insensitively.
/// </summary>
public enum SqlToken
{
    None,
    Identifier,
    QuotedIdentifier,
    BracketedIdentifier,
    Parameter,
    String,
    Integer,
    Decimal,
    Float,
    Dot,
    Comma,
    LParen,
    RParen,
    Semicolon,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
}
