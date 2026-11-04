namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>Property scope. Unprefixed names are user properties, as on Azure.</summary>
public enum SqlScope
{
    User,
    System
}

public abstract record SqlExpr;

/// <summary>A literal: <see cref="long"/>, <see cref="double"/>, <see cref="string"/>, <see cref="bool"/> or null.</summary>
public sealed record SqlLiteral(object? Value) : SqlExpr;

public sealed record SqlPropertyRef(SqlScope Scope, string Name) : SqlExpr;

/// <summary>A <c>@name</c> parameter; <see cref="Name"/> includes the <c>@</c>.</summary>
public sealed record SqlParameterRef(string Name) : SqlExpr;

public enum SqlUnaryOp
{
    Not,
    Negate,
    Plus
}

public sealed record SqlUnary(SqlUnaryOp Op, SqlExpr Operand) : SqlExpr;

public enum SqlBinaryOp
{
    Or,
    And,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo
}

public sealed record SqlBinary(SqlBinaryOp Op, SqlExpr Left, SqlExpr Right) : SqlExpr;

public sealed record SqlLike(SqlExpr Operand, SqlExpr Pattern, SqlExpr? Escape, bool Negated) : SqlExpr;

public sealed record SqlIn(SqlExpr Operand, IReadOnlyList<SqlExpr> Items, bool Negated) : SqlExpr;

/// <summary><c>IS [NOT] NULL</c>; Azure only allows it directly on a property.</summary>
public sealed record SqlIsNull(SqlPropertyRef Property, bool Negated) : SqlExpr;

public sealed record SqlExists(SqlPropertyRef Property) : SqlExpr;

/// <summary>A function call; <see cref="Name"/> is lower-cased.</summary>
public sealed record SqlFunctionCall(string Name, IReadOnlyList<SqlExpr> Arguments) : SqlExpr;

public abstract record SqlStatement;

public sealed record SqlSet(SqlPropertyRef Target, SqlExpr Value) : SqlStatement;

public sealed record SqlRemove(SqlPropertyRef Target) : SqlStatement;
