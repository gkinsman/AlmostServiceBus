using System.Globalization;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>Renders an AST as a compact, fully parenthesised string so grammar tests can compare shapes.</summary>
internal static class SqlAstPrinter
{
    public static string Print(SqlExpr e) => e switch
    {
        SqlLiteral { Value: null } => "null",
        SqlLiteral { Value: string s } => "'" + s.Replace("'", "''") + "'",
        SqlLiteral { Value: long l } => l.ToString(CultureInfo.InvariantCulture) + "L",
        SqlLiteral { Value: double d } => d.ToString("R", CultureInfo.InvariantCulture) + "D",
        SqlLiteral { Value: bool b } => b ? "true" : "false",
        SqlPropertyRef p => (p.Scope == SqlScope.System ? "sys:" : "user:") + p.Name,
        SqlParameterRef p => p.Name,
        SqlUnary u => $"({u.Op} {Print(u.Operand)})",
        SqlBinary b => $"({b.Op} {Print(b.Left)} {Print(b.Right)})",
        SqlLike l => $"({(l.Negated ? "NotLike" : "Like")} {Print(l.Operand)} {Print(l.Pattern)}"
                     + (l.Escape is null ? "" : " " + Print(l.Escape)) + ")",
        SqlIn i => $"({(i.Negated ? "NotIn" : "In")} {Print(i.Operand)} [{string.Join(", ", i.Items.Select(Print))}])",
        SqlIsNull n => $"({(n.Negated ? "IsNotNull" : "IsNull")} {Print(n.Property)})",
        SqlExists x => $"(Exists {Print(x.Property)})",
        SqlFunctionCall f => $"(Call {f.Name} [{string.Join(", ", f.Arguments.Select(Print))}])",
        _ => throw new ArgumentOutOfRangeException(nameof(e), e.GetType().Name)
    };

    public static string Print(IEnumerable<SqlStatement> statements) => string.Join("; ", statements.Select(s => s switch
    {
        SqlSet set => $"(Set {Print(set.Target)} {Print(set.Value)})",
        SqlRemove r => $"(Remove {Print(r.Target)})",
        _ => throw new ArgumentOutOfRangeException(nameof(statements), s.GetType().Name)
    }));
}
