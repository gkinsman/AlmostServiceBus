# SQL Filters and Rule Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the regex SQL handling in `RuleEntity` with a full Superpower-based implementation of the Service Bus SQL grammar for filters, rule actions and parameters. Validate rules on every creation path (in code, HTTP admin, AMQP rule manager), and prove behaviour against real Azure with a shared case table.

**Architecture:** A Superpower tokenizer and token-list grammar produce an immutable AST. `SqlFilter` and `SqlRuleAction` compile an expression plus its parameters once, and evaluate it against a `BrokeredMessage` using three-valued logic. Every behaviour that the docs leave ambiguous lives as a named constant in `SqlSemantics`, so the Azure compat run (Task 13) changes one file. A JSON case table, shared by the unit suite and the conformance suite, holds Azure's answers.

**Tech Stack:** .NET 10, Superpower 3.2.1, AMQPNetLite 2.5.5, xUnit 2.9.3, Azure.Messaging.ServiceBus 7.21.0 (tests), Node `@azure/service-bus` and Java `azure-messaging-servicebus` (smoke scripts).

**Spec:** `docs/superpowers/specs/2026-11-04-sql-filters-and-actions-design.md`. Read it before starting any task. Its "Questions the compat run settles" list is the source of every `SqlSemantics` constant.

## Global Constraints

- Parser library: **Superpower 3.2.1**, added to `src/AlmostServiceBus.Core/AlmostServiceBus.Core.csproj`. No Sprache, and no hand-written parser.
- **No fallback, ever.** No code path may turn an unparseable or unevaluable expression into a match, or into a silent no-match.
- Parse errors carry the **position** (absolute index, line, column) and the **expected tokens**.
- Filter and action expressions longer than **1024** characters are rejected at creation. This mirrors the SDK's `MaximumSqlRuleFilterStatementLength` / `MaximumSqlRuleActionStatementLength`.
- **Azure connection string.** It is resolved by one shared helper: first the `ASB_CONNECTION_STRING` environment variable, then .NET user secrets with id `almostservicebus-conformance` and key `ASB_CONNECTION_STRING`. It never touches git. Record mode is turned on by `ASB_COMPAT_RECORD=1`.
- `tests/client-sdk-smoke` must not change how it gets its connection: it reads `ASB_CONNECTION_STRING` to override the emulator, which is why the secret isn't a global environment variable.
- Commit locally on `fleet/session-c54d42a2`. **Don't push, open a PR or comment on issue #192.**
- `TreatWarningsAsErrors` is on, solution-wide. Match the surrounding comment density and naming.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Test command for all suites (from CLAUDE.md): `dotnet test AlmostServiceBus.sln --filter "FullyQualifiedName!~RealAsbConformanceTests" --verbosity quiet`. The three `RealAsbTopicRequeueReproTest` cases show as failed without `ASB_CONNECTION_STRING`; that is pre-existing and expected.

## Review Focus

1. **Framework-generated filters must still work.** NServiceBus writes `[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'` (a bracketed name containing dots). Wolverine writes `NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'`. MassTransit passes user SQL straight through. These are table cases in Task 7, and the CI framework suites must stay green.
2. **AMQP property values that aren't C# primitives.** A message property can arrive as `ulong`, `uint`, `ushort`, `sbyte`, `char`, `float`, `byte[]`, `Amqp.Types.Symbol`, or a `DateTime` with `Kind=Local`. Each must normalise to a defined SQL type, or be treated as an opaque value that compares unknown. It must never throw. The test lives in Task 4.
3. **Culture.** Under `CultureInfo.CurrentCulture = de-DE`, the literal `1.5` parses as one and a half, and a numeric string property compares with invariant rules. The test lives in Task 2 and Task 4.
4. **Deep nesting and length.** A 1024-character expression of nested parentheses parses without a stack overflow. A 1025-character expression is rejected at creation with a clear message. The test lives in Task 2.
5. **A rule changed while messages are being published.** Replacing a rule with `AddOrUpdateRule` while another thread publishes must not throw, and must not evaluate a torn half-old/half-new rule. The compiled filter and action are immutable and swapped as one reference. The test lives in Task 6.

---

## File Structure

New, under `src/AlmostServiceBus.Core/Broker/Sql/` (namespace `AlmostServiceBus.Core.Broker.Sql`):

| File | Responsibility |
|---|---|
| `SqlToken.cs` | Token kinds |
| `SqlTokenizer.cs` | Superpower tokenizer, from text to `TokenList<SqlToken>` |
| `SqlAst.cs` | AST records for expressions and action statements |
| `SqlGrammar.cs` | Combinators: `ParseFilter`, `ParseAction` |
| `SqlSyntaxException.cs` | `SqlSyntaxException` (creation-time) and `SqlEvaluationException` (runtime) |
| `SqlSemantics.cs` | Named constants for every compat-decided behaviour |
| `SqlValues.cs` | Value normalisation, comparison, arithmetic, `LIKE` regex |
| `SqlSystemProperties.cs` | `sys.` name → getter/setter on `BrokeredMessage` |
| `SqlEvaluator.cs` | Evaluates the AST against a `SqlEvaluationContext` |
| `SqlFilter.cs` | Compiled filter: parse, bind parameters, `Matches` |
| `SqlRuleAction.cs` | Compiled action: parse, bind parameters, `Apply` |

New elsewhere:

| File | Responsibility |
|---|---|
| `src/AlmostServiceBus.Core/Amqp/RuleManagementHandler.cs` | AMQP `add-rule` / `remove-rule` / `enumerate-rules` |
| `tests/shared/sql-compat-cases.json` | The shared case table (Azure's answers) |
| `tests/shared/SqlCompatCases.cs` | Table model, loader, typed values and record-mode writer (no SDK or Core references) |
| `tests/AlmostServiceBus.Tests/Broker/Sql/*Tests.cs` | Tokenizer/grammar, values, evaluator, table runner and issue-repro tests |
| `tests/AlmostServiceBus.SdkIntegration.Tests/SqlRuleTests.cs` | SDK end-to-end tests |
| `tests/AlmostServiceBus.Conformance.Tests/SqlCompatTests.cs` | Table against emulator and Azure, diff, record mode |

Modified:
- `Broker/RuleEntity.cs`, `Broker/SubscriptionEntity.cs`
- `Management/AtomXmlReader.cs`, `Management/AtomXmlWriter.cs`, `Management/ManagementApiEndpoints.cs`
- `Amqp/ManagementLinkEndpoint.cs`
- `tests/client-sdk-smoke/node/smoke.mjs`, `tests/client-sdk-smoke/java/.../Smoke.java`
- `CHANGELOG.md`, `CLAUDE.md`, and the spec's decisions log

Deleted: `Broker/SqlFilterExpression.cs` and `tests/AlmostServiceBus.Tests/Broker/SqlFilterExpressionTests.cs`, both from WIP commit `38a31bd`. Port that test file's cases into Task 7's table before deleting it.

---

### Task 1: Tokenizer and syntax exception

**Files:**
- Modify: `src/AlmostServiceBus.Core/AlmostServiceBus.Core.csproj` (add `<PackageReference Include="Superpower" Version="3.2.1" />`)
- Create: `Broker/Sql/SqlToken.cs`, `Broker/Sql/SqlTokenizer.cs`, `Broker/Sql/SqlSyntaxException.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlTokenizerTests.cs`

**Interfaces:**
- Produces:
  - `public enum SqlToken { None, Identifier, QuotedIdentifier, BracketedIdentifier, Parameter, String, Integer, Decimal, Float, Dot, Comma, LParen, RParen, Semicolon, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Plus, Minus, Star, Slash, Percent }`
    - Keywords are **not** separate tokens. They are `Identifier` tokens, matched case-insensitively by the grammar.
    - `NotEqual` covers both `<>` and `!=`.
  - `public static class SqlTokenizer { public static TokenList<SqlToken> Tokenize(string text); }`. It throws `SqlSyntaxException` on bad input.
  - `public sealed class SqlSyntaxException : FormatException`
    - Constructor: `(string message, int position, int line, int column, IReadOnlyList<string> expected)`.
    - Properties: `int Position`, `int Line`, `int Column`, `IReadOnlyList<string> Expected`.
    - `Message` includes `"at line {line}, column {column}"` and, when `Expected` is non-empty, `"expected {a}, {b} or {c}"`.
  - `public sealed class SqlEvaluationException(string message) : Exception(message)`, in the same file.

- [ ] **Step 1: Write the failing tests** in `SqlTokenizerTests`:
  - `Tokenize("user.[message-type] IN ('a''b', 1.5e3)")` yields these kinds: `Identifier, Dot, BracketedIdentifier, Identifier, LParen, String, Comma, Float, RParen`.
  - `"\"a\"\"b\""` yields a single `QuotedIdentifier` whose span text is `"\"a\"\"b\""`. Unescaping happens in the grammar.
  - `"@p1"` yields `Parameter`. `"<> != <= >= ="` yields `NotEqual, NotEqual, LessOrEqual, GreaterOrEqual, Equal`.
  - `"42 4.2 .5 1E-3"` yields `Integer, Decimal, Decimal, Float`.
  - `Tokenize("a = 'open")` throws `SqlSyntaxException` with `Column == 5`.
  - `Tokenize("a = #")` throws with `Column == 5`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlTokenizerTests`. Expected: build fails (types missing).
- [ ] **Step 3: Implement.** Use `TokenizerBuilder<SqlToken>` with `.Ignore(Span.WhiteSpace)`.
  - Strings and delimited identifiers are custom `TextParser<TextSpan>`s: an opening delimiter, then a run of (doubled closer or any non-closer), then the closer.
  - Identifiers are `[A-Za-z_][A-Za-z0-9_]*`. Match longest operators first.
  - Convert Superpower's `Result` error into `SqlSyntaxException`, using `ErrorPosition` (1-based line and column) and `Expectations`.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): Superpower tokenizer for Service Bus SQL`.

### Task 2: AST and filter grammar

**Files:**
- Create: `Broker/Sql/SqlAst.cs`, `Broker/Sql/SqlGrammar.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlGrammarTests.cs`

**Interfaces:**
- Consumes: `SqlTokenizer.Tokenize`, `SqlSyntaxException` (Task 1).
- Produces (`SqlAst.cs`):
  ```csharp
  public enum SqlScope { Unscoped, User, System }
  public abstract record SqlExpr;
  public sealed record SqlLiteral(object? Value) : SqlExpr;            // long, double, string, bool, or null
  public sealed record SqlPropertyRef(SqlScope Scope, string Name) : SqlExpr;
  public sealed record SqlParameterRef(string Name) : SqlExpr;         // name without '@'
  public enum SqlUnaryOp { Not, Negate, Plus }
  public sealed record SqlUnary(SqlUnaryOp Op, SqlExpr Operand) : SqlExpr;
  public enum SqlBinaryOp { Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Add, Subtract, Multiply, Divide, Modulo }
  public sealed record SqlBinary(SqlBinaryOp Op, SqlExpr Left, SqlExpr Right) : SqlExpr;
  public sealed record SqlLike(SqlExpr Operand, SqlExpr Pattern, SqlExpr? Escape, bool Negated) : SqlExpr;
  public sealed record SqlIn(SqlExpr Operand, IReadOnlyList<SqlExpr> Items, bool Negated) : SqlExpr;
  public sealed record SqlIsNull(SqlExpr Operand, bool Negated) : SqlExpr;
  public sealed record SqlExists(SqlPropertyRef Property) : SqlExpr;
  public sealed record SqlFunctionCall(string Name, IReadOnlyList<SqlExpr> Arguments) : SqlExpr; // "newid" | "property", lower-case
  public abstract record SqlStatement;
  public sealed record SqlSet(SqlPropertyRef Target, SqlExpr Value) : SqlStatement;
  public sealed record SqlRemove(SqlPropertyRef Target) : SqlStatement;
  ```
- Produces (`SqlGrammar.cs`):
  - `public static class SqlGrammar { public static SqlExpr ParseFilter(string text); }`. It parses an expression to end of input, or throws `SqlSyntaxException`. It also throws for an empty or whitespace-only `text`, or for text longer than 1024 characters (message: `"SQL expression exceeds the maximum length of 1024 characters"`).
  - Records use `IReadOnlyList`, so value equality on lists does not hold. Tests should compare with a helper that prints the AST (e.g. a `SqlAstPrinter.Print(SqlExpr) => string` in the test project), not with `Assert.Equal` on records.

- [ ] **Step 1: Write the failing tests** in `SqlGrammarTests`, as a `[Theory]` of expression and the expected printed AST. Use a fully parenthesised print, e.g. `(Or (Equal user:a 1) (In unscoped:b ['x','y']))`. Cases:
  - **Precedence:** `a = 1 OR b = 2 AND c = 3` gives OR at the top. `NOT a = 1 AND b = 2` gives `(And (Not (Equal ..)) ..)`. `1 + 2 * 3` gives Add at the top. `-a * 2` gives `(Multiply (Negate a) 2)`.
  - **Every grammar element from the spec's "Grammar to support":**
    - `sys.Label`, `user.x`, `"q""x"` (name `q"x`), `[b]]x]` (name `b]x`), `user.[a-b]`, `sys."Label"`.
    - Dotted regular names: `NServiceBus.EnclosedMessageTypes` is `unscoped:NServiceBus.EnclosedMessageTypes`; `[NServiceBus.EnclosedMessageTypes]` gives the same name.
    - Literals `'it''s'`, `42` (long), `4.2` and `1e3` (double), `TRUE`, `false`, `NULL`.
    - Each comparison operator, `LIKE 'a%' ESCAPE '!'`, `NOT LIKE`, `IN (1, 2)`, `NOT IN ('a')`, `IS NULL`, `IS NOT NULL`, `EXISTS(user.x)`, `newid()`, `property('x')`, `@p`.
  - **Case-insensitive keywords:** `a in ('x') and not b is null`.
  - **`SqlGrammarErrorTests`**, a `[Theory]` of expression, column and an expected-token substring:
    - `"a = "` → column 5, `expected` contains `"value"`, or whatever Superpower names the expression parser. Pin whatever names the grammar assigns with `.Named(...)`: name the expression parser `"expression"` and the property parser `"property name"`.
    - `"a IN ('x'"` → expects `")"`. `"a IN ()"` → expects `"expression"`. `"a NOT 1"` → expects `"LIKE"` or `"IN"`. `"[] = 1"` → error. `"a = 1 b"` → expects end of input.
    - `"sys. = 1"` → expects `"property name"`. `"foo(1)"` → message contains `"unknown function 'foo'"`. `"newid(1)"` → message contains `"takes 0 arguments"`.
  - **Review Focus 4:** a nesting test (`new string('(', 500) + "1=1" + new string(')', 500)`, which is 1006 chars) parses. A 1025-char expression throws with `"1024"` in the message.
  - **Review Focus 3:** under `CultureInfo.CurrentCulture = new("de-DE")`, `ParseFilter("a = 1.5")` yields the literal `1.5d`. Restore the culture in `finally`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~SqlGrammar"`. Expected: build fails.
- [ ] **Step 3: Implement.**
  - Build the combinators bottom-up: `primary` (literal, parameter, function call, property, or parenthesised expression via `Parse.Ref`), then `unary`, `multiplicative` and `additive` via `Parse.Chain`, then `predicate` (additive followed by an optional comparison, LIKE, IN or IS tail; `EXISTS` is handled here too), then `not`, `and` and `or` (via `Parse.Chain`). Finish with `.AtEnd()`.
  - Use `Token.EqualToValueIgnoreCase(SqlToken.Identifier, "AND")` and the like for keywords. A regular identifier equal to a reserved word (`AND OR NOT IN LIKE ESCAPE IS NULL EXISTS TRUE FALSE`) is not a property name.
  - A scope applies only when the first part is the regular identifier `sys` or `user` (case-insensitive) followed by `Dot`.
  - Convert parse failures into `SqlSyntaxException`, mapping the token-list error position back to text line and column.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): filter grammar and AST`.

### Task 3: Action grammar

**Files:**
- Modify: `Broker/Sql/SqlGrammar.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlActionGrammarTests.cs`

**Interfaces:**
- Produces: `public static IReadOnlyList<SqlStatement> ParseAction(string text)` on `SqlGrammar`. It applies the same empty-input and 1024-character rules as `ParseFilter`.

- [ ] **Step 1: Write the failing tests:**
  - `SET user.a = 1` gives one `SqlSet`.
  - `SET a = b + 1; REMOVE sys.Label;` gives a `SqlSet` and then a `SqlRemove` (the trailing `;` is allowed).
  - `set [x-y] = @p ; remove "z"` works (keywords are case-insensitive).
  - The errors below throw `SqlSyntaxException` with a column:
    - `SET a 1` expects `"="`.
    - `REMOVE` expects `"property name"`.
    - `SET a = 1 REMOVE b` expects `";"` or end of input.
    - `UPDATE a = 1` expects `"SET"` or `"REMOVE"`.
    - `;` is an error.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlActionGrammarTests`. Expected: FAIL.
- [ ] **Step 3: Implement** `statement = SET property '=' expression | REMOVE property`, and `action = statement (';' statement)* ';'? end`, reusing Task 2's property and expression parsers.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): SET/REMOVE action grammar`.

### Task 4: Values, system properties and semantics constants

**Files:**
- Create: `Broker/Sql/SqlSemantics.cs`, `Broker/Sql/SqlValues.cs`, `Broker/Sql/SqlSystemProperties.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlValuesTests.cs`

**Interfaces:**
- Produces: `internal static class SqlSemantics`. Every member is a provisional doc-based answer, each with a `// compat Q<n>` comment naming the spec question it answers:
  ```csharp
  internal const SqlScope UnscopedNamesResolveTo = SqlScope.User;            // Q1
  internal const bool SystemNamesIgnoreCase = true;                          // Q2
  internal const bool UserNamesIgnoreCase = false;                           // Q2
  internal const bool UnknownSystemPropertyIsSyntaxError = true;             // Q3
  internal const bool StringEqualityIgnoresCase = false;                     // Q4
  internal const bool LikeIgnoresCase = false;                               // Q4
  internal const bool StringsAreOrdered = true;                              // Q4 (< > on strings)
  internal const bool TypeMismatchIsEvaluationError = false;                 // Q5 (false => unknown)
  internal const bool NumericStringsCoerce = false;                          // Q5 ("27" = 27)
  internal const bool DivideByZeroIsEvaluationError = false;                 // Q6 (false => unknown)
  internal const bool ActionStatementsSeeEarlierStatements = true;           // Q8
  internal const bool OneCopyPerMatchingActionRule = false;                  // Q9
  internal const bool EmptyExpressionIsSyntaxError = true;                   // Q11
  internal const bool UnboundParameterIsSyntaxError = true;                  // Q12
  internal const bool UnusedParameterIsSyntaxError = false;                  // Q12
  internal const bool ParameterNamesIgnoreCase = true;                       // Q12 (SDK PropertyDictionary is OrdinalIgnoreCase)
  ```
  Notes on the provisional values:
  - `NumericStringsCoerce = false` departs from the WIP behaviour, where `"27" = 27` was true. The docs give no coercion. Q5 settles it.
  - `LikeIgnoresCase = false` departs from today's emulator. Q4 settles it.
  - If the compat run disagrees with `EmptyExpressionIsSyntaxError`, empty expressions become match-all **only** if Azure does that.
- Produces: `internal static class SqlValues`:
  - `object? Normalize(object? value)` maps every input to one of these:
    - Integer types (`sbyte`, `byte`, `short`, `ushort`, `int`, `uint`, `long`) become `long`. A `ulong` above `long.MaxValue` becomes `double`.
    - `float`, `double` and `decimal` become `double`.
    - `char` and `Amqp.Types.Symbol` become `string`.
    - `DateTime` is converted to UTC. `DateTimeOffset` becomes its `UtcDateTime`.
    - `bool`, `Guid`, `TimeSpan` and `string` stay as they are.
    - `null` stays `null`.
    - Anything else (e.g. `byte[]`) becomes `SqlValues.Opaque` (a private sealed class wrapping the value), which compares unknown with everything.
  - `int? Compare(object? a, object? b, bool ordered)` normalises both sides and returns `null` (unknown) on a `NULL` operand or an incomparable pair. When `SqlSemantics.TypeMismatchIsEvaluationError` is true, an incomparable pair throws `SqlEvaluationException` instead.
  - `object? Arithmetic(SqlBinaryOp op, object? a, object? b)` promotes `long`/`long` to `long` (with `checked`; overflow is unknown) and anything else numeric to `double`. Non-numeric operands are unknown. Division by zero follows `SqlSemantics`.
  - `Regex BuildLikeRegex(string pattern, char? escape)` translates `%` to `.*` and `_` to `.`. The escape character escapes the next character. A pattern ending in the escape character throws `SqlSyntaxException`. Uses `RegexOptions.Singleline`, plus `IgnoreCase` per `SqlSemantics.LikeIgnoresCase`.
- Produces: `internal static class SqlSystemProperties`:
  - `bool IsKnown(string name)`, `bool IsWritable(string name)`.
  - `object? Get(BrokeredMessage message, string name)`, `void Set(BrokeredMessage message, string name, object? value)`, `void Remove(BrokeredMessage message, string name)`.
  - Names are matched per `SqlSemantics.SystemNamesIgnoreCase`. Mapping:
    - `MessageId`, `CorrelationId`, `To`, `ReplyTo`, `SessionId`, `ReplyToSessionId`, `ContentType`, `PartitionKey`: the field of the same name.
    - `Label` and `Subject`: `Subject`.
    - `SequenceNumber` and `EnqueuedSequenceNumber`: `SequenceNumber`.
    - `DeliveryCount`: `DeliveryCount`.
    - `EnqueuedTimeUtc`: `EnqueuedTimeUtc.UtcDateTime`.
    - `ScheduledEnqueueTimeUtc`: the UTC `DateTime`, or null.
    - `ExpiresAtUtc`: `EnqueuedTimeUtc + TimeToLive`, saturating when TTL is `MaxValue`.
    - `TimeToLive`: `TimeToLive`.
    - `Size`: `(long)Body.Length`.
  - Provisionally writable: `MessageId`, `CorrelationId`, `To`, `ReplyTo`, `Label`/`Subject`, `SessionId`, `ReplyToSessionId`, `ContentType`, `PartitionKey`, `TimeToLive`, `ScheduledEnqueueTimeUtc`. That answers Q7, and Task 13 settles it.

- [ ] **Step 1: Write the failing tests:**
  - **`Normalize`** (Review Focus 2): a `[Theory]` over each input type in the list above, asserting the output type and value.
    - `byte[]` compares unknown with `"x"` and with itself.
    - `new Amqp.Types.Symbol("abc")` equals `"abc"`.
    - A `DateTime` with `Kind=Local` equals its UTC counterpart.
  - **`Compare`:**
    - `(5L, 5.0d)` gives 0. `("a", "b", ordered: true)` gives a negative number. `(true, true, ordered: false)` gives 0. `(true, false, ordered: true)` gives null.
    - `("27", 27L)` gives null (per `NumericStringsCoerce = false`). `(null, 1L)` gives null.
    - `(Guid g, g)` gives 0.
  - **`Arithmetic`:** `long.MaxValue + 1` gives null. `7 % 2` gives `1L`. `7 / 2` gives `3L`. `7 / 2.0` gives `3.5d`. `1 / 0` gives null.
  - **Culture** (Review Focus 3): under `de-DE`, `Compare(1.5d, 1.5d)` gives 0.
  - **`BuildLikeRegex`:**
    - `'50!%' ESCAPE '!'` matches `"50%"` and not `"500"`.
    - `'a_c'` matches `"abc"`. `'a%'` matches `"a\nb"`.
    - `'abc!'` with escape `'!'` throws.
  - **`SqlSystemProperties`:**
    - `Get(msg, "label")` returns `Subject`.
    - `IsKnown("NoSuch")` is false.
    - `Set(msg, "Label", "x")` sets `Subject`.
    - `IsWritable("SequenceNumber")` is false.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlValuesTests`. Expected: FAIL.
- [ ] **Step 3: Implement** the three classes. Use `CultureInfo.InvariantCulture` in every conversion.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): value semantics and system property map`.

### Task 5: Evaluator, `SqlFilter`, `SqlRuleAction`, parameters

**Files:**
- Create: `Broker/Sql/SqlEvaluator.cs`, `Broker/Sql/SqlFilter.cs`, `Broker/Sql/SqlRuleAction.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlEvaluatorTests.cs`

**Interfaces:**
- Consumes: Tasks 2–4.
- Produces:
  - `public sealed record SqlEvaluationContext(BrokeredMessage Message, IReadOnlyDictionary<string, object?> Parameters)`. Parameter keys are stored without `@`, using a comparer chosen by `SqlSemantics.ParameterNamesIgnoreCase`.
  - `internal static class SqlEvaluator { public static object? Evaluate(SqlExpr expr, SqlEvaluationContext ctx); }`. It returns `bool`, `long`, `double`, `string`, `DateTime`, `Guid`, `TimeSpan` or `null` (unknown / NULL).
    - Three-valued `AND`/`OR`/`NOT`, as in the spec's "Semantics".
    - An unscoped name resolves per `SqlSemantics.UnscopedNamesResolveTo`.
    - `property(x)` evaluates `x`, and a string result is looked up as `SqlScope.Unscoped` (Task 13 confirms).
    - `newid()` returns `Guid.NewGuid()`.
    - `IN` returns true on any equal item. Otherwise it returns unknown if any comparison was unknown, else false.
    - A non-literal `LIKE` pattern or escape is evaluated at runtime. A non-string result is unknown.
  - `public sealed class SqlFilter`:
    - Members: `public string Expression { get; }`, `public IReadOnlyDictionary<string, object?> Parameters { get; }`, `public bool Matches(BrokeredMessage message)` (true only for `true`), and `public static SqlFilter Parse(string expression, IReadOnlyDictionary<string, object?>? parameters = null)`.
    - `Parse` normalises parameter keys by stripping one leading `@`, and normalises values with `SqlValues.Normalize`.
    - It walks the AST and throws `SqlSyntaxException` for an unbound `@p` (per `UnboundParameterIsSyntaxError`) and for an unknown `sys.` name (per `UnknownSystemPropertyIsSyntaxError`).
  - `public sealed class SqlRuleAction`:
    - Members: `Expression`, `Parameters` and `static Parse(...)`, with the same validation as `SqlFilter`. `Parse` also checks that every `SET`/`REMOVE` target is either a user property or `SqlSystemProperties.IsWritable`; anything else throws `SqlSyntaxException`.
    - `public void Apply(BrokeredMessage message)` runs the statements in order. Each statement sees the earlier statements' effects per `SqlSemantics.ActionStatementsSeeEarlierStatements`. When that is false, the evaluation context reads a `Clone()` snapshot taken before the first statement.
    - `SET user.x = <NULL>` stores no value: it removes `x`. Task 13 confirms this.

- [ ] **Step 1: Write the failing tests** in `SqlEvaluatorTests`:
  - The issue's five expressions × `Created`/`Unrelated` (expected: true for `Created` only).
  - Three-valued truth tables: `NULL AND FALSE` is false, `NULL OR TRUE` is true, `NOT NULL` is unknown.
  - Missing property: `color <> 'red'` and `NOT (color = 'red')` don't match; `color IS NULL` matches.
  - Parameters:
    - `SqlFilter.Parse("a = @p", new Dictionary<string, object?> { ["@p"] = 5 })` matches `a = 5L`. A key of `"p"` is accepted too.
    - `Parse("a = @missing")` throws `SqlSyntaxException`.
  - `Parse("sys.Nope = 1")` throws.
  - Action: `SqlRuleAction.Parse("SET user.b = user.a * 2; REMOVE user.a; SET sys.Label = 'routed'").Apply(msg)` with `a = 3L` leaves `b == 6L`, `a` absent and `Subject == "routed"`.
  - Action: `Parse("SET sys.SequenceNumber = 1")` throws.
  - Action: `Parse("SET user.x = @v", {["v"]="p"})` sets `x = "p"`.
  - `newid()`: `newid() IS NOT NULL` matches.
  - `property('a') = 1` matches `a = 1L`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlEvaluatorTests`. Expected: FAIL.
- [ ] **Step 3: Implement** the evaluator as a `switch` over the AST record types, then the two compiled classes.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): evaluator, compiled filters and actions with parameters`.

### Task 6: Broker integration (rules, validation, actions on delivery)

**Files:**
- Modify: `Broker/RuleEntity.cs`, `Broker/SubscriptionEntity.cs`
- Delete: `Broker/SqlFilterExpression.cs`, `tests/AlmostServiceBus.Tests/Broker/SqlFilterExpressionTests.cs`. First copy its cases into a scratch list for Task 7.
- Test: `tests/AlmostServiceBus.Tests/Broker/SubscriptionEntityTests.cs` (keep the existing SQL tests; they must still pass) and a new `tests/AlmostServiceBus.Tests/Broker/RuleActionDeliveryTests.cs`

**Interfaces:**
- Consumes: `SqlFilter`, `SqlRuleAction`, `SqlSyntaxException`, `SqlEvaluationException`.
- Produces on `RuleEntity`:
  - New properties: `public Dictionary<string, object?>? FilterParameters { get; set; }` and `public Dictionary<string, object?>? ActionParameters { get; set; }`.
  - `public void Validate()` compiles the filter (when `FilterType == SqlFilter`) and the action (when `ActionExpression` is not null), or throws `SqlSyntaxException`.
  - `public bool Matches(BrokeredMessage message)` keeps its signature. It may throw `SqlEvaluationException`. It throws `SqlSyntaxException` for a rule that was never validated and is invalid.
  - `public bool HasAction { get; }` and `public void ApplyAction(BrokeredMessage message)`.
  - Compiled objects are cached in one immutable `private sealed record Compiled(string? FilterText, IReadOnlyDictionary<...>? FilterParams, SqlFilter? Filter, string? ActionText, ..., SqlRuleAction? Action)`. The cache is a single reference that is rebuilt when any input changes.
  - All regex code is deleted.
- Produces on `SubscriptionEntity`:
  - `AddOrUpdateRule` calls `rule.Validate()` before storing.
  - `DeliverMessage` changes as follows:
    1. Evaluate every rule, ordered by name with `StringComparer.OrdinalIgnoreCase`, so the result is deterministic.
    2. If a rule throws `SqlEvaluationException`: when `DeadLetteringOnFilterEvaluationExceptions` is set, dead-letter the message on arrival with reason `"FilterEvaluationException"` and the exception message as the description (Task 13 replaces both with Azure's strings), then stop. Otherwise skip that rule.
    3. With `SqlSemantics.OneCopyPerMatchingActionRule == false`, deliver one copy and apply every matching rule's action to it, in rule order.
    4. When it is true, deliver one separate `Clone()` per matching rule that has an action, plus one unmodified copy if any matching rule has no action.
    5. Actions run before the session-null check and before `Queue.Enqueue`. With `ForwardTo` set, `Queue.Enqueue` forwards the modified copy.

- [ ] **Step 1: Write the failing tests** in `RuleActionDeliveryTests`:
  - `AddOrUpdateRule_InvalidFilter_Throws_AndRuleAbsent` uses `"color IN ('a'"`.
  - `AddOrUpdateRule_InvalidAction_Throws` uses `"SET"`.
  - `Publish_RuleWithAction_SubscriptionCopyHasProperty_OriginalAndOtherSubscriptionUntouched`:
    - Topic with subscription A (rule `1=1`, action `SET user.tag = 'A'`) and subscription B (`$Default`).
    - Publish one message.
    - A's received copy has `tag == "A"`. B's copy and the original have no `tag`.
  - `Publish_ActionSetsLabel_Forwarded_TargetQueueSeesLabel`: subscription with `ForwardTo` a queue and action `SET sys.Label = 'fwd'`; the queue's message has `Subject == "fwd"`.
  - `Publish_TwoMatchingActionRules_OneCopy_BothActionsApplied`. This asserts the provisional Q9 answer; Task 13 may flip it.
  - `Publish_FilterEvaluationError_DeadLetters`. Skip it with a `[Fact(Skip = "enabled in Task 13 if Azure has evaluation errors")]` until Task 13 knows which expressions raise errors. Keep the mechanism covered with an internal test seam: a `RuleEntity` whose compiled filter throws, injected through an `internal` constructor overload. The `InternalsVisibleTo` for the test project is already there.
  - `ConcurrentRuleReplaceDuringPublish_NeverThrows` (Review Focus 5): one thread alternates `AddOrUpdateRule` between `a = 1` and `a = 2` 10,000 times, while another publishes 10,000 messages with `a = 1`. There must be no exceptions, and every delivered copy must have matched one of the two versions.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~RuleActionDeliveryTests|FullyQualifiedName~SubscriptionEntityTests"`. Expected: FAIL.
- [ ] **Step 3: Implement** the `RuleEntity` and `SubscriptionEntity` changes.
- [ ] **Step 4: Run** the full `tests/AlmostServiceBus.Tests` project. Expected: everything passes, except `SubscriptionEntityTests` cases whose expectation conflicts with a provisional `SqlSemantics` value. Update each such case to the provisional answer with a `// compat Q<n>` comment. The only expected conflict is `ignore NOT LIKE 'true'` case handling, and only if case matters.
- [ ] **Step 5: Commit** with `feat(broker): compiled SQL rules, validation on add, actions applied to delivered copy`.

### Task 7: Shared case table and broker-level table runner

**Files:**
- Create: `tests/shared/sql-compat-cases.json`, `tests/shared/SqlCompatCases.cs`
- Modify: `tests/AlmostServiceBus.Tests/AlmostServiceBus.Tests.csproj`. Add `<Compile Include="..\shared\SqlCompatCases.cs" Link="Shared\SqlCompatCases.cs" />` and `<None Include="..\shared\sql-compat-cases.json" Link="Shared\sql-compat-cases.json" CopyToOutputDirectory="PreserveNewest" />`.
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlCaseTableTests.cs`, `tests/AlmostServiceBus.Tests/Broker/Sql/Issue192ReproTests.cs`

**Interfaces:**
- Produces the JSON schema below. `System.Text.Json`, camelCase.
  ```json
  { "cases": [ {
      "id": "in.string.nomatch",              // unique, stable, [a-z0-9.-]
      "area": "in",                            // grammar/semantic area, for the report
      "rules": [ { "filter": "color IN ('red','blue')",
                   "filterParameters": { "p": { "type": "long", "value": "5" } },
                   "action": null, "actionParameters": null } ],
      "message": { "sys": { "Label": "x", "CorrelationId": "c", "To": null, "ReplyTo": null,
                            "SessionId": null, "ReplyToSessionId": null, "ContentType": null,
                            "MessageId": null, "TimeToLive": null },
                   "user": { "color": { "type": "string", "value": "green" } } },
      "expected": { "outcome": "NoMatch",     // Match | NoMatch | Rejected | DeadLettered
                    "copies": 1,               // Match only
                    "user": null, "sys": null }, // Match only: full user property set / sys values after actions
      "verified": "doc",                       // doc | azure
      "note": null
  } ] }
  ```
  - Typed `type` values: `string`, `int`, `long`, `double`, `bool`, `datetime` (ISO-8601 UTC), `guid`, `timespan` (`c` format), `null`.
  - A rule's `filter` may be null, meaning a `TrueFilter`.
- Produces (`SqlCompatCases.cs`, namespace `AlmostServiceBus.Tests.Shared`, no SDK or Core references):
  - Records `SqlCompatCase`, `SqlCompatRule`, `SqlCompatMessage`, `SqlCompatExpected`, `TypedValue` (with `object? ToClr()` and `static TypedValue From(object? value)`).
  - `static IReadOnlyList<SqlCompatCase> SqlCompatCases.Load()`, which reads the copy in the output directory.
  - `static string SqlCompatCases.SourcePath()`, which walks up from `AppContext.BaseDirectory` to the directory containing `AlmostServiceBus.sln` and returns `tests/shared/sql-compat-cases.json`.
  - `static void SqlCompatCases.Save(IReadOnlyList<SqlCompatCase>)`, which writes to `SourcePath()` (indented, stable ordering).

- [ ] **Step 1: Write `Issue192ReproTests`** with the issue's program body verbatim, inside a `[Fact]`. Replace `Console.WriteLine` with collecting mismatch lines, and `Assert.Empty(mismatches)`.
- [ ] **Step 2: Write the table.** Use at least the cases below, each with doc-based `expected` and `"verified": "doc"`:
  - All of the WIP `SqlFilterExpressionTests` cases (ported).
  - Every grammar element, as a match case and a no-match case.
  - Every spec question Q1–Q12, with one or more cases each. Examples:
    - Q1: `label = 'x'` where only `sys.Label` is `x`, and again where only user `label` is `x`.
    - Q2: `sys.label` versus `sys.Label`; user `Color` versus `color`.
    - Q5: `'abc' > 1`; user `"27"` versus `27`; `true = 1`.
    - Q9: two action rules.
    - Q11: an empty filter.
    - Q12: an unbound parameter; an unused parameter; `@P` versus key `p`; each parameter type.
  - Review Focus 1: the NServiceBus, Wolverine and a MassTransit-style user SQL case.
  - Rejected cases (one per parse-error class from Task 2 and Task 3).
  - Action cases for `SET` and `REMOVE` on user and sys targets, and statement ordering.
- [ ] **Step 3: Write `SqlCaseTableTests`**, a `[Theory]` with `[MemberData]` yielding case ids. For each case:
  - Build a `TopicEntity` with one subscription. Remove `$Default`. Add the case's rules as `rule0`, `rule1`, …, mapping typed parameters with `ToClr()`. A `SqlSyntaxException` gives the outcome `Rejected`.
  - Build a `BrokeredMessage` from `message.sys` and `message.user`, then publish.
  - Outcome:
    - `DeadLettered` if the subscription's dead-letter store holds the message.
    - `Match` if the active queue has copies. Compare `copies`, the full user property set and the listed sys values.
    - `NoMatch` otherwise.
  - Assert equality with `expected`. The failure message includes the case `id` and `note`.
- [ ] **Step 4: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~SqlCaseTableTests|FullyQualifiedName~Issue192ReproTests"`. Expected: PASS. A failure means a table guess and the code disagree; fix whichever contradicts the spec.
- [ ] **Step 5: Commit** with `test(sql): shared compat case table, broker-level runner, issue #192 repro`.

### Task 8: HTTP management API — parameters and validation

**Files:**
- Modify:
  - `Management/AtomXmlReader.cs`: add `FilterParameters` and `ActionParameters` to the `RuleProperties` record.
  - `Management/AtomXmlWriter.cs`
  - `Management/ManagementApiEndpoints.cs`: keep the WIP's `InvalidSqlFilter` / `InvalidDefaultRuleFilter`, generalised to rule validation.
- Test: `tests/AlmostServiceBus.Tests/Management/AtomXmlReaderTests.cs`, `AtomXmlWriterTests.cs`, `ManagementApiRuleTests.cs`

**Interfaces:**
- The XML format comes from the SDK:
  ```xml
  <Parameters>
    <KeyValueOfstringanyType>
      <Key>…</Key>
      <Value i:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">…</Value>
    </KeyValueOfstringanyType>
  </Parameters>
  ```
  - All elements are in the ServiceBus namespace `http://schemas.microsoft.com/netservices/2010/10/servicebus/connect`. `i` is `http://www.w3.org/2001/XMLSchema-instance`.
  - Types: `string`, `int`, `long`, `boolean`, `double`, `dateTime` (UTC), `duration` (TimeSpan, XML duration).
  - Resolve the prefix through the element's namespace scope; don't hard-code `l28`.
  - Keys are stored exactly as sent (the SDK doesn't add `@`).
  - The SDK always sends `<Parameters/>`, even when empty.
- The writer emits parameters in the same shape, using the `l28` prefix, so `GetRuleAsync` round-trips.
- Validation: rule PUT and subscription PUT with a `DefaultRuleDescription` both run `RuleEntity.Validate()`:
  - A `SqlSyntaxException` returns `ManagementApiErrors.BadRequest(...)`.
  - The rule isn't added, and the subscription isn't created.
  - Body text: `"The SQL expression is invalid: {ex.Message} SubCode=40000."`. Task 13 replaces it with Azure's wording.

- [ ] **Step 1: Write the failing tests:**
  - **Reader:** a `SqlFilter` with two parameters (`string` and `long`) gives `FilterParameters` equal to `{["@a"]="x", ["@b"]=5L}`.
  - **Reader:** an action with a `duration` parameter gives `TimeSpan`.
  - **Writer:** a round-trip of a rule with each type gives equal values and types.
  - **API:** `CreateRule_WithParameters_GetRuleReturnsThem`.
  - **API:** `CreateRule_InvalidAction_Returns400`.
  - **API:** the WIP's two 400 tests (an invalid filter, and an invalid default-rule subscription that isn't created) stay.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~AtomXml|FullyQualifiedName~ManagementApiRuleTests"`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(mgmt): SQL parameters over Atom XML; reject invalid rules with 400`.

### Task 9: AMQP rule manager operations

**Files:**
- Create: `src/AlmostServiceBus.Core/Amqp/RuleManagementHandler.cs`
- Modify: `src/AlmostServiceBus.Core/Amqp/ManagementLinkEndpoint.cs` (dispatch the three operations)
- Test: `tests/AlmostServiceBus.Tests/Amqp/RuleManagementHandlerTests.cs`. These are unit tests over AMQPNetLite `Message` and `Map` objects. Check the folder for an existing pattern of driving `ManagementLinkEndpoint` handlers, and follow it if there is one.

**Interfaces:**
- Wire format, from the Azure SDK source:
  - **`com.microsoft:add-rule`**: body `Map` with `"rule-name"` (string) and `"rule-description"` (`Map`). The description map has exactly one filter key, an optional action key and `"rule-name"`:
    - `"sql-filter"` → `{ "expression": string }`
    - `"correlation-filter"` → keys `correlation-id`, `message-id`, `to`, `reply-to`, `label`, `session-id`, `reply-to-session-id`, `content-type`, `properties` (`Map`)
    - `"sql-rule-action"` → `{ "expression": string }`
    - The SDK sends `TrueRuleFilter` and `FalseRuleFilter` as `sql-filter` `1=1` and `1=0`.
    - **The AMQP path carries no parameters.**
    - Read map entries by enumerating them, accepting both `string` and `Symbol` keys (CLAUDE.md decision 9).
  - **`com.microsoft:remove-rule`**: body `{ "rule-name": string }`.
  - **`com.microsoft:enumerate-rules`**: body `{ "top": int, "skip": int }`.
    - Reply status 200 with body `{ "rules": List<Map{ "rule-description": DescribedValue }> }`, or status 204 with no rules when `skip` is past the end.
  - **Described types.** Use these descriptor codes exactly as the SDK writes them (note the differing digit counts):

    | Type | Code | Fields |
    |---|---|---|
    | RuleDescription | `0x0000013700000004` | `List[filter, action, rule-name, created-at (DateTime)]` |
    | SqlFilter | `0x000001370000006` | `[expression, compatibility-level(20)]` |
    | CorrelationFilter | `0x000001370000009` | `[correlation-id, message-id, to, reply-to, label, session-id, reply-to-session-id, content-type, properties(Map)]` |
    | TrueFilter | `0x000001370000007` | `[]` |
    | FalseFilter | `0x000001370000008` | `[]` |
    | EmptyRuleAction | `0x0000013700000005` | `[]` |
    | SqlRuleAction | `0x0000013700000006` | `[expression, compatibility-level(20)]` |

  - **Errors.** Reply application properties `statusCode`, `statusDescription` and `errorCondition`, using the same helper as the existing lock-lost replies. Provisional values (Task 13 confirms):
    - Invalid expression: `400` with `com.microsoft:argument-error` (SDK → `ArgumentException`).
    - Duplicate rule name: `409` with `com.microsoft:entity-already-exists` (SDK → `ServiceBusException` with reason `MessagingEntityAlreadyExists`).
    - Missing rule on remove: `404` with `amqp:not-found` (reason `MessagingEntityNotFound`).
    - Missing subscription: `404` with `amqp:not-found`.
  - The target subscription comes from the request's `associated-link-name` or the management link address, `{topic}/Subscriptions/{sub}/$management`. Follow how existing handlers resolve the entity, and use the namespace from `ResolveNamespace`.
- Produces: `internal sealed class RuleManagementHandler` with `void AddRule(RequestContext)`, `void RemoveRule(RequestContext)` and `void EnumerateRules(RequestContext)`.
- Out of scope, but leave a `// TODO(#192 follow-up)` comment at the `default:` branch: an unknown operation still replies OK.

- [ ] **Step 1: Write the failing tests:**
  - `AddRule_SqlFilterAndAction_StoresCompiledRule`.
  - `AddRule_StringAndSymbolKeys_BothAccepted`.
  - `AddRule_InvalidExpression_Replies400ArgumentError`.
  - `AddRule_Duplicate_Replies409`.
  - `RemoveRule_Missing_Replies404`.
  - `EnumerateRules_ReturnsDescribedTypesWithExactCodes`. Assert the `Descriptor` ulong of each filter and action type, including `$Default` as TrueFilter.
  - `EnumerateRules_SkipPastEnd_Replies204`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~RuleManagementHandlerTests`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(amqp): add-rule, remove-rule and enumerate-rules on $management`.

### Task 10: SDK end-to-end tests

**Files:**
- Create: `tests/AlmostServiceBus.SdkIntegration.Tests/SqlRuleTests.cs`. Follow `AdminClientTests` for fixture and admin-client setup. Use `ServiceBusClient(_fixture.ConnectionString)` for data and `client.CreateRuleManager(topic, sub)` for the rule manager.

- [ ] **Step 1: Write the tests.** Each one creates its own topic, named with a GUID suffix.
  - `AdminClient_InRule_RoutesOnlyMatching`: rule `"message-type" IN ('Created','Updated')`. Send `Created` and `Unrelated`; receive only `Created`.
  - `AdminClient_RuleWithParametersAndAction_SetsProperty`: filter `amount > @min` with `Parameters["@min"] = 10L`, and action `SET user.routed = 'yes'`. A message with `amount = 11` arrives with `routed == "yes"`; `amount = 5` doesn't arrive. `GetRuleAsync` returns both parameters.
  - `RuleManager_CreateListDelete_RoutesAndActs`:
    - `CreateRuleAsync("r", new SqlRuleFilter("[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'"), new SqlRuleAction("SET sys.Label = 'nsb'"))`, after deleting `$Default`.
    - A matching message arrives with `Subject == "nsb"`.
    - `GetRulesAsync` lists `r` as a `SqlRuleFilter` with the expression, and the action as a `SqlRuleAction`.
    - After `DeleteRuleAsync("r")`, nothing is delivered.
  - `RuleManager_InvalidExpression_ThrowsArgumentException`.
  - `AdminClient_InvalidExpression_Throws`: assert the exception type the admin client raises for the WIP's 400 body today. Inspect it once, then pin it; Task 13 re-pins it to Azure's.
  - `RuleManager_DuplicateName_ThrowsEntityAlreadyExists`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.SdkIntegration.Tests --filter FullyQualifiedName~SqlRuleTests`. Expected: PASS. Tasks 6–9 are already in place; any failure is a real bug to fix in the owning task's code.
- [ ] **Step 3: Commit** with `test(sdk): end-to-end SQL rules via admin client and rule manager`.

### Task 11: Node and Java smoke coverage for the rule manager

**Files:**
- Modify: `tests/client-sdk-smoke/node/smoke.mjs`, `tests/client-sdk-smoke/java/src/main/java/io/almostservicebus/smoke/Smoke.java`

- [ ] **Step 1: Add a step to each script, after its existing topic steps, using the script's existing `TOPIC`/`SUBSCRIPTION`.**
  - Create the rule manager: Node `sbClient.createRuleManager(TOPIC, SUBSCRIPTION)`; Java `new ServiceBusClientBuilder().connectionString(...).ruleManager().topicName(..).subscriptionName(..).buildClient()`.
  - Delete `$Default`, then create rule `in-rule` with filter `color IN ('red','blue')` and action `SET user.routed = 'yes'`.
  - List the rules and assert `in-rule` is present with its expression.
  - Send `red` and `green`. Assert exactly one message is received: `red`, with `routed == 'yes'`.
  - Create `bad-rule` with `color IN ('red'` and assert it throws.
  - Delete `in-rule`, recreate `$Default` as a true filter, and continue with the script's remaining steps.
- [ ] **Step 2: Run against a local emulator.** Start it with `cd src/AlmostServiceBus.Host && dotnet run` in the background (it owns 5672/5300). Then:
  - `cd tests/client-sdk-smoke/node && npm ci && node smoke.mjs && node admin.mjs && node concurrent-connections.mjs`
  - `cd tests/client-sdk-smoke/java && mvn -q -B compile exec:java`
  - `cd tests/client-sdk-smoke/python && python smoke.py`

  Expected: all exit 0. Stop only the emulator process you started.
- [ ] **Step 3: Commit** with `test(smoke): rule manager create/list/route/delete in Node and Java`.

### Task 12: Compatibility suite (emulator vs Azure, diff, record mode)

**Files:**
- Modify: `tests/AlmostServiceBus.Conformance.Tests/AlmostServiceBus.Conformance.Tests.csproj`:
  - Add `<UserSecretsId>almostservicebus-conformance</UserSecretsId>`.
  - Add `<PackageReference Include="Microsoft.Extensions.Configuration.UserSecrets" Version="10.0.*" />`. Pin it to the version that matches the repo's other 10.0.x Microsoft packages (10.0.14 at the time of writing). The repo has no central package management.
  - Add the same shared `Compile` and `None` links as Task 7.
- Create: `tests/AlmostServiceBus.Conformance.Tests/RealAsbConnection.cs`. It provides `internal static class RealAsbConnection { public static string? ConnectionString { get; } }`, which returns the environment variable `ASB_CONNECTION_STRING` if it is non-empty, otherwise user secrets (`new ConfigurationBuilder().AddUserSecrets<RealAsbConnection>()["ASB_CONNECTION_STRING"]`).
- Modify: `RealAsbConformanceTests.cs` and `TopicRequeueReproTest.cs` to use `RealAsbConnection.ConnectionString` instead of reading the environment variable themselves. Update their skip messages to mention both sources.
- Modify: the "real ASB" sections of `CLAUDE.md` and `README.md` to document `dotnet user-secrets set ASB_CONNECTION_STRING "..." --id almostservicebus-conformance`.
- Create: `tests/AlmostServiceBus.Conformance.Tests/SqlCompatTests.cs`, which sits next to `RealAsbConformanceTests`.

**Interfaces:**
- Consumes: `SqlCompatCases.Load/Save/SourcePath`, `TypedValue.ToClr/From` (Task 7), and `ServiceBusEmulatorFixture` (as in `EmulatorConformanceTests`).
- Produces one `[Fact] SqlCompatTable_EmulatorMatchesAzure`, with this algorithm:
  1. Run the table against the **emulator** through the SDK (HTTP admin plus AMQP data), using `RunTableAsync(ServiceBusClient, ServiceBusAdministrationClient)`.
  2. If `RealAsbConnection.ConnectionString` is non-null, run the same `RunTableAsync` against **Azure**.
  3. Report rows where the emulator differs from Azure. When Azure is absent, compare the emulator with `expected`.
  4. With `ASB_COMPAT_RECORD=1` and Azure present, write Azure's outcome, copies and properties into each case's `expected`, set `"verified": "azure"`, and `Save`. For `Rejected` cases, also put the Azure exception type and message into `note`.
  5. Fail with a markdown table: `id | area | table | azure | emulator`.

  `RunTableAsync`, per run:
  - Create one topic, `sqlcompat-{8 hex}`.
  - Per case, create subscription `c{index:D4}` with `DeadLetteringOnFilterEvaluationExceptions = true`, via `CreateSubscriptionAsync(options, ruleOptions)` for rule 0 and `CreateRuleAsync` for the rest. Each rule's filter is `user.caseId = '<id>' AND (<filter>)`; a null filter becomes `user.caseId = '<id>'`. An exception during creation gives `Rejected`.
  - Publish one `ServiceBusMessage` per non-rejected case, with `caseId` plus the case's properties.
  - Drain each subscription and its DLQ: `ReceiveMessagesAsync(10, TimeSpan.FromSeconds(3))`, with a parallelism of 8.
  - Delete the topic in `finally`.
  - Sys values the SDK can't set (e.g. `SequenceNumber`) can't be message inputs; the table only uses settable ones.
- Skip behaviour: the emulator half always runs, so the test always runs.

- [ ] **Step 1: Write the test.**
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Conformance.Tests --filter FullyQualifiedName~SqlCompatTests`. Expected: PASS, because the emulator agrees with the doc-based table.
- [ ] **Step 3: Commit** with `test(conformance): SQL compat suite with Azure diff and record mode`.

### Task 13: Azure compat run, reconcile, document

- [ ] **Step 1: Check the connection string.** It comes from `RealAsbConnection` (the environment variable, or user secrets `almostservicebus-conformance`). If neither is set, say so in the summary to George and carry on with everything that doesn't need Azure, leaving the cases marked `"verified": "doc"`.
- [ ] **Step 2: Record:** `ASB_COMPAT_RECORD=1 dotnet test tests/AlmostServiceBus.Conformance.Tests --filter FullyQualifiedName~SqlCompatTests`. Check that `git diff` touches only `tests/shared/sql-compat-cases.json`, and that every case now reads `"verified": "azure"`.
- [ ] **Step 3: Reconcile.** For each divergence:
  - Flip the matching `SqlSemantics` constant, or fix the code.
  - Re-pin the error strings and exception types (HTTP body and AMQP condition) in Tasks 8, 9 and 10's tests to Azure's.
  - Enable or delete the skipped evaluation-error test from Task 6.
  - If a case can't apply to the emulator, write why in the case's `note` and keep Azure's value.
  - Rerun until `SqlCompatTests` is green with and without Azure.
- [ ] **Step 4: Document.**
  - Fill the spec's **Decisions log** with one line per Q1–Q12: Azure's answer and the constant or code that implements it.
  - Add a `## [Unreleased]` section to `CHANGELOG.md`, in the file's existing style. Include:
    - "Fixed: SQL filters (#192)".
    - "Added: SQL rule actions, SQL parameters, AMQP rule manager".
    - A **Behaviour changes** list: unprefixed names if Q1 changed them, case sensitivity if changed, empty expressions, rejection at creation, numeric-string coercion if changed.
  - Update `CLAUDE.md`:
    - Add `Broker/Sql/` to Key Components.
    - Update the test counts table.
    - Add a Key Design Decision: "SQL semantics follow the recorded Azure table (`tests/shared/sql-compat-cases.json`); change `SqlSemantics` only with a recorded Azure case".
- [ ] **Step 5: Full verification.**
  - Run the full solution test command from Global Constraints.
  - Run the Task 11 smoke scripts.
  - Run the external framework suites from CLAUDE.md's "Running" section. The submodules need `git submodule update --init` first, and NServiceBus needs the emulator to own port 5300.

  Expected: all green apart from the known `RealAsbTopicRequeueReproTest` skips.
- [ ] **Step 6: Commit** with `docs: SQL compat decisions, changelog and CLAUDE.md for #192`. Then report to George, without pushing.
