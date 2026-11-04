# SQL Filters and Rule Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the regex SQL handling in `RuleEntity` with a Superpower-based implementation of the Service Bus SQL grammar for filters, rule actions and parameters. Rules are validated on every creation path (in code, HTTP admin, AMQP rule manager). Behaviour matches real Azure, as recorded on 2026-11-04.

**Architecture:**
- A Superpower tokenizer and two token-list grammars (predicates for filters, scalar expressions for actions) produce an immutable AST.
- A binder checks semantics at creation: system properties, parameters, functions and literals.
- `SqlFilter` and `SqlRuleAction` evaluate lazily, left to right, over CLR values. They track a distinct *Missing* state, follow C# operator rules, and throw `SqlEvaluationException` with Azure's messages.
- A JSON case table seeded from the Azure probe drives a broker-level unit runner and a conformance suite that re-checks Azure.

**Tech Stack:** .NET 10, Superpower 3.2.1, AMQPNetLite 2.5.5, xUnit 2.9.3, Azure.Messaging.ServiceBus 7.21.0 (tests), Microsoft.Extensions.Configuration.UserSecrets (Conformance.Tests).

**Spec:** `docs/superpowers/specs/2026-11-04-sql-filters-and-actions-design.md`.
- **Its "Decisions log — confirmed against real Azure" section is the authority for every behaviour.** Quote it; don't re-derive it.
- The raw probe output is in `docs/superpowers/specs/sql-azure-probe/round1.md`–`round3.md`. The probe program is `probe-program.cs.txt` in the same folder.

## Global Constraints

- **Parser library:** Superpower 3.2.1 in `src/AlmostServiceBus.Core/AlmostServiceBus.Core.csproj`. No Sprache, and no hand-written parser.
- **No fallback, ever.** Every expression either is rejected at creation, evaluates to true / false / unknown, or raises `SqlEvaluationException`, which the broker turns into a dead-letter (or a drop) exactly as Azure does. Nothing becomes match-all.
- **Error text copies Azure's wording** (decisions log, Q10), including position and expected tokens: `There was an error parsing the SQL expression. [Token line={l}, column={c}, Token in error={t}, Additional details={d}]`. The line is 1-based and the column is 0-based. End of input is `line=2, column=0`.
- **Length limit:** expressions longer than **1024** characters are rejected at creation (the SDK's client-side limit).
- **Azure connection string:** one helper, `RealAsbConnection.ConnectionString`. It checks the `ASB_CONNECTION_STRING` environment variable first, then user secrets id `almostservicebus-conformance` with key `ASB_CONNECTION_STRING`. Never print it or put it in git. Compat runs use a unique `sqlcompat-<random>` topic and delete it in `finally`. Record mode is `ASB_COMPAT_RECORD=1`.
- **`tests/client-sdk-smoke`:** don't change how it obtains its connection string.
- **Git:** commit locally on `fleet/session-c54d42a2`. **Don't push, open a PR or comment on #192.** Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Code style:** `TreatWarningsAsErrors` is on. Match the surrounding comment density and naming.
- **Full test command:** `dotnet test AlmostServiceBus.sln --filter "FullyQualifiedName!~RealAsbConformanceTests" --verbosity quiet`. `RealAsbTopicRequeueReproTest` shows 3 failures without Azure; that is pre-existing. Once Task 12 lands, that test resolves Azure through `RealAsbConnection`, so it may run for real when the secret is present.

## Review Focus

1. **Framework filters.** NServiceBus `[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'`, Wolverine `NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'`, and MassTransit's pass-through SQL must keep routing exactly as on Azure. They are already confirmed table cases (`fw.*`); the CI framework suites must stay green (Task 13).
2. **SDK-encoded property types arriving over AMQP.** The .NET SDK sends `TimeSpan`, `Uri` and `DateTimeOffset` application properties as AMQP described values (`com.microsoft:timespan`, `com.microsoft:uri`, `com.microsoft:datetime-offset`). Filters must see them as the CLR types, as Azure does (it reports `System.TimeSpan` and `System.Uri` in its errors). A `SET` that produces one of these types must be re-encoded the same way, so the message still serialises. Tests go in Task 4 (decode) and Task 6 (re-encode on delivery through the SDK).
3. **Culture.** Under `de-DE`, the literal `1.5` is one and a half, and `ToString()` used by `LIKE` and string concatenation uses invariant formatting (`d LIKE '1.5'` matches `1.5d`). Tests go in Task 2 and Task 5.
4. **Nesting and length.** 500 nested parentheses parse without a stack overflow. 1025 characters are rejected. Test in Task 2.
5. **A rule replaced during a publish.** No exception, and no torn rule: the compiled form is one immutable reference. Test in Task 6.

---

## File Structure

New, in `src/AlmostServiceBus.Core/Broker/Sql/` (namespace `AlmostServiceBus.Core.Broker.Sql`):

| File | Responsibility |
|---|---|
| `SqlToken.cs` | Token kinds |
| `SqlTokenizer.cs` | Superpower tokenizer |
| `SqlAst.cs` | AST records |
| `SqlGrammar.cs` | `ParseFilter` (predicate grammar) and `ParseAction` (statements over scalar expressions) |
| `SqlErrors.cs` | `SqlSyntaxException`, `SqlEvaluationException`, and Azure message builders |
| `SqlBinder.cs` | Creation-time semantic checks: system properties, settable targets, parameters, functions, `LIKE` literal patterns |
| `SqlSystemProperties.cs` | Readable and settable `sys.` names: getters, setters, the sender-preset list |
| `SqlValues.cs` | Missing sentinel, C# operator table, conversions, `LIKE`, AMQP described-value decode/encode |
| `SqlEvaluator.cs` | Lazy evaluation of AST nodes |
| `SqlFilter.cs` | Compiled filter |
| `SqlRuleAction.cs` | Compiled action |

New elsewhere:

| File | Responsibility |
|---|---|
| `src/AlmostServiceBus.Core/Amqp/RuleManagementHandler.cs` | AMQP `add-rule` / `remove-rule` / `enumerate-rules` |
| `tests/shared/sql-compat-cases.json` + `tests/shared/SqlCompatCases.cs` | Shared table (no SDK or Core references) |
| `tests/AlmostServiceBus.Tests/Broker/Sql/*Tests.cs` | Unit tests |
| `tests/AlmostServiceBus.SdkIntegration.Tests/SqlRuleTests.cs` | SDK end-to-end tests |
| `tests/AlmostServiceBus.Conformance.Tests/RealAsbConnection.cs` | The connection-string helper |
| `tests/AlmostServiceBus.Conformance.Tests/SqlCompatTests.cs` | Table against emulator and Azure |

Modified:
- `Broker/RuleEntity.cs`, `Broker/SubscriptionEntity.cs`, `Broker/QueueEntity.cs` (`DeadLetterOnArrival` gains a `source` argument)
- `Management/AtomXmlReader.cs`, `Management/AtomXmlWriter.cs`, `Management/ManagementApiEndpoints.cs`
- `Amqp/ManagementLinkEndpoint.cs`
- `RealAsbConformanceTests.cs`, `TopicRequeueReproTest.cs`
- `CHANGELOG.md`, `CLAUDE.md`, `README.md`

Deleted: `Broker/SqlFilterExpression.cs` and `tests/AlmostServiceBus.Tests/Broker/SqlFilterExpressionTests.cs` (WIP commit `38a31bd`).

---

### Task 1: Tokenizer and error types

**Files:**
- Modify: `AlmostServiceBus.Core.csproj` (add `Superpower` 3.2.1)
- Create: `Sql/SqlToken.cs`, `Sql/SqlTokenizer.cs`, `Sql/SqlErrors.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlTokenizerTests.cs`

**Interfaces:**
- Produces:
  - `public enum SqlToken { None, Identifier, QuotedIdentifier, BracketedIdentifier, Parameter, String, Integer, Decimal, Float, Dot, Comma, LParen, RParen, Semicolon, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Plus, Minus, Star, Slash, Percent }`
    - Keywords are `Identifier` tokens, matched case-insensitively by the grammar.
    - Regular identifiers are a Unicode letter or `_`, then letters, digits or `_`.
    - `Decimal` is `digits.digits` (no `.5`, no `1.`). `Float` has an exponent with digits (`1e1`, `1.5E-3`). `1e` is an error.
  - `public static class SqlTokenizer { public static TokenList<SqlToken> Tokenize(string text); }`
  - `public sealed class SqlSyntaxException : ArgumentException`
    - Properties: `int Line` (1-based), `int Column` (0-based), `string TokenInError`, `string Details`, `IReadOnlyList<string> Expected`.
    - `Message` is exactly `There was an error parsing the SQL expression. [Token line={Line}, column={Column}, Token in error={TokenInError}, Additional details={Details}]`.
    - `static SqlSyntaxException Semantic(string message)` builds a creation-time semantic rejection whose `Message` is the given Azure text verbatim, with `Line`/`Column` set to `0`.
  - `public sealed class SqlEvaluationException(string message) : Exception(message)`
  - `internal static class SqlErrorText` holds message builders for every Azure string quoted in the decisions log. Examples:
    - `OperatorNotApplicable(string op, Type left, Type right)` gives `Operator '==' cannot be applied to operands of type 'string' and 'long'`, using the C# keyword/full-name rule from Q5.
    - `UnknownGettable(name)`, `UnknownSettable(name)`, `CannotRemoveSystem(name)`, `UnboundParameter(name)`, `UnusedParameter(name)`, `UnknownFunction(name)`, `FunctionArity(name, expected, actual)`, `ScopeNotSupported(scope)`, `Int64Overflow`.
    - Multi-line Azure messages use `"\r\nParameter name: …"`, as received.
- Tokenizer error details use Azure's wording: `Unrecognized character. '{c}'`, and `The string ({rest}) was not terminated properly.`, where `rest` is the remaining text after the opening quote.

- [ ] **Step 1: Write the failing tests.**
  - `Tokenize("user.[message-type] IN ('a''b', 1.5e3)")` yields these kinds: `Identifier, Dot, BracketedIdentifier, Identifier, LParen, String, Comma, Float, RParen`.
  - `"café"` yields a single `Identifier`. `"@p1"` yields `Parameter`. `"<> != <= >= ="` yields `NotEqual, NotEqual, LessOrEqual, GreaterOrEqual, Equal`.
  - `"--x"` yields `Minus, Minus, Identifier` (there are no comments).
  - These throw `SqlSyntaxException`, each asserting `Line`, `Column` and `Details`:
    - `"a = #"`: line 1, column 4, details `Unrecognized character. '#'`.
    - `"a$b"`: column 1, `Unrecognized character. '$'`.
    - `"a = 'red"`: line 2, column 0, details starting `The string (red`.
    - `"n = .5"`, `"n = 1."`, `"n = 1e"`: each a syntax error at the offending token.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlTokenizerTests`. Expected: build fails.
- [ ] **Step 3: Implement** with `TokenizerBuilder<SqlToken>` and `.Ignore(Span.WhiteSpace)`.
  - Strings and delimited identifiers use custom `TextParser<TextSpan>`s with doubled-closer escapes.
  - Match the longest operator first.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): Superpower tokenizer and Azure-shaped syntax errors`.

### Task 2: AST and grammars

**Files:**
- Create: `Sql/SqlAst.cs`, `Sql/SqlGrammar.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlGrammarTests.cs`, `SqlGrammarErrorTests.cs`; add a test helper `SqlAstPrinter` in the test project

**Interfaces:**
- Produces (`SqlAst.cs`):
  ```csharp
  public enum SqlScope { User, System }                                    // unprefixed => User (Q1)
  public abstract record SqlExpr;
  public sealed record SqlLiteral(object? Value) : SqlExpr;                // long, double, string, bool, or null
  public sealed record SqlPropertyRef(SqlScope Scope, string Name) : SqlExpr;
  public sealed record SqlParameterRef(string Name) : SqlExpr;             // includes the '@'
  public enum SqlUnaryOp { Not, Negate, Plus }
  public sealed record SqlUnary(SqlUnaryOp Op, SqlExpr Operand) : SqlExpr;
  public enum SqlBinaryOp { Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Add, Subtract, Multiply, Divide, Modulo }
  public sealed record SqlBinary(SqlBinaryOp Op, SqlExpr Left, SqlExpr Right) : SqlExpr;
  public sealed record SqlLike(SqlExpr Operand, SqlExpr Pattern, SqlExpr? Escape, bool Negated) : SqlExpr;
  public sealed record SqlIn(SqlExpr Operand, IReadOnlyList<SqlExpr> Items, bool Negated) : SqlExpr;
  public sealed record SqlIsNull(SqlPropertyRef Property, bool Negated) : SqlExpr;  // property only
  public sealed record SqlExists(SqlPropertyRef Property) : SqlExpr;
  public sealed record SqlFunctionCall(string Name, IReadOnlyList<SqlExpr> Arguments) : SqlExpr; // name lower-cased
  public abstract record SqlStatement;
  public sealed record SqlSet(SqlPropertyRef Target, SqlExpr Value) : SqlStatement;
  public sealed record SqlRemove(SqlPropertyRef Target) : SqlStatement;
  ```
- Produces (`SqlGrammar.cs`):
  - `public static SqlExpr ParseFilter(string text)`
  - `public static IReadOnlyList<SqlStatement> ParseAction(string text)`
- **Predicate grammar** (filters):
  - `or := and (OR and)*`, `and := not (AND not)*`, `not := NOT not | primaryPredicate`.
  - `primaryPredicate := '(' or ')' | EXISTS '(' identifier ')' | property IS [NOT] NULL | scalar compOp scalar | scalar [NOT] LIKE scalar [ESCAPE scalar] | scalar [NOT] IN '(' scalar (',' scalar)* ')'`.
  - **A bare scalar is not a predicate** (Q5: `b`, `TRUE`, `NULL` and `NOT FALSE` are rejected).
  - Parenthesised predicate versus parenthesised scalar: try the predicate form first, then backtrack (`.Try()`).
- **Scalar grammar:**
  - `additive := mult (('+'|'-') mult)*`, `mult := unary (('*'|'/'|'%') unary)*`, `unary := ('-'|'+') unary | primary`.
  - `primary := literal | parameter | function '(' args ')' | property | '(' additive ')'`.
- **Property:**
  - `sys.` or `user.` followed by one regular or delimited identifier, or one unprefixed regular or delimited identifier.
  - An unprefixed regular identifier followed by `.` is a syntax error with details `The specified scope ({first}) is not supported.` at the second part's position (decisions log, grammar facts).
  - `user.a.b` is a syntax error at the second `.`.
  - Reserved words (`AND OR NOT IN LIKE ESCAPE IS NULL EXISTS TRUE FALSE SET REMOVE`) can't be regular identifiers. `sys` and `user` alone are ordinary user names.
- **Literals:**
  - `Integer` is parsed as `long`. If it overflows, the error is a syntax error with details `Value was either too large or too small for an Int64.`
  - `Decimal` and `Float` are parsed as `double` with `CultureInfo.InvariantCulture`.
- **Action grammar:** `statement (';' statement)* ';'? EOF`, where `statement := SET property '=' scalar | REMOVE property`. Comparisons, `LIKE` and `IN` are not allowed in a scalar.
- **Expected tokens.** Use `.Named(...)` names in Azure's style for "expecting" lists: `')'`, `AND`, `OR`, `LIKE`, `IN`, `NULL`, `NOT`, `'='`, `SET`, `REMOVE`, `EOF`, `IDENTIFIER`, `DELIMITED_IDENTIFIER`.
  - Details format: `syntax error, unexpected {TOKENCLASS}[, expecting {a}, or {b}]`.
  - `TOKENCLASS` is `EOF`, `IDENTIFIER`, `STRING`, `EQ`, `IN`, `IS`, `'.'`, `')'`, `'*'` or `'+'`. Use the token's quoted text for single characters, and an upper-case keyword name for keywords.
  - Exact "expecting" lists only need to match Azure where the probe recorded them (assert those). Elsewhere, any accurate list is acceptable.
- Both entry points reject empty or whitespace-only text (a syntax error at `line=2, column=0`, `unexpected EOF`) and text over 1024 characters (`SqlSyntaxException.Semantic("The SQL expression exceeds the maximum length of 1024 characters.")`).

- [ ] **Step 1: Write the failing tests.**
  - **`SqlGrammarTests`** (printed AST):
    - Precedence: `a = 1 OR b = 2 AND c = 3`; `NOT a = 1 AND b = 2`; `1 + 2 * 3 = 7`; `-a * 2 = 1`; `NOT NOT a = 1`.
    - Each identifier form and scope: `sys.Label`, `SYS.label`, `user.[a-b]`, `user."a""b"`, `[a]]b]`, `[]`, `café`, `sys`, `user`, `[NServiceBus.EnclosedMessageTypes]`.
    - Each operator. `LIKE` with a property pattern and a property escape. `IN` with arithmetic and property items. `IS NULL`, `EXISTS(sys.Label)`, `newid()`, `property(which)`, `@p`.
    - Keywords in mixed case.
    - Actions: `SET a = 1;`, `SET user.b = user.n * 2; REMOVE sys.Label`, `set [a b] = @v`.
  - **`SqlGrammarErrorTests`**: one row per rejected case in round1–round3 whose expression is syntax-only, asserting Azure's `Details` text.
    - Strip the probe's `user.caseId = '…' AND (` wrapper and recompute the column from the unwrapped text.
    - Cases include: `b` (bare), `TRUE`, `NULL`, `NOT FALSE`, `missing + 1 IS NULL`, `newid() IS NOT NULL`, `a.b = 'x'` (`The specified scope (a) is not supported.`), `user.a.b = 'x'`, `color IN ()`, `color === 'red'`, `in = 1`, `color NOT 'red'` (`syntax error, unexpected STRING, expecting LIKE, or IN`), `color IS 'red'` (`… expecting NULL, or NOT`), `EXISTS('x')` (`… expecting IDENTIFIER, or DELIMITED_IDENTIFIER`), `EXISTS(n + 1)`, `color /* c */ = 'red'`, `1abc = 'x'`, `n = 0x10`, `n = 99999999999999999999`, `color = N'red'`, `.5 = 0.5`.
    - Action cases: `SET user.a` (`syntax error, unexpected EOF, expecting '='`), `UPDATE user.a = 1` (`… unexpected IDENTIFIER, expecting SET, or REMOVE`), `;`, `SET user.a = 1;;` (`… expecting SET, or REMOVE, or EOF`), `; SET user.a = 1`, `SET user.x = 1 = 1`, `SET user.x = color IN ('red')`, `SET user.x = color LIKE 'r%'`.
  - **Review Focus 4:** 500 nested parentheses around `a = 1` parse. A 1025-character expression is rejected.
  - **Review Focus 3:** under `de-DE`, `n = 1.5` gives the literal `1.5d`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~SqlGrammar"`. Expected: build fails.
- [ ] **Step 3: Implement** the combinators. Map Superpower failures to `SqlSyntaxException`, converting the failing token's absolute position to 1-based line and 0-based column.
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): predicate and action grammars`.

### Task 3: Binder and system properties

**Files:**
- Create: `Sql/SqlBinder.cs`, `Sql/SqlSystemProperties.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlBinderTests.cs`

**Interfaces:**
- Produces `internal static class SqlSystemProperties`:
  - `bool IsReadable(string name)` and `bool IsSettable(string name)` (case-insensitive).
  - `object? Get(BrokeredMessage m, string name)`. Mapping, from decisions log Q3:
    - `MessageId`, `CorrelationId`, `To`, `ReplyTo`, `SessionId`, `ReplyToSessionId`, `ContentType`, `PartitionKey`: the field of the same name.
    - `Label`: `Subject`.
    - `ViaPartitionKey`: `null`.
    - `SequenceNumber` and `EnqueuedSequenceNumber`: `long SequenceNumber`.
    - `DeliveryCount`: `int`, `DeliveryCount + 1` (filters run before the first delivery).
    - `EnqueuedTimeUtc`: `DateTime` (UTC).
    - `ScheduledEnqueueTimeUtc`: the UTC `DateTime`, or `DateTime.MinValue` when unset.
    - `ExpiresAtUtc`: `EnqueuedTimeUtc + TimeToLive`, saturating at `DateTime.MaxValue`.
    - `TimeToLive`: `TimeSpan`.
    - `Size`: `long Body.Length`.
    - `LockToken`: `Guid.Empty`.
    - `LockedUntilUtc`: `DateTime.MinValue`.
    - `DeadLetterSource`: `null`.
    - `State`: the `MessageState` enum.
    - `ForcePersistence`: `false`.
  - Settable: `Label`, `To`, `ReplyTo`, `CorrelationId`, `ContentType`, `ReplyToSessionId`, `SessionId`, `PartitionKey`, `TimeToLive`, `ScheduledEnqueueTimeUtc`, `MessageId`.
  - `IReadOnlySet<string> SenderPresetWins`, containing `To`, `ReplyTo`, `CorrelationId`, `ReplyToSessionId`, `MessageId` and `SessionId` (the quirk).
  - `void Set(BrokeredMessage m, string name, object value)`. It converts `value` to the property's type with `SqlValues.ConvertTo`. Setting `SessionId` also sets `PartitionKey`.
  - `MessageState` needs no change; Azure's error names `Microsoft.ServiceBus.Messaging.MessageState`, so special-case that type name in `SqlErrorText`.
- Produces `internal static class SqlBinder`:
  - `static void BindFilter(SqlExpr expr, IReadOnlyDictionary<string, object?> parameters)`
  - `static void BindAction(IReadOnlyList<SqlStatement> statements, IReadOnlyDictionary<string, object?> parameters)`
  - Both throw `SqlSyntaxException.Semantic(…)` with Azure text for:
    - `sys.X` not readable, including `sys.Subject` (`UnknownGettable`).
    - A `SET sys.X` target that isn't settable (`UnknownSettable`).
    - An unknown function (`UnknownFunction`). Functions are `newid` (0 arguments) and `property` (1 argument).
    - Wrong arity (`FunctionArity`).
    - A `LIKE` pattern or escape that is a non-string literal. The message is Azure's text from round1 `r.like-nonstring-pattern`, verbatim.
    - An unbound `@p` (`UnboundParameter`) and an unused parameter key (`UnusedParameter`). Parameter keys must start with `@`; compare case-insensitively.
  - `REMOVE sys.X` is **not** rejected here; it fails at run time.

- [ ] **Step 1: Write the failing tests:**
  - Each rejection above, asserting the exact `Message`.
  - `sys.label IS NULL` binds.
  - `REMOVE sys.NoSuch` binds.
  - A parameter key `"p"` with expression `@p` gives `UnboundParameter("@p")`.
  - Keys `"@P"` and `@p` bind.
  - `SqlSystemProperties.Get(new BrokeredMessage(), "ScheduledEnqueueTimeUtc")` is `DateTime.MinValue`.
  - `Get(…, "DeliveryCount")` is `1`.
  - `Set(m, "SessionId", "s")` also sets `PartitionKey`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlBinderTests`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): creation-time binder and system property map`.

### Task 4: Values (Missing, C# operator table, conversions, LIKE, AMQP types)

**Files:**
- Create: `Sql/SqlValues.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlValuesTests.cs`

**Interfaces:**
- Produces `internal static class SqlValues`:
  - `public static readonly object Missing`, a sentinel distinct from `null`.
  - `object? FromMessage(object? raw)` decodes AMQP values for evaluation (Review Focus 2):
    - A `DescribedValue` with descriptor `com.microsoft:timespan` (ticks as `long`) becomes `TimeSpan`.
    - `com.microsoft:uri` (string) becomes `Uri`.
    - `com.microsoft:datetime-offset` (ticks as `long`) becomes a UTC `DateTimeOffset`.
    - `Symbol` becomes `string`. A `DateTime` with `Kind` other than Utc becomes Utc.
    - Everything else is returned unchanged, so CLR types are kept (`int` stays `int`).
  - `object? ToMessage(object? value)` is the inverse, for values written by actions.
  - `object? Equal(object? a, object? b)` and `object? Compare(SqlBinaryOp op, object? a, object? b)`:
    - Return `Missing` if either side is `Missing`.
    - C# `null` semantics for `=` and `<>`: `null = null` is true, and `null = x` is false.
    - `<`, `>` and the other orderings with `null` are false (C# lifted operators). This wasn't probed; document it.
    - Otherwise apply the C# operator table from decisions log Q5, or throw `SqlEvaluationException(SqlErrorText.OperatorNotApplicable(…))`:
      - Integer types combine using C# implicit conversions; `ulong` with a signed type throws.
      - `float` widens to `double`. `decimal` with `double` throws. `char` with `string` throws.
      - `bool` supports only `=` and `<>`.
      - A `string` supports only `=` and `<>`, and only against a `string`.
      - `Guid` against a `string` parses the string (`Guid.Parse`; a `FormatException` message becomes the evaluation error), and supports only `=` and `<>`.
      - `DateTime`, `DateTimeOffset`, `TimeSpan`, `Uri` and enums compare only with their own type.
  - `object? Arithmetic(SqlBinaryOp op, object? a, object? b)`:
    - `Missing` propagates. `null` gives `null`.
    - `+` with any `string` operand concatenates, using invariant `ToString()`.
    - Numbers use C# promotion with `checked` integer arithmetic. Overflow throws `Arithmetic operation resulted in an overflow.`, and integer `/` or `%` by zero throws `Attempted to divide by zero.`.
    - Doubles follow IEEE.
    - Any other combination throws `OperatorNotApplicable`.
  - `object? Negate(object? v)`.
  - `object? ConvertTo(Type target, object? value)` is `Convert.ChangeType` with `InvariantCulture`, plus `TimeSpan` (`TimeSpan.Parse` from a string, `Invalid cast from 'System.Int64' to 'System.TimeSpan'.` from a number) and `Guid` (`Guid.Parse`). Exceptions surface as `SqlEvaluationException` carrying the .NET message.
  - `bool? Like(object? value, object? pattern, object? escape)`:
    - `Missing` in any argument gives `null` (unknown). A `null` value gives `false`.
    - A non-string value uses invariant `ToString()`; a `bool` gives `"True"`/`"False"`.
    - The escape character is the first character of the escape string. An empty escape means none, and a trailing escape character is ignored.
    - `%` becomes `[^\n]*` and `_` becomes `[^\n]`. Case-sensitive.
    - Cache compiled regexes per (pattern, escape) in a bounded `ConcurrentDictionary`.

- [ ] **Step 1: Write the failing tests**, a `[Theory]` per area with values taken from the probe:
  - Ok cases: `int` 27 = `long` 27; `int` 7 vs `double` 7.0; `uint` 7 vs `long` -1 is greater; `byte`/`short`/`sbyte` 7 = 7L.
  - Throwing cases, asserting the exact message:
    - `float` 0.1f vs 0.1d is not equal, and does not throw.
    - `decimal` vs `double`: `…'decimal' and 'double'`. `ulong` vs `long`: `…'ulong' and 'long'`. `char` vs `string`. `"27"` vs `27L`: `…'string' and 'long'`.
    - `true` vs `1L`. `true < true`: `…'bool' and 'bool'`. `"a" < "b"`.
    - `DateTime` vs `string`: `…'System.DateTime' and 'string'`. `TimeSpan`, `Uri` and `DateTimeOffset` vs `string`.
    - `MessageState.Active` vs `0L`: `…'Microsoft.ServiceBus.Messaging.MessageState' and 'long'`.
  - `Guid` vs its string is equal. `Guid` vs `"not-a-guid"` throws the Guid format message. `Guid > string` throws.
  - Null: `null = null` is true, `"a" = null` is false, `null <> "x"` is true. `Missing = null` is `Missing`.
  - Arithmetic:
    - `7/2 = 3L`, `-7/2 = -3L`, `-7%3 = -1L`. `7/0` throws, and `7.0/0` is +∞.
    - `long.MaxValue*2` throws. `int` 2 * 2147483647L is 4294967294L.
    - `"red" + 1L` is `"red1"`.
  - `ConvertTo`: `int` ← 1.5 is 2. `string` ← 5L is `"5"`. `bool` ← `"false"`. `int` ← 9999999999L throws `Value was either too large or too small for an Int32.`. `int` ← `"text"` throws `Input string was not in a correct format.`. `TimeSpan` ← 5L throws the cast message.
  - `Like`:
    - `"a\nb"` against `a%` is false. `'50!%' ESCAPE '!'` matches `"50%"`. `ESCAPE '!x'` behaves the same.
    - `ESCAPE ''` treats `%` as a wildcard. `'a!' ESCAPE '!'` matches `"a"`.
    - `true` matches `'True'`. `1.5d` matches `'1.5'` under `de-DE` (Review Focus 3). `27` matches `'2%'`.
    - `'r.d'` doesn't match `"red"`, and `'[r]ed'` doesn't match `"red"`.
  - `FromMessage`/`ToMessage` round-trip the three described types (Review Focus 2).
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlValuesTests`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): Azure value semantics`.

### Task 5: Evaluator, `SqlFilter`, `SqlRuleAction`

**Files:**
- Create: `Sql/SqlEvaluator.cs`, `Sql/SqlFilter.cs`, `Sql/SqlRuleAction.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlEvaluatorTests.cs`

**Interfaces:**
- Produces:
  - `internal sealed class SqlEvaluationContext(BrokeredMessage message, IReadOnlyDictionary<string, object?> parameters)`.
    - It resolves user properties case-insensitively: an exact key first, then a case-insensitive scan.
    - Each value goes through `SqlValues.FromMessage`. An absent key gives `SqlValues.Missing`.
    - It also holds `Dictionary<string, object?> SystemOverrides`, which actions write to; reads check it first.
  - `internal static class SqlEvaluator`:
    - `public static object? Evaluate(SqlExpr e, SqlEvaluationContext ctx)` returns `true`/`false`, a CLR value, `null`, or `SqlValues.Missing`. Predicates return `true`, `false` or `Missing` (Missing doubles as unknown).
    - AND/OR evaluate left to right with three-valued logic and short-circuit: `false AND x` doesn't evaluate `x`, and `true OR x` doesn't evaluate `x`. `NOT Missing` is `Missing`.
    - `IN` compares items in order and returns true at the first equal item. Otherwise it returns `Missing` if the operand or any item was `Missing`, and false if not.
    - `IS NULL` is true when the value is `null` or `Missing`.
    - `EXISTS(user.x)` is true when the key is present, even with a null value. `EXISTS(sys.X)` is always true.
    - `property(e)` evaluates `e`. A string result is looked up as a user property; anything else is `Missing`.
    - `newid()` returns `Guid.NewGuid()`.
  - `public sealed class SqlFilter`:
    - Members: `string Expression`, `IReadOnlyDictionary<string, object?> Parameters`, `static SqlFilter Parse(string expression, IReadOnlyDictionary<string, object?>? parameters = null)` (grammar plus binder), and `bool Matches(BrokeredMessage m)`.
    - `Matches` is true only for `true`. It may throw `SqlEvaluationException`.
  - `public sealed class SqlRuleAction`:
    - Members: `string Expression`, `Parameters`, `static Parse(…)`, and `void Apply(BrokeredMessage copy, string ruleName)`.
    - `Apply` sets `copy.ApplicationProperties["RuleName"] = ruleName` first, then runs the statements in order against a context over `copy`. On `SqlEvaluationException` it stops, leaving earlier effects in place, and rethrows.
    - `SET user.x = v`: if `x` exists (case-insensitive key match), store `ToMessage(ConvertTo(existing.GetType(), v))` under the existing key; otherwise store `ToMessage(v)`. A `Missing` or `null` `v` stores `null`.
    - `REMOVE user.x` removes the key if present.
    - `REMOVE sys.X` throws `SqlEvaluationException(SqlErrorText.CannotRemoveSystem(X))`.
    - `SET sys.X = null` does nothing.
    - `SET sys.X = v` records `SystemOverrides[X] = converted`, so later statements read it. It writes through to `copy` unless `X` is in `SenderPresetWins` **and** the copy's original value was non-null. The original value is captured before the first statement.

- [ ] **Step 1: Write the failing tests.** Expected values come from the probe:
  - The issue's five expressions × `Created`/`Unrelated`.
  - Missing versus null:
    - Missing `<> 'x'`, `NOT (missing = 'x')`, `missing NOT IN ('x')` and `NOT (missing < 1)` don't match.
    - Null-valued `p`: `p = NULL`, `p <> 'x'`, `p NOT IN ('x')` and `p NOT LIKE 'x'` match.
    - `NULL = NULL` matches. `color IN ('red', NULL)` and `color NOT IN ('x', NULL)` match.
  - Laziness:
    - `1 = 1 OR color > 1` matches. `color > 1 OR 1 = 1` throws. `1 = 0 AND color > 1` doesn't match.
    - `color IN ('red', 1)` matches. `color IN (1, 'red')` throws. `missing > 'a'` doesn't match and doesn't throw.
  - `property('COLOR') = 'red'` matches.
  - `sys.To IS NULL`, `sys.To = NULL` and `sys.To <> 'x'` match when `To` is unset.
  - `EXISTS(sys.To)` matches when `To` is unset. `sys.ScheduledEnqueueTimeUtc IS NULL` doesn't match.
  - Actions:
    - `SET user.b = user.n * 2` with `n` = int 3 gives `b` = 6L.
    - `SET user.n = 5; SET user.m = user.n` with int `n` gives `n` = 5 (int) and `m` = 5 (int).
    - `SET user.x = 1; SET user.x = 'a'` throws, and `x` stays 1L.
    - `REMOVE user.color; SET user.x = user.color` gives `x` = null.
    - `SET sys.To = 'new'; SET user.t = sys.To` with `To = "o"` gives `t = "new"` and `copy.To == "o"`.
    - `SET sys.Label = 'new'` with `Subject = "o"` gives `"new"`.
    - `SET sys.SessionId = 'new'` with `SessionId = "o"` leaves `SessionId` at `"o"` and sets `PartitionKey` to `"new"`.
    - `SET sys.ReplyTo = NULL` makes no change.
    - `SET user.RuleName = 'mine'` gives `RuleName = "mine"`.
    - `REMOVE sys.Label` throws `A system property cannot be removed. The system property specified was 'Label'.\r\nParameter name: name`.
  - Parameters: `SqlFilter.Parse("color = @P", {["@p"]="red"})` matches.
  - Review Focus 3: under `de-DE`, `SET user.x = color + 1.5` gives `"red1.5"`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~SqlEvaluatorTests`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sql): lazy evaluator, compiled filters and actions`.

### Task 6: Broker integration

**Files:**
- Modify: `Broker/RuleEntity.cs`, `Broker/SubscriptionEntity.cs`, `Broker/QueueEntity.cs`
- Delete: `Broker/SqlFilterExpression.cs`, `tests/AlmostServiceBus.Tests/Broker/SqlFilterExpressionTests.cs`
- Test: `tests/AlmostServiceBus.Tests/Broker/RuleDeliveryTests.cs`; keep `SubscriptionEntityTests`, updating its SQL cases to the Azure answers

**Interfaces:**
- `RuleEntity` gains:
  - `Dictionary<string, object?>? FilterParameters` and `ActionParameters` (keys include `@`).
  - `void Validate()`, which throws `SqlSyntaxException`.
  - `bool Matches(BrokeredMessage)`, which may throw `SqlEvaluationException`. On an invalid, never-validated rule it throws `SqlSyntaxException`.
  - `bool HasAction`, and `void ApplyAction(BrokeredMessage copy)`, which uses `Name` as the `RuleName`.
  - `bool IsTrueFilter`: `FilterType == TrueFilter`.
  - The compiled form is one immutable record, swapped as a single reference. All regex code is deleted.
  - A whitespace-only `SqlExpression` is invalid (Q11).
- `QueueEntity.DeadLetterOnArrival(BrokeredMessage message, string reason, string? description = null, string? source = null)` sets a new `BrokeredMessage.DeadLetterSource`. The existing outbound encoding of dead-letter fields must include `DeadLetterSource` as an application property, the same way `DeadLetterReason` is written today; find where that happens and extend it.
- `SubscriptionEntity.AddOrUpdateRule` calls `rule.Validate()` first.
- `SubscriptionEntity.DeliverMessage(BrokeredMessage message)` follows decisions log Q9 and the evaluation-error rules:
  1. Iterate rules ordered by name (`StringComparer.OrdinalIgnoreCase`).
  2. For each rule, evaluate `Matches(message)`. On `SqlEvaluationException`, dead-letter a `Clone()` (when `DeadLetteringOnFilterEvaluationExceptions`) with reason `"FilterException"`, the exception message, and source `"FilteringEngine"`, then continue with the next rule. When the flag is off, just continue.
  3. A matching rule with an action produces `var copy = message.Clone(); rule.ApplyAction(copy);`. On `SqlEvaluationException`, dead-letter `copy` (with its partial effects) with reason `"RuleActionException"`, or drop it when the flag is off. On success, enqueue `copy`.
  4. If at least one matching rule has no action, enqueue exactly one `message.Clone()`.
  5. Each enqueued copy goes through the existing session-null check and `Queue.Enqueue`, so `ForwardTo` forwards the action's result.

- [ ] **Step 1: Write the failing tests** in `RuleDeliveryTests`:
  - `TwoActionRules_TwoCopies_EachWithOwnRuleNameAndEffect`.
  - `ActionRuleAndPlainRule_TwoCopies_PlainHasNoRuleName`.
  - `TwoPlainRules_OneCopy`.
  - `FilterError_DeadLettersOneCopy_OtherRulesStillDeliver`: rules `color > 1` and true, against message `color = 'red'`. Expect 1 active copy and 1 dead-lettered copy with `DeadLetterReason == "FilterException"`, `DeadLetterErrorDescription == "Operator '>' cannot be applied to operands of type 'string' and 'long'"` and `DeadLetterSource == "FilteringEngine"`.
  - `ActionError_DeadLettersPartialCopy`: `SET user.a = 1; SET user.b = 1/0` gives a dead-lettered copy with `a = 1L` and `RuleName`, and reason `"RuleActionException"`.
  - `ErrorsWithDeadLetteringDisabled_AreDropped`.
  - `ActionOnForwardingSubscription_TargetQueueReceivesModifiedCopy`.
  - `ActionNeverTouchesOriginalOrOtherSubscriptions`.
  - `AddOrUpdateRule_InvalidFilter_Throws_RuleAbsent` and `AddOrUpdateRule_InvalidAction_Throws`.
  - `ConcurrentRuleReplaceDuringPublish_NeverThrows` (Review Focus 5): 10,000 iterations on each side.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~RuleDeliveryTests|FullyQualifiedName~SubscriptionEntityTests"`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run all of `tests/AlmostServiceBus.Tests`.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(broker): Azure rule delivery — per-rule copies, RuleName, filter/action exceptions`.

### Task 7: Shared table seeded from the probe, and broker-level runner

**Files:**
- Create: `tests/shared/sql-compat-cases.json`, `tests/shared/SqlCompatCases.cs`
- Modify: `tests/AlmostServiceBus.Tests/AlmostServiceBus.Tests.csproj`. Link both shared files: `<Compile Include="..\shared\SqlCompatCases.cs" Link="Shared\SqlCompatCases.cs" />`, and the JSON as `None` with `CopyToOutputDirectory=PreserveNewest`.
- Test: `tests/AlmostServiceBus.Tests/Broker/Sql/SqlCaseTableTests.cs`, `tests/AlmostServiceBus.Tests/Broker/Sql/Issue192ReproTests.cs`

**Interfaces:**
- **JSON schema** (camelCase):
  ```json
  { "cases": [ {
      "id": "in.with-null-nomatch", "area": "in",
      "rules": [ { "filter": "color IN ('x', NULL)", "filterParameters": null,
                   "action": null, "actionParameters": null, "correlation": false } ],
      "subscription": { "deadLetterOnFilterExceptions": true },
      "message": { "sys": { "Label": null, "To": null, "ReplyTo": null, "CorrelationId": null, "ContentType": null,
                            "ReplyToSessionId": null, "SessionId": null, "PartitionKey": null, "MessageId": null, "TimeToLive": null },
                   "user": { "color": { "type": "string", "value": "red" } } },
      "expected": { "outcome": "NoMatch",
                    "copies": [ { "user": { }, "sys": { } } ],
                    "deadLettered": [ { "reason": "FilterException", "description": "…", "user": { } } ],
                    "error": { "type": "ArgumentException", "message": "…" } },
      "verified": "azure", "note": null } ] }
  ```
  - `outcome` is one of `Match`, `NoMatch`, `Rejected`, `DeadLettered`. `copies` and `deadLettered` list every resulting message, with the full user property set (including `RuleName`) and only the sys values the case cares about. `error` is set only for `Rejected`.
  - Typed `type` values: `string`, `int`, `long`, `uint`, `ulong`, `short`, `byte`, `sbyte`, `char`, `float`, `double`, `decimal`, `bool`, `datetime` (ISO-8601 UTC), `datetimeoffset`, `guid`, `timespan` (the `c` format), `uri`, `null`.
  - A rule's `filter` may be null, meaning a true filter. `correlation: true` means a correlation filter that matches only the case (used with an action).
- **`SqlCompatCases.cs`** (namespace `AlmostServiceBus.Tests.Shared`): records for the schema, `TypedValue.ToClr()` / `TypedValue.From(object?)`, `Load()`, `SourcePath()` (walks up to `AlmostServiceBus.sln`) and `Save(…)` (indented, preserving case order).
- **Seeding.** Build the table from the probe:
  - Write a one-off converter: a C# script, or extend `docs/superpowers/specs/sql-azure-probe/probe-program.cs.txt`'s case list. Map each probe case's definition and its recorded Azure result (`round*.md` and the scratch `results*.json`, or reconstruct from the `.md` rows) into JSON with `"verified": "azure"`.
  - Exclude the two `q5.bytes-*` cases: the SDK refuses to send `byte[]`.
  - Exclude probe-only `extra` checks; those become Task 10 tests.
  - Strip the probe's `user.caseId` guard from expressions and from rejected-case columns. The runner adds its own guard only in the conformance suite.
  - Keep `caseId` out of `expected.user`.

- [ ] **Step 1: Write `Issue192ReproTests`.** Use the issue's program body verbatim in a `[Fact]`, collecting mismatch lines and asserting `Assert.Empty(mismatches)`.
- [ ] **Step 2: Write the converter, run it, and commit the generated `sql-compat-cases.json`.** Expect about 410 cases. Spot-check five against `round*.md` by hand.
- [ ] **Step 3: Write `SqlCaseTableTests`**, a `[Theory]` with `[MemberData]` over case ids. For each case:
  - Create a `TopicEntity` with one subscription. Set its dead-lettering flag. Remove `$Default`, then add the rules `r0…rN`. A `SqlSyntaxException` means `Rejected`; compare its message with `expected.error.message` after trimming Azure's trailing `TrackingId…`.
  - Publish a `BrokeredMessage` built from `message`, then drain the active queue and the dead-letter store, and compare with `expected`.
  - The failure message shows `id`, `note`, the expected and actual JSON.
- [ ] **Step 4: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~SqlCaseTableTests|FullyQualifiedName~Issue192ReproTests"`. Expected: PASS.
  - Every failure is an emulator bug against recorded Azure behaviour. Fix the code; never the table.
  - The only allowed exception is a case the emulator can't model. Mark it `"skipEmulator": true` with a `note`, and list every such case in the final summary.
- [ ] **Step 5: Commit** with `test(sql): Azure-recorded case table, broker-level runner, issue #192 repro`.

### Task 8: HTTP management API

**Files:**
- Modify: `Management/AtomXmlReader.cs` (`RuleProperties` gains `FilterParameters` and `ActionParameters`), `Management/AtomXmlWriter.cs`, `Management/ManagementApiEndpoints.cs`
- Test: `AtomXmlReaderTests.cs`, `AtomXmlWriterTests.cs`, `ManagementApiRuleTests.cs`

**Interfaces:**
- **Parameter XML** (the SDK's shape): `<Parameters><KeyValueOfstringanyType><Key>@a</Key><Value i:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">x</Value></KeyValueOfstringanyType></Parameters>`.
  - Elements are in the ServiceBus namespace.
  - Types: `string`, `int`, `long`, `boolean`, `double`, `dateTime` (UTC), `duration` (`TimeSpan`, XML duration).
  - Resolve the `i:type` prefix through the element's namespace scope. Keys are kept as sent.
  - The writer emits the same shape, so `GetRuleAsync` round-trips the CLR types.
- **Validation:** rule `PUT` and subscription `PUT` (with `DefaultRuleDescription`) call `RuleEntity.Validate()` before mutating anything. A `SqlSyntaxException` returns `ManagementApiErrors.BadRequest(ex.Message)`; the SDK raises it as `ArgumentException`.
- **Duplicate rule:** a rule `PUT` without `If-Match` for an existing name returns 409, via `ManagementApiErrors.EntityAlreadyExists("{ns}:Topic:{topic}|{sub}|{rule}")` with Azure's text `The messaging entity '…' already exists.`. Check what the current code does for duplicates and align it.
- **Filter types:** a `TrueFilter` created over HTTP reads back as `TrueFilter`; a `SqlFilter` with `1=1` stays a `SqlFilter`.

- [ ] **Step 1: Write the failing tests:**
  - Reader: two parameters give `{["@a"]="x", ["@b"]=5L}`, and a `duration` gives a `TimeSpan`.
  - Writer: every type round-trips.
  - API:
    - `CreateRule_WithParameters_GetRuleReturnsSameTypes`.
    - `CreateRule_InvalidFilter_400_AzureText`: the body contains `There was an error parsing the SQL expression. [Token line=2, column=0`.
    - `CreateRule_UnusedParameter_400`.
    - `CreateRule_Duplicate_409`.
    - `CreateSubscription_InvalidDefaultRule_400_NotCreated`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter "FullyQualifiedName~AtomXml|FullyQualifiedName~ManagementApiRuleTests"`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(mgmt): SQL parameters over Atom XML; Azure-shaped rule errors`.

### Task 9: AMQP rule manager operations

**Files:**
- Create: `src/AlmostServiceBus.Core/Amqp/RuleManagementHandler.cs`
- Modify: `src/AlmostServiceBus.Core/Amqp/ManagementLinkEndpoint.cs` (add three `case`s; put a `// TODO(#192 follow-up)` at `default:`, which still replies OK)
- Test: `tests/AlmostServiceBus.Tests/Amqp/RuleManagementHandlerTests.cs`

**Interfaces:**
- **Wire format** (Azure SDK source):
  - **`com.microsoft:add-rule`:** body `{ "rule-name", "rule-description": { "sql-filter": {"expression"} | "correlation-filter": {correlation-id, message-id, to, reply-to, label, session-id, reply-to-session-id, content-type, properties}, "sql-rule-action"?: {"expression"}, "rule-name" } }`.
    - The SDK sends true and false filters as `sql-filter` `1=1` / `1=0`; store them as SQL filters, since Azure reads them back as `SqlRuleFilter`.
    - The AMQP path carries no parameters.
    - Enumerate map entries, accepting `string` or `Symbol` keys.
  - **`com.microsoft:remove-rule`:** `{ "rule-name" }`.
  - **`com.microsoft:enumerate-rules`:** `{ "top": int, "skip": int }`.
    - Reply 200 with `{ "rules": List<Map{ "rule-description": DescribedValue }> }`, or 204 when `skip` is past the end.
  - **Described types**, exactly as the SDK codes them:

    | Type | Code | Fields |
    |---|---|---|
    | RuleDescription | `0x0000013700000004` | `[filter, action, rule-name, created-at]` |
    | SqlFilter | `0x000001370000006` | `[expression, 20]` |
    | CorrelationFilter | `0x000001370000009` | `[9 fields as above]` |
    | TrueFilter | `0x000001370000007` | `[]` (used for `$Default`) |
    | FalseFilter | `0x000001370000008` | `[]` |
    | EmptyRuleAction | `0x0000013700000005` | `[]` |
    | SqlRuleAction | `0x0000013700000006` | `[expression, 20]` |

- **Error replies** (from the decisions log, Q10), using the existing reply helper's `statusCode`, `statusDescription` and `errorCondition`:
  - Invalid expression: status 500, condition `amqp:internal-error`, description `The service was unable to process the request; please retry the operation.`. The SDK maps that to `GeneralError`, as Azure does.
  - Duplicate: 409 `com.microsoft:entity-already-exists`, giving `MessagingEntityAlreadyExists`.
  - Missing rule or subscription: 404 `amqp:not-found`, giving `MessagingEntityNotFound`.
- **Target subscription:** resolve it from the management link's address (`{topic}/Subscriptions/{sub}/$management`) the way the existing handlers resolve their entity, within `ResolveNamespace(requestContext)`.

- [ ] **Step 1: Write the failing tests:**
  - `AddRule_SqlFilterAndAction_Stored`.
  - `AddRule_SymbolKeys_Accepted`.
  - `AddRule_Invalid_Replies500InternalError`.
  - `AddRule_Duplicate_Replies409`.
  - `RemoveRule_Missing_Replies404`.
  - `EnumerateRules_ExactDescriptorCodes`: `$Default` is `TrueFilter`, and a rule added as `1=1` is `SqlFilter`.
  - `EnumerateRules_SkipPastEnd_204`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.Tests --filter FullyQualifiedName~RuleManagementHandlerTests`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(amqp): add-rule, remove-rule, enumerate-rules`.

### Task 10: SDK end-to-end tests

**Files:**
- Create: `tests/AlmostServiceBus.SdkIntegration.Tests/SqlRuleTests.cs`. Use `AdminClientTests` as the template for fixture and admin setup. The data client is `new ServiceBusClient(_fixture.ConnectionString, new ServiceBusClientOptions { RetryOptions = new() { MaxRetries = 0 } })`, because Azure-shaped `GeneralError` is retried and that would slow the tests.

- [ ] **Step 1: Write the tests.** Each uses its own GUID-suffixed topic.
  - `AdminClient_IssueRepro_RoutesOnlyCreated`: `"message-type" IN ('Created','Updated')`.
  - `AdminClient_ParametersAndAction`: filter `amount > @min` (`@min` = 10L) and action `SET user.routed = 'yes'`. Received: `routed == "yes"` and `RuleName == <rule>`. `GetRuleAsync` returns `@min` as `long`.
  - `AdminClient_TimeSpanPropertyFilter_AndActionWritingTimeSpan`: filter `ts = @t` with `@t = TimeSpan.FromHours(1)`, and action `SET user.copy = ts`. The received `copy` is a `TimeSpan` (Review Focus 2).
  - `AdminClient_InvalidExpression_ThrowsArgumentException`: the message starts `There was an error parsing the SQL expression.`.
  - `AdminClient_DuplicateRule_ThrowsEntityAlreadyExists`.
  - `AdminClient_FilterException_DeadLettersWithReason`: a received dead-letter message has `DeadLetterReason == "FilterException"` and `ApplicationProperties["DeadLetterSource"] == "FilteringEngine"`.
  - `RuleManager_CreateListDelete`:
    - Create `[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'` with action `SET sys.Label = 'nsb'` (after deleting `$Default`). A message with no `Subject` arrives with `Subject == "nsb"`.
    - `GetRulesAsync` returns `SqlRuleFilter` and `SqlRuleAction` with the exact text.
    - After `DeleteRuleAsync`, nothing arrives.
  - `RuleManager_TrueFilter_ReadsBackAsSql11`.
  - `RuleManager_InvalidExpression_ThrowsGeneralError`.
  - `RuleManager_Duplicate_EntityAlreadyExists`.
  - `RuleManager_DeleteMissing_EntityNotFound`.
- [ ] **Step 2: Run** `dotnet test tests/AlmostServiceBus.SdkIntegration.Tests --filter FullyQualifiedName~SqlRuleTests`. Expected: PASS. Fix any failure in the owning task's code.
- [ ] **Step 3: Commit** with `test(sdk): end-to-end SQL rules via admin client and rule manager`.

### Task 11: Node and Java smoke coverage

Do this task **only if George confirms it**. The open question is whether "don't touch `tests/client-sdk-smoke`" covers adding test steps. If he doesn't confirm, skip it and say so in the summary.

**Files:** `tests/client-sdk-smoke/node/smoke.mjs`, `tests/client-sdk-smoke/java/src/main/java/io/almostservicebus/smoke/Smoke.java`. Connection handling stays unchanged.

- [ ] **Step 1: Add a rule-manager step after each script's existing topic steps:**
  - Delete `$Default`, then create `in-rule` with `color IN ('red','blue')` and action `SET user.routed = 'yes'`.
  - List the rules and assert `in-rule` is there.
  - Send `red` and `green`, and assert that only `red` arrives, with `routed == 'yes'`.
  - Assert that creating `color IN ('red'` throws.
  - Delete `in-rule`, then recreate `$Default`.
- [ ] **Step 2: Run** `node smoke.mjs`, `node admin.mjs`, `node concurrent-connections.mjs`, `mvn -q -B compile exec:java` and `python smoke.py` against a locally started emulator (`cd src/AlmostServiceBus.Host && dotnet run`). Stop only the PID you started. Expected: all exit 0.
- [ ] **Step 3: Commit** with `test(smoke): rule manager in Node and Java`.

### Task 12: Connection helper and the conformance compat suite

**Files:**
- Modify: `tests/AlmostServiceBus.Conformance.Tests/AlmostServiceBus.Conformance.Tests.csproj`:
  - Add `<UserSecretsId>almostservicebus-conformance</UserSecretsId>`.
  - Add `<PackageReference Include="Microsoft.Extensions.Configuration.UserSecrets" Version="10.0.x" />`, choosing the newest 10.0.x on NuGet (there is no central package management).
  - Add the shared links from Task 7.
- Create: `RealAsbConnection.cs`:
  ```csharp
  internal static class RealAsbConnection { public static string? ConnectionString { get; } }
  ```
  It returns the environment variable `ASB_CONNECTION_STRING` if non-empty, else `new ConfigurationBuilder().AddUserSecrets(typeof(RealAsbConnection).Assembly, optional: true).Build()["ASB_CONNECTION_STRING"]`.
- Modify: `RealAsbConformanceTests.cs` and `TopicRequeueReproTest.cs` to use it. Skip reasons mention both sources.
- Modify: the "real ASB" sections of `CLAUDE.md` and `README.md` to add `dotnet user-secrets set ASB_CONNECTION_STRING "..." --id almostservicebus-conformance`.
- Create: `SqlCompatTests.cs`, with one `[Fact] SqlCompatTable_EmulatorAndAzureAgreeWithTable`.

**Interfaces / algorithm:**
- `RunTableAsync(ServiceBusClient, ServiceBusAdministrationClient)` returns `Dictionary<string, ObservedResult>`. This is the probe's algorithm, ported from `probe-program.cs.txt`:
  - One topic, `sqlcompat-{8 hex}`. One subscription per case (`c{index:D3}`), with the case's dead-lettering flag.
  - Filters are wrapped as `user.caseId = '{id}' AND ({filter})`. A null filter becomes the guard alone. A correlation rule matches on `caseId`.
  - One message per case. Drain the active queue and the DLQ, with a parallelism of 16.
  - Delete the topic in `finally`.
- Run it against the emulator (`ServiceBusEmulatorFixture`), and against Azure when `RealAsbConnection.ConnectionString` is non-null.
- Compare both with `expected`. Rejected-case messages are compared after removing Azure's `TrackingId…` tail; column numbers inside them shift by the guard's length, so compare the guarded text.
- With `ASB_COMPAT_RECORD=1` and Azure present, overwrite `expected` from Azure, set `verified = "azure"`, and `Save()`.
- Fail with a markdown table: `id | table | azure | emulator`.

- [ ] **Step 1: Write `RealAsbConnection`, switch the two existing classes over, and update the docs.**
- [ ] **Step 2: Write `SqlCompatTests`.**
- [ ] **Step 3: Run** `dotnet test tests/AlmostServiceBus.Conformance.Tests --filter "FullyQualifiedName~SqlCompatTests"`. Expected: PASS. With the user secret present this also runs against Azure.
- [ ] **Step 4: Commit** with `test(conformance): connection helper (env, then user secrets) and SQL compat suite`.

### Task 13: Reconcile, document, verify

- [ ] **Step 1: Re-run against Azure in record mode:** `ASB_COMPAT_RECORD=1 dotnet test tests/AlmostServiceBus.Conformance.Tests --filter FullyQualifiedName~SqlCompatTests`. `git diff` must touch only `tests/shared/sql-compat-cases.json`. Any change from the 2026-11-04 recording is Azure drift; note it in the spec's decisions log.
- [ ] **Step 2: Fix every divergence** in the emulator, until the suite is green both with and without Azure.
- [ ] **Step 3: Update `CHANGELOG.md`.** Add a `## [Unreleased]` section in the file's style.
  - **Fixed:** SQL filters (#192): `IN`, delimited identifiers, no more match-all fallback.
  - **Added:** SQL rule actions, SQL parameters, AMQP rule manager operations, filter and action exception dead-lettering.
  - **Behaviour changes:**
    - Unprefixed names are user properties (Q1).
    - Invalid expressions are rejected at creation.
    - `LIKE` is case-sensitive.
    - Numeric strings no longer equal numbers; type mismatches dead-letter with `FilterException`.
    - String ordering comparisons dead-letter.
    - Empty or whitespace expressions are rejected.
- [ ] **Step 4: Update `CLAUDE.md`.**
  - Add `Broker/Sql/` to Key Components and update the test counts table.
  - Add a Key Design Decision: "SQL semantics follow the Azure-recorded table `tests/shared/sql-compat-cases.json`; change behaviour only with a recorded Azure case."
- [ ] **Step 5: Run everything.**
  - The full test command, and Task 11's smoke scripts if they were done.
  - The external framework suites from CLAUDE.md "Running": run `git submodule update --init` first; NServiceBus needs the emulator on port 5300.

  Expected: green, apart from known pre-existing failures, which must be listed.
- [ ] **Step 6: Commit** with `docs: SQL compat changelog and CLAUDE.md for #192`. Report to George, without pushing.
