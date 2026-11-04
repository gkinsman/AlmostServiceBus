# SQL Filters and Rule Actions — Design

Issue: [#192](https://github.com/gkinsman/AlmostServiceBus/issues/192) — SQL `IN` and delimited
identifiers silently match unrelated messages.

## Intent

Subscription routing is the part of a broker that users can't see when it goes wrong. Today
`RuleEntity` evaluates SQL filters with a chain of regexes and returns `true` for anything it
doesn't recognise *and* on any exception. A filter the emulator doesn't understand quietly
becomes match-all. A team developing against the emulator then ships routing that behaves
differently in Azure. That is the worst failure an emulator can have: it hides the bug it
exists to catch.

Replace it with a real implementation of the Service Bus SQL grammar, for filters **and**
`SqlRuleAction`. Azure decides every behaviour; the docs are only the starting point.

Reference docs:
- SQL filter syntax: https://learn.microsoft.com/azure/service-bus-messaging/service-bus-messaging-sql-filter
- SQL rule action syntax: https://learn.microsoft.com/azure/service-bus-messaging/service-bus-messaging-sql-rule-action

## Goals

1. Parse the full documented grammar for filters and actions, and evaluate it as Azure does.
2. Reject invalid or unsupported expressions **when the rule is created**, on every path that
   creates rules, with the error shape the SDK expects.
3. Execute rule actions (`SET`, `REMOVE`) on the copy of the message delivered to the
   subscription.
4. Prove fidelity with a compatibility suite that runs one shared table of cases against
   real Azure and the emulator, and diffs the results.

## Non-goals

- Performance work beyond "parse once per rule, not per message".
- Persistence. The emulator has none: rules live in memory and are created at runtime through
  the HTTP admin API, AMQP `$management`, or in code (`SubscriptionEntity.AddOrUpdateRule`).
  There are no stored rules to migrate.
- Dashboard changes.
- Correlation filter semantics (unchanged, except that correlation filters can now carry an
  action).

## Anti-goals

- **No fallback, ever.** No code path may turn an unparseable or unevaluable expression into
  "match" — or into a silent "no match" — without an explicit, Azure-confirmed reason.
- No guessing where Azure can be asked. If a behaviour is ambiguous, the compat run settles it.
- The connection string never lands in git.

## Decisions already made (George)

- **Parser library: Superpower** (Datalust; latest is 3.2.1 on NuGet). Not Sprache and not
  hand-written. Tokenizer first (`Tokenizer<SqlToken>`/`TokenizerBuilder`), then token-list
  combinators building an AST. Parse errors report **position and the expected token**
  (Superpower's `TokenListParserResult` provides both; surface them in the exception message).
- **Scope:** filters and `SqlRuleAction`, **including SQL parameters** (`@name`, from
  `SqlRuleFilter.Parameters` / `SqlRuleAction.Parameters`).
- **Goal: as full compatibility with Azure Service Bus as possible.** Where Azure and the docs
  disagree, Azure wins.
- **Fidelity:** Azure is the reference. George provides a real namespace for the compat run.
  It must be Standard tier or above, because Basic has no topics.
- **Secrets:** the connection string comes from an environment variable and never touches git.
- **Workflow:** commit locally on the branch. Don't push, open a PR or comment on the issue
  until George says so.

## Grammar to support

From the two reference pages.

**Identifiers.** Each can carry a scope: `sys.` or `user.`.
- Regular: `[A-Za-z_][A-Za-z0-9_]*`.
- `"double-quoted"`, with `""` as an escaped quote.
- `[bracketed]`, with `]]` as an escaped bracket.

A scope applies to any identifier form (`user.[message-type]`, `sys."Label"`).

**Literals.**
- Strings `'...'` with `''` as an escape.
- Integers.
- Decimals (`1.5`, `.5`).
- Approximate numbers (`1e10`, `1.5E-3`).
- `TRUE`, `FALSE`, `NULL`.

**Operators** (lowest to highest precedence):
- `OR`
- `AND`
- `NOT`
- comparisons: `=` `<>` `!=` `<` `<=` `>` `>=`, `[NOT] LIKE pattern [ESCAPE 'c']`,
  `[NOT] IN (list)`, `IS [NOT] NULL`
- `+` `-`
- `*` `/` `%`
- unary `+` and `-`

**Predicates and functions:** `EXISTS(property)`, `newid()`, `property(name)`.

**Parameters:** `@name` (a regular identifier after `@`) is a value bound from the rule's
parameter collection. It is allowed anywhere a literal is allowed, in filters and in action
expressions.

**Actions:** `SET <property> = <expression>` and `REMOVE <property>`, separated by `;`, with an
optional trailing `;`. Action expressions use the same expression grammar.

**Keywords** are case-insensitive. Whether `sys.` property names are case-insensitive is a
compat question (see below).

## Architecture

Everything new goes in `src/AlmostServiceBus.Core/Broker/Sql/` (namespace
`AlmostServiceBus.Core.Broker.Sql`):

| Piece | Role |
|---|---|
| `SqlToken` | Token enum |
| `SqlTokenizer` | Superpower tokenizer |
| `SqlGrammar` | Combinators: `Filter` (expression to end of input), `Action` (statement list) |
| `SqlAst` | Immutable records: literals, property refs (scope + name), unary/binary, `In`, `Like`, `IsNull`, `Exists`, function calls; `SetStatement`, `RemoveStatement` |
| `SqlEvaluator` | Three-valued evaluation and numeric promotion over an AST and a message |
| `SqlFilter` / `SqlRuleAction` | Compiled, immutable objects: `Parse(string)` → instance, or throws `SqlSyntaxException` |
| `SqlSyntaxException` | `FormatException` subclass carrying position and expected tokens |
| `SqlEvaluationException` | Runtime evaluation error, if the compat run shows Azure has them (see open questions) |

`RuleEntity` holds the compiled filter and action. It compiles them when the expression is
assigned, or lazily with a cache keyed on the expression text. The cache must be a single
reference, so concurrent publishes can't read a half-written value. `RuleEntity.Matches` uses
only the compiled filter. The regex code is deleted.

### Existing work on the branch

Commit `38a31bd` (WIP) holds a hand-written parser for filters (`SqlFilterExpression.cs`) and
tests for it. **The parser is replaced by Superpower; the WIP is not kept as is.** Its test
cases (`SqlFilterExpressionTests`), its evaluation rules (three-valued AND/OR/NOT, the
value-comparison helper) and its management-API 400 wiring are worth carrying over into the
new structure.

## Semantics

> **Superseded where it disagrees by the Decisions log at the end, which records real Azure behaviour.**

Below are the defaults to implement from the docs. Any of them can be overridden by the compat
run, and the results get recorded in the case table.

- **Three-valued logic.** A missing property is `NULL`. A comparison, `LIKE` or `IN` with a
  `NULL` operand is unknown. `NOT unknown` is unknown. `AND`/`OR` follow SQL truth tables. A
  rule matches only when the filter is definitely `TRUE`. `IS NULL` and `EXISTS` are never
  unknown.
- **Numeric promotion.** Per the docs:
  - integer constants are `Int64`, and decimal and approximate constants are `Double`
  - integer property types promote to `Int64`, floating types to `Double`
  - mixed `Int64`/`Double` operations promote to `Double`

  How `System.Decimal` properties and numeric strings behave (e.g. `"27" = 27`) is a compat
  question.
- **String comparison.** Case sensitivity for `=` and for `LIKE`, and whether `<` and `>` work
  on strings, are compat questions. Today `=` is ordinal and `LIKE` ignores case.
- **`sys.` mapping.** Map every system property the docs list, onto the `BrokeredMessage`
  fields the emulator has. Candidates:
  - `MessageId`, `CorrelationId`, `To`, `ReplyTo`, `Label`, `SessionId`, `ReplyToSessionId`,
    `ContentType`, `PartitionKey`
  - `SequenceNumber`, `EnqueuedSequenceNumber`, `DeliveryCount`
  - `EnqueuedTimeUtc`, `ScheduledEnqueueTimeUtc`, `ExpiresAtUtc`, `TimeToLive`, `Size`

  Whether an unknown `sys.` name is rejected at creation or evaluates to `NULL` is a compat
  question.
- **Unprefixed names.** Today they resolve to system properties first. The docs say an
  unprefixed name is a user property. **Decide from the compat run, not the docs.** If the
  behaviour changes, record it in `CHANGELOG.md` under a new `## [Unreleased]` section as a
  behaviour change.
- **`newid()`** returns a new `Guid`.
- **`property(name)`** reads a property named at runtime. Confirm against the docs whether
  `name` is a string expression or an identifier, then confirm against Azure.
- **Comparisons across types** (string vs number, bool vs number, Guid vs string). The question
  is whether Azure treats them as unknown (no match) or as an evaluation error. See open
  questions.

### Actions

- `SET user.x = <expr>` sets or overwrites an application property. `SET sys.<name> = <expr>`
  works only for writable system properties; which ones are writable is a compat question.
- `REMOVE user.x` deletes an application property; `REMOVE` on a missing property is a no-op.
  Whether `REMOVE sys.*` is allowed is a compat question.
- Expressions on the right-hand side read the message state as it stands **before** the action
  starts, or after each statement? Compat question. The docs imply statements run in order.
- Actions apply to the copy the topic gives each subscription (`TopicEntity` already calls
  `message.Clone()` per subscription). They must never touch the original message or another
  subscription's copy. With auto-forwarding, the action applies before the copy is forwarded.
- **More than one matching rule on a subscription.** Delivering one copy per matching rule that
  has an action, versus one copy in total, is a compat question. The emulator currently
  delivers one copy when any rule matches.

### Parameters

- **Storage.** `RuleEntity` gains `FilterParameters` and `ActionParameters` (name → value).
  Parameter names are stored without their leading `@`; the builder confirms from the SDK
  source whether the SDK sends `@x` or `x`, and normalises.
- **Transport.**
  - HTTP: `AtomXmlReader` and `AtomXmlWriter` read and write the `<Parameters>` element of
    `SqlFilter` and `SqlRuleAction`. That element is a list of `KeyValueOfstringanyType`
    entries whose values have typed `i:type` attributes; follow the SDK's serializer for the
    types.
  - AMQP: `add-rule` decodes the `parameters` map in the rule description, and
    `enumerate-rules` encodes it back.
  - Round-trip: `GetRuleAsync` / `GetRulesAsync` must return the same parameter names, values
    and types that were sent.
- **Binding.** Parameters are bound when the rule is compiled, so the compiled filter carries
  the values.
- **Compat questions** (settled in the Azure run):
  - Is a referenced but unbound parameter rejected at creation, or `NULL`?
  - Is a bound but unreferenced parameter allowed?
  - Are parameter names case-sensitive?
  - Which value types are supported (string, long, int, double, bool, DateTime, Guid,
    TimeSpan)?

## Validation when a rule is created

Every path that creates a rule compiles the filter and action first, and rejects the rule when
either fails:

1. **`SubscriptionEntity.AddOrUpdateRule`** compiles, or throws `SqlSyntaxException`; the rule
   is not added. This covers code paths and tests.
2. **HTTP management API.**
   - Rule `PUT` returns 400 and adds no rule.
   - Subscription `PUT` with an invalid `DefaultRuleDescription` returns 400 and **creates no
     subscription**. Validate before `topic.AddSubscription`.
   - Match the body and status to what Azure returns, so the admin client throws the same
     exception type. The compat run records Azure's response.
3. **AMQP `$management`.** `com.microsoft:add-rule`, `com.microsoft:remove-rule` and
   `com.microsoft:enumerate-rules` **are not implemented today**: `ManagementLinkEndpoint`
   replies OK to any unknown operation, so `ServiceBusRuleManager.CreateRuleAsync` "succeeds"
   without adding anything. Implement all three:
   - Decode the SDK's AMQP rule description: filter types sql / correlation / true / false, and
     sql / empty actions. Take the wire format from the `Azure.Messaging.ServiceBus` source
     (`AmqpMessageConverter`, the rule-description encoding).
   - Reply with the status code and `error-condition` Azure uses for an invalid expression and
     for a rule that already exists, so the SDK raises the same `ServiceBusException` reason.
     The compat run records both.
   - Also change the default branch so an unknown operation no longer replies OK. Out of scope
     for this issue, but worth a follow-up note.

`RuleEntity.Matches` on a rule that was never validated (constructed directly, as the issue's
repro does) throws `SqlSyntaxException`. It neither matches nor silently doesn't match.

## Tests

1. **The issue's repro, verbatim**, as a unit test in `AlmostServiceBus.Tests`: the five
   expressions × `Created`/`Unrelated`, with zero mismatches.
2. **Shared case table**, the heart of the suite. One data file (or a C# source of cases)
   listing cases of the form *expression × message (sys + user properties) → expected
   outcome*. Outcomes are `Match`, `NoMatch`, `RejectedAtCreation`, and `EvaluationError` if it
   exists. Action cases also carry the expected properties of the delivered message.
   - Cover every grammar element above, every semantic question above, and every
     open-questions item below.
   - The expected values in the table **are Azure's answers**. They start as doc-based guesses
     and get replaced by the recorded Azure results after the compat run. Each case can carry a
     note explaining surprising Azure behaviour.
3. **Unit runner over the table.** It runs every case against `SqlFilter`/`SqlRuleAction`
   directly (no broker) in `AlmostServiceBus.Tests`.
4. **Parse-error tests.** Position and expected token appear in the message, and rejected
   expressions throw at creation (unit, HTTP 400, AMQP add-rule error).
5. **SDK end-to-end** in `AlmostServiceBus.SdkIntegration.Tests`:
   - Create a rule via `ServiceBusAdministrationClient` and via `ServiceBusRuleManager`, publish,
     and check routing (including `IN` and a delimited identifier).
   - A rule whose action `SET`s a property, where the received message carries it.
   - An invalid expression via each client, which throws the same exception Azure throws.
   - `GetRulesAsync` / `DeleteRuleAsync` through `ServiceBusRuleManager`.
6. **Compatibility suite** in `AlmostServiceBus.Conformance.Tests`. It runs the shared table
   against the emulator and against Azure and diffs the two.
   - **Connection string:** reuse the existing `ASB_CONNECTION_STRING` environment variable that
     `RealAsbConformanceTests` already reads (`.env` is already gitignored). The suite is
     skipped when the variable is unset.
   - **Efficient case layout:**
     - One topic per run, one subscription per case. The filter is
       `user.caseId = <n> AND (<expression>)`, so only the case's own message can match, and
       `TRUE AND x` keeps `x`'s three-valued result.
     - Publish one message per case, then drain each subscription.
     - Rejected-at-creation cases are checked by creating the rule and capturing the exception
       type, reason and message.
     - Clean up the topic afterwards.
   - **Record mode** (e.g. `ASB_COMPAT_RECORD=1`) writes Azure's results back into the case
     table (or a sibling golden file), so later runs without Azure still test the emulator
     against Azure's answers.
   - **Diff report.** On any divergence the suite fails with a readable table: case, Azure,
     emulator.

All existing suites must stay green, including the CI framework suites (MassTransit and
Wolverine both create SQL filters, e.g. Wolverine's
`NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'`).

## Questions the compat run settles

Collect these as cases from the start, so one Azure run answers them all:

1. Do unprefixed names resolve to user properties, system properties, or both?
2. Are `sys.` names case-insensitive? Are user property names case-sensitive?
3. Is an unknown `sys.` name rejected at creation, or `NULL`?
4. Is string `=` case-sensitive? Is `LIKE`? Do `<`/`>` work on strings?
5. Type mismatches (`'abc' > 1`, `"27" = 27`, bool vs int): unknown, coercion, or evaluation
   error? If they are evaluation errors, does Azure dead-letter the message
   (`DeadLetteringOnFilterEvaluationExceptions`), and with what reason?
6. Division by zero, overflow.
7. Which `sys.` properties can an action `SET` or `REMOVE`? Does an invalid action target fail
   at creation?
8. Do action statements see earlier statements' effects?
9. With two matching rules that have actions, does the subscription get one copy or two?
10. HTTP admin and AMQP rule-manager error shapes for an invalid filter, an invalid action, and
    a duplicate rule name.
11. Is an empty or whitespace `SqlExpression` rejected? (The emulator treats it as match-all
    today.)
12. Parameters: is an unbound reference rejected or `NULL`; is an unused binding allowed; are
    names case-sensitive; how is each value type compared; and do parameters work in actions?

Each answer goes into the table as an expected outcome and into this document's decisions log
(below). Answers that change existing emulator behaviour also go into the changelog.

## Sequencing

1. Superpower tokenizer, grammar and AST, with parse-error tests.
2. Evaluator, the case table (doc-based guesses), the unit table runner, and the issue's repro.
3. `RuleEntity` / `SubscriptionEntity` integration and validation. Delete the regex code.
4. HTTP management validation; AMQP `add-rule` / `remove-rule` / `enumerate-rules`.
5. Actions: grammar, evaluation, and applying them to the delivered copy.
5a. Parameters: grammar, binding, and HTTP and AMQP round-trip.
6. SDK end-to-end tests.
7. Compat suite. **Then ask George for the namespace**, run in record mode, and reconcile every
   divergence (fix the emulator, or note why the case can't apply).
8. CHANGELOG `[Unreleased]` entry; update the CLAUDE.md test counts and "Key components"; final
   full test run.

## AMQP clients beyond .NET (my call)

The `add-rule` / `enumerate-rules` / `remove-rule` decoding follows CLAUDE.md decision 9:
- Enumerate `Map` entries, accepting string or symbol keys; never index by key type.
- Take the .NET SDK's encoding as the primary reference.
- Add rule-manager checks (create, list, delete, invalid expression) to the Node and Java
  scripts in `tests/client-sdk-smoke`, and run all three smoke scripts before finishing.

Python is excluded, because its rule management goes through the HTTPS admin client, which the
emulator can't reach.

## Compat run is a gate (my call, from the brief)

Steps 1–6 can be built on doc-based expectations. No behaviour change to existing emulator
behaviour lands as final, though, until the Azure run has confirmed it:
- unprefixed names
- string case sensitivity
- empty expressions
- multi-rule copies

The CHANGELOG entry, the CLAUDE.md updates and the final full test run come after step 7. If
the namespace is delayed, work stops at step 6 and reports which table cases are still
unconfirmed.

## Left to the builder

- Exact AST shape, file split inside `Broker/Sql/`, and how combinators are factored.
- The case table's format (C# `TheoryData` versus a JSON/YAML data file), as long as the unit
  runner and the compat suite share it and record mode can update it.
- The exception message wording (it must include position and expected token).

## Decisions log — confirmed against real Azure (2026-11-04)

These are the results of a probe run against a real Standard-tier namespace on 2026-11-04: 420 cases over three rounds. Raw results are in `docs/superpowers/specs/sql-azure-probe/` (`round1.md`–`round3.md`). The probe program is kept as `probe-program.cs.txt`.

**This section overrides "Semantics", "Actions", "Parameters" and "Questions the compat run settles" above wherever they disagree.** Every row below becomes a case in the shared table, marked `"verified": "azure"`.

### Value model

Azure behaves like .NET expression evaluation over CLR values, plus one extra state:

| Operand | Behaviour |
|---|---|
| **Missing user property** (key not on the message) | **Unknown.** Every comparison, arithmetic, `LIKE` or `IN` involving it is unknown. `NOT` unknown is unknown. `AND`/`OR` use SQL three-valued truth tables. A rule matches only if the result is definitely true. An unknown operand never raises a type error (`missing > 'a'` is just unknown). Only `IS [NOT] NULL` and `[NOT] EXISTS` give a definite answer. |
| **`NULL` literal**, a user property **present with a null value**, or an **unset system property** | **A C# `null` value, not unknown.** `NULL = NULL` is true. `x = NULL` is false. `<>` is the inverse of `=`. `IN` is true if any item is equal (`color IN ('red', NULL)` matches; `color NOT IN ('x', NULL)` matches; `p NOT IN ('x')` matches when `p` is null). `LIKE` on null is false, so `NOT LIKE` is true. `p IS NULL` and `EXISTS(p)` are both true for a null-valued property. `sys.To IS NULL`, `sys.To = NULL` and `sys.To <> 'x'` are all true when `To` is unset. |

**Q1 — unprefixed names are user properties.** `label = 'L'` does not see `sys.Label`. `EXISTS(Label)` is false when only the system Label is set. `SET tag = …` and `REMOVE color` act on user properties. The bare names `sys` and `user` are user property names too.

This **changes existing emulator behaviour**, which looked up system properties first. It goes in the CHANGELOG.

**Q2 — names are case-insensitive; string values are case-sensitive.**
- Case-insensitive: keywords, the `sys`/`user` scopes, system property names, **user property names** (`user.Color` and `COLOR` both find `color`), function names, parameter names, and the name passed to `property()`.

**Q3 — system properties.**
- **Readable:** `MessageId`, `CorrelationId`, `To`, `ReplyTo`, `Label`, `SessionId`, `ReplyToSessionId`, `ContentType`, `PartitionKey`, `ViaPartitionKey`, `SequenceNumber`, `EnqueuedSequenceNumber` (equals `SequenceNumber`), `DeliveryCount`, `EnqueuedTimeUtc`, `ScheduledEnqueueTimeUtc`, `ExpiresAtUtc`, `TimeToLive`, `Size`, `LockToken`, `LockedUntilUtc`, `DeadLetterSource`, `State` and `ForcePersistence`.
- **`DeliveryCount` is 1 when filters run.**
- **`ScheduledEnqueueTimeUtc` is `DateTime.MinValue`, not null, when unset.** So `IS NULL` is false.
- **`ExpiresAtUtc` is always present.**
- **`State` is an enum**, so comparing it with a number is a type error.
- **`sys.Subject` and any unknown name are rejected when the rule is created.** The error is `The specified property {Name} does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.` followed by `Parameter name: qualifiedPropertyName`.
- **`EXISTS(sys.X)` is true for any readable property, even an unset one.**

**Q4 — strings.**
- `=` is case-sensitive, and so is `LIKE`.
- There is no padding: `'red '` doesn't equal `'red'`.
- **`<`, `<=`, `>` and `>=` on strings are evaluation errors** (`Operator '<' cannot be applied to operands of type 'string' and 'string'`).
- `+` concatenates when either side is a string, using `ToString()` on the other side (`'red' + 1 = 'red1'`).
- Unicode and empty strings work. String literals may contain newlines.

**Q5 — types follow C# operator rules**, using the CLR type each value arrived as:

| Rule | Detail |
|---|---|
| Literal types | Integer literals are `long`. Decimal and exponent literals are `double`. |
| Mixed integer types | `int`, `long`, `uint`, `short`, `byte` and `sbyte` combine freely (C# implicit conversions). |
| `float` | Widens to `double`, so `f = 0.1` is false for `0.1f`. |
| No implicit conversion | `decimal` vs `double`, `ulong` vs `long`, `char` vs `string`, a string vs any number (including numeric strings: `n = 27` where `n = "27"`), `bool` vs a number, `bool` vs `string`, and `DateTime`/`DateTimeOffset`/`TimeSpan`/`Uri`/enum vs `string`. All of these are **evaluation errors**. |
| `bool` | Supports only `=` and `<>`. `<` is an error. |
| **`Guid` vs `string`** | The string is **parsed as a Guid**. A malformed string is an evaluation error (`Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).`). `<` and `>` are errors. |
| Error message | `Operator '{op}' cannot be applied to operands of type '{left}' and '{right}'`. `op` uses the C# spelling (`==` and `!=` for `=` and `<>`). Type names are C# keywords for primitives (`string`, `long`, `int`, `bool`, `decimal`, `double`, `char`, `ulong`), otherwise full names (`System.DateTime`, `System.TimeSpan`, `System.Uri`, `System.DateTimeOffset`, `System.Guid`, `Microsoft.ServiceBus.Messaging.MessageState`). |
| Bare values | A bare value is not a predicate. `b`, `TRUE`, `NULL` and `NOT FALSE` on their own are **rejected at creation**, because the grammar requires a comparison. `b = TRUE` works. |

**Q6 — arithmetic follows C#.**
- Integer `/` truncates toward zero, and `%` takes the sign of the dividend.
- Integer division or modulo by zero is an evaluation error (`Attempted to divide by zero.`).
- `long` overflow is an evaluation error (`Arithmetic operation resulted in an overflow.`). `int * long` is a `long` operation.
- Double division by zero gives infinity (no error).
- Unary `+` and `-` work.
- Errors in literal-only expressions (`1/0`) are raised **when the message is evaluated**, not when the rule is created.
- **Integer literals that overflow `long` are rejected at creation** (`Value was either too large or too small for an Int64.`).

**Evaluation errors.**
- **Raised:** type mismatches, integer divide-by-zero, overflow and Guid parse failures.
- **Lazy:** evaluation goes left to right with short-circuiting. `1 = 1 OR color > 1` matches. `color > 1 OR 1 = 1` errors. `IN` stops at the first equal item. An operand that is unknown short-circuits before any type check.
- **Effect:** when `EnableDeadLetteringOnFilterEvaluationExceptions` is on (the SDK default), the message is **dead-lettered in that subscription** with:
  - `DeadLetterReason = "FilterException"`
  - `DeadLetterErrorDescription` set to the .NET message
  - `DeadLetterSource = "FilteringEngine"`

  These are also written as application properties. With the flag off, the copy is dropped.
- **Per rule:** the error affects only that rule's contribution. The subscription's other rules still produce their copies.

**`LIKE`.**
- `%` matches any run of characters **except a newline**. `_` matches exactly one character. Matching is case-sensitive. `.` and `[` are literals.
- **`ESCAPE` uses the first character of its string.** An empty string means no escape character, and a trailing escape character is ignored.
- The pattern and the escape can be any expression: a property, a parameter or a literal. A **non-string literal** pattern is rejected at creation.
- A non-string operand is compared via `ToString()` (`b LIKE 'True'`, `d LIKE '1.5'`, Guids, ints). An unknown pattern gives unknown.

**Grammar facts.**
- **`IS [NOT] NULL` applies only to a property.** On an expression or function it is rejected (`missing + 1 IS NULL`, `newid() IS NOT NULL`).
- **`EXISTS(...)` takes only an identifier.**
- **An unprefixed dotted name** (`a.b`) is rejected: `The specified scope (a) is not supported.`. `user.a.b` is also rejected. Delimited names may contain dots (`[a.b]`, `[NServiceBus.EnclosedMessageTypes]`). `[]` is the empty name.
- **Regular identifiers:** Unicode letters, digits and `_`. No leading digit, and no `$`.
- **Numbers:** no `.5`, `1.`, `1e`, hex or `N'…'`.
- **There are no comments.** `--` is two unary minuses, and `/* */` is rejected.
- **`IN (...)` items** can be any expression (properties, arithmetic). An empty list is rejected.
- **`newid()`** returns a Guid. `NEWID() = NEWID()` is false. `newid(1)` is rejected: `The 'newid' function expects 0 argument(s), but 1 argument(s) was/were specified.`.
- **`property(expr)`** looks the evaluated string up **as a user property** (case-insensitive). `property('sys.Label')` does not see the system Label.
- **Unknown functions** are rejected: `The specified function name ({name}) is unknown. Make sure the function name is correct.`.

**Q12 — parameters.**
- **Keys must include the `@`.** A key of `p` does not bind `@p`.
- Matching is case-insensitive.
- **An unbound reference is rejected at creation** (`A parameter (@x) used in the expression specified in SqlExpression is missing from the Parameters property.`).
- **An unused parameter is rejected at creation** (`The specified parameter (@x) in Parameters property was not used in the expression specified in SqlExpression.`).
- Values keep their CLR type (string, int, long, double, bool, DateTime, TimeSpan) and round-trip through `GetRuleAsync` unchanged.
- Parameters work in `IN`, in `LIKE` patterns, and in actions.
- The AMQP rule manager cannot send parameters. A filter using `@p` created over AMQP fails with `GeneralError`.

### Actions

- **`RuleName`.** Every copy produced by a rule with an action gets the user property `RuleName = <rule name>`, before the statements run. The action can overwrite it.
- **Statements run in order** and see the effects of earlier statements (Q8). If a statement fails, the copy is dead-lettered with `DeadLetterReason = "RuleActionException"`, the .NET message as the description, and `DeadLetterSource = "FilteringEngine"`. **The effects of earlier statements remain on the dead-lettered copy.** With the subscription's dead-lettering-on-filter-exceptions flag off, the copy is dropped.
- **Action expressions are scalar.** `SET x = 1 = 1`, `SET x = c IN (…)` and `SET x = c LIKE …` are rejected at creation.
- **`SET user.x = v`:**
  - **When `x` already exists, `v` is converted to `x`'s existing CLR type**, like `Convert.ChangeType`:
    - int `1` ← `1.5` gives `2` (banker's rounding)
    - string ← `5` gives `"5"`
    - bool ← `'false'` gives `false`
    - double ← `2` gives `2.0`
    - int ← `9999999999` is an error (`Value was either too large or too small for an Int32.`)
    - int ← `'text'` is an error (`Input string was not in a correct format.`)
    - Guid ← `'abc'` is an error

    This also applies to a property created by an earlier statement in the same action: `SET x = 1; SET x = 'a'` fails.
  - When `x` is new, it takes `v`'s type (an integer literal gives `long`; a parameter or property keeps its own type).
  - `SET x = NULL`, or setting `x` from a missing property, stores `x` **with a null value**.
- **`REMOVE user.x`** deletes the property. On a missing property it does nothing.
- **`REMOVE sys.X`** is accepted at creation, but always fails at run time (`A system property cannot be removed. The system property specified was '{X}'.` followed by `Parameter name: name`). This applies even to unknown names.
- **`SET sys.X`:**
  - **Allowed:** `Label`, `To`, `ReplyTo`, `CorrelationId`, `ContentType`, `ReplyToSessionId`, `SessionId`, `PartitionKey`, `TimeToLive`, `ScheduledEnqueueTimeUtc`, `MessageId`. The value is converted to the property type: `TimeToLive = '02:00:00'` works, `TimeToLive = 5` is an error (`Invalid cast from 'System.Int64' to 'System.TimeSpan'.`), and `Label = 5` gives `"5"`.
  - **Rejected at creation:** `SequenceNumber`, `DeliveryCount`, `EnqueuedTimeUtc`, `Size` and unknown names (`The specified property ({X}) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.` followed by `Parameter name: property`).
  - **Quirk:** when the sender set `To`, `ReplyTo`, `CorrelationId`, `ReplyToSessionId`, `MessageId` or `SessionId`, `SET sys.<that>` has **no visible effect on the delivered message**, although later statements read the new value. The SDK always sets a `MessageId`, so `SET sys.MessageId` never shows. `SET sys.SessionId` still changes `PartitionKey`. `Label`, `ContentType`, `TimeToLive` and `PartitionKey` are overwritten normally.
  - **`SET sys.X = NULL` never changes anything.**
- **Statement separators.** One trailing `;` is allowed. `;;` and a leading `;` are rejected.
- **Correlation filters** can carry a SQL action.

### Q9 — more than one matching rule

The subscription receives **one copy per matching rule that has an action**. Each copy gets only that rule's action and that rule's `RuleName`. On top of those, it receives **one plain copy if at least one matching rule has no action**. Two matching plain rules still give one copy. Errors are per rule (see "Evaluation errors").

### Q10 — error shapes and read-back

| Path | Case | Azure result |
|---|---|---|
| HTTP admin | Syntax error | 400. The admin client throws **`ArgumentException`**. The detail reads `There was an error parsing the SQL expression. [Token line={l}, column={c}, Token in error={token}, Additional details={details}]`. The line is 1-based and the **column is 0-based**. End of input is reported as `line=2, column=0`. Details look like `syntax error, unexpected EOF, expecting ')', or AND, or OR`, `syntax error, unexpected IDENTIFIER`, `The string (red)) was not terminated properly.`, `Unrecognized character. '#'` or `The specified scope (a) is not supported.`. |
| HTTP admin | Semantic error | 400 `ArgumentException`, with the messages quoted above (gettable/settable property, parameters, function). |
| HTTP admin | Duplicate rule | 409. `ServiceBusException(MessagingEntityAlreadyExists)` with `The messaging entity '{ns}:Topic:{topic}\|{sub}\|{rule}' already exists.`. |
| HTTP admin | Missing rule | 404 `MessagingEntityNotFound`. |
| HTTP admin | Subscription with an invalid default rule | 400 `ArgumentException`, and **the subscription is not created**. |
| AMQP rule manager | Invalid filter, invalid action or unbound parameter | **`ServiceBusException` with reason `GeneralError`**: `The service was unable to process the request; please retry the operation.` The SDK treats it as transient and retries. |
| AMQP rule manager | Duplicate rule | `MessagingEntityAlreadyExists`. |
| AMQP rule manager | Deleting a missing rule | `MessagingEntityNotFound`. |
| Read-back | `TrueRuleFilter` created over HTTP | Reads back as `TrueRuleFilter`. |
| Read-back | `TrueRuleFilter` / `FalseRuleFilter` created over AMQP | Read back as `SqlRuleFilter('1=1')` / `SqlRuleFilter('1=0')`. |
| Read-back | `SqlRuleFilter("1=1")` | Stays a `SqlRuleFilter`. |
| Read-back | `$Default` | `TrueRuleFilter`. |
| Read-back | Expression text | Preserved verbatim. |

**Q11 — empty expressions.** The SDK rejects null or empty expressions on the client. A whitespace-only filter is a syntax error at the service. Whitespace-only actions are also rejected by the SDK on the client.

### Consequences for the build

- The value model needs a distinct **Missing** sentinel next to C# `null`. AND/OR/NOT are three-valued over {true, false, unknown}.
- Binary operators need a **C#-style type table** (implicit numeric conversions, the Guid/string special case, string concatenation). They must produce Azure's exact error messages, and evaluate lazily left to right.
- **Two expression grammars.** Predicates must be comparison-style: there are no bare values, and `IS NULL` applies only to properties. Action expressions are scalar only.
- **Rejection at creation is broader than syntax.** It also covers unknown or unsettable system properties, `sys.Subject`, parameter binding (unbound and unused), unknown functions, function arity, integer literal overflow and non-string literal `LIKE` patterns.
- **Delivery** needs per-rule copies, `RuleName`, the type-converting `SET`, per-rule error handling with dead-letter reasons, and the AMQP-preset quirk for `SET sys.*`.
- **Error replies:**
  - HTTP gives 400 with Azure's parse-error text (line, 0-based column, token, details including "expecting …"). This satisfies George's "position and expected token" requirement in Azure's own format.
  - AMQP `add-rule` with an invalid expression replies with a status and condition that the SDK maps to `GeneralError`. A non-`com.microsoft` condition does that, e.g. `amqp:internal-error` with status 500.
