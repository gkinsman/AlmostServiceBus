| id | outcome | detail |
|---|---|---|
| z.null.missing-eq-null | NoMatch |  |
| z.null.missing-neq-null | NoMatch |  |
| z.null.prop-neq-null | Match | $MessageId=m-z.null.prop-neq-null, color=red (String) |
| z.null.not-prop-eq-null | Match | $MessageId=m-z.null.not-prop-eq-null, color=red (String) |
| z.null.null-neq-null | NoMatch |  |
| z.null.literal-eq-null | NoMatch |  |
| z.null.missing-eq-missing | NoMatch |  |
| z.null.nullprop-eq-null | Match | $MessageId=m-z.null.nullprop-eq-null, p=<null> |
| z.null.nullprop-eq-x | NoMatch |  |
| z.null.nullprop-neq-x | Match | $MessageId=m-z.null.nullprop-neq-x, p=<null> |
| z.null.nullprop-in | NoMatch |  |
| z.null.nullprop-notin | Match | $MessageId=m-z.null.nullprop-notin, p=<null> |
| z.null.nullprop-like | Match | $MessageId=m-z.null.nullprop-like, p=<null> |
| z.null.in-with-null-match | Match | $MessageId=m-z.null.in-with-null-match, color=red (String) |
| z.null.notin-null-only | Match | $MessageId=m-z.null.notin-null-only, color=red (String) |
| z.null.missing-notin-null | NoMatch |  |
| z.null.missing-plus-1-eq | NoMatch |  |
| z.null.missing-lt-or-true | Match | $MessageId=m-z.null.missing-lt-or-true |
| z.null.not-missing-lt | NoMatch |  |
| z.null.missing-gt-and-true | NoMatch |  |
| z.null.like-missing-pattern | NoMatch |  |
| z.err.or-true-first | Match | $MessageId=m-z.err.or-true-first, color=red (String) |
| z.err.or-error-first | DeadLettered | $MessageId=m-z.err.or-error-first, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| z.err.and-false-first | NoMatch |  |
| z.err.and-error-first | DeadLettered | $MessageId=m-z.err.and-error-first, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| z.err.in-match-before-error | Match | $MessageId=m-z.err.in-match-before-error, color=red (String) |
| z.err.in-error-before-match | DeadLettered | $MessageId=m-z.err.in-error-before-match, color=red (String), DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'string' and 'long' |
| z.err.not-error | DeadLettered | $MessageId=m-z.err.not-error, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| z.err.missing-gt-string | NoMatch |  |
| z.err.missing-eq-int-vs-string-literal | NoMatch |  |
| z.err.no-dlq-flag | NoMatch |  |
| z.err.action-no-dlq-flag | NoMatch |  |
| z.err.literal-overflow | DeadLettered | $MessageId=m-z.err.literal-overflow, DeadLetterErrorDescription=Arithmetic operation resulted in an overflow. (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Arithmetic operation resulted in an overflow. |
| z.err.literal-div0 | DeadLettered | $MessageId=m-z.err.literal-div0, DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Attempted to divide by zero. |
| z.err.literal-type-mismatch | DeadLettered | $MessageId=m-z.err.literal-type-mismatch, DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| z.num.int-plus-double | Match | $MessageId=m-z.num.int-plus-double, n=7 (Int32) |
| z.num.int-times-int-overflow | Match | $MessageId=m-z.num.int-times-int-overflow, n=2 (Int32) |
| z.num.double-literal-int-prop | Match | $MessageId=m-z.num.double-literal-int-prop, n=7 (Int32) |
| z.num.float-vs-double | NoMatch |  |
| z.num.int-lt-double | Match | $MessageId=m-z.num.int-lt-double, n=7 (Int32) |
| z.num.uint-vs-long | Match | $MessageId=m-z.num.uint-vs-long, n=7 (UInt32) |
| z.num.exp-literal-type | Match | $MessageId=m-z.num.exp-literal-type, n=10 (Int64) |
| z.num.string-plus-int | Match | $MessageId=m-z.num.string-plus-int, color=red (String) |
| z.num.unary-plus | Match | $MessageId=m-z.num.unary-plus, n=5 (Int32) |
| z.num.neg-mod | Match | $MessageId=m-z.num.neg-mod, n=-7 (Int32) |
| z.num.int-div-negative | Match | $MessageId=m-z.num.int-div-negative, n=-7 (Int32) |
| z.num.guid-neq-string | NoMatch |  |
| z.num.guid-eq-bad-string | DeadLettered | $MessageId=m-z.num.guid-eq-bad-string, DeadLetterErrorDescription=Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid), $reason=FilterException, $description=Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). |
| z.num.guid-gt-string | DeadLettered | $MessageId=m-z.num.guid-gt-string, DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'System.Guid' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'System.Guid' and 'string' |
| z.num.datetime-param-lt | Match | $MessageId=m-z.num.datetime-param-lt |
| z.num.bool-lt | DeadLettered | $MessageId=m-z.num.bool-lt, b=False (Boolean), DeadLetterErrorDescription=Operator '<' cannot be applied to operands of type 'bool' and 'bool' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '<' cannot be applied to operands of type 'bool' and 'bool' |
| z.num.bool-eq-false | Match | $MessageId=m-z.num.bool-eq-false, b=False (Boolean) |
| z.num.bool-neq | Match | $MessageId=m-z.num.bool-neq, b=False (Boolean) |
| z.misc.exists-sys | Match | $MessageId=m-z.misc.exists-sys, $Subject=L |
| z.misc.exists-sys-unset | Match | $MessageId=m-z.misc.exists-sys-unset |
| z.misc.newid-upper | NoMatch |  |
| z.misc.property-upper | Match | $MessageId=m-z.misc.property-upper, color=red (String) |
| z.misc.property-missing | NoMatch |  |
| z.misc.property-name-case | Match | $MessageId=m-z.misc.property-name-case, color=red (String) |
| z.misc.like-escape-pct-literal | Match | $MessageId=m-z.misc.like-escape-pct-literal, code=50% (String) |
| z.misc.like-escape-underscore | Match | $MessageId=m-z.misc.like-escape-underscore, code=a_b (String) |
| z.misc.like-escape-underscore-nomatch | NoMatch |  |
| z.misc.like-escape-trailing | Match | $MessageId=m-z.misc.like-escape-trailing, code=a (String) |
| z.misc.like-multichar-escape-semantics | Match | $MessageId=m-z.misc.like-multichar-escape-semantics, code=50% (String) |
| z.misc.like-empty-escape | Match | $MessageId=m-z.misc.like-empty-escape, code=50x (String) |
| z.misc.like-double | Match | $MessageId=m-z.misc.like-double, d=1.5 (Double) |
| z.misc.like-bool | Match | $MessageId=m-z.misc.like-bool, b=True (Boolean) |
| z.misc.like-guid | Match | $MessageId=m-z.misc.like-guid, g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid) |
| z.misc.like-prop-escape | Match | $MessageId=m-z.misc.like-prop-escape, code=50% (String), esc=! (String) |
| z.misc.empty-bracket-name | Match | =x (String), $MessageId=m-z.misc.empty-bracket-name |
| z.misc.sys-unprefixed-as-user | Match | $MessageId=m-z.misc.sys-unprefixed-as-user, sys=1 (Int32) |
| z.misc.user-unprefixed-as-user | Match | $MessageId=m-z.misc.user-unprefixed-as-user, user=1 (Int32) |
| z.misc.keyword-name-quoted | Match | $MessageId=m-z.misc.keyword-name-quoted, AND=1 (Int32) |
| z.misc.not-not | Match | $MessageId=m-z.misc.not-not, color=red (String) |
| z.misc.nested-parens | Match | $MessageId=m-z.misc.nested-parens, color=red (String) |
| z.misc.ne-bang | Match | $MessageId=m-z.misc.ne-bang, color=red (String) |
| z.misc.tab-newline-ws | Match | $MessageId=m-z.misc.tab-newline-ws, color=red (String) |
| z.misc.comment | NoMatch |  |
| z.misc.block-comment | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=49, Token in error= *, Additional details= syntax error, unexpected '*'] |
| z.misc.long-identifier | Match | $MessageId=m-z.misc.long-identifier, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=x (String) |
| z.misc.unicode-identifier | Match | $MessageId=m-z.misc.unicode-identifier, café=x (String) |
| z.misc.identifier-digit-start | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=52, Token in error= abc, Additional details= syntax error, unexpected IDENTIFIER] |
| z.misc.identifier-dollar | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=47, Token in error= $, Additional details= Unrecognized character. '$'] |
| z.misc.string-with-newline | Match | $MessageId=m-z.misc.string-with-newline, s=a b (String) |
| z.misc.hex-literal | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=45, Token in error= x10, Additional details= syntax error, unexpected IDENTIFIER] |
| z.misc.big-int-literal | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=48, Token in error= 99999999999999999999, Additional details= Value was either too large or too small for an Int64.] |
| z.misc.exp-no-digits | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=47, Token in error= e, Additional details= syntax error, unexpected IDENTIFIER] |
| z.misc.decimal-trailing-dot | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=54, Token in error= ., Additional details= syntax error, unexpected '.'] |
| z.act.set-sys-to-unset | Match | $MessageId=m-z.act.set-sys-to-unset, $To=v, color=red (String), RuleName=r0 (String) |
| z.act.set-sys-messageid | Match | $MessageId=m-z.act.set-sys-messageid, color=red (String), RuleName=r0 (String) |
| z.act.set-sys-label-int | Match | $MessageId=m-z.act.set-sys-label-int, $Subject=5, color=red (String), RuleName=r0 (String) |
| z.act.set-sys-ttl-int | DeadLettered | $MessageId=m-z.act.set-sys-ttl-int, color=red (String), DeadLetterErrorDescription=Invalid cast from 'System.Int64' to 'System.TimeSpan'. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=Invalid cast from 'System.Int64' to 'System.TimeSpan'. |
| z.act.set-sys-replyto-null | Match | $MessageId=m-z.act.set-sys-replyto-null, $ReplyTo=r, color=red (String), RuleName=r0 (String) |
| z.act.set-user-int-to-double | Match | $MessageId=m-z.act.set-user-int-to-double, n=2 (Int32), RuleName=r0 (String) |
| z.act.set-user-int-to-long | DeadLettered | $MessageId=m-z.act.set-user-int-to-long, DeadLetterErrorDescription=Value was either too large or too small for an Int32. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), n=1 (Int32), RuleName=r0 (String), $reason=RuleActionException, $description=Value was either too large or too small for an Int32. |
| z.act.set-user-string-to-int | Match | $MessageId=m-z.act.set-user-string-to-int, RuleName=r0 (String), s=5 (String) |
| z.act.set-user-bool-to-string | Match | $MessageId=m-z.act.set-user-bool-to-string, b=False (Boolean), RuleName=r0 (String) |
| z.act.set-user-guid | DeadLettered | $MessageId=m-z.act.set-user-guid, DeadLetterErrorDescription=Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid), RuleName=r0 (String), $reason=RuleActionException, $description=Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). |
| z.act.set-user-double-from-int | Match | $MessageId=m-z.act.set-user-double-from-int, d=2 (Double), RuleName=r0 (String) |
| z.act.set-new-bool | Match | $MessageId=m-z.act.set-new-bool, color=red (String), RuleName=r0 (String), x=True (Boolean) |
| z.act.set-new-from-param-int | Match | $MessageId=m-z.act.set-new-from-param-int, color=red (String), RuleName=r0 (String), x=5 (Int32) |
| z.act.set-type-from-prop | Match | $MessageId=m-z.act.set-type-from-prop, n=7 (Int32), RuleName=r0 (String), x=7 (Int32) |
| z.act.set-string-concat | Match | $MessageId=m-z.act.set-string-concat, color=red (String), RuleName=r0 (String), x=red! (String) |
| z.act.set-unscoped-ref | Match | $MessageId=m-z.act.set-unscoped-ref, color=red (String), RuleName=r0 (String), x=red (String) |
| z.act.set-sys-ref-unprefixed | Match | $MessageId=m-z.act.set-sys-ref-unprefixed, $Subject=L, RuleName=r0 (String), x=<null> |
| z.act.remove-then-set | Match | $MessageId=m-z.act.remove-then-set, RuleName=r0 (String), x=<null> |
| z.act.set-then-remove | Match | $MessageId=m-z.act.set-then-remove, color=red (String), RuleName=r0 (String) |
| z.act.set-twice | DeadLettered | $MessageId=m-z.act.set-twice, color=red (String), DeadLetterErrorDescription=Input string was not in a correct format. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), x=1 (Int64), $reason=RuleActionException, $description=Input string was not in a correct format. |
| z.act.set-rulename | Match | $MessageId=m-z.act.set-rulename, color=red (String), RuleName=mine (String) |
| z.act.newid-type-into-string | Match | $MessageId=m-z.act.newid-type-into-string, color=b74e26ee-89fb-4476-8f93-f74ba8c878e6 (String), RuleName=r0 (String) |
| z.act.property-fn | Match | $MessageId=m-z.act.property-fn, color=red (String), RuleName=r0 (String), x=red (String) |
| z.act.empty-trailing-semis | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=15, Token in error= ;, Additional details= syntax error, unexpected ';', expecting SET, or REMOVE, or EOF] |
| z.act.multiple-semis-leading | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=0, Token in error= ;, Additional details= syntax error, unexpected ';', expecting SET, or REMOVE] |
| z.act.remove-sys-unknown | DeadLettered | $MessageId=m-z.act.remove-sys-unknown, color=red (String), DeadLetterErrorDescription=A system property cannot be removed. The system property specified was 'NoSuch'. Parameter name: name (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=A system property cannot be removed. The system property specified was 'NoSuch'. Parameter name: name |
| z.act.set-bracketed-target | Match | $MessageId=m-z.act.set-bracketed-target, a b=1 (Int64), color=red (String), RuleName=r0 (String) |
| z.act.like-in-action | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=19, Token in error= LIKE, Additional details= syntax error, unexpected LIKE] |
| z.act.set-sys-label-from-user | Match | $MessageId=m-z.act.set-sys-label-from-user, $Subject=red, color=red (String), RuleName=r0 (String) |
| z.act.error-then-statements | DeadLettered | $MessageId=m-z.act.error-then-statements, a=1 (Int64), color=red (String), DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=Attempted to divide by zero. |
| z.multi.three-actions-one-plain | Match | copies=3 / $MessageId=m-z.multi.three-actions-one-plain, color=red (String), r=1 (String), RuleName=r0 (String) / $MessageId=m-z.multi.three-actions-one-plain, color=red (String), r=2 (String), RuleName=r1 (String) / $MessageId=m-z.multi.three-actions-one-plain, color=red (String) |
| z.multi.error-rule-plus-plain | DeadLettered | $MessageId=m-z.multi.error-rule-plus-plain, color=red (String) / $MessageId=m-z.multi.error-rule-plus-plain, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| z.multi.action-error-plus-plain | DeadLettered | $MessageId=m-z.multi.action-error-plus-plain, color=red (String) / $MessageId=m-z.multi.action-error-plus-plain, color=red (String), DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=Attempted to divide by zero. |
| z.multi.action-error-plus-action | DeadLettered | $MessageId=m-z.multi.action-error-plus-action, color=red (String), RuleName=r1 (String), y=1 (Int64) / $MessageId=m-z.multi.action-error-plus-action, color=red (String), DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=Attempted to divide by zero. |
| z.multi.true-filter-and-action | Match | copies=2 / $MessageId=m-z.multi.true-filter-and-action, color=red (String) / $MessageId=m-z.multi.true-filter-and-action, color=red (String), RuleName=r1 (String), y=1 (Int64) |

## extra
