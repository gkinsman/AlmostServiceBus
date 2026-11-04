| id | outcome | detail |
|---|---|---|
| q1.label-sys-only | NoMatch |  |
| q1.label-user-only | Match | $MessageId=m-q1.label-user-only, $Subject=other, label=L (String) |
| q1.to-sys | NoMatch |  |
| q1.correlationid-sys | NoMatch |  |
| q1.messageid-sys | NoMatch |  |
| q1.contenttype-sys | NoMatch |  |
| q1.user-control | Match | $MessageId=m-q1.user-control, color=red (String) |
| q1.exists-unprefixed-sys | NoMatch |  |
| q2.sys-lower | Match | $MessageId=m-q2.sys-lower, $Subject=L |
| q2.sys-upper | Match | $MessageId=m-q2.sys-upper, $Subject=L |
| q2.sys-scope-upper | Match | $MessageId=m-q2.sys-scope-upper, $Subject=L |
| q2.user-name-case | Match | $MessageId=m-q2.user-name-case, color=red (String) |
| q2.unprefixed-name-case | Match | $MessageId=m-q2.unprefixed-name-case, color=red (String) |
| q2.keywords-lower | Match | $MessageId=m-q2.keywords-lower, color=red (String) |
| q3.sys-messageid-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-messageid-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-correlationid-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-correlationid-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-to-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-to-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-replyto-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-replyto-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-label-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-label-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-subject-notnull | Rejected | ArgumentException: The specified property Subject does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: qualifiedPropertyName |
| q3.sys-sessionid-notnull | NoMatch |  |
| q3.sys-replytosessionid-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-replytosessionid-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-contenttype-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-contenttype-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-partitionkey-notnull | NoMatch |  |
| q3.sys-sequencenumber-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-sequencenumber-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-enqueuedsequencenumber-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-enqueuedsequencenumber-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-deliverycount-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-deliverycount-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-enqueuedtimeutc-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-enqueuedtimeutc-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-scheduledenqueuetimeutc-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-scheduledenqueuetimeutc-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-expiresatutc-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-expiresatutc-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-timetolive-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-timetolive-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-size-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-size-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-locktoken-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-locktoken-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-lockeduntilutc-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-lockeduntilutc-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-deadlettersource-notnull | NoMatch |  |
| q3.sys-state-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-state-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-viapartitionkey-notnull | NoMatch |  |
| q3.sys-forcepersistence-notnull | Match | $ContentType=c, $CorrelationId=c, $MessageId=m-q3.sys-forcepersistence-notnull, $ReplyTo=r, $ReplyToSessionId=rs, $Subject=l, $TimeToLive=01:00:00, $To=t |
| q3.sys-nosuchproperty-notnull | Rejected | ArgumentException: The specified property NoSuchProperty does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: qualifiedPropertyName |
| q3.sys-sessionid-value | Match | $MessageId=m-q3.sys-sessionid-value, $PartitionKey=s1, $SessionId=s1 |
| q3.sys-deliverycount-0 | NoMatch |  |
| q3.sys-deliverycount-1 | Match | $MessageId=m-q3.sys-deliverycount-1 |
| q3.sys-sequencenumber-gt0 | Match | $MessageId=m-q3.sys-sequencenumber-gt0 |
| q3.sys-size-gt0 | Match | $MessageId=m-q3.sys-size-gt0 |
| q3.sys-ttl-eq-string | DeadLettered | $MessageId=m-q3.sys-ttl-eq-string, $TimeToLive=01:00:00, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'System.TimeSpan' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'System.TimeSpan' and 'string' |
| q3.sys-enqueued-gt-string | DeadLettered | $MessageId=m-q3.sys-enqueued-gt-string, DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'System.DateTime' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'System.DateTime' and 'string' |
| q3.sys-unknown-isnull | Rejected | ArgumentException: The specified property NoSuchProperty does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: qualifiedPropertyName |
| q4.eq-case | NoMatch |  |
| q4.like-case | NoMatch |  |
| q4.like-case-pct | NoMatch |  |
| q4.lt-strings | DeadLettered | $MessageId=m-q4.lt-strings, color=red (String), DeadLetterErrorDescription=Operator '<' cannot be applied to operands of type 'string' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '<' cannot be applied to operands of type 'string' and 'string' |
| q4.gt-strings | DeadLettered | $MessageId=m-q4.gt-strings, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'string' |
| q4.trailing-space-literal | NoMatch |  |
| q4.trailing-space-value | NoMatch |  |
| q4.unicode | Match | $MessageId=m-q4.unicode, name=café (String) |
| q4.empty-string | Match | $MessageId=m-q4.empty-string, color= (String) |
| q5.string-gt-int | DeadLettered | $MessageId=m-q5.string-gt-int, color=red (String), DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'string' and 'long' |
| q5.string-eq-int | DeadLettered | $MessageId=m-q5.string-eq-int, color=red (String), DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'string' and 'long' |
| q5.numstr-eq-int | DeadLettered | $MessageId=m-q5.numstr-eq-int, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=27 (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'string' and 'long' |
| q5.int-eq-numstr | DeadLettered | $MessageId=m-q5.int-eq-numstr, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'int' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=27 (Int32), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'int' and 'string' |
| q5.bool-eq-int | DeadLettered | $MessageId=m-q5.bool-eq-int, b=True (Boolean), DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'bool' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'bool' and 'long' |
| q5.bool-eq-true | Match | $MessageId=m-q5.bool-eq-true, b=True (Boolean) |
| q5.bool-eq-string | DeadLettered | $MessageId=m-q5.bool-eq-string, b=True (Boolean), DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'bool' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'bool' and 'string' |
| q5.bool-bare-true | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.bool-bare-false | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.bool-not-false | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=44, Token in error= ), Additional details= syntax error, unexpected ')'] |
| q5.string-bare | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.int-bare | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.int-eq-double-literal | Match | $MessageId=m-q5.int-eq-double-literal, n=27 (Int32) |
| q5.long-eq-int-literal | Match | $MessageId=m-q5.long-eq-int-literal, n=27 (Int64) |
| q5.double | Match | $MessageId=m-q5.double, d=1.5 (Double) |
| q5.double-gt-int | Match | $MessageId=m-q5.double-gt-int, d=1.5 (Double) |
| q5.float | Match | $MessageId=m-q5.float, f=1.5 (Single) |
| q5.decimal | DeadLettered | $MessageId=m-q5.decimal, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'decimal' and 'double' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), m=1.5 (Decimal), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'decimal' and 'double' |
| q5.char | DeadLettered | $MessageId=m-q5.char, c=x (Char), DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'char' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'char' and 'string' |
| q5.byte | Match | $MessageId=m-q5.byte, n=7 (Byte) |
| q5.short | Match | $MessageId=m-q5.short, n=7 (Int16) |
| q5.uint | Match | $MessageId=m-q5.uint, n=7 (UInt32) |
| q5.ulong | DeadLettered | $MessageId=m-q5.ulong, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'ulong' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=7 (UInt64), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'ulong' and 'long' |
| q5.sbyte | Match | $MessageId=m-q5.sbyte, n=7 (SByte) |
| q5.guid-eq-string | Match | $MessageId=m-q5.guid-eq-string, g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid) |
| q5.guid-notnull | Match | $MessageId=m-q5.guid-notnull, g=6f9619ff-8b86-d011-b42d-00c04fc964ff (Guid) |
| q5.datetime-eq-string | DeadLettered | $MessageId=m-q5.datetime-eq-string, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'System.DateTime' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), dt=2/01/2024 3:04:05 AM (DateTime), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'System.DateTime' and 'string' |
| q5.datetime-gt-string | DeadLettered | $MessageId=m-q5.datetime-gt-string, DeadLetterErrorDescription=Operator '>' cannot be applied to operands of type 'System.DateTime' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), dt=2/01/2024 3:04:05 AM (DateTime), $reason=FilterException, $description=Operator '>' cannot be applied to operands of type 'System.DateTime' and 'string' |
| q5.datetimeoffset-eq-string | DeadLettered | $MessageId=m-q5.datetimeoffset-eq-string, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'System.DateTimeOffset' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), dt=2/01/2024 3:04:05 AM +00:00 (DateTimeOffset), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'System.DateTimeOffset' and 'string' |
| q5.timespan-eq-string | DeadLettered | $MessageId=m-q5.timespan-eq-string, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'System.TimeSpan' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), ts=01:00:00 (TimeSpan), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'System.TimeSpan' and 'string' |
| q5.timespan-notnull | Match | $MessageId=m-q5.timespan-notnull, ts=01:00:00 (TimeSpan) |
| q5.bytes-eq-string | NoMatch | SEND Serialization operation failed due to unsupported type System.Byte[] for key 'bytes'. TrackingId:11246d3c0000001e0059dafb6aeb09a8_G6_B1, SystemTracker:gi::G6:54858632:amqps://<ns>/-cff1eb87;0:5:6, bi::link5888763(54858633_G6), Timestamp:2026-11-04T10:32:11 (MessageSizeExceeded). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot. |
| q5.bytes-notnull | NoMatch | SEND Serialization operation failed due to unsupported type System.Byte[] for key 'bytes'. TrackingId:11246d3c0000001e0059dafb6aeb09a8_G6_B1, SystemTracker:gi::G6:54858632:amqps://<ns>/-cff1eb87;0:5:6, bi::link5888763(54858633_G6), Timestamp:2026-11-04T10:32:11 (MessageSizeExceeded). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot. |
| q5.uri-eq-string | DeadLettered | $MessageId=m-q5.uri-eq-string, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'System.Uri' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), u=http://a/ (Uri), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'System.Uri' and 'string' |
| q5.literal-str-eq-int | DeadLettered | $MessageId=m-q5.literal-str-eq-int, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'string' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'string' and 'long' |
| q5.literal-int-eq-double | Match | $MessageId=m-q5.literal-int-eq-double |
| q5.null-literal-bare | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.true-literal | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF] |
| q5.false-literal-not | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=51, Token in error= ), Additional details= syntax error, unexpected ')'] |
| q5.null-eq-null | Match | $MessageId=m-q5.null-eq-null |
| q5.prop-eq-null | NoMatch |  |
| q5.null-valued-prop-isnull | Match | $MessageId=m-q5.null-valued-prop-isnull, p=<null> |
| q5.null-valued-prop-exists | Match | $MessageId=m-q5.null-valued-prop-exists, p=<null> |
| q6.div0-int | DeadLettered | $MessageId=m-q6.div0-int, DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=1 (Int32), $reason=FilterException, $description=Attempted to divide by zero. |
| q6.div0-double | NoMatch |  |
| q6.mod0 | DeadLettered | $MessageId=m-q6.mod0, DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=1 (Int32), $reason=FilterException, $description=Attempted to divide by zero. |
| q6.overflow | DeadLettered | $MessageId=m-q6.overflow, DeadLetterErrorDescription=Arithmetic operation resulted in an overflow. (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=2 (Int64), $reason=FilterException, $description=Arithmetic operation resulted in an overflow. |
| q6.int-division-3 | Match | $MessageId=m-q6.int-division-3, n=7 (Int32) |
| q6.int-division-3.5 | NoMatch |  |
| q6.mod | Match | $MessageId=m-q6.mod, n=7 (Int32) |
| q6.mod-double | Match | $MessageId=m-q6.mod-double, d=3.5 (Double) |
| q6.string-concat | Match | $MessageId=m-q6.string-concat, color=red (String) |
| q6.unary-minus | Match | $MessageId=m-q6.unary-minus, n=5 (Int32) |
| q6.precedence | Match | $MessageId=m-q6.precedence |
| q6.int-plus-missing | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=53, Token in error= IS, Additional details= syntax error, unexpected IS] |
| tvl.missing-eq | NoMatch |  |
| tvl.missing-neq | NoMatch |  |
| tvl.missing-bangeq | NoMatch |  |
| tvl.not-missing-eq | NoMatch |  |
| tvl.missing-or-true | Match | $MessageId=m-tvl.missing-or-true |
| tvl.not-missing-and-false | Match | $MessageId=m-tvl.not-missing-and-false |
| tvl.missing-in | NoMatch |  |
| tvl.missing-notin | NoMatch |  |
| tvl.missing-notlike | NoMatch |  |
| tvl.missing-isnull | Match | $MessageId=m-tvl.missing-isnull |
| tvl.missing-notexists | Match | $MessageId=m-tvl.missing-notexists |
| tvl.missing-lt | NoMatch |  |
| in.match | Match | $MessageId=m-in.match, color=red (String) |
| in.nomatch | NoMatch |  |
| notin.match | Match | $MessageId=m-notin.match, color=red (String) |
| notin.nomatch | NoMatch |  |
| in.numbers | Match | $MessageId=m-in.numbers, n=2 (Int32) |
| in.mixed-types | DeadLettered | $MessageId=m-in.mixed-types, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'int' and 'string' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), n=2 (Int32), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'int' and 'string' |
| in.with-null-nomatch | NoMatch |  |
| notin.with-null | Match | $MessageId=m-notin.with-null, color=red (String) |
| in.expression-items | Match | $MessageId=m-in.expression-items, n=2 (Int32) |
| in.property-items | Match | $MessageId=m-in.property-items, m=2 (Int32), n=2 (Int32) |
| in.empty | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=40, Token in error= ), Additional details= syntax error, unexpected ')'] |
| in.keyword-case | Match | $MessageId=m-in.keyword-case, color=red (String) |
| i192.eq-control-match | Match | $MessageId=m-i192.eq-control-match, message-type=Created (String), messageType=Created (String) |
| i192.eq-control-nomatch | NoMatch |  |
| i192.in-match | Match | $MessageId=m-i192.in-match, message-type=Created (String), messageType=Created (String) |
| i192.in-nomatch | NoMatch |  |
| i192.quoted-match | Match | $MessageId=m-i192.quoted-match, message-type=Created (String), messageType=Created (String) |
| i192.quoted-nomatch | NoMatch |  |
| i192.bracketed-match | Match | $MessageId=m-i192.bracketed-match, message-type=Created (String), messageType=Created (String) |
| i192.bracketed-nomatch | NoMatch |  |
| i192.quoted-in-match | Match | $MessageId=m-i192.quoted-in-match, message-type=Created (String), messageType=Created (String) |
| i192.quoted-in-nomatch | NoMatch |  |
| delim.user-bracketed | Match | $MessageId=m-delim.user-bracketed, message-type=Created (String), messageType=Created (String) |
| delim.user-quoted | Match | $MessageId=m-delim.user-quoted, message-type=Created (String), messageType=Created (String) |
| delim.sys-bracketed | Match | $MessageId=m-delim.sys-bracketed, $Subject=L |
| delim.quoted-escape | Match | $MessageId=m-delim.quoted-escape, a"b=x (String) |
| delim.bracket-escape | Match | $MessageId=m-delim.bracket-escape, a]b=x (String) |
| delim.reserved-word | Match | $MessageId=m-delim.reserved-word, in=x (String) |
| delim.space | Match | $MessageId=m-delim.space, my prop=x (String) |
| delim.dotted-bracketed | Match | $MessageId=m-delim.dotted-bracketed, a.b=x (String) |
| delim.dotted-regular | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=44, Token in error= b, Additional details= The specified scope (a) is not supported.] |
| delim.dotted-user | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=45, Token in error= ., Additional details= syntax error, unexpected '.'] |
| delim.empty-brackets | NoMatch |  |
| fw.nsb | Match | $MessageId=m-fw.nsb, NServiceBus.EnclosedMessageTypes=Ns.MyEvent, Asm, Version=1.0.0.0;Ns.IEvent, Asm (String) |
| fw.nsb-nomatch | NoMatch |  |
| fw.wolverine-missing | Match | $MessageId=m-fw.wolverine-missing |
| fw.wolverine-false | Match | $MessageId=m-fw.wolverine-false, ignore=false (String) |
| fw.wolverine-true | NoMatch |  |
| like.pct | Match | $MessageId=m-like.pct, color=red (String) |
| like.underscore | Match | $MessageId=m-like.underscore, color=red (String) |
| like.notlike | Match | $MessageId=m-like.notlike, color=red (String) |
| like.escape | Match | $MessageId=m-like.escape, code=50% (String) |
| like.escape-nomatch | NoMatch |  |
| like.regex-dot | NoMatch |  |
| like.char-class | NoMatch |  |
| like.on-int | Match | $MessageId=m-like.on-int, n=27 (Int32) |
| like.multiline | NoMatch |  |
| like.escape-multichar | Match | $MessageId=m-like.escape-multichar, code=50% (String) |
| like.pattern-property | Match | $MessageId=m-like.pattern-property, color=red (String), pat=r% (String) |
| lit.string-escape | Match | $MessageId=m-lit.string-escape, name=O'Brien (String) |
| lit.exponent | Match | $MessageId=m-lit.exponent, n=10 (Int32) |
| lit.leading-dot | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=37, Token in error= ., Additional details= syntax error, unexpected '.'] |
| lit.negative | Match | $MessageId=m-lit.negative, n=-1 (Int32) |
| lit.N-string | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=47, Token in error= ', Additional details= syntax error, unexpected STRING] |
| lit.double-quoted-string-as-name | Match | $MessageId=m-lit.double-quoted-string-as-name, color=red (String), red=red (String) |
| fn.newid-notnull | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=46, Token in error= IS, Additional details= syntax error, unexpected IS] |
| fn.newid-eq | NoMatch |  |
| fn.property | Match | $MessageId=m-fn.property, color=red (String) |
| fn.property-dynamic | Match | $MessageId=m-fn.property-dynamic, color=red (String), which=color (String) |
| fn.property-sys | NoMatch |  |
| fn.property-unprefixed-sys | NoMatch |  |
| fn.unknown | Rejected | ArgumentException: The specified function name (upper) is unknown. Make sure the function name is correct. |
| fn.newid-arg | Rejected | ArgumentException: The 'newid' function expects 0 argument(s), but 1 argument(s) was/were specified. Parameter name: arguments |
| p.string | Match | $MessageId=m-p.string, color=red (String) |
| p.long | Match | $MessageId=m-p.long, n=5 (Int32) |
| p.int | Match | $MessageId=m-p.int, n=5 (Int64) |
| p.double | Match | $MessageId=m-p.double, d=1.5 (Double) |
| p.bool | Match | $MessageId=m-p.bool, b=True (Boolean) |
| p.datetime | Match | $MessageId=m-p.datetime, dt=2/01/2024 3:04:05 AM (DateTime) |
| p.timespan | Match | $MessageId=m-p.timespan, ts=01:00:00 (TimeSpan) |
| p.unbound | Rejected | ArgumentException: A parameter (@missing) used in the expression specified in SqlExpression is missing from the Parameters property. |
| p.unused | Rejected | ArgumentException: The specified parameter (@unused) in Parameters property was not used in the expression specified in SqlExpression. |
| p.case | Match | $MessageId=m-p.case, color=red (String) |
| p.no-at-key | Rejected | ArgumentException: A parameter (@p) used in the expression specified in SqlExpression is missing from the Parameters property. |
| p.in-list | Match | $MessageId=m-p.in-list, color=red (String) |
| p.like-pattern | Match | $MessageId=m-p.like-pattern, color=red (String) |
| a.set-user-literal | Match | $MessageId=m-a.set-user-literal, color=red (String), RuleName=r0 (String), tag=A (String) |
| a.set-unprefixed | Match | $MessageId=m-a.set-unprefixed, color=red (String), RuleName=r0 (String), tag=A (String) |
| a.set-unprefixed-existing-sys-name | Match | $MessageId=m-a.set-unprefixed-existing-sys-name, $Subject=orig, Label=A (String), RuleName=r0 (String) |
| a.set-user-expr | Match | $MessageId=m-a.set-user-expr, b=6 (Int64), n=3 (Int32), RuleName=r0 (String) |
| a.remove-user | Match | $MessageId=m-a.remove-user, RuleName=r0 (String) |
| a.remove-unprefixed | Match | $MessageId=m-a.remove-unprefixed, RuleName=r0 (String) |
| a.remove-missing | Match | $MessageId=m-a.remove-missing, color=red (String), RuleName=r0 (String) |
| a.set-sys-label | Match | $MessageId=m-a.set-sys-label, $Subject=v-Label, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-to | Match | $MessageId=m-a.set-sys-to, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-correlationid | Match | $CorrelationId=v-CorrelationId, $MessageId=m-a.set-sys-correlationid, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-messageid | Match | $MessageId=m-a.set-sys-messageid, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-replyto | Match | $MessageId=m-a.set-sys-replyto, $ReplyTo=v-ReplyTo, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-replytosessionid | Match | $MessageId=m-a.set-sys-replytosessionid, $ReplyToSessionId=v-ReplyToSessionId, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-contenttype | Match | $ContentType=v-ContentType, $MessageId=m-a.set-sys-contenttype, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-sessionid | Match | $MessageId=m-a.set-sys-sessionid, $PartitionKey=v-SessionId, $SessionId=v-SessionId, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-partitionkey | Match | $MessageId=m-a.set-sys-partitionkey, $PartitionKey=v-PartitionKey, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-timetolive | Match | $MessageId=m-a.set-sys-timetolive, $Subject=orig, $TimeToLive=02:00:00, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-scheduledenqueuetimeutc | Match | $MessageId=m-a.set-sys-scheduledenqueuetimeutc, $ScheduledEnqueueTime=2020-01-01T00:00:00.0000000+00:00, $Subject=orig, $To=orig, color=red (String), RuleName=r0 (String) |
| a.set-sys-sequencenumber | Rejected | ArgumentException: The specified property (SequenceNumber) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: property |
| a.set-sys-deliverycount | Rejected | ArgumentException: The specified property (DeliveryCount) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: property |
| a.set-sys-enqueuedtimeutc | Rejected | ArgumentException: The specified property (EnqueuedTimeUtc) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: property |
| a.set-sys-size | Rejected | ArgumentException: The specified property (Size) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: property |
| a.set-sys-nosuchproperty | Rejected | ArgumentException: The specified property (NoSuchProperty) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class. Parameter name: property |
| a.remove-sys-label | DeadLettered | $MessageId=m-a.remove-sys-label, $Subject=orig, color=red (String), DeadLetterErrorDescription=A system property cannot be removed. The system property specified was 'Label'. Parameter name: name (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=A system property cannot be removed. The system property specified was 'Label'. Parameter name: name |
| a.remove-sys-to | DeadLettered | $MessageId=m-a.remove-sys-to, $To=orig, color=red (String), DeadLetterErrorDescription=A system property cannot be removed. The system property specified was 'To'. Parameter name: name (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=A system property cannot be removed. The system property specified was 'To'. Parameter name: name |
| a.ordering | Match | $MessageId=m-a.ordering, a=1 (Int64), b=2 (Int64), color=red (String), RuleName=r0 (String) |
| a.ordering-overwrite | Match | $MessageId=m-a.ordering-overwrite, m=5 (Int32), n=5 (Int32), RuleName=r0 (String) |
| a.set-null | Match | $MessageId=m-a.set-null, color=<null>, RuleName=r0 (String) |
| a.set-missing-ref | Match | $MessageId=m-a.set-missing-ref, color=red (String), RuleName=r0 (String), x=<null> |
| a.result-double | Match | $MessageId=m-a.result-double, color=red (String), RuleName=r0 (String), x=1.5 (Double) |
| a.result-intdiv | Match | $MessageId=m-a.result-intdiv, color=red (String), RuleName=r0 (String), x=3 (Int64) |
| a.result-bool | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=15, Token in error= =, Additional details= syntax error, unexpected '='] |
| a.result-in | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=19, Token in error= IN, Additional details= syntax error, unexpected IN] |
| a.newid | Match | $MessageId=m-a.newid, color=red (String), id=f2b471ff-9f70-43c2-9862-3993184e57a6 (Guid), RuleName=r0 (String) |
| a.trailing-semicolon | Match | $MessageId=m-a.trailing-semicolon, a=1 (Int64), color=red (String), RuleName=r0 (String) |
| a.overwrite-type | DeadLettered | $MessageId=m-a.overwrite-type, DeadLetterErrorDescription=Input string was not in a correct format. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), n=1 (Int32), RuleName=r0 (String), $reason=RuleActionException, $description=Input string was not in a correct format. |
| a.copy-sys-to-user | Match | $MessageId=m-a.copy-sys-to-user, $Subject=L, l=L (String), RuleName=r0 (String) |
| a.invalid-no-eq | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting '='] |
| a.invalid-update | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=0, Token in error= UPDATE, Additional details= syntax error, unexpected IDENTIFIER, expecting SET, or REMOVE] |
| a.invalid-empty-statement | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=0, Token in error= ;, Additional details= syntax error, unexpected ';', expecting SET, or REMOVE] |
| a.param | Match | $MessageId=m-a.param, color=red (String), RuleName=r0 (String), x=pv (String) |
| a.param-unbound | Rejected | ArgumentException: A parameter (@nope) used in the expression specified in SqlExpression is missing from the Parameters property. |
| a.whitespace-only | Rejected | ArgumentException: Value cannot be empty or contain only white-space characters. (Parameter 'sqlExpression') |
| a.on-div0 | DeadLettered | $MessageId=m-a.on-div0, color=red (String), DeadLetterErrorDescription=Attempted to divide by zero. (String), DeadLetterReason=RuleActionException (String), DeadLetterSource=FilteringEngine (String), RuleName=r0 (String), $reason=RuleActionException, $description=Attempted to divide by zero. |
| a.with-correlation-filter | Match | $MessageId=m-a.with-correlation-filter, color=red (String), RuleName=r0 (String), tag=corr (String) |
| m.two-action-rules | Match | copies=2 / $MessageId=m-m.two-action-rules, color=red (String), r1=1 (String), RuleName=r0 (String) / $MessageId=m-m.two-action-rules, color=red (String), r2=2 (String), RuleName=r1 (String) |
| m.action-and-plain | Match | copies=2 / $MessageId=m-m.action-and-plain, color=red (String), r1=1 (String), RuleName=r0 (String) / $MessageId=m-m.action-and-plain, color=red (String) |
| m.two-plain | Match | $MessageId=m-m.two-plain, color=red (String) |
| m.action-rule-nomatch-plain-match | Match | $MessageId=m-m.action-rule-nomatch-plain-match, color=red (String) |
| e.whitespace-filter | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=42, Token in error= ), Additional details= syntax error, unexpected ')'] |
| r.trailing-token | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=52, Token in error= blue, Additional details= syntax error, unexpected IDENTIFIER] |
| r.unterminated-string | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= The string (red)) was not terminated properly.] |
| r.missing-rparen | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting ')', or AND, or OR] |
| r.triple-eq | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=40, Token in error= =, Additional details= syntax error, unexpected EQ] |
| r.double-eq | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=40, Token in error= =, Additional details= syntax error, unexpected EQ] |
| r.keyword-as-name | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=39, Token in error= in, Additional details= syntax error, unexpected IN] |
| r.sys-alone | NoMatch |  |
| r.not-without-like | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=54, Token in error= ', Additional details= syntax error, unexpected STRING, expecting LIKE, or IN] |
| r.dangling-and | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=53, Token in error= ), Additional details= syntax error, unexpected ')'] |
| r.hash | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=36, Token in error= #, Additional details= Unrecognized character. '#'] |
| r.semicolon-in-filter | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=56, Token in error= ;, Additional details= Unrecognized character. ';'] |
| r.like-nonstring-pattern | Rejected | ArgumentException: Expression of type 'System.Int64' cannot be used for parameter of type 'System.Object' of method 'System.Nullable`1[System.Boolean] LikeMethod(System.Object, System.Object, System.Object)' |
| r.is-without-null | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=52, Token in error= ', Additional details= syntax error, unexpected STRING, expecting NULL, or NOT] |
| r.exists-literal | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=47, Token in error= ', Additional details= syntax error, unexpected STRING, expecting IDENTIFIER, or DELIMITED_IDENTIFIER] |
| r.exists-expression | Rejected | ArgumentException: There was an error parsing the SQL expression. [Token line=1, column=50, Token in error= +, Additional details= syntax error, unexpected '+', expecting ')'] |

## extra
- **amqp.create-with-action-and-list**: ["$Default: TrueRuleFilter \u00271=1\u0027 action=","amqpact: SqlRuleFilter \u0027color IN (\u0027red\u0027)\u0027 action=SET user.x = 1","amqpcorr: CorrelationRuleFilter \u0027\u0027 action=","amqpfalse: SqlRuleFilter \u00271=0\u0027 action=","amqptrue: SqlRuleFilter \u00271=1\u0027 action=","dup: SqlRuleFilter \u00271=1\u0027 action=","params: SqlRuleFilter \u0027a = @a AND b = @b AND c = @c AND d = @d AND e = @e AND g = @g AND h = @h\u0027 action=","sql11: SqlRuleFilter \u00271=1\u0027 action=","sql11sp: SqlRuleFilter \u00271 = 1\u0027 action=","tf: TrueRuleFilter \u00271=1\u0027 action="]
- **amqp.delete-missing**: {"type":"ServiceBusException","message":"The messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|nope\u0027 could not be found. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:b1c4e700-c5b5-46c4-831d-a494013005b0_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:46 Reference:176961b8-e22a-4e59-b76a-1dd8d7671272, TrackingId:d2f935c4-cb59-44a2-be4b-b027c3efaf03_G6, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:46 (MessagingEntityNotFound). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"MessagingEntityNotFound"}
- **amqp.dup-rule**: {"type":"ServiceBusException","message":"The messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|dup\u0027 already exists. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:c9a1317f-a350-49f2-9277-b8e49e93ca47_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:46 Reference:4d80a376-ba21-4df3-9158-58fee171421c, TrackingId:23b3f1b6-e185-4fab-9c83-f82f5b137d7b_G6, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:46 (MessagingEntityAlreadyExists). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"MessagingEntityAlreadyExists"}
- **amqp.invalid-action**: {"type":"ServiceBusException","message":"The service was unable to process the request; please retry the operation. For more information on exception types and proper exception handling, please refer to http://go.microsoft.com/fwlink/?LinkId=761101 Reference:a8077aee-1594-41d4-bd94-fbcd5a5afe9c, TrackingId:036ba10e-8cc7-41ae-a973-7b4a55651450_G6, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:45 (GeneralError). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"GeneralError"}
- **amqp.invalid-filter**: {"type":"ServiceBusException","message":"The service was unable to process the request; please retry the operation. For more information on exception types and proper exception handling, please refer to http://go.microsoft.com/fwlink/?LinkId=761101 Reference:fb0556d0-4c78-4cb2-a462-fa60f703545b, TrackingId:0cd75276-f918-4b73-9c42-0700040ff0c9_G6, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:34 (GeneralError). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"GeneralError"}
- **amqp.unbound-param-expression**: {"type":"ServiceBusException","message":"The service was unable to process the request; please retry the operation. For more information on exception types and proper exception handling, please refer to http://go.microsoft.com/fwlink/?LinkId=761101 Reference:3368bdc9-6e57-44cd-87ad-231618b735a0, TrackingId:c8936983-ca93-4561-8017-c8dca35a7f4f_G6, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:57 (GeneralError). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"GeneralError"}
- **http.default-rule-readback**: "TrueRuleFilter \u00271=1\u0027 action="
- **http.dup-rule.first**: "ok"
- **http.dup-rule.second**: {"type":"ServiceBusException","message":"The messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|dup\u0027 already exists. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:20316046-aa06-4db2-8140-83c7e681960e_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:16\r\nStatus: 409 (Conflict)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E409\u003C/Code\u003E\u003CDetail\u003EThe messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|dup\u0027 already exists. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:20316046-aa06-4db2-8140-83c7e681960e_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:16\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:16 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n (MessagingEntityAlreadyExists). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"MessagingEntityAlreadyExists","inner":"RequestFailedException: The messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|dup\u0027 already exists. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:20316046-aa06-4db2-8140-83c7e681960e_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:16\r\nStatus: 409 (Conflict)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E409\u003C/Code\u003E\u003CDetail\u003EThe messaging entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|dup\u0027 already exists. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:20316046-aa06-4db2-8140-83c7e681960e_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:16\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:16 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n"}
- **http.get-missing-rule**: {"type":"ServiceBusException","message":"Entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|nope\u0027 was not found. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:ee336766-ec7f-4832-b1a4-e3d570e41c66_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:19\r\nStatus: 404 (Not Found)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E404\u003C/Code\u003E\u003CDetail\u003EEntity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|nope\u0027 was not found. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:ee336766-ec7f-4832-b1a4-e3d570e41c66_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:19\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:19 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n (MessagingEntityNotFound). For troubleshooting information, see https://aka.ms/azsdk/net/servicebus/exceptions/troubleshoot.","reason":"MessagingEntityNotFound","inner":"RequestFailedException: Entity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|nope\u0027 was not found. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:ee336766-ec7f-4832-b1a4-e3d570e41c66_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:19\r\nStatus: 404 (Not Found)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E404\u003C/Code\u003E\u003CDetail\u003EEntity \u0027\u003Cns\u003E:Topic:sqlprobe-dc3d688d574|shapes|nope\u0027 was not found. To know more visit https://aka.ms/sbResourceMgrExceptions.  TrackingId:ee336766-ec7f-4832-b1a4-e3d570e41c66_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:19\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:19 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n"}
- **http.invalid-action**: {"type":"ArgumentException","message":"There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting IDENTIFIER, or DELIMITED_IDENTIFIER]  TrackingId:ca0ee08a-04f6-436c-bc66-34c71729755c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:18\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting IDENTIFIER, or DELIMITED_IDENTIFIER]  TrackingId:ca0ee08a-04f6-436c-bc66-34c71729755c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:18\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:18 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n","inner":"RequestFailedException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting IDENTIFIER, or DELIMITED_IDENTIFIER]  TrackingId:ca0ee08a-04f6-436c-bc66-34c71729755c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:18\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF, expecting IDENTIFIER, or DELIMITED_IDENTIFIER]  TrackingId:ca0ee08a-04f6-436c-bc66-34c71729755c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:18\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:18 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n"}
- **http.invalid-filter**: {"type":"ArgumentException","message":"There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:3734266f-b745-4355-a6cd-b1ca75a6f0ac_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:17\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:3734266f-b745-4355-a6cd-b1ca75a6f0ac_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:17\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:17 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n","inner":"RequestFailedException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:3734266f-b745-4355-a6cd-b1ca75a6f0ac_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:17\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:3734266f-b745-4355-a6cd-b1ca75a6f0ac_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:17\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:17 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n"}
- **http.params-roundtrip**: {"@a":"x (String)","@b":"5 (Int32)","@c":"6 (Int64)","@d":"1.5 (Double)","@e":"True (Boolean)","@g":"2/01/2024 3:04:05 AM (DateTime)","@h":"01:00:00 (TimeSpan)"}
- **http.subscription-invalid-default-rule**: {"type":"ArgumentException","message":"There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:e103bdf7-e6a2-46ae-97f4-0c75884cf56c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:20\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:e103bdf7-e6a2-46ae-97f4-0c75884cf56c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:20\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:21 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n","inner":"RequestFailedException: There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:e103bdf7-e6a2-46ae-97f4-0c75884cf56c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:20\r\nStatus: 400 (Bad Request)\r\n\r\nContent:\r\n\u003CError\u003E\u003CCode\u003E400\u003C/Code\u003E\u003CDetail\u003EThere was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF]  TrackingId:e103bdf7-e6a2-46ae-97f4-0c75884cf56c_B1, SystemTracker:NoSystemTracker, Timestamp:2026-11-04T10:33:20\u003C/Detail\u003E\u003C/Error\u003E\r\n\r\nHeaders:\r\nTransfer-Encoding: chunked\r\nETag: 639293851101100000\r\nServer: Microsoft-HTTPAPI/2.0\r\nStrict-Transport-Security: REDACTED\r\nDate: Wed, 04 Nov 2026 10:33:21 GMT\r\nContent-Type: application/xml; charset=utf-8\r\n"}
- **http.subscription-invalid-default-rule.exists-after**: false
- **http.true-filter-readback**: ["$Default: TrueRuleFilter \u00271=1\u0027 action=","dup: SqlRuleFilter \u00271=1\u0027 action=","params: SqlRuleFilter \u0027a = @a AND b = @b AND c = @c AND d = @d AND e = @e AND g = @g AND h = @h\u0027 action=","sql11: SqlRuleFilter \u00271=1\u0027 action=","sql11sp: SqlRuleFilter \u00271 = 1\u0027 action=","tf: TrueRuleFilter \u00271=1\u0027 action="]
