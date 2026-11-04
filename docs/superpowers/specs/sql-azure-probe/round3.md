| id | outcome | detail |
|---|---|---|
| y.set-preset-to | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-to, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-to | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-to, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-replyto | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-replyto, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-replyto | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-replyto, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-correlationid | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-correlationid, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-correlationid | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-correlationid, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-contenttype | Match | $ContentType=new, $CorrelationId=o, $MessageId=m-y.set-preset-contenttype, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-contenttype | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-contenttype, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-replytosessionid | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-replytosessionid, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-replytosessionid | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-replytosessionid, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-label | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-label, $ReplyTo=o, $ReplyToSessionId=o, $Subject=new, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-null-preset-label | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-null-preset-label, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-ttl | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.set-preset-ttl, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=02:00:00, $To=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-sessionid | Match | $MessageId=m-y.set-preset-sessionid, $PartitionKey=new, $SessionId=o, color=red (String), RuleName=r0 (String) |
| y.set-preset-partitionkey | Match | $MessageId=m-y.set-preset-partitionkey, $PartitionKey=new, color=red (String), RuleName=r0 (String) |
| y.set-preset-messageid-explicit | Match | $MessageId=explicit, color=red (String), RuleName=r0 (String) |
| y.read-after-set | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.read-after-set, $ReplyTo=o, $ReplyToSessionId=o, $Subject=new, $TimeToLive=01:00:00, $To=o, color=red (String), l=new (String), RuleName=r0 (String) |
| y.read-after-set-to | Match | $ContentType=o, $CorrelationId=o, $MessageId=m-y.read-after-set-to, $ReplyTo=o, $ReplyToSessionId=o, $Subject=o, $TimeToLive=01:00:00, $To=o, color=red (String), RuleName=r0 (String), t=new (String) |
| y.set-unset-correlation-null | Match | $MessageId=m-y.set-unset-correlation-null, color=red (String), RuleName=r0 (String) |
| y.filter-sys-to-null-isnull | Match | $MessageId=m-y.filter-sys-to-null-isnull |
| y.filter-sys-to-eq-null | Match | $MessageId=m-y.filter-sys-to-eq-null |
| y.filter-sys-to-neq-x | Match | $MessageId=m-y.filter-sys-to-neq-x |
| y.filter-sys-sessionid-neq-x | Match | $MessageId=m-y.filter-sys-sessionid-neq-x |
| y.filter-sys-label-like | Match | $MessageId=m-y.filter-sys-label-like, $Subject=Label |
| y.filter-sys-ttl-param | Rejected | ArgumentException: A parameter (@t) used in the expression specified in SqlExpression is missing from the Parameters property. |
| y.filter-sys-ttl-param2 | Match | $MessageId=m-y.filter-sys-ttl-param2 |
| y.filter-sys-expiresat-gt-enqueued | Match | $MessageId=m-y.filter-sys-expiresat-gt-enqueued, $TimeToLive=01:00:00 |
| y.filter-sys-expiresat-no-ttl | Match | $MessageId=m-y.filter-sys-expiresat-no-ttl |
| y.filter-sys-size-range | Match | $MessageId=m-y.filter-sys-size-range |
| y.filter-sys-state | DeadLettered | $MessageId=m-y.filter-sys-state, DeadLetterErrorDescription=Operator '==' cannot be applied to operands of type 'Microsoft.ServiceBus.Messaging.MessageState' and 'long' (String), DeadLetterReason=FilterException (String), DeadLetterSource=FilteringEngine (String), $reason=FilterException, $description=Operator '==' cannot be applied to operands of type 'Microsoft.ServiceBus.Messaging.MessageState' and 'long' |
| y.filter-sys-messageid-like | Match | $MessageId=m-y.filter-sys-messageid-like |
| y.filter-sys-scheduled-null | NoMatch |  |
| y.filter-sys-deliverycount-type | Match | $MessageId=m-y.filter-sys-deliverycount-type |
| y.filter-sys-seq-type | Match | $MessageId=m-y.filter-sys-seq-type |
| y.filter-sys-enqseq-eq-seq | Match | $MessageId=m-y.filter-sys-enqseq-eq-seq |

## extra
