# DataPort MQTT

## Node properties

| Name                     | Incoming | Outgoing | Typ    | Values                                                                         |
|--------------------------|:--------:|:--------:|--------|--------------------------------------------------------------------------------|
| Retain                   |    ❌    |    ✔️    | `bool` | `true`/`false`                                                                 |
| Serializer               |    ✔️    |    ✔️    | `byte` | `0`=Inherit from broker, `1`=JSON, `2`=PlainText                               |
| QualityOfServiceOverride |    ✔️    |    ✔️    | `byte` | `0`=Inherit from broker, `1`=At most once, `2`=At least once, `3`=Exactly once |

A data point's quality-of-service override applies to its subscription as well as to what it
publishes. Where several data points share a topic and their effective levels disagree, the subscription
uses the highest requested level and a warning naming the topic is logged — a data point then
receives a stronger guarantee than it asked for, never a weaker one.

Node properties apply to data points. A folder that is itself configured with a transfer mode
publishes its children as one grouped message, and that message uses the broker-level settings -
a folder carries no node properties of its own.

## Envelope children

A data point may carry child data points that address the envelope of its parent's message instead
of a topic of their own. They require MQTT 5.0, and only what the tree declares is carried — a data
point without children sends no user properties at all.

| Child node    | Key on the wire | Transferred | Linkable | Value                                                     |
|---------------|-----------------|-------------|----------|-----------------------------------------------------------|
| User property | the node name   | both        | both     | written by the engine; `String`, `Int64`, `Float64`, `Bool` or `DateTime` |
| Timestamp     | `Timestamp`     | both        | inbound  | the timestamp of the parent value, ISO 8601 in UTC        |
| Validity      | `Validity`      | outbound    | never    | the engine validity of the parent value, `0` when invalid |
| Type          | `Type`          | outbound    | never    | the .NET assembly-qualified type name of the value, as a `String` |

*Transferred* is the direction the value travels on the wire, *linkable* the direction in which the
engine may connect the child. `Timestamp` is written by the port outbound and can only be linked
inbound. `Validity` and `Type` are predefined: the port fills them from the parent value and the
published type, so the editor offers no connector for them and a cluster that links one is rejected
at deployment.

`Validity` is written as the engine validity itself, the integer the engine gave the value, of which
everything but `0` means valid. Incoming, the validity is read the same way, and a `true` or `false`
a publisher that is not this port sent is understood as well. A text that is neither is reported,
and the values of the message are forwarded as invalid.

A user property key may not be `Timestamp`, `Validity` or `Type`, and this data port
puts at most 16 of them on a message. A `DateTime` the engine wrote without a kind is sent as UTC,
the way a received one without a zone is read.

Envelope children can only be declared under a data point of their own. A folder published as one
group message carries the envelope it always has: the latest `Timestamp` of its values, the lowest
`Validity`, and a `Type` that names the data type of each member.

Outgoing, a child never produces a message of its own: its last value is remembered and put on the
next message of its parent, whatever the validity that value was written with. The keys a message
carries are the ones the tree declares, and a `User property` carries no validity of its own on the
wire, so nothing about the value decides whether its key travels. A child the engine has not
written yet is the one key a message leaves out: there is no value to put on it. The fixed children
are all derived by the port and none of them is written by the engine: `Timestamp` is the timestamp
of the parent value, `Validity` its validity, and `Type` the assembly-qualified name of the
published value's runtime type — `System.Object` for a null value.

Incoming, a received message is published as the value of its data point followed by one value per
child that reads a value of its own — only `User property` and `Timestamp` do; `Validity` and `Type`
never fan out — in the order the tree declares
them. The validity of the received message reaches the engine as the validity of the data point and
of every child read from it, so a child is never more valid than the message that carried it; a
message that declares no validity is valid, and its children are too. A child of a message declared
invalid still carries the value read from it, flagged rather than dropped, so the engine sees the
reading together with the state its sender gave it. A user property key is matched exactly, the
reserved keys are matched ignoring case, and every value is read as the data type its child
declares. A point in time is read from plain ISO 8601, with a fraction of one to seven digits or
none at all and with the zone written as `Z`, as an offset, or left off to mean the UTC this port
writes — the port writes the round-trip format back, which demands exactly seven digits, but no
sender other than this one has a reason to produce that. A `Timestamp` this port cannot read is
reported, and the time the message was received stands in for it; a message that names no time at
all is only missing one and is not reported. Nothing the message did not carry reaches the engine: a payload the configured data type
cannot read forwards no value for its data point, nor for the member of a group message it belongs
to, and a property the message leaves out or writes as a text that is not a value of its child's
data type forwards no value for that child. The default of a data type would be a reading its
sender could have taken, so the engine could not tell the two apart from the value alone; the
channel simply keeps what it had. A payload that cannot be read is still only its own data point's
business — the envelope children are read all the same, as each of them carries its own text.

Whatever a sender wrote that this port cannot read is reported once per text and topic, not once per
message, so a sender that keeps writing it does not drown the log in the same warning.

A tree the port cannot serve is refused when the port starts rather than failing message by message:
envelope children on MQTT 3.1.1, a child under another child, a duplicate or reserved key, more than
16 user properties, a parent that transfers no value of its own, and a `User property` whose own transfer is
not connected — the fixed children need no channel of their own.

The payload of a received message is read as the data type its data point is configured with, or as
the type the sender named in a `Type` user property when the configured type can hold it. That is
what carries a value of a derived type across a link of two of these ports: the payload itself
carries no type of its own, so the members the configured type does not know would be lost without
it. The configured type stays the contract — a sender may narrow what it sends, never widen it and
never pick a type of its own, so a topic cannot decide which type this port loads. A name the
configured type cannot hold is read as the configured type without a word, because the port names
the runtime type it published: `System.Object` for a null value, and a narrower primitive than the
tree declares whenever the engine delivered one, an `Int32` for an `Int64` data point. An abstract
type or an interface is ignored the same way — the name comes from a value that existed, so no port
wrote it. A name is looked up only among the assemblies this port has loaded already, so it never
makes one load, and each name is looked up once. A name no loaded assembly knows is the one that
says something is wrong: the sender knows a type this port does not, and it is reported as a
warning.

A member of a group message is read the same way, with the name taken from its entry in the `Type`
of the group message, which maps each member to the data type the sender published.
