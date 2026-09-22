# DataPort MQTT

## Node properties

| Name       | Incoming | Outgoing | Typ    | Values                                                |
|------------|:--------:|:--------:|--------|-------------------------------------------------------|
| Retain     |    ❌    |    ✔️    | `bool` | `true`/`false`                                        |
| Serializer |    ✔️    |    ✔️    | `byte` | `0`=Inherit from parent node, `1`=JSON, `2`=PlainText |

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
a publisher that is not this port sent is understood as well.

A user property key may not be `Timestamp`, `Validity` or `Type`, and this data port
puts at most 16 of them on a message.

Envelope children can only be declared under a data point of their own. A folder published as one
group message carries no user properties at all, not even a timestamp or a validity.

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
travel outbound only and never fan out — in the order the tree declares
them. The validity of the received message reaches the engine as the validity of the data point and
of every child read from it, so a child is never more valid than the message that carried it; a
message that declares no validity is valid, and its children are too. A child of a message declared
invalid still carries the value read from it, flagged rather than dropped, so the engine sees the
reading together with the state its sender gave it. A user property key is matched exactly, the
reserved keys are matched ignoring case, and every value is read as the data type its child
declares. A property the message does not carry, or one whose text is not a value of that data
type, leaves its child invalid; an invalid child carries the default of its data type, because a
typed incoming link cannot take a null. A payload the configured data type cannot read reaches the
engine as no value at all, and the member of a group message it belongs to as none either: the
default of the data type would be indistinguishable from a reading its sender took, and a payload
that does not fit the type it is configured with names a sender this port does not speak the same
language as. The envelope children are read all the same, as each of them carries its own text.

A tree the port cannot serve is refused when the port starts rather than failing message by message:
envelope children on MQTT 3.1.1, a child under another child, a duplicate or reserved key, more than
16 user properties, a parent that transfers no value of its own, and a `User property` whose own transfer is
not connected — the fixed children need no channel of their own.

The payload of a received message is always read as the data type the data point is configured with;
a `Type` user property a publisher sent is ignored.
