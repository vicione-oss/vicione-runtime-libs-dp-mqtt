# 1. The `Type` user property stays on the wire

Date: 2026-09-23

## Status

Accepted and implemented, with the envelope-children work in
[!13](https://gitlab.com/vicione-oss/vicione/runtime-libs/dp-mqtt/-/merge_requests/13), released
as `2.0.0`.

## Context

Every message this port publishes carries a `Type` user property naming the .NET type of the
published value, and a group message carries one entry per member.

When the message envelope became something the tree declares rather than something the port always
sends, `Type` was the candidate to drop. It is the only user property no engine can link, it costs
bytes on every message, and reading it means resolving a type name that came off the broker —
which, unconstrained, lets whoever may publish on a subscribed topic decide which type the port
loads.

Dropping it, however, costs more than it saves. An MQTT payload carries no type of its own, so a
receiving port reads it as the data type its data point is configured with. A sender that published
a derived type has no way to say so, and every member the configured type does not know is lost
without a word. That hits the case the two ports exist for: an outgoing port linked to an incoming
port of the same kind cannot round-trip its own values.

## Decision

`Type` stays on the wire, and a received `Type` is used — but the **configured data type remains
the contract**.

- The payload is read as the data type the data point is configured with, or as the type the
  sender names in `Type` when the configured type can hold it.
- A sender may therefore **narrow** what it sends. It may never widen it, and it may never pick a
  type of its own: a topic cannot decide which type this port loads.
- A member of a group message is read the same way, with its name taken from that member's entry
  in the group's `Type`.
- A name the configured type cannot hold is read as the configured type **without a warning**. The
  port itself produces such names routinely — `System.Object` for a null value, and whatever
  narrower primitive the engine delivered, an `Int32` for an `Int64` data point. An abstract type
  or an interface is ignored the same way, because the name always comes from a value that
  existed.
- A name **no assembly of this port knows** is the one case that is reported: the sender knows a
  type this port does not, which is a genuine mismatch between the two sides.
- A name is resolved **only against assemblies the port has already loaded**. A sender cannot make
  the port load one, and the answer is remembered per name, so a repeated name costs one lookup
  rather than one per message.

Outgoing, `Type` is one of the envelope children a data point declares. The port fills it from the
runtime type of the published value; the engine never writes it and the editor offers no connector
for it.

## Consequences

- Two of these ports linked to each other round-trip derived types. That is the case this decision
  exists for.
- The port keeps a type-name resolution on the receiving path, driven by whoever may publish on a
  subscribed topic. `NamedTypeResolver` is what keeps that bounded, and the warning behind it is
  reported once per topic rather than once per message — see the `dataport-conventions` skill on
  what a foreign sender may drive.
- The wire format carries a .NET type name, so it is not protocol-neutral. A non-.NET publisher
  simply omits `Type` and is read as the configured type, which is the documented fallback rather
  than an error.
- Because a name the configured type cannot hold is ignored silently, a genuinely wrong `Type`
  from a foreign publisher looks exactly like the port's own narrower names. The warning is
  reserved for the unresolvable case, where silence would hide a real version mismatch.
- `README.md` documents this for the people configuring the port, and stays the user-facing
  description of the behaviour.
