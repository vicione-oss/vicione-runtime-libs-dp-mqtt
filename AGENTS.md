# AGENTS.md — dp-mqtt

The ViciOne MQTT DataPort provider: one package, `ViciOne.Suite.DataPort.Mqtt`
(`src/Mqtt/`), with an incoming and an outgoing port built on `ViciOne.Suite.DataPort`.
It speaks MQTT 3.1.1 and 5.0 through `MQTTnet.Extensions`.

**Solution file**: `dp-mqtt.slnx`

## Read these first

- **`dataport-conventions`** (skill, from the `dataports` plugin) — the rules every ViciOne
  DataPort provider follows: what the built tree guarantees, how a value that cannot be read is
  handled, what may travel on the wire, where validation belongs.
- **`vicione-conventions`** (skill) — the build gate, package management, test layout and naming,
  and the recurring review points.
- **`docs/adr/`** — the decisions that are specific to this port, with the reasoning that produced
  them. Read the one that covers the area you are changing before you change it.
- **`README.md`** — the wire format as its users see it. It is the user-facing documentation of
  this port, so a change to what goes on the wire changes it too.

## Conventions (binding)

- `net10.0`, `Nullable` enabled. The source project builds for `linux-x64`, `linux-arm64` and
  `win-x64`.
- `TreatWarningsAsErrors=true` and `EnforceCodeStyleInBuild=true` — a **single** warning breaks
  the build. `CS1591` is suppressed repo-wide; nothing else is.
- NuGet versions live centrally in `Directory.Packages.props`, with `packages.lock.json` per
  project. CI restores locked, so a package change is never one file.
- `Mqtt.Tests` sees `internal` types through the central `InternalsVisibleTo`.

## Key contracts

**`src/Mqtt/Mqtt.yaml` is the source of truth for the node tree.** It defines what the editor
offers and what the port serves; the code follows it, not the other way round. It is copied to the
output directory on every build, and `YamlTests` and `RulesetTests` check it against the
implementation.

Its `Common.Version` is the ruleset format version and moves with `ViciOne.Tree.Builder`.
Currently ruleset `2.0.0` against `ViciOne.Tree.Builder` `3.0.0`, with `ViciOne.Suite.DataPort`
`0.32.0` and `ViciOne.ManagedEngine` `1.0.0`; the `2.0.0` port additionally requires cluster
management `2.4.0`. Raising one of them is a deliberate step that the clusters built against this
port have to follow — never a drive-by update.

**Envelope children are the second contract surface.** A data point may declare child data points
that travel in the MQTT 5.0 user properties of its own message — a named user property, or one of
the reserved `Timestamp`, `Validity` and `Type`. They decide what a message carries, so a change
to them is a change to the wire format. `README.md` documents them, and
[ADR 1](docs/adr/0001-the-type-user-property-stays-on-the-wire.md) records why `Type` is among
them.

`VERSION` and `VERSION_CORE` carry the package version; CI derives the rest from them.

## Build & test

```sh
dotnet build dp-mqtt.slnx                  # whole solution
dotnet test dp-mqtt.slnx                   # all tests
dotnet test tests/Mqtt.Tests/              # the test project alone
dotnet run -c Release --project samples/Benchmarks/   # BenchmarkDotNet
```

Tests run on the **Microsoft Testing Platform** (`global.json` pins the runner), so the test
project is an executable and `dotnet test` forwards MTP arguments rather than VSTest ones.

`testconfig.json` configures the broker the integration-style tests talk to.

## CI

`.gitlab-ci.yml` is assembled from shared CI components (`dotnet-component/*`,
`module-packaging/*`, `version/version-calc`, `jfrog/*`) — the jobs are mostly component
instantiations, not scripts. Version calculation, packing, the NuGet push and the JFrog
release/promote steps all come from there, so a pipeline problem is usually a component version
or an input, not a local job definition.
