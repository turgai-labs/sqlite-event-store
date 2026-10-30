# SQLite Event Store

SQLite implementations of `EventStore` and `SnapshotStore` for
[cluster-event-sourcing](https://github.com/akbashev/cluster-event-sourcing),
built directly on [SQLiteNIO](https://github.com/vapor/sqlite-nio).

The event journal is append-only and lives in one database file; replay reads it
a page at a time. Snapshots are optional and live in a separate table of the same
file. SQLite is embedded: there's no server to run, and SQLiteNIO compiles its own
copy of SQLite into the binary.

## Installation

```swift
dependencies: [
  .package(
    url: "https://github.com/turgai-labs/sqlite-event-store.git",
    branch: "main"
  )
]
```

Add the product to your target and import it:

```swift
import DistributedCluster
import EventSourcing
import SQLiteEventStore
import SQLiteNIO
```

## Connection and stores

Open one long-lived `SQLiteConnection` on a local file and share it between the
journal and the snapshot store:

```swift
let connection = try await SQLiteConnection.open(storage: .file(path: "/path/to/journal.sqlite"))

let eventStore = SQLiteEventStore(connection: connection)
let snapshotStore = SQLiteSnapshotStore(connection: connection)

try await eventStore.setupDatabase()
try await snapshotStore.setupDatabase()
```

Close the connection during shutdown. `setupDatabase()` sets the connection up
and creates its table with `CREATE TABLE IF NOT EXISTS`, so call it each time a
connection opens:

- **Write-ahead logging** (`journal_mode = WAL`): readers go on while one
  connection writes. It stays with the file.
- **Full sync** (`synchronous = FULL`): every commit is synced to disk before
  it's acknowledged, so an acknowledged event survives a power cut.
- **A busy timeout** of five seconds: another connection, in this process or
  another one, waits for the writer instead of failing.

Keep the file on a local disk. SQLite's locking isn't reliable on network file
systems.

## Cluster integration

Return the already-created stores from the journal plugin's factories:

```swift
let system = await ClusterSystem("my-node") {
  $0.plugins.install(
    plugin: ClusterJournalPlugin(
      factory: { _ in eventStore },
      snapshotFactory: { _ in snapshotStore }
    )
  )
}
```

Omit `snapshotFactory` when snapshots are not required.

## Event journal semantics

`persistEvent(_:id:sequenceNumber:)` performs a plain SQL `INSERT`. The primary
key is `(persistence_id, sequence_number)`, so every attempt to reuse a
sequence number fails with `SQLiteError` reason `constraintPrimaryKeyFailed`,
even when the payload is identical. Writes are not retried, merged, or made
idempotent by the store; the entity recovers uncertain write outcomes by
replaying its journal.

`eventStream(id:fromSequenceNumber:)` reads events in ascending sequence order,
a page at a time as they're consumed, so the complete journal is never in memory:

```swift
let stream: EventStream<OrderEvent> = try await eventStore.eventStream(
  id: "order-42",
  fromSequenceNumber: 10
)

for try await envelope in stream {
  print(envelope.sequenceNumber, envelope.event)
}
```

Events are stored as JSON text using their `Codable` conformance, so the
`sqlite3` command-line tool can read them:

```sh
sqlite3 journal.sqlite "SELECT sequence_number, event FROM journal WHERE persistence_id = 'order-42'"
```

## Snapshot semantics

`SQLiteSnapshotStore` keeps one row per persistence ID. Saving a snapshot with a
higher covered sequence number replaces the existing row; saving an equal or
lower sequence number is a no-op. This prevents an older actor incarnation from
replacing a newer snapshot.

Snapshot deletion accepts `SnapshotSelectionCriteria`. Because the store keeps
only the latest snapshot, deletion either leaves that row intact or removes it
when its sequence number matches the criteria. A snapshot payload that can no
longer be decoded as the requested state type is treated as absent so recovery
can fall back to the event journal.

## Testing

The tests use a temporary database file each, so they need nothing installed:

```sh
swift test
```
