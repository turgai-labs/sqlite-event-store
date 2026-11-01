import EventSourcing
import SQLiteNIO
import Testing

@testable import SQLiteEventStore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

@Suite("SQLiteEventStore Tests")
struct SQLiteEventStoreTests {

  struct TestEvent: Codable, Sendable, Equatable {
    let value: String
  }

  struct TestState: Codable, Sendable, Equatable {
    let value: String
  }

  /// A database file of the test's own, opened, set up and given to `body`, then closed and
  /// removed with its write-ahead log.
  private func withDatabase<Result>(
    _ body: (_ path: String, _ connection: SQLiteConnection) async throws -> Result
  ) async throws -> Result {
    let path = FileManager.default.temporaryDirectory
      .appendingPathComponent("journal-\(UUID().uuidString).sqlite").path
    let connection = try await SQLiteConnection.open(storage: .file(path: path))
    defer {
      for suffix in ["", "-wal", "-shm"] { try? FileManager.default.removeItem(atPath: path + suffix) }
    }
    do {
      try await SQLiteEventStore(connection: connection).setupDatabase()
      try await SQLiteSnapshotStore(connection: connection).setupDatabase()
      let result = try await body(path, connection)
      try await connection.close()
      return result
    } catch {
      try? await connection.close()
      throw error
    }
  }

  /// Readers go on while one writes, every commit is synced before it's acknowledged, and a
  /// second writer waits rather than fails.
  @Test("A set-up connection logs ahead, syncs fully, and waits for another writer")
  func connectionSettings() async throws {
    try await withDatabase { _, connection in
      let mode = try await connection.query("PRAGMA journal_mode").first?.column("journal_mode")?.string
      let synchronous = try await connection.query("PRAGMA synchronous").first?.column("synchronous")?.integer
      let timeout = try await connection.query("PRAGMA busy_timeout").first?.column("timeout")?.integer
      #expect(mode == "wal")
      #expect(synchronous == 2)
      #expect(timeout == JournalDatabase.busyTimeout)
    }
  }

  @Test("Save, load, and delete snapshots by criteria")
  func snapshots() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteSnapshotStore(connection: connection)
      let persistenceID = "snapshot-test"

      try await store.save(TestState(value: "one"), id: persistenceID, coveredSequenceNumber: 1)
      // Equal and lower sequence numbers are monotonic no-ops.
      try await store.save(TestState(value: "replacement"), id: persistenceID, coveredSequenceNumber: 1)
      try await store.save(TestState(value: "three"), id: persistenceID, coveredSequenceNumber: 3)
      try await store.save(TestState(value: "two"), id: persistenceID, coveredSequenceNumber: 2)

      var latest: Snapshot<TestState>? = try await store.latestSnapshot(id: persistenceID)
      #expect(latest?.coveredSequenceNumber == 3)
      #expect(latest?.state == TestState(value: "three"))

      try await store.deleteSnapshots(id: persistenceID, matching: .init(minSequenceNumber: 1, maxSequenceNumber: 2))
      latest = try await store.latestSnapshot(id: persistenceID)
      #expect(latest?.coveredSequenceNumber == 3)
      #expect(latest?.state == TestState(value: "three"))

      try await store.deleteSnapshots(id: persistenceID, matching: .through(3))
      latest = try await store.latestSnapshot(id: persistenceID)
      #expect(latest?.coveredSequenceNumber == nil)
    }
  }

  @Test("An undecodable latest snapshot is treated as absent")
  func undecodableSnapshot() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteSnapshotStore(connection: connection)
      try await connection.query(
        "INSERT INTO snapshots (persistence_id, sequence_number, state) VALUES (?, 1, ?)",
        [.text("snapshot-decode-test"), .text(#"{"unexpected":true}"#)]
      )

      let latest: Snapshot<TestState>? = try await store.latestSnapshot(id: "snapshot-decode-test")
      #expect(latest?.coveredSequenceNumber == nil)
    }
  }

  @Test("Persist and replay events")
  func persistAndReplay() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      for sequenceNumber: Int64 in 1...3 {
        try await store.persistEvent(TestEvent(value: "event-\(sequenceNumber)"), id: "test", sequenceNumber: sequenceNumber, tags: [])
      }

      let events: [TestEvent] = try await store.eventsFor(id: "test")
      #expect(events == (1...3).map { TestEvent(value: "event-\($0)") })
    }
  }

  @Test("Stream events with metadata from a sequence number")
  func eventStream() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      for sequenceNumber: Int64 in 1...3 {
        try await store.persistEvent(TestEvent(value: "event-\(sequenceNumber)"), id: "test", sequenceNumber: sequenceNumber, tags: [])
      }

      var envelopes: [EventEnvelope<TestEvent>] = []
      let stream: EventStream<TestEvent> = try await store.eventStream(id: "test", fromSequenceNumber: 2)
      for try await envelope in stream {
        envelopes.append(envelope)
      }

      #expect(envelopes.map(\.sequenceNumber) == [2, 3])
      #expect(envelopes.map(\.persistenceID) == ["test", "test"])
      #expect(envelopes.map(\.event.value) == ["event-2", "event-3"])
    }
  }

  @Test("Persisting the same event twice fails unconditionally")
  func duplicatePersist() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      let event = TestEvent(value: "event-1")
      try await store.persistEvent(event, id: "test", sequenceNumber: 1, tags: [])

      // Let-it-crash: even an identical rewrite is a conflict — idempotency
      // is resolved by journal replay on recovery, not by the store.
      let error = await #expect(throws: SQLiteError.self) {
        try await store.persistEvent(event, id: "test", sequenceNumber: 1, tags: [])
      }
      #expect(error?.reason == .constraintUniqueFailed)

      let events: [TestEvent] = try await store.eventsFor(id: "test")
      #expect(events == [event])
    }
  }

  @Test("Persisting a different event at the same sequence number fails")
  func conflictingPersist() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      try await store.persistEvent(TestEvent(value: "original"), id: "test", sequenceNumber: 1, tags: [])

      let error = await #expect(throws: SQLiteError.self) {
        try await store.persistEvent(TestEvent(value: "different"), id: "test", sequenceNumber: 1, tags: [])
      }
      #expect(error?.reason == .constraintUniqueFailed)

      let events: [TestEvent] = try await store.eventsFor(id: "test")
      #expect(events == [TestEvent(value: "original")])
    }
  }

  @Test("A replay longer than a page reads every event, in order")
  func replayAcrossPages() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      let count = Int64(SQLiteEventStore.pageSize * 2 + 10)
      for sequenceNumber: Int64 in 1...count {
        try await store.persistEvent(TestEvent(value: "event-\(sequenceNumber)"), id: "long", sequenceNumber: sequenceNumber, tags: [])
      }
      // Another journal's events, interleaved by key, are never read as this one's.
      try await store.persistEvent(TestEvent(value: "other"), id: "lone", sequenceNumber: 1, tags: [])

      var sequenceNumbers: [Int64] = []
      let stream: EventStream<TestEvent> = try await store.eventStream(id: "long", fromSequenceNumber: 3)
      for try await envelope in stream {
        sequenceNumbers.append(envelope.sequenceNumber)
      }
      #expect(sequenceNumbers == Array(3...count))
    }
  }

  @Test("Every event with a tag reads back after an offset, across entities, in the order kept")
  func eventsByTag() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      let tag = EventTag("test")
      try await store.persistEvent(TestEvent(value: "one"), id: "a", sequenceNumber: 1, tags: [tag])
      try await store.persistEvent(TestEvent(value: "untagged"), id: "b", sequenceNumber: 1, tags: [])
      try await store.persistEvent(TestEvent(value: "two"), id: "b", sequenceNumber: 2, tags: [tag, EventTag("other")])
      try await store.persistEvent(TestEvent(value: "three"), id: "a", sequenceNumber: 2, tags: [tag])

      let tagged = try await Self.read(tag, after: .start, in: store)
      #expect(tagged.map(\.event.value) == ["one", "two", "three"])
      #expect(tagged.map(\.persistenceID) == ["a", "b", "a"])
      #expect(tagged.map(\.sequenceNumber) == [1, 2, 2])
      #expect(tagged.map(\.offset) == tagged.map(\.offset).sorted())

      let rest = try await Self.read(tag, after: tagged[0].offset, in: store)
      #expect(rest.map(\.event.value) == ["two", "three"])
      #expect(try await Self.read(EventTag("other"), after: .start, in: store).map(\.event.value) == ["two"])
      #expect(try await Self.read(EventTag("none"), after: .start, in: store).isEmpty)

      // A replay's envelopes carry the same offsets.
      var replayed: [Offset] = []
      let stream: EventStream<TestEvent> = try await store.eventStream(id: "a", fromSequenceNumber: 1)
      for try await envelope in stream { replayed.append(envelope.offset) }
      #expect(replayed == [tagged[0].offset, tagged[2].offset])
    }
  }

  @Test("A read by tag longer than a page reads every event, in order")
  func eventsByTagAcrossPages() async throws {
    try await withDatabase { _, connection in
      let store = SQLiteEventStore(connection: connection)
      let tag = EventTag("long")
      let count = Int64(SQLiteEventStore.pageSize * 2 + 10)
      for sequenceNumber: Int64 in 1...count {
        try await store.persistEvent(
          TestEvent(value: "event-\(sequenceNumber)"), id: "long", sequenceNumber: sequenceNumber, tags: [tag])
        try await store.persistEvent(
          TestEvent(value: "other-\(sequenceNumber)"), id: "other", sequenceNumber: sequenceNumber, tags: [])
      }

      let tagged = try await Self.read(tag, after: .start, in: store)
      #expect(tagged.map(\.sequenceNumber) == Array(1...count))
    }
  }

  /// The test events tagged `tag` after `offset`, read whole.
  private static func read(
    _ tag: EventTag,
    after offset: Offset,
    in store: SQLiteEventStore
  ) async throws -> [EventEnvelope<TestEvent>] {
    let events: EventStream<TestEvent> = try await store.currentEventsByTag(tag, after: offset)
    var envelopes: [EventEnvelope<TestEvent>] = []
    for try await envelope in events {
      envelopes.append(envelope)
    }
    return envelopes
  }

  @Test("What's written survives closing and opening the database again")
  func survivesReopening() async throws {
    try await withDatabase { path, connection in
      try await SQLiteEventStore(connection: connection).persistEvent(TestEvent(value: "kept"), id: "test", sequenceNumber: 1, tags: [])

      let reopened = try await SQLiteConnection.open(storage: .file(path: path))
      let store = SQLiteEventStore(connection: reopened)
      try await store.setupDatabase()
      let events: [TestEvent] = try await store.eventsFor(id: "test")
      try await reopened.close()
      #expect(events == [TestEvent(value: "kept")])
    }
  }

  /// Two connections to one file, as two processes of one server have: each reads what the
  /// other wrote, and both write.
  @Test("Two connections share the journal")
  func twoConnections() async throws {
    try await withDatabase { path, connection in
      let other = try await SQLiteConnection.open(storage: .file(path: path))
      let first = SQLiteEventStore(connection: connection)
      let second = SQLiteEventStore(connection: other)
      try await second.setupDatabase()

      try await first.persistEvent(TestEvent(value: "from-first"), id: "a", sequenceNumber: 1, tags: [])
      try await second.persistEvent(TestEvent(value: "from-second"), id: "b", sequenceNumber: 1, tags: [])

      let readBySecond: [TestEvent] = try await second.eventsFor(id: "a")
      let readByFirst: [TestEvent] = try await first.eventsFor(id: "b")
      try await other.close()
      #expect(readBySecond == [TestEvent(value: "from-first")])
      #expect(readByFirst == [TestEvent(value: "from-second")])
    }
  }
}
