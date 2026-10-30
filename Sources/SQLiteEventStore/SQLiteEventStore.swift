import EventSourcing
import SQLiteNIO

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// An SQLite-backed event journal: one append-only table in one database file, with each
/// `(persistence_id, sequence_number)` stored once.
public actor SQLiteEventStore: EventStore {

  public typealias PersistenceID = String

  /// How many events a replay reads at a time: the next page is read only once the previous
  /// one is consumed, so a long journal is never loaded whole.
  static let pageSize = 256

  private let connection: SQLiteConnection

  public init(
    connection: SQLiteConnection
  ) {
    self.connection = connection
  }

  /// Persists an event for a given `PersistenceID`.
  ///
  /// A write is never retried or merged: any event already stored at
  /// `(id, sequenceNumber)` — identical or not — fails with the raw
  /// `SQLiteError` (`constraintPrimaryKeyFailed`). The journal stays dumb on
  /// purpose (let-it-crash); whether a write actually landed is resolved by
  /// replaying the journal on entity recovery, not by SQL-level idempotency.
  public func persistEvent<Event>(_ event: Event, id: String, sequenceNumber: Int64) async throws
  where Event: Decodable, Event: Encodable, Event: Sendable {
    try await self.connection.query(
      """
      INSERT INTO journal (persistence_id, sequence_number, event)
      VALUES (?, ?, ?)
      """,
      [.text(id), .integer(SQLiteInt64(sequenceNumber)), .text(try JSONText.encode(event))]
    )
  }

  /// Native `EventStore.eventStream`: events are read a page at a time, as they're consumed.
  public func eventStream<Event: Codable & Sendable>(
    id: PersistenceID,
    fromSequenceNumber: Int64 = 1
  ) async throws -> EventStream<Event> {
    JournalPages(connection: self.connection, id: id, from: fromSequenceNumber)
  }

  /// Sets the connection up for a journal and creates its table, if it isn't there yet.
  public func setupDatabase() async throws {
    try await JournalDatabase.configure(self.connection)
    try await self.connection.query(
      """
      CREATE TABLE IF NOT EXISTS journal (
          persistence_id  TEXT    NOT NULL,
          sequence_number INTEGER NOT NULL,
          event           TEXT    NOT NULL,
          created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (persistence_id, sequence_number)
      ) WITHOUT ROWID
      """
    )
  }
}

/// One journal's events from a sequence number on, read a page at a time.
struct JournalPages<Event: Codable & Sendable>: AsyncSequence, Sendable {
  typealias Element = EventEnvelope<Event>

  let connection: SQLiteConnection
  let id: String
  let from: Int64

  struct AsyncIterator: AsyncIteratorProtocol {
    let connection: SQLiteConnection
    let id: String
    /// The sequence number the next page starts at.
    var next: Int64
    /// What's read and not yet consumed, last first.
    var page: [EventEnvelope<Event>] = []
    /// Whether the last page read was the journal's last.
    var exhausted = false

    mutating func next() async throws -> EventEnvelope<Event>? {
      if self.page.isEmpty, !self.exhausted {
        let rows = try await self.connection.query(
          """
          SELECT sequence_number, event FROM journal
          WHERE persistence_id = ? AND sequence_number >= ?
          ORDER BY sequence_number ASC
          LIMIT ?
          """,
          [.text(self.id), .integer(SQLiteInt64(self.next)), .integer(SQLiteInt64(SQLiteEventStore.pageSize))]
        )
        self.exhausted = rows.count < SQLiteEventStore.pageSize
        self.page = try rows.map { row in
          guard let sequenceNumber = row.column("sequence_number")?.integer,
            let event = row.column("event")?.string
          else { throw SQLiteEventStoreError.invalidData }
          return EventEnvelope(
            persistenceID: self.id,
            sequenceNumber: Int64(sequenceNumber),
            event: try JSONText.decode(Event.self, from: event)
          )
        }.reversed()
        if let last = self.page.first { self.next = last.sequenceNumber + 1 }
      }
      return self.page.popLast()
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(connection: self.connection, id: self.id, next: self.from)
  }
}

public enum SQLiteEventStoreError: Swift.Error {
  case invalidData
}
