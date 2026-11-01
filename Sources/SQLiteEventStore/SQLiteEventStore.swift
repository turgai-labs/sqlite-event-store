import EventSourcing
import SQLiteNIO

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// An SQLite-backed event journal: one append-only table in one database file, with each
/// `(persistence_id, sequence_number)` stored once, every event at the next `ordering`, which is
/// its offset, and its tags indexed beside it.
public actor SQLiteEventStore: EventStore {

  public typealias PersistenceID = String

  /// How many events a read takes at a time: the next page is read only once the previous one
  /// is consumed, so a long journal is never loaded whole.
  static let pageSize = 256

  private let connection: SQLiteConnection

  public init(
    connection: SQLiteConnection
  ) {
    self.connection = connection
  }

  /// Persists an event for a given `PersistenceID`, with its tags, in one `INSERT`: a trigger
  /// indexes the tags in the same write, so an event is never kept without them. SQLite has one
  /// writer at a time, so a later event never gets a lower offset.
  ///
  /// A write is never retried or merged: any event already stored at
  /// `(id, sequenceNumber)` — identical or not — fails with the raw
  /// `SQLiteError` (`constraintUniqueFailed`). The journal stays dumb on
  /// purpose (let-it-crash); whether a write actually landed is resolved by
  /// replaying the journal on entity recovery, not by SQL-level idempotency.
  public func persistEvent<Event>(_ event: Event, id: String, sequenceNumber: Int64, tags: Set<EventTag>) async throws
  where Event: Decodable, Event: Encodable, Event: Sendable {
    try await self.connection.query(
      """
      INSERT INTO journal (persistence_id, sequence_number, event, tags)
      VALUES (?, ?, ?, ?)
      """,
      [
        .text(id), .integer(SQLiteInt64(sequenceNumber)), .text(try JSONText.encode(event)),
        .text(try JSONText.encode(tags.map(\.rawValue).sorted())),
      ]
    )
  }

  /// Native `EventStore.eventStream`: events are read a page at a time, as they're consumed.
  public func eventStream<Event: Codable & Sendable>(
    id: PersistenceID,
    fromSequenceNumber: Int64 = 1
  ) async throws -> EventStream<Event> {
    JournalPages(connection: self.connection, read: .entity(id, from: fromSequenceNumber))
  }

  /// Native `EventStore.currentEventsByTag`: events are read a page at a time, as they're
  /// consumed, through the tags' index.
  public func currentEventsByTag<Event: Codable & Sendable>(
    _ tag: EventTag,
    after offset: Offset
  ) async throws -> EventStream<Event> {
    JournalPages(connection: self.connection, read: .tag(tag, after: offset))
  }

  /// Sets the connection up for a journal and creates its tables, if they aren't there yet.
  public func setupDatabase() async throws {
    try await JournalDatabase.configure(self.connection)
    for statement in [
      """
      CREATE TABLE IF NOT EXISTS journal (
          ordering        INTEGER PRIMARY KEY AUTOINCREMENT,
          persistence_id  TEXT    NOT NULL,
          sequence_number INTEGER NOT NULL,
          event           TEXT    NOT NULL,
          tags            TEXT    NOT NULL DEFAULT '[]',
          created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE (persistence_id, sequence_number)
      )
      """,
      // Every tag's events, by offset: what a read by tag scans.
      """
      CREATE TABLE IF NOT EXISTS journal_tags (
          tag      TEXT    NOT NULL,
          ordering INTEGER NOT NULL REFERENCES journal (ordering),
          PRIMARY KEY (tag, ordering)
      ) WITHOUT ROWID
      """,
      // An event's tags are indexed in the same write that keeps it.
      """
      CREATE TRIGGER IF NOT EXISTS journal_tags_kept
      AFTER INSERT ON journal
      BEGIN
          INSERT INTO journal_tags (tag, ordering) SELECT value, NEW.ordering FROM json_each(NEW.tags);
      END
      """,
    ] {
      try await self.connection.query(statement)
    }
  }
}

/// Events read a page at a time, as they're consumed: one entity's from a sequence number on, or
/// every event with a tag after an offset.
struct JournalPages<Event: Codable & Sendable>: AsyncSequence, Sendable {
  typealias Element = EventEnvelope<Event>

  /// What's read, and where the next page starts.
  enum Read: Sendable {
    case entity(String, from: Int64)
    case tag(EventTag, after: Offset)

    /// Where the page after one ending with `last` starts.
    func after(_ last: EventEnvelope<Event>) -> Self {
      switch self {
      case .entity(let id, _): .entity(id, from: last.sequenceNumber + 1)
      case .tag(let tag, _): .tag(tag, after: last.offset)
      }
    }
  }

  let connection: SQLiteConnection
  let read: Read

  struct AsyncIterator: AsyncIteratorProtocol {
    let connection: SQLiteConnection
    var read: Read
    /// What's read and not yet consumed, last first.
    var page: [EventEnvelope<Event>] = []
    /// Whether the last page read was the journal's last.
    var exhausted = false

    mutating func next() async throws -> EventEnvelope<Event>? {
      if self.page.isEmpty, !self.exhausted {
        let limit = SQLiteData.integer(SQLiteInt64(SQLiteEventStore.pageSize))
        let rows =
          switch self.read {
          case .entity(let id, let from):
            try await self.connection.query(
              """
              SELECT ordering, persistence_id, sequence_number, event FROM journal
              WHERE persistence_id = ? AND sequence_number >= ?
              ORDER BY sequence_number ASC
              LIMIT ?
              """,
              [.text(id), .integer(SQLiteInt64(from)), limit]
            )
          case .tag(let tag, let after):
            try await self.connection.query(
              """
              SELECT journal.ordering, persistence_id, sequence_number, event
              FROM journal_tags JOIN journal ON journal.ordering = journal_tags.ordering
              WHERE journal_tags.tag = ? AND journal_tags.ordering > ?
              ORDER BY journal_tags.ordering ASC
              LIMIT ?
              """,
              [.text(tag.rawValue), .integer(SQLiteInt64(after.rawValue)), limit]
            )
          }
        self.exhausted = rows.count < SQLiteEventStore.pageSize
        self.page = try rows.map { row in
          guard let ordering = row.column("ordering")?.integer,
            let persistenceID = row.column("persistence_id")?.string,
            let sequenceNumber = row.column("sequence_number")?.integer,
            let event = row.column("event")?.string
          else { throw SQLiteEventStoreError.invalidData }
          return EventEnvelope(
            persistenceID: persistenceID,
            sequenceNumber: Int64(sequenceNumber),
            offset: Offset(Int64(ordering)),
            event: try JSONText.decode(Event.self, from: event)
          )
        }.reversed()
        if let last = self.page.first { self.read = self.read.after(last) }
      }
      return self.page.popLast()
    }
  }

  func makeAsyncIterator() -> AsyncIterator {
    AsyncIterator(connection: self.connection, read: self.read)
  }
}

public enum SQLiteEventStoreError: Swift.Error {
  case invalidData
}
