import SQLiteNIO

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// How a connection to a journal's database is set up. Write-ahead logging lets readers go on
/// while one connection writes; a full sync on every commit keeps an event that's acknowledged
/// through a power cut; and a busy timeout makes a second process wait for the writer instead of
/// failing. The journal mode stays with the file, the others with the connection, so each
/// connection is set up when it opens.
enum JournalDatabase {
  /// How long a connection waits for another's write, in milliseconds.
  static let busyTimeout = 5_000

  static func configure(_ connection: SQLiteConnection) async throws {
    _ = try await connection.query("PRAGMA journal_mode = WAL")
    _ = try await connection.query("PRAGMA synchronous = FULL")
    _ = try await connection.query("PRAGMA busy_timeout = \(Self.busyTimeout)")
  }
}

/// Values as JSON text: what the journal and the snapshots keep, readable with `sqlite3`.
enum JSONText {
  static func encode(_ value: some Encodable) throws -> String {
    String(decoding: try JSONEncoder().encode(value), as: UTF8.self)
  }

  static func decode<Value: Decodable>(_ type: Value.Type, from text: String) throws -> Value {
    try JSONDecoder().decode(type, from: Data(text.utf8))
  }
}
