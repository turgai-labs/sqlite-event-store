public import EventSourcing
public import SQLiteNIO

/// SQLite-backed snapshot storage, separate from the event journal in the
/// same way Akka configures journal and snapshot-store plugins independently.
public actor SQLiteSnapshotStore: SnapshotStore {
  private let connection: SQLiteConnection

  public init(connection: SQLiteConnection) {
    self.connection = connection
  }

  /// Keeps one snapshot per persistence ID, as akka-persistence-r2dbc does.
  /// The conditional upsert also preserves this package's monotonic contract:
  /// equal and out-of-order saves are no-ops.
  public func save<State: Codable & Sendable>(
    _ state: State,
    id: PersistenceID,
    coveredSequenceNumber: Int64
  ) async throws {
    _ = try await self.connection.query(
      """
      INSERT INTO snapshots (persistence_id, sequence_number, state)
      VALUES (?, ?, ?)
      ON CONFLICT (persistence_id) DO UPDATE
        SET sequence_number = excluded.sequence_number,
            state = excluded.state,
            created_at = CURRENT_TIMESTAMP
        WHERE snapshots.sequence_number < excluded.sequence_number
      """,
      [.text(id), .integer(SQLiteInt64(coveredSequenceNumber)), .text(try JSONText.encode(state))]
    )
  }

  public func latestSnapshot<State: Codable & Sendable>(
    id: PersistenceID
  ) async throws -> Snapshot<State>? {
    let rows = try await self.connection.query(
      """
      SELECT sequence_number, state
      FROM snapshots
      WHERE persistence_id = ?
      LIMIT 1
      """,
      [.text(id)]
    )
    guard let row = rows.first,
      let sequenceNumber = row.column("sequence_number")?.integer,
      let text = row.column("state")?.string,
      let state = try? JSONText.decode(State.self, from: text)
    else { return nil }
    return Snapshot(
      state: state,
      coveredSequenceNumber: Int64(sequenceNumber)
    )
  }

  public func deleteSnapshots(
    id: PersistenceID,
    matching criteria: SnapshotSelectionCriteria
  ) async throws {
    switch (criteria.minSequenceNumber, criteria.maxSequenceNumber) {
    case (.some(let minimum), .some(let maximum)):
      _ = try await self.connection.query(
        """
        DELETE FROM snapshots
        WHERE persistence_id = ?
          AND sequence_number >= ?
          AND sequence_number <= ?
        """,
        [.text(id), .integer(SQLiteInt64(minimum)), .integer(SQLiteInt64(maximum))]
      )
    case (.some(let minimum), .none):
      _ = try await self.connection.query(
        "DELETE FROM snapshots WHERE persistence_id = ? AND sequence_number >= ?",
        [.text(id), .integer(SQLiteInt64(minimum))]
      )
    case (.none, .some(let maximum)):
      _ = try await self.connection.query(
        "DELETE FROM snapshots WHERE persistence_id = ? AND sequence_number <= ?",
        [.text(id), .integer(SQLiteInt64(maximum))]
      )
    case (.none, .none):
      _ = try await self.connection.query("DELETE FROM snapshots WHERE persistence_id = ?", [.text(id)])
    }
  }

  /// Sets the connection up and creates the snapshots table, if it isn't there yet.
  public func setupDatabase() async throws {
    try await JournalDatabase.configure(self.connection)
    _ = try await self.connection.query(
      """
      CREATE TABLE IF NOT EXISTS snapshots (
        persistence_id  TEXT    NOT NULL PRIMARY KEY,
        sequence_number INTEGER NOT NULL,
        state           TEXT    NOT NULL,
        created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
      ) WITHOUT ROWID
      """
    )
  }
}
