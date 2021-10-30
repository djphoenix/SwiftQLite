import SQLite3

public struct SQLiteError: Error {
  public let code: Int32
  public let localizedDescription: String
  @inlinable internal init?(_ db: OpaquePointer) {
    let code = sqlite3_errcode(db)
    guard code != SQLITE_OK, code != SQLITE_ROW, code != SQLITE_DONE else { return nil }
    self.code = code
    localizedDescription = String(cString: sqlite3_errmsg(db))
  }
}
