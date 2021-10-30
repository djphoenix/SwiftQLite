import SQLite3
import ThreadSafeContainer
import Foundation

class Prepared {
  class Query {
    let prep: OpaquePointer
    init(_ prep: OpaquePointer) { self.prep = prep }
    deinit { sqlite3_finalize(prep) }
  }
  var db: OpaquePointer!
  private static var dbTypeNames = ThreadSafeDictionary<ObjectIdentifier, String>(wrappedValue: [:])
  private static var dbTypeFields = ThreadSafeDictionary<ObjectIdentifier, FieldType>(wrappedValue: [:])
  private static func rawMirror<T>(for object: T) throws -> Mirror {
    return (object as? CustomReflectable)?.customMirror ?? Mirror(reflecting: object)
  }
  private static func fields<T: DBObject>(for object: T) throws -> FieldType {
    let mirror = Mirror(reflecting: object)
    guard mirror.displayStyle == .struct else {
      throw DecodingError.typeMismatch(T.self, .init(codingPath: [], debugDescription: "should be struct", underlyingError: nil))
    }
    return .dict(.init(try mirror.children.compactMap({ c in
      guard let n = c.label else { return nil }
      let vt = try rawMirror(for: c.value).subjectType
      return (n, .single(vt))
    }), uniquingKeysWith: { a,_ in a }))
  }
  private static func dbType<T: DBObject>(_ type: T.Type) -> String {
    return dbTypeNames[ObjectIdentifier(T.self), { String(describing: T.self) }]
  }
  private static func dbType<T: DBObject>(_ object: T) throws -> (String, FieldType) {
    return (
      dbTypeNames[ObjectIdentifier(T.self), { String(describing: T.self) }],
      try dbTypeFields[ObjectIdentifier(T.self), { try fields(for: object) }]
    )
  }
  private struct FieldDef: Hashable, Equatable {
    let name: String
    let type: String
    let nonnull: Bool
    let pk: Bool
    @inlinable var sql: String { return "\(name) \(type)\(nonnull ? " NOT NULL" : "")\(pk ? " PRIMARY KEY" : "")" }
  }
  private struct FieldChange: Hashable, Equatable {
    let from, to: FieldDef
    @inlinable var compatible: Bool {
      if from.pk || to.pk { return false }
      if to.nonnull, !from.nonnull { return false }
      switch (from.type, to.type) {
      case ("REAL", "INTEGER"), ("INTEGER", "REAL"): return true
      default: return from.type == to.type
      }
    }
  }
  func checkTable<T: DBObject>(_ object: T) throws {
    let (typename, fields) = try Self.dbType(object)
    let fieldlist: [FieldDef]
    switch fields {
    case .single(_): fatalError("Single field in object \(typename)")
    case .array(_): fatalError("Array field in object \(typename)")
    case .dict(let dict):
      guard !dict.isEmpty else { fatalError("No fields in object \(typename)") }
      fieldlist = dict.map({ (key, type) -> FieldDef in
        let primaryKey = key == T.primaryKey
        let typename: String
        let null: Bool
        switch type {
        case .array(_): fatalError("Array type for field \(key)")
        case .dict(_): fatalError("Dict type for field \(key)")
        case .single(let t):
          let nt: Any.Type
          if let t = t as? AnyOptional.Type {
            nt = t.wrappedType
            null = true
          } else {
            nt = t
            null = false
          }
          if nt is String.Type || nt is UUID.Type {
            typename = "TEXT"
          } else if nt is Bool.Type {
            typename = "BOOLEAN"
          } else if nt is UInt8.Type || nt is UInt16.Type || nt is UInt32.Type || nt is UInt64.Type || nt is UInt.Type || nt is Int8.Type || nt is Int16.Type || nt is Int32.Type || nt is Int64.Type || nt is Int.Type {
            typename = "INTEGER"
          } else if nt is Double.Type || nt is Float.Type || nt is Date.Type {
            typename = "REAL"
          } else {
            fatalError("Unsupported\(null ? "" : " optional") type: \(nt)")
          }
        }
        return .init(name: key, type: typename, nonnull: !null, pk: primaryKey)
      })
    }
    var add = Set<FieldDef>(fieldlist)
    var del = Set<FieldDef>()
    var chg = Set<FieldChange>()
    var ex: Bool = false
    do {
      var prep: OpaquePointer?
      sqlite3_prepare_v2(db, "PRAGMA table_info('\(typename)')", -1, &prep, nil)
      typename.withCString({
        _ = sqlite3_bind_text(prep, 0, $0, -1, nil)
      })
      while sqlite3_step(prep) == SQLITE_ROW {
        let name = sqlite3_column_text(prep, 1).withMemoryRebound(to: CChar.self, capacity: 0, { String(validatingUTF8: $0)! })
        let type = sqlite3_column_text(prep, 2).withMemoryRebound(to: CChar.self, capacity: 0, { String(validatingUTF8: $0)! })
        let nonnull = sqlite3_column_int(prep, 3) != 0
        let pk = sqlite3_column_int(prep, 5) != 0
        let old = FieldDef.init(name: name, type: type, nonnull: nonnull, pk: pk)
        add.subtract(add.filter({ $0.name == name }))
        if let f = fieldlist.first(where: { $0.name == name }) {
          if f.type != type || f.nonnull != nonnull || f.pk != pk {
            chg.insert(.init(from: old, to: f))
          }
        } else {
          del.insert(old)
        }
        ex = true
      }
      sqlite3_finalize(prep)
    }
    while ex, !add.isEmpty || !chg.isEmpty || !del.isEmpty {
      if let f = chg.first(where: { !$0.from.pk && !$0.to.pk && !$0.compatible }) {
        del.insert(f.from)
        add.insert(f.to)
        chg.remove(f)
      } else if chg.allSatisfy({ $0.compatible }), del.allSatisfy({ !$0.pk }), !chg.isEmpty || !del.isEmpty {
        let tmplist = fieldlist.filter({ f in !add.contains(where: { $0.name == f.name }) })
        let fieldsql = tmplist.map({ $0.sql }).joined(separator: ",\n")
        let column_list = tmplist.map({ $0.name }).joined(separator: ", ")
        let sql = [
          "PRAGMA foreign_keys=off",
          "BEGIN TRANSACTION",
          "CREATE TABLE IF NOT EXISTS \"_tmp_\(typename)\" (\n\(fieldsql)\n)",
          "INSERT INTO \"_tmp_\(typename)\"(\(column_list)) SELECT \(column_list) FROM \"\(typename)\"",
          "DROP TABLE \"\(typename)\"",
          "ALTER TABLE \"_tmp_\(typename)\" RENAME TO \"\(typename)\"",
          "COMMIT",
          "PRAGMA foreign_keys=on"
        ].joined(separator: ";\n")
        sqlite3_exec(db, sql, nil, nil, nil)
        if let err = SQLiteError(db) { throw err }
        del.removeAll()
        chg.removeAll()
      } else if let f = add.first(where: { !$0.pk && !$0.nonnull }) {
        add.remove(f)
        sqlite3_exec(db, "ALTER TABLE \"\(typename)\" ADD COLUMN \(f.sql)", nil, nil, nil)
        if let err = SQLiteError(db) { throw err }
      } else {
        if !del.isEmpty { print("TODO: MIGRATE: DEL", del) }
        if !chg.isEmpty { print("TODO: MIGRATE: CHG", chg) }
        if !add.isEmpty { print("TODO: MIGRATE: ADD", add) }

        sqlite3_exec(db, "DROP TABLE IF EXISTS \"\(typename)\"", nil, nil, nil)
        if let err = SQLiteError(db) { throw err }
        ex = false

        break
      }
    }

    if !ex { // Create table
      let fieldsql = fieldlist.map({ $0.sql }).joined(separator: ",\n")
      sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS \"\(typename)\" (\n\(fieldsql)\n)", nil, nil, nil)
      if let err = SQLiteError(db) { throw err }
    }
  }
  @inlinable func insert<T: DBObject>(_ object: T) throws -> Query {
    let (typename, fields) = try Self.dbType(object)
    try checkTable(object)
    var prep: OpaquePointer?
    let fieldlist: [String]
    switch fields {
    case .array(_): fatalError("Array fields in \(T.self)")
    case .single(_): fatalError("Single field in \(T.self)")
    case .dict(let dict): fieldlist = Array(dict.keys)
    }
    let vals = fieldlist.map({ "$" + $0 }).joined(separator: ", ")
    sqlite3_prepare_v2(db, "REPLACE INTO \"\(typename)\"(\(fieldlist.joined(separator: ", "))) VALUES(\(vals))", -1, &prep, nil)
    if let err = SQLiteError(db) { throw err }
    return .init(prep!)
  }
  @inlinable func delete<T: DBObject>(_ type: T.Type) throws -> Query {
    let typename = Self.dbType(type)
    var prep: OpaquePointer?
    sqlite3_prepare_v2(db, "DELETE FROM \(typename) WHERE \(T.primaryKey) = $\(T.primaryKey)", -1, &prep, nil)
    if let err = SQLiteError(db) { throw err }
    return .init(prep!)
  }
  @inlinable func getAll<T: DBObject>(_ type: T.Type) throws -> Query {
    let typename = Self.dbType(type)
    var prep: OpaquePointer?
    sqlite3_prepare_v2(db, "SELECT * FROM \(typename)", -1, &prep, nil)
    if let err = SQLiteError(db) { throw err }
    return .init(prep!)
  }
  @inlinable func get<T: DBObject>(_ type: T.Type) throws -> Query {
    let typename = Self.dbType(type)
    var prep: OpaquePointer?
    sqlite3_prepare_v2(db, "SELECT * FROM \(typename) WHERE \(T.primaryKey) = $\(T.primaryKey)", -1, &prep, nil)
    if let err = SQLiteError(db) { throw err }
    return .init(prep!)
  }
}

