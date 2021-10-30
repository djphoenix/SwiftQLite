import SQLite3

private struct PrimaryKey<T: DBObject>: CodingKey {
  @inlinable var stringValue: String { return T.primaryKey }
  @inlinable init?(stringValue: String) { return nil }
  @inlinable var intValue: Int? { return nil }
  @inlinable init?(intValue: Int) { return nil }
  @inlinable init() {}
}

public class DBInstance {
  private let db: OpaquePointer
  private init(db: OpaquePointer) {
    do {
      pthread_rwlock_wrlock(Self.rwlock)
      defer { pthread_rwlock_unlock(Self.rwlock) }

      var prep: OpaquePointer?
      sqlite3_prepare_v2(db, "PRAGMA journal_mode=WAL", -1, &prep, nil)
      sqlite3_step(prep)
      sqlite3_finalize(prep)
    }

    self.db = db
    self.prepared.db = self.db
  }
  public convenience init(file: String) throws {
    var db: OpaquePointer?
    sqlite3_open_v2(file, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil)
    if let err = SQLiteError(db!) { throw err }
    self.init(db: db!)
  }
  deinit {
    sqlite3_close_v2(db)
  }

  private var prepared = Prepared()

  private static let rwlock = { () -> UnsafeMutablePointer<pthread_rwlock_t> in
    let m = UnsafeMutablePointer<pthread_rwlock_t>.allocate(capacity: 1)
    pthread_rwlock_init(m, nil)
    return m
  }()

  public func insert<T: DBObject, C: Collection>(_ objects: C) throws where C.Element == T {
    guard !objects.isEmpty else { return }
    pthread_rwlock_wrlock(Self.rwlock)
    defer { pthread_rwlock_unlock(Self.rwlock) }

    let prep = try prepared.insert(objects.first!)
    for object in objects {
      sqlite3_clear_bindings(prep.prep)
      sqlite3_reset(prep.prep)
      try object.encode(to: Filler.root(prep.prep))
      if let err = SQLiteError(db) { throw err }
      sqlite3_step(prep.prep)
      if let err = SQLiteError(db) { throw err }
    }
  }
  @inlinable public func insert<T: DBObject>(_ objects: T...) throws {
    try insert(objects)
  }
  public func delete<T: DBObject, C: Collection>(_ type: T.Type, _ keys: C) throws where C.Element == T.KeyType {
    pthread_rwlock_wrlock(Self.rwlock)
    defer { pthread_rwlock_unlock(Self.rwlock) }

    let prep = try prepared.delete(T.self)
    for key in keys {
      sqlite3_reset(prep.prep)
      try key.encode(to: Filler.child(of: Filler.root(prep.prep), key: PrimaryKey<T>()))
      sqlite3_step(prep.prep)
      if let err = SQLiteError(db) { throw err }
    }
  }
  @inlinable public func delete<T: DBObject>(_ type: T.Type, _ keys: T.KeyType...) throws {
    try delete(type, keys)
  }
  public func get<T: DBObject>(allOf type: T.Type) throws -> [T] {
    pthread_rwlock_rdlock(Self.rwlock)
    defer { pthread_rwlock_unlock(Self.rwlock) }
    let prep = try prepared.getAll(T.self)
    return try [T].init(from: Filler.root(prep.prep))
  }
  public func get<T: DBObject>(_ key: T.KeyType, of: T.Type) throws -> T? {
    pthread_rwlock_rdlock(Self.rwlock)
    defer { pthread_rwlock_unlock(Self.rwlock) }
    let prep = try prepared.get(T.self)
    let filler = Filler.root(prep.prep)
    try key.encode(to: Filler.child(of: filler, key: PrimaryKey<T>()))
    return try [T].init(from: filler).first
  }
}
