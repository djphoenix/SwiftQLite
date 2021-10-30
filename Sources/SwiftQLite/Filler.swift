import SQLite3

enum FieldType: CustomStringConvertible {
  case single(Any.Type)
  case array([FieldType])
  case dict([String:FieldType])

  @inlinable var description: String {
    switch self {
    case .single(let type):
      return String(describing: type)
    case .array(let arr):
      return "[" + arr.map({ $0.description }).joined(separator: ",") + "]"
    case .dict(let dict):
      return "[" + (dict.isEmpty ? ":" : "") + dict.map({ $0.key + ":" + $0.value.description }).joined(separator: ",") + "]"
    }
  }
}

struct IntKey: CodingKey {
  let intValue: Int?
  @inlinable var stringValue: String { return String(intValue ?? 0) }
  @inlinable init?(stringValue: String) {
    guard let int = Int(stringValue) else { return nil }
    intValue = int
  }
  @inlinable init(intValue int: Int) { intValue = int }
}

struct FillerKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol, KeyedEncodingContainerProtocol {
  let decoder: Filler
  @inlinable var codingPath: [CodingKey] { return decoder.codingPath }

  @inlinable func encodeNil(forKey key: Key) throws {
    let container = superEncoder(forKey: key).singleValueContainer()
    return try (container as! FillerSingleContainer).encodeNil()
  }
  @inlinable func encode<T>(_ value: T, forKey key: Key) throws where T : Encodable {
    let container = superEncoder(forKey: key).singleValueContainer()
    return try (container as! FillerSingleContainer).encode(value)
  }

  @inlinable var allKeys: [Key] {
    return (0 ..< sqlite3_column_count(decoder.statement)).compactMap({
      .init(stringValue: String(cString: sqlite3_column_name(decoder.statement, $0)))
    })
  }
  @inlinable func contains(_ key: Key) -> Bool {
    return allKeys.filter({ type(of: $0) == Key.self }).contains(where: { $0.stringValue == key.stringValue })
  }
  @inlinable func decodeNil(forKey key: Key) throws -> Bool {
    return try superDecoder(forKey: key).singleValueContainer().decodeNil()
  }
  @inlinable func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
    return try superDecoder(forKey: key).singleValueContainer().decode(type)
  }
  @inlinable func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
    return try superDecoder(forKey: key).container(keyedBy: NestedKey.self)
  }
  @inlinable func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
    return try superDecoder(forKey: key).unkeyedContainer()
  }
  @inlinable func superDecoder() throws -> Decoder {
    guard let parent = decoder.parent else {
      throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: codingPath, debugDescription: "Root container"))
    }
    return parent
  }
  @inlinable func superDecoder(forKey key: Key) throws -> Decoder {
    if key.stringValue == "super" && key.intValue == 0 { return try superDecoder() }
    return Filler.child(of: decoder, key: key)
  }
  @inlinable func superEncoder() -> Encoder {
    guard let parent = decoder.parent else {
      return decoder
    }
    return parent
  }
  @inlinable func superEncoder(forKey key: Key) -> Encoder {
    if key.stringValue == "super" && key.intValue == 0 { return superEncoder() }
    return Filler.child(of: decoder, key: key)
  }
  @inlinable func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
    return superEncoder(forKey: key).container(keyedBy: keyType)
  }
  @inlinable func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
    return superEncoder(forKey: key).unkeyedContainer()
  }
}

struct FillerArrayEncodingContainer: UnkeyedEncodingContainer {
  let decoder: Filler
  @inlinable var codingPath: [CodingKey] { return decoder.codingPath }

  private(set) var count: Int = 0
  private var nextChild: Filler {
    mutating get {
      defer { count += 1 }
      return Filler.child(of: decoder, key: IntKey(intValue: count))
    }
  }
  @inlinable mutating func encode<T>(_ value: T) throws where T : Encodable {
    let container: SingleValueEncodingContainer = nextChild.singleValueContainer()
    return try (container as! FillerSingleContainer).encode(value)
  }
  @inlinable mutating func encodeNil() throws {
    let container: SingleValueEncodingContainer = nextChild.singleValueContainer()
    return try (container as! FillerSingleContainer).encodeNil()
  }
  @inlinable mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
    return nextChild.container(keyedBy: keyType)
  }
  @inlinable mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
    return nextChild.unkeyedContainer()
  }
  @inlinable func superEncoder() -> Encoder {
    guard let parent = decoder.parent else { return decoder }
    return parent
  }
}

struct FillerArrayDecodingContainer: UnkeyedDecodingContainer {
  let decoder: Filler
  @inlinable var codingPath: [CodingKey] { return decoder.codingPath }

  @inlinable var count: Int? {
    return nil // TODO: count statement
  }
  @inlinable var isAtEnd: Bool {
    let res = sqlite3_step(decoder.statement)
    if res == SQLITE_ROW {
      return false
    }
    return true
  }
  var currentIndex: Int = 0
  private var currentChild: Filler {
    get {
      return Filler.child(of: decoder, key: IntKey(intValue: currentIndex))
    }
  }
  @inlinable func decodeNil() throws -> Bool {
    return try currentChild.singleValueContainer().decodeNil()
  }
  @inlinable func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
    return try currentChild.singleValueContainer().decode(type)
  }
  @inlinable func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
    return try currentChild.container(keyedBy: type)
  }
  @inlinable func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
    return try currentChild.unkeyedContainer()
  }
  @inlinable func superDecoder() throws -> Decoder {
    guard let parent = decoder.parent else {
      throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: codingPath, debugDescription: "Root container"))
    }
    return parent
  }
}

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

struct FillerSingleContainer: SingleValueDecodingContainer, SingleValueEncodingContainer {
  let decoder: Filler

  @inlinable var codingPath: [CodingKey] { return decoder.codingPath }
  private var field: String { return "$" + decoder.codingKey!.stringValue }
  @inlinable func encodeNil() throws {
    guard decoder.parent?.codingKey == nil else {
      throw EncodingError.invalidValue(nil as Optional<Any> as Any, EncodingError.Context.init(codingPath: codingPath, debugDescription: "Not implemented"))
    }
    let idx = sqlite3_bind_parameter_index(decoder.statement, field)
    sqlite3_bind_null(decoder.statement, idx)
  }
  @inlinable func encode(_ value: Bool) throws {
    guard decoder.parent?.codingKey == nil else {
      throw EncodingError.invalidValue(value, EncodingError.Context.init(codingPath: codingPath, debugDescription: "Not implemented"))
    }
    let idx = sqlite3_bind_parameter_index(decoder.statement, field)
    sqlite3_bind_int(decoder.statement, idx, value ? 1 : 0)
  }
  @inlinable func encode(_ value: String) throws {
    guard decoder.parent?.codingKey == nil else {
      throw EncodingError.invalidValue(value, EncodingError.Context.init(codingPath: codingPath, debugDescription: "Not implemented"))
    }
    let idx = sqlite3_bind_parameter_index(decoder.statement, field)
    value.utf8CString.withUnsafeBytes({
      let buf = strdup($0.bindMemory(to: CChar.self).baseAddress!)!
      _ = sqlite3_bind_text(decoder.statement, idx, buf, -1, { $0?.deallocate() })
    })
  }
  @inlinable func encode(_ value: Double) throws {
    guard decoder.parent?.codingKey == nil else {
      throw EncodingError.invalidValue(value, EncodingError.Context.init(codingPath: codingPath, debugDescription: "Not implemented"))
    }
    let idx = sqlite3_bind_parameter_index(decoder.statement, field)
    sqlite3_bind_double(decoder.statement, idx, value)
  }
  @inlinable func encode(_ value: Float) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: Int) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: Int8) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: Int16) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: Int32) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: Int64) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: UInt) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: UInt8) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: UInt16) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: UInt32) throws {
    try encode(Double(value))
  }
  @inlinable func encode(_ value: UInt64) throws {
    try encode(Double(value))
  }
  @inlinable func encode<T>(_ value: T) throws where T : Encodable {
    try value.encode(to: decoder)
  }

  private func columnIdx(_ statement: OpaquePointer) throws -> Int32 {
    guard let column = decoder.codingKey?.stringValue else {
      throw DecodingError.dataCorruptedError(in: self, debugDescription: "Column name is not set")
    }
    if sqlite3_data_count(statement) == 0 {
      throw DecodingError.valueNotFound(Any.self, DecodingError.Context(codingPath: decoder.codingPath, debugDescription: "No data in query"))
    }
    for i in (0 ..< sqlite3_column_count(statement)) {
      let n = String(cString: sqlite3_column_name(statement, i))
      if n == column { return Int32(i) }
    }
    throw DecodingError.dataCorruptedError(in: self, debugDescription: "Column \(column) not found")
  }
  @inlinable func decodeNil() -> Bool {
    guard let idx = try? columnIdx(decoder.statement) else { return false }
    return sqlite3_column_type(decoder.statement, idx) == SQLITE_NULL
  }
  @inlinable func decode(_ type: Bool.Type) throws -> Bool {
    return sqlite3_column_int(decoder.statement, try columnIdx(decoder.statement)) != 0
  }
  @inlinable func decode(_ type: String.Type) throws -> String {
    let idx = try columnIdx(decoder.statement)
    guard let val = sqlite3_column_text(decoder.statement, idx) else {
      throw DecodingError.typeMismatch(type, DecodingError.Context(codingPath: decoder.codingPath, debugDescription: "Not a string"))
    }
    let len = Int(sqlite3_column_bytes(decoder.statement, idx))
    return val.withMemoryRebound(to: CChar.self, capacity: len, { String(validatingUTF8: $0) }) ?? ""
  }
  @inlinable func decode(_ type: Double.Type) throws -> Double {
    return sqlite3_column_double(decoder.statement, try columnIdx(decoder.statement))
  }
  @inlinable func decode(_ type: Float.Type) throws -> Float {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: Int.Type) throws -> Int {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: Int8.Type) throws -> Int8 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: Int16.Type) throws -> Int16 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: Int32.Type) throws -> Int32 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: Int64.Type) throws -> Int64 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: UInt.Type) throws -> UInt {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: UInt8.Type) throws -> UInt8 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: UInt16.Type) throws -> UInt16 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: UInt32.Type) throws -> UInt32 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode(_ type: UInt64.Type) throws -> UInt64 {
    return type.init(try decode(Double.self))
  }
  @inlinable func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
    return try type.init(from: decoder)
  }
}

indirect enum Filler: Decoder, Encoder {
  case root(OpaquePointer)
  case child(of: Filler, key: CodingKey)

  fileprivate var codingKey: CodingKey? {
    switch self {
    case .root(_): return nil
    case .child(of: _, key: let key): return key
    }
  }
  fileprivate var parent: Filler? {
    switch self {
    case .root(_): return nil
    case .child(of: let parent, key: _): return parent
    }
  }
  fileprivate var statement: OpaquePointer {
    switch self {
    case .root(let stmt): return stmt
    case .child(of: let parent, key: _): return parent.statement
    }
  }

  @inlinable var codingPath: [CodingKey] {
    var path = [CodingKey]()
    var f: Filler = self
    while case .child(of: let parent, key: let key) = f {
      path.insert(key, at: 0)
      f = parent
    }
    return path
  }
  @inlinable var userInfo: [CodingUserInfoKey : Any] { return [:] }
  @inlinable func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
    return KeyedDecodingContainer<Key>(FillerKeyedContainer<Key>(decoder: self))
  }
  @inlinable func unkeyedContainer() throws -> UnkeyedDecodingContainer {
    return FillerArrayDecodingContainer(decoder: self)
  }
  @inlinable func singleValueContainer() throws -> SingleValueDecodingContainer {
    return FillerSingleContainer(decoder: self)
  }
  @inlinable func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
    return KeyedEncodingContainer<Key>(FillerKeyedContainer(decoder: self))
  }
  @inlinable func unkeyedContainer() -> UnkeyedEncodingContainer {
    return FillerArrayEncodingContainer(decoder: self)
  }
  @inlinable func singleValueContainer() -> SingleValueEncodingContainer {
    return FillerSingleContainer(decoder: self)
  }
}

