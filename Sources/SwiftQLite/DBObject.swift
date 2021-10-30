public protocol DBObject: Codable {
  associatedtype KeyType: Codable
  static var primaryKey: String { get }
  var primaryKey: KeyType { get }
}
