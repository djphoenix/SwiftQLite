import Foundation
import ThreadSafeContainer
import SwiftQLite

internal let dbURL = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent("db.sqlite")

private var _dbKey = UInt8(0)
private func threadDBInstance() -> DBInstance {
  let thread = Thread.current
  if let instance = objc_getAssociatedObject(thread, &_dbKey) as? DBInstance { return instance }
  let instance = try! DBInstance(file: dbURL.path)
  objc_setAssociatedObject(thread, &_dbKey, instance, .OBJC_ASSOCIATION_RETAIN)
  return instance
}

public extension DBObject {
  static var all: Array<Self> {
    do {
      return try threadDBInstance().get(allOf: Self.self)
    } catch let e {
      print(e)
      return []
    }
  }
  static func get(_ key: KeyType) -> Self? {
    do {
      return try threadDBInstance().get(key, of: Self.self)
    } catch let e {
      if case .valueNotFound(let type, let context)? = e as? DecodingError, type == Any.self, context.debugDescription == "No data in query" {
        return nil
      }
      print(e)
      return nil
    }
  }
  func insert() {
    do {
      try threadDBInstance().insert([self])
      NotificationCenter.default.post(name: .DatabaseChanged(self), object: nil)
    } catch let e {
      print(e)
    }
  }
  static func delete(_ key: KeyType) {
    do {
      try threadDBInstance().delete(self, [key])
      NotificationCenter.default.post(name: .DatabaseChanged(self), object: nil)
    } catch let e {
      print(e)
    }
  }
  @inlinable func delete() { Self.delete(self.primaryKey) }
}

public extension Collection where Element: DBObject {
  func delete() {
    do {
      try threadDBInstance().delete(Element.self, self.lazy.map({ $0.primaryKey }))
    } catch let e {
      print(e)
    }
    NotificationCenter.default.post(name: .DatabaseChanged(Element.self), object: nil)
  }
  func insert() {
    do {
      try threadDBInstance().insert(self)
    } catch let e {
      print(e)
    }
    NotificationCenter.default.post(name: .DatabaseChanged(Element.self), object: nil)
  }
}

private var notificationNames = ThreadSafeDictionary<ObjectIdentifier, Notification.Name>(wrappedValue: [:])
public extension Notification.Name {
  static func DatabaseChanged<T: DBObject>(_ type: T.Type) -> Notification.Name {
    return notificationNames[ObjectIdentifier(type), { Self.DatabaseChanged(String(describing: type)) }]
  }
  @inlinable static func DatabaseChanged<T: DBObject>(_ object: T) -> Notification.Name {
    return .DatabaseChanged(type(of: object))
  }
  @inlinable static func DatabaseChanged(_ type: String) -> Notification.Name {
    return Notification.Name(rawValue: "SwiftQLDatabaseChanged.\(type)")
  }
}
