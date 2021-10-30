import XCTest
import Foundation
import Darwin
import ThreadSafeContainer
import SwiftQLite

struct TestObject: DBObject {
  static var primaryKey: String = "primaryKey"
  var primaryKey: String
  var value: Int
}

let dbURL = FileManager.default.temporaryDirectory.appendingPathComponent("db.sqlite")
var instances = ThreadSafeDictionary<Thread, DBInstance>(wrappedValue: [:])
func dbInstance() throws -> DBInstance {
  return try instances[Thread.current, { try DBInstance(file: dbURL.path) }]
}

final class BasicTests: XCTestCase {

  override func setUp() {
    if FileManager.default.fileExists(atPath: dbURL.path) {
      try? FileManager.default.removeItem(at: dbURL)
    }
  }
  override func tearDown() {
    instances.removeAll()
    try? FileManager.default.removeItem(at: dbURL)
  }

  func testInsertDelete() throws {
    let db = try dbInstance()
    try db.insert([
      TestObject(primaryKey: "key1", value: 1),
      TestObject(primaryKey: "key2", value: 2),
      TestObject(primaryKey: "key3", value: 3),
    ])
    XCTAssertEqual(try db.get(allOf: TestObject.self).count, 3)
    try db.insert(TestObject(primaryKey: "key3", value: 33))
    XCTAssertEqual(try db.get(allOf: TestObject.self).count, 3)
    try db.delete(TestObject.self, try db.get(allOf: TestObject.self).map({ $0.primaryKey }))
    XCTAssertTrue(try db.get(allOf: TestObject.self).isEmpty)
  }

  enum TestRawEnum: String, Codable, CustomReflectable {
    case one, two
    var customMirror: Mirror { return .init(reflecting: self.rawValue) }
  }

  struct TestRawStruct: RawRepresentable, Codable, Equatable, CustomReflectable {
    var rawValue: Double
    var customMirror: Mirror { return .init(reflecting: self.rawValue) }
  }

  func testTypes() throws {
    struct TypeTestObject: DBObject, Equatable {
      static var primaryKey: String = "primaryKey"
      var primaryKey: String = "test"
      var boolValue: Bool = true
      var intValue: Int = 1
      var floatValue: Float = 3.14
      var doubleValue: Double = 3.141592
      var intervalValue: TimeInterval = 10
      var dateValue: Date = .init(timeIntervalSinceReferenceDate: 3600)
      var uuidValue: UUID = .init()
      var rawEnumValue: TestRawEnum = .one
      var rawStructValue: TestRawStruct = .init(rawValue: 0.5)
      static let test = Self.init()
    }
    let db = try dbInstance()
    try db.insert(TypeTestObject.test)
    let arr = try db.get(allOf: TypeTestObject.self)
    XCTAssertEqual(arr.count, 1)
    XCTAssertEqual(arr[0], TypeTestObject.test)
  }

  func testOptionalTypes() throws {
    struct TypeOptTestObject: DBObject, Equatable {
      static var primaryKey: String = "primaryKey"
      var primaryKey: String
      var strValue: String?
      var boolValue: Bool?
      var intValue: Int?
      var floatValue: Float?
      var doubleValue: Double?
      var intervalValue: TimeInterval?
      var dateValue: Date?
      var uuidValue: UUID?

      static let some = Self.init(
        primaryKey: "some",
        strValue: "value",
        boolValue: true,
        intValue: 1,
        floatValue: 3.14,
        doubleValue: 3.141592,
        intervalValue: 10,
        dateValue: .init(timeIntervalSinceReferenceDate: 3600),
        uuidValue: UUID()
      )
      static let none = Self.init(primaryKey: "none")
    }
    let db = try dbInstance()
    try db.insert([
      TypeOptTestObject.some,
      TypeOptTestObject.none,
    ])
    let arr = try db.get(allOf: TypeOptTestObject.self)
    XCTAssertEqual(arr.count, 2)
    XCTAssertTrue(arr.contains(TypeOptTestObject.none))
    XCTAssertTrue(arr.contains(TypeOptTestObject.some))
  }

  func testMigrations() throws {
    enum V1 {
      struct TestMigrateObject: DBObject {
        static var primaryKey: String = "primaryKey"
        var primaryKey: String
      }
    }
    enum V2 {
      struct TestMigrateObject: DBObject {
        static var primaryKey: String = "primaryKey"
        var primaryKey: String
        var addedField: Int?
      }
    }
    enum V3 {
      struct TestMigrateObject: DBObject {
        static var primaryKey: String = "primaryKey"
        var primaryKey: String
        var addedField: Double?
      }
    }

    let db = try dbInstance()
    try db.insert(V1.TestMigrateObject(primaryKey: "test"))
    // Migrate to V2 - add "addedField"
    let arr2 = try db.get(allOf: V2.TestMigrateObject.self)
    XCTAssertEqual(arr2.count, 1)
    XCTAssertEqual(arr2.first?.addedField, nil)
    try db.insert(V2.TestMigrateObject(primaryKey: "test", addedField: 123))
    XCTAssertEqual(try db.get("test", of: V2.TestMigrateObject.self)?.addedField, 123)
    // Migrate to V1 - remove "addedField"
    XCTAssertEqual(try db.get(allOf: V1.TestMigrateObject.self).count, 1)
    // Migrate to V2 - add "addedField"
    try db.insert(V2.TestMigrateObject(primaryKey: "test", addedField: 123))
    // Migrate to V3 - change "addedField" to Double
    try db.insert(V3.TestMigrateObject(primaryKey: "test2", addedField: nil))
    try db.delete(V3.TestMigrateObject.self, "test2")
    XCTAssertEqual(try db.get(allOf: V3.TestMigrateObject.self).count, 1)
    XCTAssertEqual(try db.get("test", of: V3.TestMigrateObject.self)?.addedField, 123)
    // Migrate to V2 - change "addedField" to Int
    try db.insert(V2.TestMigrateObject(primaryKey: "test2", addedField: nil))
    try db.delete(V2.TestMigrateObject.self, "test2")
    XCTAssertEqual(try db.get(allOf: V2.TestMigrateObject.self).count, 1)
    XCTAssertEqual(try db.get("test", of: V2.TestMigrateObject.self)?.addedField, 123)
  }
}

private let testItemCount = 10000
private let dq = DispatchQueue.global(qos: .utility)

final class MultiThreadEmptyTests: XCTestCase {
  override func setUp() {
    if FileManager.default.fileExists(atPath: dbURL.path) {
      try? FileManager.default.removeItem(at: dbURL)
    }
  }
  override func tearDown() {
    instances.removeAll()
    try? FileManager.default.removeItem(at: dbURL)
  }

  func testMultiThreadWrite() throws {
    let exp = XCTestExpectation()
    exp.expectedFulfillmentCount = testItemCount
    (0 ..< testItemCount).forEach({ i in
      dq.async {
        do {
          let db = try! dbInstance()
          try! db.insert(TestObject(primaryKey: "key\(i)", value: i))
        }
        exp.fulfill()
      }
    })
    XCTWaiter().wait(for: [exp], timeout: 60)
    let db = try dbInstance()
    XCTAssertEqual(try db.get(allOf: TestObject.self).count, testItemCount)
  }

  func testMultiThreadWriteReadDelete() throws {
    let exp = XCTestExpectation()
    exp.expectedFulfillmentCount = testItemCount
    (0 ..< testItemCount).forEach({ i in
      dq.async {
        let db = try! dbInstance()
        try! db.insert(TestObject(primaryKey: "key\(i)", value: i))
        dq.async {
          let db = try! dbInstance()
          XCTAssertEqual(try! db.get("key\(i)", of: TestObject.self)?.value, i)
          dq.async {
            let db = try! dbInstance()
            try! db.delete(TestObject.self, "key\(i)")
            exp.fulfill()
          }
        }
      }
    })
    XCTWaiter().wait(for: [exp], timeout: 60)
    let db = try dbInstance()
    XCTAssertTrue(try db.get(allOf: TestObject.self).isEmpty)
  }
}

final class MultiThreadFullTests: XCTestCase {
  override func setUp() {
    if FileManager.default.fileExists(atPath: dbURL.path) {
      try? FileManager.default.removeItem(at: dbURL)
    }
    try! dbInstance().insert((0 ..< testItemCount).map({ i in TestObject(primaryKey: "key\(i)", value: i) }))
  }
  override func tearDown() {
    instances.removeAll()
    try? FileManager.default.removeItem(at: dbURL)
  }

  func testMultiThreadRead() throws {
    let exp = XCTestExpectation()
    exp.expectedFulfillmentCount = testItemCount
    (0 ..< testItemCount).forEach({ i in
      dq.async {
        let db = try! dbInstance()
        XCTAssertEqual(try! db.get("key\(i)", of: TestObject.self)?.value, i)
        exp.fulfill()
      }
    })
    XCTWaiter().wait(for: [exp], timeout: 60)
  }

  func testMultiThreadDelete() throws {
    let exp = XCTestExpectation()
    exp.expectedFulfillmentCount = testItemCount
    (0 ..< testItemCount).forEach({ i in
      dq.async {
        let db = try! dbInstance()
        try! db.delete(TestObject.self, "key\(i)")
        exp.fulfill()
      }
    })
    XCTWaiter().wait(for: [exp], timeout: 60)
    let db = try dbInstance()
    XCTAssertTrue(try db.get(allOf: TestObject.self).isEmpty)
  }

  func testMultiThreadGetDelete() throws {
    let exp = XCTestExpectation()
    exp.expectedFulfillmentCount = testItemCount
    (0 ..< testItemCount).forEach({ i in
      dq.async {
        let db = try! dbInstance()
        try! db.delete(TestObject.self, db.get("key\(i)", of: TestObject.self).flatMap({ [$0.primaryKey] }) ?? [])
        exp.fulfill()
      }
    })
    XCTWaiter().wait(for: [exp], timeout: 60)
    let db = try dbInstance()
    XCTAssertTrue(try db.get(allOf: TestObject.self).isEmpty)
  }
}

