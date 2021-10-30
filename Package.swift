// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "SwiftQLite",
  platforms: [
    .macOS(.v10_10),
    .iOS(.v10),
    .macCatalyst(.v13),
  ],
  products: [
    .library(name: "SwiftQLite", targets: ["SwiftQLite", "SwiftQLite.Default"]),
  ],
  dependencies: [
    .package(name: "ThreadSafeContainer", url: "https://github.com/djphoenix/ThreadSafeContainer.git", branch: "main")
  ],
  targets: [
    .target(name: "SwiftQLite", dependencies: [ .product(name: "ThreadSafeContainer", package: "ThreadSafeContainer") ]),
    .target(name: "SwiftQLite.Default", dependencies: ["SwiftQLite"]),
    .testTarget(name: "SwiftQLiteTests", dependencies: ["SwiftQLite.Default"]),
  ]
)

