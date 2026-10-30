// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "sqlite-event-store",
  platforms: [
    .macOS(.v26)
  ],
  products: [
    .library(name: "SQLiteEventStore", targets: ["SQLiteEventStore"])
  ],
  dependencies: [
    .package(url: "https://github.com/vapor/sqlite-nio.git", from: "1.0.0"),
    .package(url: "https://github.com/akbashev/cluster-event-sourcing.git", branch: "turgai-integration"),
  ],
  targets: [
    .target(
      name: "SQLiteEventStore",
      dependencies: [
        .product(name: "SQLiteNIO", package: "sqlite-nio"),
        .product(name: "EventSourcing", package: "cluster-event-sourcing"),
      ]
    ),
    .testTarget(
      name: "SQLiteEventStoreTests",
      dependencies: [
        "SQLiteEventStore"
      ]
    ),
  ]
)
