// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "NeXTBase",
    platforms: [.iOS(.v18), .macOS(.v15), .tvOS(.v18), .watchOS(.v10)],
    products: [
        .library(
            name: "NeXTBase",
            targets: ["NeXTBase"]),
    ],
    targets: [
        .target(
            name: "NeXTBase",
            swiftSettings: [
                // Enable whole module optimization
                .unsafeFlags(["-whole-module-optimization"], .when(configuration: .release)),
                // Optimize for size
                .unsafeFlags(["-Osize"], .when(configuration: .release)),
                // Strip all symbols
                .unsafeFlags(["-Xlinker", "-strip-all"], .when(configuration: .release)),
                // Enable dead code stripping
                .unsafeFlags(["-Xlinker", "-dead_strip"], .when(configuration: .release)),
            ]
        ),
        .testTarget(
            name: "NeXTBaseTests",
            dependencies: ["NeXTBase"]
        ),
    ]
)
