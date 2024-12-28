// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "NeXTBase",
    platforms: [.iOS(.v17), .macOS(.v14), .tvOS(.v18), .watchOS(.v10)],
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
