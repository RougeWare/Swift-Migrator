// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Migrator",
    
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "Migrator",
            targets: ["Migrator"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(name: "Atomic", url: "https://github.com/RougeWare/Swift-Atomic.git", from: "0.2.0"),
        .package(name: "FunctionTools", url: "https://github.com/RougeWare/Swift-Function-Tools.git", from: "1.2.3"),
        .package(name: "SemVer", url: "https://github.com/RougeWare/Swift-SemVer.git", from: "3.0.0-Alpha.7"),
        .package(name: "SortedArray", url: "https://github.com/ole/SortedArray.git", from: "0.7.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "Migrator",
            dependencies: [
                "Atomic",
                "FunctionTools",
                "SemVer",
                "SortedArray",
            ]),
        .testTarget(
            name: "MigratorTests",
            dependencies: ["Migrator"]),
    ]
)
