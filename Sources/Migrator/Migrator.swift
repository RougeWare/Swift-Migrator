//
//  Migrator.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-20.
//

import Foundation

import FunctionTools
import SemVer



/// Something which migrates data from one type to a new type
public struct Migrator {
    
    /// Identifies the domain of migrators which can transition a kind of data.
    ///
    /// This value is necessary for chaining together multiple migrators within the same. For example, let's say you're upgrading a user from `1.0.0` to `3.2.7`, and you have migrators that go from `1.0.0` to `1.2.0`
    ///
    /// This also ensures that domains never cross over. For example, you might have two sets of migrators: five for the User Profile and three for the Metadata Database. Both of these might migrate a `[String : Any]` dictionary, but it wouldn't make sense to migrate MyApp `1.2.3`'s User Info to MyApp `2.0.0`'s Metadata Database. This ensures that such a thing never happens.
    internal let migratorDomain: Domain
    
    /// The oldest possible version of the old executable that's being migrated from.
    /// When this migrator's `migrate` function starts, the value it is given is fit to use in this old version of the executable
    ///
    /// For example, if this migrator is in charge of migrating `struct Foo` in app version `1.2.3` to `struct Bar` in app version `2.0.0`, this value is `1.2.3`
    internal let oldExecutableVersion: SemVer
    
    
    /// The version of the new executable that's being migrated to.
    /// When this migrator's `migrate` function completes, the value it emits will be fit to use in this new version of the executable
    ///
    /// For example, if this migrator is in charge of migrating `struct Foo` in app version `1.2.3` to `struct Bar` in app version `2.0.0`, this value is `2.0.0`
    internal let newExecutableVersion: SemVer
    
    
    /// Migrates the given old data to its new format
    ///
    /// This is intended to be run synchronously; it performs its work on the thread on which it was called, and then returns normally. The migration engine will ensure that is done in a performant way.
    ///
    /// - Returns: The result of the migration
    private let migrationFunction: MigrationFunction
    
    
    /// Creates a new DataMigrator
    ///
    /// - Parameters:
    ///   - family:               Identifies the family of migrators which can transition a kind of data.
    ///
    ///     This value is necessary for chaining together multiple migrators. For example, you might have two sets of migrators: five for the User Profile and three for the Metadata Database. Both of these might migrate a `[String : Any]` dictionary, but it wouldn't make sense to migrate MyApp `1.2.3`'s User Info to MyApp `2.0.0`'s Metadata Database. This ensures that such a thing never happens.
    ///
    ///
    ///   - oldExecutableVersion: The oldest possible version of the old executable from which data might be migrated. This **must** be less than `oldExecutableVersion`; otherwise, a precondition failure is triggered and the process will crash.
    ///
    ///     For example, if this migrator is in charge of migrating `struct Foo` in app version `1.2.3` to `struct Bar` in app version `2.0.0`, this value is `1.2.3`
    ///
    ///
    ///   - newExecutableVersion: The version of the new executable to which data might be migrated. This **must** be greater than `oldExecutableVersion`; otherwise, a precondition failure is triggered and the process will crash.
    ///
    ///     For example, if this migrator is in charge of migrating `struct Foo` in app version `1.2.3` to `struct Bar` in app version `2.0.0`, this value is `2.0.0`
    ///
    ///
    ///   - migrationFunction:    The function which can perform the migration. Exactly how that is performed is up to this function.
    ///
    ///     This is intended to be run synchronously; it performs its work on the thread on which it was called, and then returns normally. The migration engine will ensure that is done in a performant way.
    public init<ID: Hashable>(family: ID, oldExecutableVersion: SemVer, newExecutableVersion: SemVer, migrationFunction: @escaping MigrationFunction) {
        
        guard oldExecutableVersion < newExecutableVersion else {
            preconditionFailure("Old version must be older than new version")
        }
        
        self.migratorDomain = family.hashValue
        self.oldExecutableVersion = oldExecutableVersion
        self.newExecutableVersion = newExecutableVersion
        self.migrationFunction = migrationFunction
    }
}



public extension Migrator {
    
    /// The result of migrating
    enum MigrationResult {
        
        /// The migration was successful
        case success
        
        /// The migration didn't have to be performed after all
        case unnecessary
        
        /// The migration failed
        /// - Parameter cause: The error which caused the failure
        case failed(cause: Error)
    }
    
    
    
    /// The type of function which migrates some old data to some new data.
    ///
    /// This is intended to be run synchronously; it performs its work on the thread on which it was called, and then returns normally. The migration engine will ensure that is done in a performant way.
    ///
    /// - Returns: The result of the migration
    typealias MigrationFunction = Generator<MigrationResult>
}



internal extension Migrator {
    
    /// Performs this migrator's migration on this thread
    ///
    /// - Returns: The result of migration
    func migrate() -> MigrationResult {
        migrationFunction()
    }
    
    
    
    /// Uniquely identifies a domain of migrators
    typealias Domain = Int
}



// MARK: - Conformance

extension Migrator: Comparable {
    
    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.oldExecutableVersion < rhs.oldExecutableVersion
    }
    
    
    public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.migratorDomain == rhs.migratorDomain
            && lhs.oldExecutableVersion == rhs.oldExecutableVersion
    }
}
