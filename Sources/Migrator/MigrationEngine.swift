//
//  MigrationEngine.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-20.
//

import Combine
import Foundation

import Atomic
import FunctionTools
import SemVer
import SortedArray



/// Coordinates all migrations in parallel, across any migrator families
public final class MigrationEngine {
    
    private let id = UUID()
    
    private var sortedMigratorPool = SortedArray<Migrator>(areInIncreasingOrder: >)
    
    private let migrationStartupCoordinatorQueue: DispatchQueue
    private let migrationPerformerQueue: DispatchQueue
    
    private var isMigrationUnderway = false
    
    @Published
    private var progress = Progress.starting
    
    private var migrationUnderwaySubscriber: AnyCancellable?
    
    
    init() {
        migrationStartupCoordinatorQueue = DispatchQueue(label: "MigrationEngine startup coordinator queue \(id)")
        migrationPerformerQueue = DispatchQueue(label: "MigrationEngine performer queue \(id)")
    }
}



// MARK: - Managing which migrators are active

public extension MigrationEngine {
    
    /// Registers the given migrator with this engine, so that it will be considered when performing migration.
    ///
    /// If the given migrator has already been registered, it **isn't** added again.
    /// Two migrators are considered equivalent if they are in the same domain, have the same old version, and have the same new version.
    ///
    /// - Parameter migrator: The migrator to consider when performing migration
    func register(migrator: Migrator) {
        if !sortedMigratorPool.contains(migrator) {
            sortedMigratorPool.insert(migrator)
        }
    }
}



// MARK: - Performing migration

public extension MigrationEngine {
    
    /// Immediately starts a background migration process using this engine's registered migrators.
    ///
    /// - Attention: Once this is called, it must not be called again until migration is complete (the returned publisher sends out a `.done`)
    ///
    /// - Parameters:
    ///   - oldExecutableVersion: The version being migrated away from
    ///   - newExecutableVersion: The version being migrated to
    ///
    /// - Throws: Any error which prevents migration from starting at all. For example, if you call this while a migration is already underway, then this will throw `MigrationEngine.StartError.migrationAlreadyUnderway`
    ///
    /// - Returns: A publisher which will send progress updates to subscribers
    func performMigration(from oldExecutableVersion: SemVer, to newExecutableVersion: SemVer) throws -> ProgressPublisher {
        try migrationStartupCoordinatorQueue.sync {
            guard !isMigrationUnderway else {
                throw StartError.migrationAlreadyUnderway
            }
            
            isMigrationUnderway = true
            migrationUnderwaySubscriber?.cancel()
            migrationUnderwaySubscriber = nil
        }
        
        let publisher = createNewProgressPublisher()
        
        migrationPerformerQueue.async {
            
            self.migrationUnderwaySubscriber = publisher
                .filter(\.isDone)
                .receive(on: self.migrationStartupCoordinatorQueue)
                .sink { _ in
                    self.isMigrationUnderway = false
                }
            
            
            let migratorQueues = assembleMigratorQueues(from: oldExecutableVersion)
            
            for migratorQueue in migratorQueues {
                migratorQueue.performMigration(publishingUpdatesTo: self.$progress)
            }
        }
        
        return publisher
    }
    
    
    
    /// Allows subscribers to respond to the progress of a migration
    enum Progress {
        case starting
        case migrating(totalMigratorCount: UInt, successCount: UInt, skipCount: UInt, failureCount: UInt)
        case done(migrationsAttemtped: UInt, failures: [Error])
    }
    
    
    
    /// Any error which occurs while migration is still starting up
    enum StartError: Error {
        case migrationAlreadyUnderway
    }
    
    
    
    typealias ProgressPublisher = AnyPublisher<Progress, Never>
}



public extension MigrationEngine.Progress {
    var isDone: Bool {
        switch self {
        case .done(migrationsAttemtped: _, failures: _):
            return true
            
        case .starting,
             .migrating(totalMigratorCount: _, successCount: _, skipCount: _, failureCount: _):
            return false
        }
    }
}



private extension MigrationEngine {
    /// Resets progress and returns the new progress publisher, destroying the previous one
    func createNewProgressPublisher() -> ProgressPublisher {
        _progress = .init(initialValue: .starting)
        return $progress.eraseToAnyPublisher()
    }
    
    
    func assembleMigratorQueues(from oldExecutableVersion: SemVer) -> [MigratorChainQueue] {
        var queues = [Migrator.Domain : MigratorChainQueue]()
        
        for migrator in self.sortedMigratorPool {
            if let existingQueue = queues[migrator.migratorDomain] {
                switch existingQueue.enqueueOldestIfAppropriate(migrator) {
                case .enqueuedSuccessfully:
                    continue
                    
                case .unableToEnqueue:
                    
                }
            }
            else {
                let chainQueue = MigratorChainQueue(startingMigrator: migrator)
            }
        }
        
        return Array(queues.values)
    }
}
