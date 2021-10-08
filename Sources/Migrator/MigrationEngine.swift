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
import SimpleLogging
import SortedArray



/// Coordinates all migrations in parallel, across any migrator families
public final class MigrationEngine {
    
    private let id = UUID()
    
    private var sortedMigratorPool = SortedArray<Migrator>(areInIncreasingOrder: >)
    
    private let migrationStartupCoordinatorQueue: DispatchQueue
    private let migrationPerformerQueue: DispatchQueue
    private let migrationProgressExclusiveAccessQueue: DispatchQueue
    
    private var isMigrationUnderway = false
    
    @Published
    private var progress = Progress.starting
    
    /// Helps us statelessly build a `Progress` value
    private var progressBuilder = [MigratorChainQueue.ID : MigratorChainQueue.Progress]() {
        didSet {
            rebuildProgress()
        }
    }
    
    private var migrationUnderwaySubscriber: AnyCancellable?
    
    
    init() {
        migrationStartupCoordinatorQueue = DispatchQueue(label: "MigrationEngine startup coordinator queue \(id)")
        migrationPerformerQueue = DispatchQueue(label: "MigrationEngine performer queue \(id)")
        migrationProgressExclusiveAccessQueue = DispatchQueue(label: "MigrationEngine progress exclusive access \(id)", qos: .userInteractive)
    }
}



// MARK: - Managing which migrators are active

public extension MigrationEngine {
    
    /// Registers the given migrator with this engine, so that it will be considered when performing migration.
    ///
    /// If the given migrator has already been registered, it **isn't** added again.
    /// Two migrators are considered equivalent if they are in the same domain, the same old version, and the same new version.
    ///
    /// - Parameter migrator: The migrator to consider when performing migration
    func register(migrator: Migrator) {
        if !sortedMigratorPool.contains(migrator) {
            sortedMigratorPool.insert(migrator)
        }
    }
    
    
    /// Registers the given migrators with this engine, so that they will be considered when performing migration.
    ///
    /// If any of the given migrators have already been registered, it **isn't** added again.
    /// Two migrators are considered equivalent if they are in the same domain, the same old version, and the same new version.
    ///
    /// - Parameter migrators: The migrators to consider when performing migration
    func register(migrators: [Migrator]) {
        migrators.forEach(register)
    }
    
    
    /// Registers the given migrators with this engine, so that they will be considered when performing migration.
    ///
    /// If any of the given migrators have already been registered, it **isn't** added again.
    /// Two migrators are considered equivalent if they are in the same domain, the same old version, and the same new version.
    ///
    /// - Parameter migrators: The migrators to consider when performing migration
    func register(migrators: Migrator...) {
        register(migrators: migrators)
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
        
        let progressPublisher = createNewProgressPublisher()
        
        defer {
        migrationPerformerQueue.async {
            
            self.migrationUnderwaySubscriber = progressPublisher
                .filter(\.isDone)
                .receive(on: self.migrationStartupCoordinatorQueue)
                .sink { _ in
                    self.isMigrationUnderway = false
                }
            
            let migratorQueues: [MigratorChainQueue]
            
            do {
                migratorQueues = try self.assembleMigratorQueues(from: oldExecutableVersion)
            }
            catch {
                log(error: error, "Unable to generate migrator queues")
                assertionFailure()
                return
            }
            
            let operationQueue = OperationQueue()
            operationQueue.qualityOfService = .userInitiated
            
            for migratorQueue in migratorQueues {
                guard let newOperation = migratorQueue.migrationOperation(
                    publishingUpdatesTo: self.createMigrationChainQueueProgressSubscriber(for: migratorQueue.id))
                else {
                    continue
                }
                
                operationQueue.addOperation(newOperation)
            }
            
            operationQueue.waitUntilAllOperationsAreFinished()
        }
        }
        
        return progressPublisher
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
    
    /// Determines whether this progress indicates that all migrations are complete
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
    
    
    func createMigrationChainQueueProgressSubscriber(for chainQueueId: MigratorChainQueue.ID) -> MigratorChainQueue.ProgressSubscriber {
        AnySubscriber(Subscribers.Sink { completion in
                assertionFailure("No defined action for completion: \(completion)")
            }
            receiveValue: { progress in
                self.progressBuilder[chainQueueId] = progress
            }
        )
    }
    
    
    func rebuildProgress() {
        migrationProgressExclusiveAccessQueue.async {
            let summary = self.progressBuilder.values.reduce(
                into: ProgressSummary(completedChainQueuesCount: 0,
                                      totalMigrators: 0,
                                      successCount: 0,
                                      failures: []))
            { summary, chainQueueProgress in
                switch chainQueueProgress {
                case .notStarted:
                    break
                    
                case .running(totalMigrators: let totalMigrators, completedMigrations: let completedMigrations):
                    summary.totalMigrators += totalMigrators
                    summary.successCount += completedMigrations
                    
                case .success(totalMigrations: let totalMigrations):
                    summary.completedChainQueuesCount += 1
                    summary.totalMigrators += totalMigrations
                    summary.successCount += 1
                    
                case .failed(cause: let cause, totalMigrators: let totalMigrators, successes: let successes):
                    summary.completedChainQueuesCount += 1
                    summary.successCount += successes
                    summary.failures.append(cause)
                    summary.totalMigrators += totalMigrators
                }
            }
            
            switch summary {
            case (completedChainQueuesCount: 0,
                  totalMigrators: let totalMigrators,
                  successCount: _,
                  failures: _):
                self.progress = .migrating(totalMigratorCount: totalMigrators, successCount: 0, skipCount: 0, failureCount: 0)
                
            case (completedChainQueuesCount: .init(self.progressBuilder.values.count),
                  totalMigrators: let totalMigrators,
                  successCount: _,
                  failures: let failures):
                // All migrations have been attempted; it's done!
                self.progress = .done(migrationsAttemtped: totalMigrators, failures: failures)
                
            case (completedChainQueuesCount: _,
                  totalMigrators: let totalMigrators,
                  successCount: let successCount,
                  failures: let failures):
                self.progress = .migrating(totalMigratorCount: totalMigrators, successCount: successCount, skipCount: 0, failureCount: .init(failures.count))
            }
        }
    }
    
    
    /// Immediately assembles all migrator queue chains. If any chain could not be assembled, this halts and throws a corresponding error
    ///
    /// - Parameter oldExecutableVersion: The executable version from which each chain queue will migrate
    /// - Throws an error if the chain queues could not be assembled (for example, if a migrator couldn't be added to a chain queue)
    /// - Returns: An array of new chain queues which migrate from the given old version
    func assembleMigratorQueues(from oldExecutableVersion: SemVer) throws -> [MigratorChainQueue] {
        var queues = [Migrator.Domain : MigratorChainQueue]()
        
        for migrator in self.sortedMigratorPool
        where migrator.oldExecutableVersion >= oldExecutableVersion {
            if let existingQueue = queues[migrator.migratorDomain] {
                switch existingQueue.enqueueOldestIfAppropriate(migrator) {
                case .enqueuedSuccessfully:
                    continue
                    
                case .unableToEnqueue:
                    throw AssemblyError.unableToEnqueueIntoChainQueue(chainQueue: existingQueue, migrator: migrator)
                }
            }
            else {
                let chainQueue = MigratorChainQueue(oldestMigrator: migrator)
                queues[migrator.migratorDomain] = chainQueue
            }
        }
        
        return Array(queues.values)
    }
    
    
    
    /// An error which might occur when assembling migrators
    enum AssemblyError: Error {
        
        /// A migrator could not be enqueued into a chain queue for some reason
        /// - Parameter migrator: The migrator that could not be enqueued
        case unableToEnqueueIntoChainQueue(chainQueue: MigratorChainQueue, migrator: Migrator)
    }
    
    
    
    private typealias ProgressSummary = (completedChainQueuesCount: UInt,
                                         totalMigrators: UInt,
                                         successCount: UInt,
                                         failures: [Error])
}
