//
//  MigratorChainQueue.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-21.
//

import Foundation

import SemVer



/// Performs chained migrations upon a single migrator domain
internal final class MigratorChainQueue {
    
    private let domain: Migrator.Domain
    private let oldExecutableVersion: SemVer
    
    private var orderedMigratorsWithNextOperationAsLastElement = [Migrator]()
    
    private let migrationStartupCoordinatorQueue: DispatchQueue
    
    @Published
    private var progress = Progress.notStarted
    
    
    
    internal init(
        domain: Migrator.Domain,
        oldExecutableVersion: SemVer)
    {
        self.domain = domain
        self.oldExecutableVersion = oldExecutableVersion
        self.migrationStartupCoordinatorQueue = DispatchQueue(label: "MigratorChainQueue startup coordinator queue for domain \(domain)")
    }
    
    
    internal convenience init(startingMigrator: Migrator) {
        self.init(
            domain: startingMigrator.migratorDomain,
            oldExecutableVersion: startingMigrator.oldExecutableVersion
        )
        
        switch self.enqueueOldestIfAppropriate(migrator) {
        case .enqueuedSuccessfully:
            continue
            
        case .unableToEnqueue:
            assertionFailure("Migrator's own old version doesn't fit criteria for chain queue based on that migrator")
        }
    }
}



// MARK: - Enqueue

internal extension MigratorChainQueue {
    
    /// Enqueues all the given migrators, silentlyignoring those which were not enqueued
    ///
    /// - Parameter migrators: The migrators to be added
    func enqueueAllAsAppropriate(_ migrators: [Migrator]) {
        for migrator in migrators {
            _ = enqueueOldestIfAppropriate(migrator)
        }
    }
    
    
    /// Attempts to enqueue the given migrator into this chain queue
    ///
    /// - Parameter candidateMigrator: The migrator to be enqueued
    ///
    /// - Returns: A result describing whether the migrator was enqueued
    func enqueueOldestIfAppropriate(_ candidateMigrator: Migrator) -> EnqueueResult {
        
        guard case .notStarted = progress else {
            // Cannot enqueue after migration has started
            return .unableToEnqueue
        }
        
        // If it's the first one being enqueued, use this chain queue's old version as if it was the new version of a previous migrator.
        // Else, use the target version of the newest existing migrator.
        let previousMigratorNewVersion = self.newestMigrator?.newExecutableVersion ?? self.oldExecutableVersion
        
        // Make sure the candidate's old version matches the new version of the next-oldest.
        if previousMigratorNewVersion == candidateMigrator.oldExecutableVersion {
            // Safe to add!
            return push(candidateMigrator)
        }
        else {
            // Each migrator needs to start where another left off, so this one can't be added
            return .unableToEnqueue
        }
    }
    
    
    
    /// The result of enqueuing a migrator into a MigratorQueue
    enum EnqueueResult {
        
        /// The migrator has been enqueued and the queue is awaiting the next migrator.
        case enqueuedSuccessfully
        
        /// For some reason, the migrator couldn't be added. See documentation for more information.
        case unableToEnqueue
    }
    
    
    
    /// Describes the migration chain progress
    enum Progress {
        
        /// The migration chain has not yet started
        case notStarted
        
        /// The migration chain has started and is still running
        ///
        /// - Parameters:
        ///   - totalMigrators:      The count of how many migrators were ever added to this queue
        ///   - completedMigrations: How many migrations have succeeded so far
        case running(totalMigrators: UInt, completedMigrations: UInt)
        
        /// One migrator failed, so the migration chain was broken and the whole chain was counted as failed
        ///
        /// - Parameters:
        ///   - cause:          The error which caused this failure
        ///   - totalMigrators: The count of how many migrators were ever added to this queue
        ///   - successes:      How many migrations succeeded before this failure
        case failed(cause: Error, totalMigrators: UInt, successes: UInt)
        
        /// The migration chain has completed successfully
        ///
        /// - Parameter totalMigration: The total number of migrators which ran
        case success(totalMigrations: UInt)
    }
}



// MARK: Private

private extension MigratorChainQueue {
    
    /// A peek at the oldest migrator in the queue
    var oldestMigrator: Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.last
    }
    
    
    /// A peek at the newest migrator in the queue
    var newestMigrator: Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.first
    }
    
    
    /// Blindly pushes the given migrator onto this queue. **Do not call this without checks!**
    /// If you're unsure, use `enqueueOldestIfAppropriate(_:)` instead.
    ///
    /// - Parameter migrator: The migrator to be blindly pushed onto this queue
    func push(_ migrator: Migrator) -> EnqueueResult {
        orderedMigratorsWithNextOperationAsLastElement.insert(migrator, at: orderedMigratorsWithNextOperationAsLastElement.startIndex)
        
        return .enqueuedSuccessfully
    }
    
    
    /// Blindly pops a migrator off this queue.
    /// - Returns: The next migrator in this queue, or `nil` if the queue is empty
    func pop() -> Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.popLast()
    }
}



// MARK: - Run

internal extension MigratorChainQueue {
    func migrationOperation() -> Operation {
        Operation(running: self)
    }
}



internal extension MigratorChainQueue {
    final class Operation: Foundation.Operation {
        
        private let queue: MigratorChainQueue
        
        
        init(running queue: MigratorChainQueue) {
            self.queue = queue
            
            super.init()
            super.qualityOfService = .userInitiated
        }
        
        
        override func main() {
            queue.runOnThisThread()
        }
    }
}



private extension MigratorChainQueue {
    
    func runOnThisThread() throws {
        let totalMigratorsAtStart: UInt = try migrationStartupCoordinatorQueue.sync {
            guard case .notStarted = self.progress else {
                throw StartError.migrationAlreadyUnderway
            }
            
            let totalMigrators = UInt(self.orderedMigratorsWithNextOperationAsLastElement.count)
            
            self.progress = .running(totalMigrators: totalMigrators, completedMigrations: 0)
            
            return totalMigrators
        }
        
        var successfulOrSkippedMigrations: UInt = 0
        
        
        while let migrator = pop() {
            switch migrator.migrate() {
            case .success,
                 .unnecessary:
                successfulOrSkippedMigrations += 1
                
            case .failed(cause: let cause):
                self.progress = .failed(cause: cause, totalMigrators: totalMigratorsAtStart, successes: successfulOrSkippedMigrations)
                throw cause
            }
            
            self.progress = .running(totalMigrators: totalMigratorsAtStart, completedMigrations: successfulOrSkippedMigrations)
        }
        
        self.progress = .success(totalMigrations: totalMigratorsAtStart)
    }
    
    
    
    /// Any error which occurs while migration is still starting up
    enum StartError: Error {
        case migrationAlreadyUnderway
    }
}
