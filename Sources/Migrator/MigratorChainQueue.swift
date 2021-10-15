//
//  MigratorChainQueue.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-21.
//  Copyright © 2021 Ky Leggiero BH-1-PS.
//

import Combine
import Foundation

import SemVer
import SimpleLogging



/// Performs chained migrations upon a single migrator domain.
///
/// After all migrators have been added and migration is ready to proceed, create an `Operation` using `migrationOperation()`, which you may either run directly or place into an `OperationQueue`.
///
/// - Note: This is meant to be used only by `MigrationEngine` rather than directly.
internal final class MigratorChainQueue: Identifiable {
    
    /// Uniquely identifies this chain queue instance
    internal let id = UUID()
    
    /// The domain which all migrators must share
    private let domain: Migrator.Domain
    
    /// The old version from which this migrator chain queue ultimately migrates
    private let oldExecutableVersion: SemVer
    
    /// All the migrators in this chain queue, sorted so that the next operation is always the last element in this array
    private var orderedMigratorsWithNextOperationAsLastElement = [Migrator]()
    
    /// The dispatch queue within which all startup operations are coordinated, to eliminate race conditions
    private let migrationStartupCoordinatorQueue: DispatchQueue
    
    /// Reports migration progress and guards this from running twice
    @Published
    private var progress = Progress.notStarted
    
    /// Whether or not we already created an `Operation` for this chain queue
    private var didCreateOperation = false
    
    
    
    /// Creates an empty migrator chain queue designed for the given domain and old executable version. All migrators added to this queue must match that domain, and the first one added must migrate from that old executable version.
    ///
    /// - Parameters:
    ///   - domain:               The domain which all migrators must share
    ///   - oldExecutableVersion: The old version from which this migrator chain queue ultimately migrates
    internal init(
        domain: Migrator.Domain,
        oldExecutableVersion: SemVer)
    {
        self.domain = domain
        self.oldExecutableVersion = oldExecutableVersion
        self.migrationStartupCoordinatorQueue = DispatchQueue(label: "MigratorChainQueue startup coordinator queue for domain \(domain)")
    }
    
    
    /// Creates a new migrator chain queue with the given migrator as the oldest.
    ///
    /// This initializes this chain queue's `domain` and `oldExecutableVersion` to be the same as the given migrator.
    ///
    /// - Parameter oldestMigrator: The oldest migrator in the chain queue
    internal convenience init(oldestMigrator: Migrator) {
        self.init(
            domain: oldestMigrator.migratorDomain,
            oldExecutableVersion: oldestMigrator.oldExecutableVersion
        )
        
        switch self.enqueueOldestIfAppropriate(oldestMigrator) {
        case .enqueuedSuccessfully:
            return
            
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
        let previousMigratorNewVersion = self.peekNewestMigrator?.newExecutableVersion ?? self.oldExecutableVersion
        
        // Make sure the candidate's old version matches the new version of the next-oldest.
        if previousMigratorNewVersion == candidateMigrator.oldExecutableVersion {
            // Safe to add!
            return _unsafe_pushNextMigrator(candidateMigrator)
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
    var peekOldestMigrator: Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.last
    }
    
    
    /// A peek at the newest migrator in the queue
    var peekNewestMigrator: Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.first
    }
    
    
    /// Blindly pushes the given migrator onto this queue. **Do not call this without checks!**
    /// If you're unsure, use `enqueueOldestIfAppropriate(_:)` instead.
    ///
    /// - Parameter migrator: The migrator to be blindly pushed onto this queue
    func _unsafe_pushNextMigrator(_ migrator: Migrator) -> EnqueueResult {
        orderedMigratorsWithNextOperationAsLastElement.insert(migrator, at: orderedMigratorsWithNextOperationAsLastElement.startIndex)
        
        return .enqueuedSuccessfully
    }
    
    
    /// Blindly pops a migrator off this queue.
    /// - Returns: The next migrator in this queue, or `nil` if the queue is empty
    func _unsafe_popNextMigrator() -> Migrator? {
        orderedMigratorsWithNextOperationAsLastElement.popLast()
    }
}



// MARK: - Run

internal extension MigratorChainQueue {
    
    /// Generates a new `Operation` that can run through this queue. This can only be called once in this object's lifetime.
    ///
    /// - Note: This must only be called once in a chain queue's lifeteime. Subsequent calls always result in `nil`.
    ///
    /// - Parameter subscriber: The subscriber that listens for migration progress
    ///
    /// - Returns: The `Operation` representation of this queue, or `nil` if that was ever returned before
    func migrationOperation(publishingUpdatesTo subscriber: ProgressSubscriber) -> Operation? {
        migrationStartupCoordinatorQueue.sync {
            defer {
                didCreateOperation = true
            }
            
            return didCreateOperation
                ? nil
                : Operation(running: self, publishingUpdatesTo: subscriber)
        }
    }
    
    
    
//    typealias ProgressPublisher = Published<MigratorChainQueue.Progress>.Publisher
    
    /// A subscriber which can receive progress updates from a migration chain queue
    typealias ProgressSubscriber = AnySubscriber<MigratorChainQueue.Progress, Never>
}



internal extension MigratorChainQueue {
    
    /// An `Operation` representation of a `MigratorChainQueue`
    final class Operation: Foundation.Operation {
        
        /// The queue over which this `Operation` operates
        private let queue: MigratorChainQueue
        
        
        
        override var isReady: Bool {
            switch queue.progress {
            case .notStarted: return true
            case .running, .failed, .success: return false
            }
        }
        
        
        fileprivate init(running queue: MigratorChainQueue, publishingUpdatesTo subscriber: ProgressSubscriber) {
            self.queue = queue
            self.queue.$progress.receive(subscriber: subscriber)
            
            super.init()
            super.qualityOfService = .userInitiated
        }
        
        
        override func main() {
            do {
                try queue.runOnceOnThisThread()
            }
            catch {
                log(error: error, "Migration operation cancelled due to error")
                self.cancel()
            }
        }
    }
}



private extension MigratorChainQueue {
    
    /// Immediately runs the migrator chain queue on this thread. If any migrator fails, this halts immediately.
    ///
    /// This can only be called once. Subsequent calls result in this immediately throwing `StartError.migrationAlreadyStarted`
    ///
    /// - Throws any error that a migrator throws, or a `StartError` if running could not start
    func runOnceOnThisThread() throws {
        let totalMigratorsAtStart: UInt = try migrationStartupCoordinatorQueue.sync {
            guard case .notStarted = self.progress else {
                throw StartError.migrationAlreadyStarted
            }
            
            let totalMigrators = UInt(self.orderedMigratorsWithNextOperationAsLastElement.count)
            
            self.progress = .running(totalMigrators: totalMigrators, completedMigrations: 0)
            
            return totalMigrators
        }
        
        var successfulOrSkippedMigrations: UInt = 0
        
        while let migrator = _unsafe_popNextMigrator() {
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
        
        /// Thrown when a migration has already started. This might indicate that is is underway, or that it has completed. Receiving this error means that the migrator refuses to run again, and that the caller has erroneously told it to run more than once.
        case migrationAlreadyStarted
    }
}
