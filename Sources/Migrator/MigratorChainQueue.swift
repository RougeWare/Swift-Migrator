//
//  MigratorChainQueue.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-21.
//

import Foundation

import SemVer



/// Performs chained migrations upon a single migrator family
internal final class MigratorChainQueue {
    
    private let family: Migrator.Domain
    private let oldExecutableVersion: SemVer
    private let newExecutableVersion: SemVer
    
    private var orderedMigratorsWithNextOperationAsLastElement = [Migrator]()
    
    @Published
    private var progress = Progress.notStarted
    
    
    
    internal init(
        family: Migrator.Domain,
        oldExecutableVersion: SemVer,
        newExecutableVersion: SemVer)
    {
        self.family = family
        self.oldExecutableVersion = oldExecutableVersion
        self.newExecutableVersion = newExecutableVersion
    }
}



internal extension MigratorChainQueue {
    
    /// Attempts to enqueue the given migrator into this chain queue
    ///
    /// - Parameter migrator: The migrator to be enqueued
    /// - Returns: A result describing
    func enqueueOldestIfAppropriate(_ migrator: Migrator) -> EnqueueResult {
        
        guard !isComplete else {
            // Cannot enqueue anything into a complete chain queue
            return .unableToEnqueue
        }
        
        guard case .notStarted = progress else {
            // Cannot enqueue after migration has started
            return .unableToEnqueue
        }
        
        
        if let newest = self.newestMigrator {
            // It's not the first one, so make sure its old version matches the new version of the next-oldest.
            if migrator.oldExecutableVersion == newest.newExecutableVersion {
                // Also make sure its new version doesn't exceed our own
                if migrator.newExecutableVersion <= newExecutableVersion {
                    // Safe to add!
                }
            }
        }
        else {
            // It's the first one being enqueued
            if migrator.oldExecutableVersion == oldExecutableVersion,
               migrator.newExecutableVersion <= newExecutableVersion {
                // It's the first one & its version is the oldest. Put it in!
                push(migrator)
                
                if migrator.newExecutableVersion == newExecutableVersion {
                    // Woo! No more to do!
                    return .enqueuedAndQueueIsComplete
                }
                else {
                    // There's still room for more migrators if necessary
                    return .enqueuedAwaitingNext
                }
            }
            else {
                // It's the first one being inserted and its version number is not the oldest. Can't use it
                return .unableToEnqueue
            }
        }
    }
    
    
    
    /// The result of enqueuing a migrator into a MigratorQueue
    enum EnqueueResult {
        
        /// The migrator has been enqueued and the queue is awaiting the next migrator. Migration can still be performed without completing a queue; it simply will migrate to the newest version it has and stop
        case enqueuedAwaitingNext
        
        /// The migrator has been enqueued and completes the chain from the old version to the new one
        case enqueuedAndQueueIsComplete
        
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



private extension MigratorChainQueue {
    
    /// Determines whether this queue is "complete", meaning its oldest migrator migrates from the old version and its newest migrator migrates to the new version
    var isComplete: Bool {
        if let newest = self.newestMigrator {
            // We'll consider it complete even through we don't know the oldest, since we've already checked that the
            // oldest ever added matches the old version of this migrator.
            //
            // Also, if this is ever needed in the middle of migration, ignoring the oldest also allows us to recognize
            // that the queue was completed when migration started
            return newest.newExecutableVersion == self.newExecutableVersion
        }
        else {
            // It's empty, so it's not complete
            return false
        }
    }
    
    
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
    func push(_ migrator: Migrator) {
        orderedMigratorsWithNextOperationAsLastElement.insert(migrator, at: orderedMigratorsWithNextOperationAsLastElement.startIndex)
    }
}
