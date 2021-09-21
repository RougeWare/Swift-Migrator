//
//  MigratorChainQueue.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-21.
//

import Foundation

import SemVer



/// Performs serial migrations upon a single migrator family
internal final class MigratorChainQueue {
    
    private let family: Migrator.Family
    private let oldExecutableVersion: SemVer
    private let newExecutableVersion: SemVer
    
    private var orderedMigratorsWithNextOperationAsLastElement = [Migrator]()
    
    
    
    internal init(
        family: Migrator.Family,
        oldExecutableVersion: SemVer,
        newExecutableVersion: SemVer)
    {
        self.family = family
        self.oldExecutableVersion = oldExecutableVersion
        self.newExecutableVersion = newExecutableVersion
    }
}



internal extension MigratorChainQueue {
    func enqueueOldestIfAppropriate(_ migrator: Migrator) -> EnqueueResult {
        if orderedMigratorsWithNextOperationAsLastElement.isEmpty {
            if migrator.oldExecutableVersion == oldExecutableVersion,
               migrator.newExecutableVersion <= newExecutableVersion {
                // It's the first one being inserted and its version is the oldest. Put it in!
                orderedMigratorsWithNextOperationAsLastElement.append(migrator)
                
                if migrator.newExecutableVersion == newExecutableVersion {
                    return .enqueuedAndQueueIsComplete
                }
                else {
                    return .enqueuedAwaitingNext
                }
            }
            else {
                // It's the first one being inserted and its version number is not the oldest. Can't use it
                return .unableToEnqueue
            }
        }
        else {
            // It's not the first one, so make sure its old version matches the new version of the next-oldest.
            // Also make sure its new version doesn't exceed our own
        }
    }
    
    
    
    /// The result of enqueuing a migrator into a MigratorQueue
    enum EnqueueResult {
        
        /// The migrator has been enqueued and the queue is awaiting the next migrator; it cannot perform migration without it
        case enqueuedAwaitingNext
        
        /// The migrator has been enqueued and completes the chain
        case enqueuedAndQueueIsComplete
        case unableToEnqueue
    }
}
