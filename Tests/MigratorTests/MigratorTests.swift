//
//  MigratorTests.swift
//  Migrator
//
//  Created by Ky Leggiero on 2021-09-20.
//  Copyright © 2021 Ky Leggiero BH-1-PS.
//

import XCTest
import SemVer
import FunctionTools
@testable import Migrator



final class MigratorTests: XCTestCase {
    
    typealias TestData = (version: SemVer, fibonacci: [Int])
    
    
    
    static let domain = "Fibonacci"
    static let data1 = TestData(version: SemVer(0,1,0), fibonacci:    [1, 1, 2, 3, 5, 8])
    static let data2 = TestData(version: SemVer(1,0,0), fibonacci: [0, 1, 1, 2, 3, 5, 8])
    static let data3 = TestData(version: SemVer(1,1,0), fibonacci: [0, 1, 1, 2, 3, 5, 8, 13])
    static let data4 = TestData(version: SemVer(2,0,0), fibonacci:    [1, 1, 2, 3, 5, 8, 13, 21])
    
    var currentFibonacci = [Int]() {
        didSet {
            print(currentFibonacci)
            fibonacciHistory.append(currentFibonacci)
        }
    }
    
    var fibonacciHistory = [[Int]]()
    
    var migrator1, migrator2, migrator3: Migrator!
    
    
    override func setUp() {
        fibonacciHistory = []
        currentFibonacci = Self.data1.fibonacci
        
        migrator1 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data1.version, newExecutableVersion: Self.data2.version) {
            self.wait()
            self.currentFibonacci.insert(0, at: self.currentFibonacci.startIndex)
            return .success
        }
        
        migrator2 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data2.version, newExecutableVersion: Self.data3.version) {
            self.wait()
            self.currentFibonacci.append(13)
            return .success
        }
        
        migrator3 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data3.version, newExecutableVersion: Self.data4.version) {
            var newFibonacci = self.currentFibonacci
            newFibonacci.removeFirst()
            self.wait()
            newFibonacci.append(21)
            self.currentFibonacci = newFibonacci
            return .success
        }
    }
    
    
    override func invokeTest() {
        for run in 1...10 {
            print("\n\n====", Self.className(), "run #\(run)", "====\n\n")
            super.invokeTest()
        }
    }
    
    
    func testSimple_fullMigration() throws {
        
        let engine = MigrationEngine()
        
        engine.register(migrators: migrator1, migrator2, migrator3)
        
        var progressReport = [MigrationEngine.Progress]()
        
        let expectation = expectation(description: "Migration")
        
        let progressSink = try engine.performMigration(from: Self.data1.version, to: Self.data4.version)
            .sink { progress in
                progressReport.append(progress)
                
                if case .done = progress {
                    expectation.fulfill()
                }
            }
        
        wait(for: [expectation], timeout: 10)
        
        blackhole(progressSink)
        
        let possibleExpectedReports: [[MigrationEngine.Progress]] = [
            [
                .starting,
                .migrating(totalMigratorCount: 3, successCount: 0, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 1, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 2, skipCount: 0, failureCount: 0),
                .done(migrationsAttemtped: 3, failures: []),
            ],
            [
                .starting,
                .migrating(totalMigratorCount: 3, successCount: 0, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 1, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 2, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 3, skipCount: 0, failureCount: 0),
                .done(migrationsAttemtped: 3, failures: []),
            ],
        ]
        
        XCTAssertTrue(possibleExpectedReports.contains(progressReport), "Progress report invalid: \(progressReport)")
        
        XCTAssertEqual(currentFibonacci, Self.data4.fibonacci)
        
        XCTAssertEqual(fibonacciHistory, [
            Self.data1.fibonacci,
            Self.data2.fibonacci,
            Self.data3.fibonacci,
            Self.data4.fibonacci,
        ])
    }
    
    
    func testSimple_fullMigration_currentVersionNewerThanNewestMigrator() throws {
        
        let engine = MigrationEngine()
        
        engine.register(migrators: migrator1, migrator2, migrator3)
        
        var progressReport = [MigrationEngine.Progress]()
        
        let expectation = expectation(description: "Migration")
        
        var newVersion = Self.data4.version
        newVersion.minor += 4
        
        let progressSink = try engine.performMigration(from: Self.data1.version, to: newVersion)
            .sink { progress in
                progressReport.append(progress)
                
                if case .done = progress {
                    expectation.fulfill()
                }
            }
        
        wait(for: [expectation], timeout: 10)
        
        blackhole(progressSink)
        
        let possibleExpectedReports: [[MigrationEngine.Progress]] = [
            [
                .starting,
                .migrating(totalMigratorCount: 3, successCount: 0, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 1, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 2, skipCount: 0, failureCount: 0),
                .done(migrationsAttemtped: 3, failures: []),
            ],
            [
                .starting,
                .migrating(totalMigratorCount: 3, successCount: 0, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 1, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 2, skipCount: 0, failureCount: 0),
                .migrating(totalMigratorCount: 3, successCount: 3, skipCount: 0, failureCount: 0),
                .done(migrationsAttemtped: 3, failures: []),
            ],
        ]
        
        XCTAssertTrue(possibleExpectedReports.contains(progressReport), "Progress report invalid: \(progressReport)")
        
        XCTAssertEqual(currentFibonacci, Self.data4.fibonacci)
        
        XCTAssertEqual(fibonacciHistory, [
            Self.data1.fibonacci,
            Self.data2.fibonacci,
            Self.data3.fibonacci,
            Self.data4.fibonacci,
        ])
    }
    
    
    
    private let waitQueue = DispatchQueue(label: "wait", qos: .userInteractive)
    
    
    private func wait(_ timeout: TimeInterval = 0.5) {
        let semaphore = DispatchSemaphore(value: 0)
        
        waitQueue.schedule(after: .init(.now() + timeout), tolerance: 0, options: .init(qos: .userInteractive, flags: [.barrier, .enforceQoS], group: nil)) {
            semaphore.signal()
        }
        
        semaphore.wait()
    }
}
