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
    
    var currentFibonacci = [Int]()
    
    var migrator1, migrator2, migrator3: Migrator!
    
    
    override init() {
        
        super.init()
        
        migrator1 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data1.version, newExecutableVersion: Self.data2.version) {
            self.currentFibonacci.insert(0, at: self.currentFibonacci.startIndex)
            return .success
        }
        
        migrator2 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data2.version, newExecutableVersion: Self.data2.version) {
            self.currentFibonacci.append(13)
            return .success
        }
        
        migrator3 = Migrator(domain: Self.domain, oldExecutableVersion: Self.data3.version, newExecutableVersion: Self.data4.version) {
            self.currentFibonacci.removeFirst()
            self.currentFibonacci.append(21)
            return .success
        }
    }
    
    
    override func setUp() {
        self.currentFibonacci = Self.data1.fibonacci
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
        
        wait(for: [expectation], timeout: 2)
        
        blackhole(progressSink)
        
        XCTAssertEqual(progressReport, [
            .starting,
            .migrating(totalMigratorCount: 3, successCount: 0, skipCount: 0, failureCount: 0),
            .migrating(totalMigratorCount: 3, successCount: 1, skipCount: 0, failureCount: 0),
            .migrating(totalMigratorCount: 3, successCount: 2, skipCount: 0, failureCount: 0),
            .migrating(totalMigratorCount: 3, successCount: 3, skipCount: 0, failureCount: 0),
            .done(migrationsAttemtped: 3, failures: []),
        ])
        
        XCTAssertEqual(currentFibonacci, Self.data4.fibonacci)
    }
}



extension MigrationEngine.Progress: Equatable {
    
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch lhs {
        case .starting:
            if case .starting = rhs { return true }
            else { return false }
            
            
        case .migrating(totalMigratorCount: let lhsMigratorCount, successCount: let lhsSuccessCount, skipCount: let skipCount, failureCount: let failureCount):
            if case .migrating(totalMigratorCount: lhsMigratorCount, successCount: lhsSuccessCount, skipCount: skipCount, failureCount: failureCount) = rhs {
                return true
            }
            else { return false }
            
            
        case .done(migrationsAttemtped: let lhsMigrationsAttempted, failures: let lhsFailures):
            switch rhs {
            case .done(migrationsAttemtped: lhsMigrationsAttempted, failures: let rhsFailures)
                where lhsFailures.count == rhsFailures.count:
                for (lhsFailure, rhsFailure) in zip(lhsFailures, rhsFailures) {
                    guard (lhsFailure as NSError) == (rhsFailure as NSError) else {
                        return false
                    }
                }
                
                return true
                
            default:
                return false
            }
        }
    }
}
