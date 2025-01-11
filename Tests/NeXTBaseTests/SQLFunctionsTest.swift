//
//  SQLFunctionsTest.swift
//  NeXTBase
//
//  Created by Jason Jobe on 1/11/25.
//

import Testing
@testable import NeXTBase
import Foundation

extension SQLStatement {
    
    public func compactMap<V>(_ fn: (SQLStatement) -> V?) throws -> [V] {
        var result: [V] = []
        while try self.step() {
            if let v = fn(self) {
                result.append(v)
            }
        }
        return result
    }

    public func map<V>(_ fn: (SQLStatement) -> V) throws -> [V] {
        var result: [V] = []
        while try self.step() {
            result.append(fn(self))
        }
        return result
    }
}

struct SQLFunctionsTest {

//    @Test func testCustomCollation() {
//        let connection = try! Connection()
//
//        try! connection.addCollation("reversed", { (a, b) -> ComparisonResult in
//            return b.compare(a)
//        })
//
//        try! connection.execute(sql: "create table t1(a text);")
//
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: "a")
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: ["c"])
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: .string("z"))
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: [.string("e")])
//
//        var str = ""
//        let s = try! connection.prepare(sql: "select * from t1 order by a collate reversed;")
//        try! s.results { row in
//            let c = try row.get(.string, at: 0)
//            str.append(c)
//        }
//
//        XCTAssertEqual(str, "zeca")
//    }

    @Test func testCustomFunction() throws {
        let connection = try! Connection()

        let rot13key: [Character: Character] = [
            "A": "N", "B": "O", "C": "P", "D": "Q", "E": "R", "F": "S", "G": "T", "H": "U", "I": "V", "J": "W", "K": "X", "L": "Y", "M": "Z",
            "N": "A", "O": "B", "P": "C", "Q": "D", "R": "E", "S": "F", "T": "G", "U": "H", "V": "I", "W": "J", "X": "K", "Y": "L", "Z": "M",
            "a": "n", "b": "o", "c": "p", "d": "q", "e": "r", "f": "s", "g": "t", "h": "u", "i": "v", "j": "w", "k": "x", "l": "y", "m": "z",
            "n": "a", "o": "b", "p": "c", "q": "d", "r": "e", "s": "f", "t": "g", "u": "h", "v": "i", "w": "j", "x": "k", "y": "l", "z": "m"]

        func rot13(_ s: String) -> String {
            return String(s.map { rot13key[$0] ?? $0 })
        }

        try connection.addFunction("rot13", arity: 1) { values in
            let value = values.first.unsafelyUnwrapped
            switch value {
            case let s as String:
                return rot13(s)
            default:
                return value
            }
        }

        try connection.execute(sql: "create table t1(a);")

//        let sample = Query("insert into t1(a) values (\(param: "this"));")
//        print (sample.query.sql, sample.query.values)

        try connection.execute("insert into t1(a) values (\(param: "this"));")
        try connection.execute("insert into t1(a) values (\(param: "is"));")
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: "this")
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: ["is"])
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: .string("only"))
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: [.string("a")])
//        try! connection.execute(sql: "insert into t1(a) values (?);", parameters: "test")
//
        let s = try connection.prepareStatement("select a, rot13(a) from t1;")
        while try s.step() {
            print(s.row as Any)
        }
        print("done")
//        let results = s.map { try! $0.get(.string, at: 0) }
//
//        XCTAssertEqual(results, ["guvf", "vf", "bayl", "n", "grfg"])
//
//        try! connection.removeFunction("rot13", arity: 1)
//        XCTAssertThrowsError(try connection.prepare(sql: "select rot13(a) from t1;"))
    }

    @Test func testCustomAggregateFunction() throws {
        let connection = try! Connection()

        class IntegerSumAggregateFunction: SQLAggregateFunction {
            func step(_ values: [DatabaseValue]) throws {
                let value = values.first.unsafelyUnwrapped
                switch value {
                case let i as Int64:
                    sum += i
                default:
                    throw SQLError("Only integer values supported")
                }
            }

            func final() throws -> DatabaseValue {
                defer {
                    sum = 0
                }
                return sum
            }

            var sum: Int64 = 0
        }

        try! connection.addAggregateFunction("integer_sum", arity: 1, IntegerSumAggregateFunction())

        try! connection.execute(sql: "create table t1(a);")

        for i in  0..<10 {
            try! connection.execute("insert into t1(a) values (\(param: i));")
        }

        let s = try! connection.prepareStatement("select integer_sum(a) from t1;")
        while try s.step() {
            print(s.row as Any)
        }
        print("done")
//        XCTAssertEqual(s, 45)

//        let ss = try! connection.prepare(sql: "select integer_sum(a) from t1;").step()!.get(.int64, at: 0)
//        XCTAssertEqual(ss, 45)
//
//        try! connection.removeFunction("integer_sum", arity: 1)
//        XCTAssertThrowsError(try connection.prepare(sql: "select integer_sum(a) from t1;"))
    }

    @Test func testCustomAggregateWindowFunction() throws {
        let connection = try! Connection()
        
        class IntegerSumAggregateWindowFunction: SQLAggregateWindowFunction {
            func step(_ values: [DatabaseValue]) throws {
                let value = values.first.unsafelyUnwrapped
                switch value {
                case let i as Int64:
                    sum += i
                default:
                    throw SQLError("Only integer values supported")
                }
            }
            
            func inverse(_ values: [DatabaseValue]) throws {
                let value = values.first.unsafelyUnwrapped
                switch value {
                case let i as Int64:
                    sum -= i
                default:
                    throw SQLError("Only integer values supported")
                }
            }
            
            func value() throws -> DatabaseValue {
                return sum
            }
            
            func final() throws -> DatabaseValue {
                defer {
                    sum = 0
                }
                return sum
            }
            
            var sum: Int64 = 0
        }
        
        try! connection.addAggregateWindowFunction("integer_sum", arity: 1, IntegerSumAggregateWindowFunction())
        
        try! connection.execute(sql: "create table t1(a);")
        
        for i in  0..<10 {
            try connection.execute("insert into t1(a) values (\(param: i));")
        }
        
        let s = try connection.prepareStatement("select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum from t1;")
        
//        while try s.step() {
//            print(s.row)
//        }
        let results = try s.compactMap { $0.row?.values.first }
        print(results)
        print("done")
//
//        XCTAssertEqual(results, [1, 3, 6, 9, 12, 15, 18, 21, 24, 17])
//        
//        try! connection.removeFunction("integer_sum", arity: 1)
//        XCTAssertThrowsError(try connection.prepare(sql: "select integer_sum(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t1;"))
    }

}
