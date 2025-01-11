//
//  Bindable.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import Foundation

public protocol Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws
}

public struct SQLString {
    var sql: String
    var values: [Bindable]
}

extension SQLString: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.sql = value
        self.values = []
    }
}

extension SQLString: ExpressibleByStringInterpolation {
    public typealias StringInterpolation = SQLString
    
    public init(stringInterpolation: SQLString) {
        self.sql = stringInterpolation.sql
        self.values = stringInterpolation.values
    }
}

extension SQLString: StringInterpolationProtocol {
    public init(literalCapacity: Int, interpolationCount: Int) {
        self.sql = ""
        self.values = []
    }
    
    mutating public func appendLiteral(_ literal: String) {
        sql += literal
    }
    
    mutating public func appendInterpolation(param value: Bindable) {
        sql += "$\(values.count + 1)"
        values.append(value)
    }
    
    mutating public func appendInterpolation(raw value: String) {
        sql += value
    }
}

// MARK: Alt Database.execute()

public extension NeXTBase {

    func execute(_ sql: SQLString) throws {
        let stm = try self.prepareStatement(sql.sql)
        for (n, v) in sql.values.enumerated() {
            try stm.bind(value: v, at: n + 1)
        }
        try stm.execute()
    }

    func execute(query: String, params: Bindable...) throws {
        var statement: OpaquePointer?
        try checkError {
            query.withCString {
                sqlite3_prepare_v3(ref, $0, -1, 0, &statement, nil)
            }
        }
        for (param, ix) in zip(params, (1 as Int32)...) {
            try param.bind(statement: statement, column: ix)
        }
        try checkError {
            sqlite3_step(statement)
        }
        try checkError { sqlite3_finalize(statement) }
    }
}


extension FixedWidthInteger where Self: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        sqlite3_bind_int64(statement, column, Int64(self))
    }
}

extension Int:    Bindable {}
extension Int8:   Bindable {}
extension UInt8:  Bindable {}
extension Int16:  Bindable {}
extension UInt16: Bindable {}
extension Int32:  Bindable {}
extension UInt32: Bindable {}
extension Int64:  Bindable {}
extension UInt64: Bindable {}

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

extension String: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        try checkError {
            withCString {
                sqlite3_bind_text(statement, column, $0, -1, SQLITE_TRANSIENT)
            }
        }
    }
}

extension URL: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        try absoluteString.bind(statement: statement, column: column)
    }
}

extension Date: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        try Int64(timeIntervalSince1970).bind(statement: statement, column: column)
    }
}

extension UUID: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        try uuidString.bind(statement: statement, column: column)
    }
}

extension Data: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        try checkError {
            withUnsafeBytes {
                sqlite3_bind_blob(statement, column, $0.baseAddress, Int32($0.count), SQLITE_TRANSIENT)
            }
        }
    }
}

extension Optional: Bindable where Wrapped: Bindable {
    public func bind(statement: OpaquePointer?, column: Int32) throws {
        switch self {
            case .none: sqlite3_bind_null(statement, column)
            case .some(let x): try x.bind(statement: statement, column: column)
        }
    }
}
