//
//  Bindable.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import Foundation

protocol Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws
}

// MARK: Alt Database.execute()
extension SQLDatabase {

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
    func bind(statement: OpaquePointer?, column: Int32) throws {
        sqlite3_bind_int64(statement, column, Int64(self))
    }
}
extension Int8: Bindable {}
extension UInt8: Bindable {}
extension Int16: Bindable {}
extension UInt16: Bindable {}
extension Int32: Bindable {}
extension UInt32: Bindable {}
extension Int64: Bindable {}
extension UInt64: Bindable {}

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

extension String: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        try checkError {
            withCString {
                sqlite3_bind_text(statement, column, $0, -1, SQLITE_TRANSIENT)
            }
        }
    }
}

extension URL: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        try absoluteString.bind(statement: statement, column: column)
    }
}

extension Date: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        try Int64(timeIntervalSince1970).bind(statement: statement, column: column)
    }
}

extension UUID: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        try uuidString.bind(statement: statement, column: column)
    }
}

extension Data: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        try checkError {
            withUnsafeBytes {
                sqlite3_bind_blob(statement, column, $0.baseAddress, Int32($0.count), SQLITE_TRANSIENT)
            }
        }
    }
}

extension Optional: Bindable where Wrapped: Bindable {
    func bind(statement: OpaquePointer?, column: Int32) throws {
        switch self {
            case .none: sqlite3_bind_null(statement, column)
            case .some(let x): try x.bind(statement: statement, column: column)
        }
    }
}
