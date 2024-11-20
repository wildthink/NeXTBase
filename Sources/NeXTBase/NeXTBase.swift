/*
 * NeXTBase
 * Copyright (C) 2024 WildThink, Inc - All Rights Reserved
 */

 @_exported import SQLite3
import Foundation

public class SQLDatabase {
    public private(set) var ref: OpaquePointer?
    
    public init(path: String) throws {
        try checkError {
            sqlite3_open(path, &ref)
        }
    }
    
    deinit {
        sqlite3_close(ref)
    }
    
    public func execute(sql: String) throws {
        try checkError {
            sqlite3_exec(ref, sql, nil, nil, nil)
        }
    }
    
    public func prepareStatement(_ sql: String) throws -> SQLStatement {
        var statement: OpaquePointer?
        try checkError {
            sql.withCString {
                sqlite3_prepare_v3(ref, $0, -1, 0, &statement, nil)
            }
        }
        guard let statement
        else { throw SQLError() }
        return SQLStatement(pointer: statement)
    }
    
    public func read(_ sql: String, call: (SQLStatement) throws -> Void) throws {
        let statement: SQLStatement = try prepareStatement(sql)
        try execute(sql: "START TRANSACTION;")
        // Since we are read-only
        while try statement.step() {
            try call(statement)
        }
        try execute(sql: "ROLLBACK TRANSACTION;")
    }
    
}

struct SQLError: Error {
    var file: String
    var line: UInt
    var code: Int32
    var message: String
    
    init(
        file: String = #fileID, line: UInt = #line,
        code: Int32 = 0, _ message: String = "unknown"
    ) {
        self.file = file
        self.line = line
        self.code = code
        self.message = message
    }
}

@discardableResult
func checkError( _ code: Int32, file: String = #fileID, line: UInt = #line) throws -> Int32 {
    try checkError(file: file, line: line, { code })
}

@discardableResult
func checkError(file: String = #fileID, line: UInt = #line, _ fn: () -> Int32) throws -> Int32 {
    let code = fn()
    guard code == SQLITE_OK || code == SQLITE_DONE || code == SQLITE_ROW
    else {
        let str = String(cString: sqlite3_errstr(code), else: "unknown")
        throw SQLError(file: file, line: line, code: code, str)
    }
    return code
}

extension String {
    
    @_disfavoredOverload
    init?(cString: UnsafePointer<UInt8>?) {
        guard let cString else { return nil }
        self = String(cString: cString)
    }
    
    init(cString: UnsafePointer<UInt8>?, else alt: String) {
        self = if let cString {
            String(cString: cString)
        } else {
            alt
        }
    }

    @_disfavoredOverload
    init?(cString: UnsafePointer<CChar>?) {
        guard let cString else { return nil }
        self = String(cString: cString)
    }

    init(cString: UnsafePointer<CChar>?, else alt: String) {
        self = if let cString {
            String(cString: cString)
        } else {
            alt
        }
    }
}
