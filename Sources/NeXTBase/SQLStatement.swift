//
//  SQLStatement.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//

import Foundation


public class SQLStatement {
    public private(set) var ref: OpaquePointer
    
    public init(pointer: OpaquePointer) {
        self.ref = pointer
    }
    
    deinit {
        sqlite3_finalize(ref)
    }
    
    var sql: String {
        String(cString: sqlite3_sql(ref), else: "<sql>")
    }
    
    var normalized_sql: String {
        String(cString: sqlite3_normalized_sql(ref), else: "<sql>")
    }
    
    var expanded_sql: String {
        String(cString: sqlite3_expanded_sql(ref), else: "<sql>")
    }
    
    public func bind(value: Any, at index: Int) throws {
        let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        
        let index = Int32(index)
        switch value {
            case let x as Bool:
                sqlite3_bind_int64(ref, index, x ? 1 : 0)
            case let x as Data:
                sqlite3_bind_blob(ref, index, Array(x), Int32(x.count), SQLITE_TRANSIENT)
            case let x as (any FixedWidthInteger):
                sqlite3_bind_int64(ref, index, Int64(x))
            case let x as (any BinaryFloatingPoint):
                sqlite3_bind_double(ref, index, Double(x))
            case let x as String:
                sqlite3_bind_text(ref, index, x, -1, SQLITE_TRANSIENT)
            case let x as (String.SubSequence):
                sqlite3_bind_text(ref, index, String(x), -1, SQLITE_TRANSIENT)
            case let x as (any AnyOptional):
                if let y = x.wrapped {
                    try bind(value: y, at: Int(index))
                } else {
                    sqlite3_bind_null(ref, index)
                }
            case let x as (any Encodable):
                let blob = try JSONEncoder().encode(x)
                sqlite3_bind_blob(ref, index, Array(blob), Int32(blob.count), SQLITE_TRANSIENT)
            default:
                sqlite3_bind_null(ref, index)
        }
    }

    public func execute() throws {
        try checkError { sqlite3_step(ref) }
    }
    
    public func step() throws -> Bool {
        try SQLITE_ROW == checkError { sqlite3_step(ref) }
    }
    
    public func reset() throws {
        try checkError(sqlite3_reset(ref))
    }

    public func columnValue<A>(at index: Int32, as ct: A.Type = A.self) throws -> A? {
        try columnValue(at: index) as? A
    }
    
    @_disfavoredOverload
    public func columnValue(at index: Int32) throws -> Any? {
        let idx = Int32(index)
        guard idx >= 0, idx < sqlite3_column_count(ref)
        else { throw SQLError(code: 0, "Column Index Out of Bounds") }
        
        let type = columnType(at: idx)
        switch type {
            case .integer, .primaryIntegerKey:
                return sqlite3_column_int64(ref, idx)
            case .float:
                return sqlite3_column_double(ref, idx)
            case .text:
                let value = sqlite3_column_text(ref, idx)
                return String(cString: value, else: "")
            case .blob:
                guard let b = sqlite3_column_blob(ref, idx) else {
                    // A zero-length BLOB is returned as a null pointer
                    // However, a null pointer may also indicate an error condition
                    return nil
                }
                let count = Int(sqlite3_column_bytes(ref, idx))
                let data = Data(bytes: b.assumingMemoryBound(to: UInt8.self), count: count)
                return data
            case .null:
                return nil
        }
    }
}

public extension SQLStatement {
    
    var row: [String: Any]? {
        guard let cols = try? columnSpecs(), !cols.isEmpty
        else { return nil }
        var dictionary:[String: Any] = [:]
        for (ndx, name) in cols {
            if let value = try? columnValue(at: ndx) {
                dictionary[name] = value
            }
        }
        return dictionary
    }
    
    var columnCount: Int32 { sqlite3_column_count(ref) }
    
    func columnDeclType(at ndx: Int32) -> String  {
        String(cString: sqlite3_column_decltype(ref, ndx), else: "_no_decl_")
    }
    
    func columnType(at ndx: Int32) -> SQLStorageType  {
        let decl = columnDeclType(at: ndx)
        return switch decl {
            case "INTEGER": .integer
            case "FLOAT":   .float
            case "TEXT":    .text
            case "BLOB":    .blob
            case "NULL":    .null
            default:
                    .text
        }
    }
    
    func columnName(at ndx: Int32) -> String? {
        return String(cString: sqlite3_column_name(ref, ndx))
    }
    
    func column(at ndx: Int32) -> SQLColumn {
        SQLColumn(at: ndx, in: self)
    }
    
    func columns() throws -> [SQLColumn] {
        var cols: [SQLColumn] = []
        for i in 0..<columnCount {
            let col = column(at: i)
            cols.append(col)
        }
        return cols
    }
    
    func columnSpecs() throws -> [(ndx: Int32, name: String)] {
        var cols: [(Int32, String)] = []
        for i in 0..<columnCount {
            guard let name = columnName(at: i)
            else { continue }
            cols.append((i, name))
        }
        return cols
    }
}
