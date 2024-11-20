//
//  SQLiteDataFrame.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import CoreGraphics
import Foundation
import TabularData

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

public typealias SQLiteConnection = SQLDatabase
public typealias SQLiteStatement = SQLStatement
public typealias SQLiteType = SQLStorageType

extension DataFrame {
    /**
     Intializes a DataFrame from a prepared statement.
     
     - Parameter statement: The prepared statement. The statement will be finalalized by the initializer.
     - Parameter types;            An optional dictionary of column names to `SQLiteType`s. The data frame
                            infers the types for column names that aren’t in the dictionary.
     - Parameter capacity:  The initial capacity of each column. It is normally fine to leave this as the default value.

     Columns in the types dictionary which are not returned by the select statement are ignored.
    
     The DataFrame's column types are determined by the columns' declared types, using a modified version of the
     SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
     If the column's type can't be determined, then the `.any` type is used.
     */
    public init(
        statement: SQLiteStatement,
        types: [String:SQLiteType]? = nil,
        capacity: Int = 0
    ) throws {
        
        let columnCount = Int(sqlite3_column_count(statement.ref))
        let columns = (0..<columnCount).map { _ndx in
            let ndx = Int32(_ndx)
            let col_name = String(cString:sqlite3_column_name(statement.ref, ndx),
                                  else: "<column>")
            let declType = types?[col_name] ?? statement.columnType(at: ndx)
            return AnyColumn(col_name, for: declType)
        }
        self.init(columns: columns)
        try readSQL(statement: statement)
    }
    
    /**
     Read the contents of the given table into this DataFrame.
     
     - Parameter statement: the prepared statement.
     - Parameter finalizeStatement: If true, the prepared statement will be finalized after the read completes.
     
     Columns are matched ito statement parameters n DataFrame column order.
     */
    mutating func readSQL(statement: SQLiteStatement) throws {
        
        var rowIndex = 0
        while try statement.step() {
            self.appendEmptyRow()
            for (col, column) in columns.enumerated() {
                let columnIndex = Int32(col)
                let sqlColumnType = sqlite3_column_type(statement.ref, columnIndex)
                if sqlColumnType == SQLITE_NULL {
                    continue
                }
                switch column.wrappedElementType {
                    case is Bool.Type:
                        rows[rowIndex][col] = statement.int64Value(at: columnIndex) != 0

                    case is any FixedWidthInteger.Type:
                        if let I = column.wrappedElementType as? any FixedWidthInteger.Type {
                            let iv = I.init(statement.int64Value(at: columnIndex))
                            rows[rowIndex][col] = iv
                        }

                    case is String.Type:
                        rows[rowIndex][col] = statement.stringValue(at: columnIndex)
                    case is Float.Type:
                        rows[rowIndex][col] = Float(statement.doubleValue(at: columnIndex))
                    case is Double.Type:
                        rows[rowIndex][col] = Double(statement.doubleValue(at: columnIndex))
                    case is Data.Type:
                        rows[rowIndex][col] = statement.dataValue(at: columnIndex)
                    case is Date.Type:
                        if let date = Date(sqlValue: statement.anyValue(at: columnIndex) as Any) {
                            rows[rowIndex][col] = date
                        }
                    case let decodable as Decodable.Type:
                        let data = statement.dataValue(at: columnIndex)
                        if let it = try? JSONDecoder().decode(decodable, from: data) {
                            rows[rowIndex][col] = it
                        }
                    default:
                        let value = statement.anyValue(at: columnIndex)
                        rows[rowIndex][col] = value
                }
            }
            rowIndex += 1
        }
    }
    
    /**
     Write a dataFrame to a sqlite prepared statement.
     - Parameter statement: The prepared statement.
     
     The columns of the dataframe are bound to the statement parameters in column index order.
     If there are more dataframe columns than table columns, the extra table columns will be written as null.
     If there are more DataFrame columns than table columns, only the first N columns
     will be transferred.
     */
    func writeRows(statement: SQLiteStatement) throws {
        let columns = columns.prefix(Int(sqlite3_bind_parameter_count(statement.ref)))
        for rowIndex in 0..<shape.rows {
            for (i, column) in columns.enumerated() {
                let positionalIndex = Int32(1 + i)
                guard let item = column[rowIndex] else {
                    try checkError(sqlite3_bind_null(statement.ref, positionalIndex))
                    continue
                }
                try DataFrame.writeItem(statement:statement, positionalIndex:positionalIndex, item:item)
            }
            _ = try statement.step()
            try statement.reset()
        }
    }
    
    private static func writeItem(
        statement: SQLiteStatement, positionalIndex: Int32, item: Any
    ) throws {
        func bind_int<I: FixedWidthInteger>(_ v: I) -> Int32 {
            if I.bitWidth <= 32 {
                sqlite3_bind_int(statement.ref, positionalIndex, Int32(v))
            } else {
                sqlite3_bind_int64(statement.ref, positionalIndex, Int64(v))
            }
        }
        switch item {
            case let value as Bool:
                try checkError(bind_int(value ? 1 : 0))
            case let value as any FixedWidthInteger:
                try checkError(bind_int(value))
            case let value as Float:
                try checkError(sqlite3_bind_double(statement.ref, positionalIndex, Double(value)))
            case let value as CGFloat:
                try checkError(sqlite3_bind_double(statement.ref, positionalIndex, Double(value)))
            case let value as Double:
                try checkError(sqlite3_bind_double(statement.ref, positionalIndex, value))
            case let value as String:
                try checkError(sqlite3_bind_text(statement.ref, positionalIndex, value.cString(using: .utf8),-1,SQLITE_TRANSIENT))
            case let value as Data:
                try value.withUnsafeBytes {
                    _ = try checkError(sqlite3_bind_blob64(statement.ref, positionalIndex, $0.baseAddress, sqlite3_uint64($0.count), SQLITE_TRANSIENT))
                }
            case let value as Date:
                let formatter = DateFormatter()
                formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format.
                let dateString = formatter.string(from: value)
                try checkError(sqlite3_bind_text(statement.ref, positionalIndex, dateString.cString(using: .utf8),-1,SQLITE_TRANSIENT))

            case let cd as Encodable:
                let data = try JSONEncoder().encode(cd)
                try data.withUnsafeBytes {
                    _ = try checkError(sqlite3_bind_blob64(statement.ref, positionalIndex, $0.baseAddress, sqlite3_uint64($0.count), SQLITE_TRANSIENT))
                }
            case let csc as CustomStringConvertible:
                let s = csc.description
                try checkError(sqlite3_bind_text(statement.ref, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
            default:
                let value = String(reflecting:item)
                try checkError(sqlite3_bind_text(statement.ref, positionalIndex, value.cString(using: .utf8),-1,SQLITE_TRANSIENT))
        }
        
    }
    
    /**
     Write a dataFrame to a sqlite table.
     - Parameter connection: The SQlite database connection
     - Parameter table: The name of the table to write.
     - Parameter createTable: will execute a CREATE TABLE

     The columns of the dataframe are written to an SQL table. If the table already exists,
     then it will be replaced.
     
     The DataFrame column names and wrapped types will be used to create the
     SQL column names.
     */
    public func writeSQL(connection: SQLiteConnection, table: String, createTable: Bool) throws {
        
        if createTable {
            let columnDefs = columns.map {column -> String in
                let name = column.name
                let sqlType: String? = switch column.wrappedElementType {
                    case is String.Type: "TEXT"
                    case is Bool.Type: "BOOLEAN"
                    case is any FixedWidthInteger: "INT"
                    case is Float.Type: "FLOAT"
                    case is Double.Type: "DOUBLE"
                    case is Date.Type: "DATE"
                    case is Data.Type: "BLOB"
                    default:
                        nil
                }
                if let sqlType = sqlType {
                    return "\(name) \(sqlType)"
                }
                return name
            }
            let columnSpec = columnDefs.joined(separator: ",")
            try connection.execute(sql: "create table if not exists \(table) (\(columnSpec))")
        }
        
        let questionMarks = Array(repeating:"?", count:shape.columns).joined(separator: ",")
        let sql = "insert into \(table) values (\(questionMarks))"
        let statement = try connection.prepareStatement(sql)
        defer { sqlite3_finalize(statement.ref) }
        try writeRows(statement: statement)
    }
}

extension Date {
    init?(sqlValue: Any) {
        // See "Date and Time Datatype" https://www.sqlite.org/datatype3.html
        // TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
        // REAL as Julian day numbers, the number of days since noon
        // in Greenwich on November 24, 4714 B.C. according
        // INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.

        guard let it = switch sqlValue {
            case let s as String: {
                let formatter = DateFormatter()
                // SQLite format
                formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
                return formatter.date(from:s)
            }()
            case let i as Int64:
                Date(timeIntervalSince1970:TimeInterval(i))
            case let julianDay as Double: {
                let SECONDS_PER_DAY = 86400.0
                let JULIAN_DAY_OF_ZERO_UNIX_TIME = 2440587.5
                let unixTime = (julianDay - JULIAN_DAY_OF_ZERO_UNIX_TIME) * SECONDS_PER_DAY
                return Date(timeIntervalSince1970:TimeInterval(unixTime))
            }()
            default:
                Optional<Date>.none
        }
        else { return nil }
        self = it
    }
}

extension SQLiteStatement {

    func anyValue(at ndx: Int32) -> Any? {
        sqlite3_column_value(ref, ndx)
    }

    func int64Value(at ndx: Int32) -> Int64 {
        sqlite3_column_int64(ref, ndx)
    }
    
    func doubleValue(at ndx: Int32) -> Double {
        sqlite3_column_double(ref, ndx)
    }
    
    func stringValue(at ndx: Int32) -> String {
        (String(cString:sqlite3_column_text(ref, ndx)))
    }

    func dataValue(at ndx: Int32) -> Data {
        Data(bytes:sqlite3_column_blob(ref, ndx),
             count:Int(sqlite3_column_bytes(ref, ndx)))
    }
}

protocol AnyOptional {
    static var wrappedType: Any.Type { get }
    var wrapped: Any? { get }
}

extension Optional: AnyOptional {
    static var wrappedType: Any.Type { Wrapped.self }
    
    var wrapped: Any? {
        switch self {
            case let .some(value):
                return value
            case .none:
                return nil
        }
    }
}

public extension AnyColumn {
    init(_ name: String, for t: SQLStorageType, capacity: Int = 0) {
        func mk<T>(_ t: T.Type) -> AnyColumn {
            Column<T>(name: name, capacity: capacity)
                .eraseToAnyColumn()
        }
        self = if let a = t.sqlType as? AnyOptional.Type {
            _openExistential(a, do: mk)
        } else {
            _openExistential(t.sqlType, do: mk)
        }
    }
}
