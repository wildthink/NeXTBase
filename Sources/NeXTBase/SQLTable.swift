//
//  SQLTable.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import Foundation


public class SQLTable {
    // TODO: Consider null safety handling
    private weak var database: NeXTBase!
    public private(set) var tableName: SQLTable.Name
    public private(set) var columns: [SQLColumn] = []
    
    public init(database: NeXTBase, tableName: SQLTable.Name) {
        self.database = database
        self.tableName = tableName
    }
    
    public func createTable(columns: [SQLColumn]) throws {
        let columnsDefinition = columns
            .filter({ $0.name != "id" })
            .map { $0.declaration }.joined(separator: ", ")
        let createTableString = "CREATE TABLE IF NOT EXISTS \(tableName) (id INTEGER PRIMARY KEY, \(columnsDefinition));"
        try database.execute(sql: createTableString)
        self.columns = columns
    }
    
    public func addColumnIfNeeded(column: SQLColumn) throws {
        if !columnExists(columnName: column.name) {
            let addColumnString = generateAddColumnSQL(column: column)
            try database.execute(sql: addColumnString)
        }
    }
    
    public func getTableMetadata() throws -> [String] {
        var columnNames: [String] = []
        let query = "PRAGMA table_info(\(tableName));"
        let statement = try database.prepareStatement(query)
        
        while try statement.step() {
            if let name = try statement.columnValue(at: 1, as: String.self) {
                columnNames.append(name)
            }
        }
        return columnNames
    }
    
    private func columnExists(columnName: String) -> Bool {
        return (try? getTableMetadata().contains(columnName)) ?? false
    }
    
    public func createOrUpdateTable<T: Codable>(for type: T.Type) throws {
        let cur_cols = (try? getTableMetadata()) ?? []
        var missingColumns: [SQLColumn] = []
        
        enumerateFields(of: type) { (name, stype) in
            if cur_cols.contains(name) {
                return
            }
            if let evt = stype.erasedValueType as? Codable.Type {
                var column = SQLColumn(name: name, dataType: evt)
                if name == "id" && column.sqlType == .integer {
                    column.sqlType = .primaryIntegerKey
                }
                missingColumns.append(column)
            }
        }
        
        if cur_cols.isEmpty {
            // If table doesn't exist, create it
            try createTable(columns: missingColumns)
        } else {
            // Add any missing columns
            for column in missingColumns {
                try addColumnIfNeeded(column: column)
            }
        }
    }
    
    private func generateAddColumnSQL(column: SQLColumn) -> String {
        return "ALTER TABLE \(tableName) ADD COLUMN \(column.declaration);"
    }
}

extension SQLTable {
    static let keywords: Set<String> = [
        "set", "table", "values", "group"]
    
    func format(columnName: String) -> String {
        Self.keywords.contains(columnName.lowercased())
        ? "'\(columnName)'" : columnName
    }
    
    // performs an "upsert"
    public func write<T: Encodable>(_ nob: T) throws {
        var keys: [String] = []
        var values: [String] = []
        var bindings: [Codable] = []
        
        var ndx = 1
        enumerateFields(of: T.self) { (name, keyPath) in
            keys.append(format(columnName: name))
            if let value = nob[keyPath: keyPath] as? Codable {
                values.append("?\(ndx)")
                ndx += 1
                bindings.append(value)
            }
        }
        
        let keysString = keys.joined(separator: ", ")
        let valuesString = values.joined(separator: ", ")
        
        let insertSQL = """
                  INSERT INTO \(tableName) (\(keysString)) VALUES (\(valuesString))
                  ON CONFLICT(id) DO UPDATE SET
                 (\(keys.filter({$0 != "id"}).joined(separator: ", "))) = (\(values.dropFirst().joined(separator:", ")));
             """
        
        let statement = try database.prepareStatement(insertSQL)
        
        for (index, binding) in bindings.enumerated() {
            try statement.bind(value: binding, at: Int(Int32(index + 1)))
        }
        try statement.execute()
    }
    
    public func read<T: Decodable>(as type: T.Type = T.self, where condition: String? = nil, limit: Int? = nil) throws -> [T] {
        var query = "SELECT * FROM \(tableName)"
        if let condition = condition {
            query += " WHERE \(condition)"
        }
        if let limit = limit {
            query += " LIMIT \(limit)"
        }
        query += ";"
        
        return try read(as: type, with: query)
    }
    
    func read<T: Decodable>(as type: T.Type, with query: String) throws -> [T]  {
        var results: [T] = []
        let statement = try database.prepareStatement(query)
        
        while try statement.step() {
            guard let row = statement.row else { continue }
            
            let jsonData = try JSONSerialization.data(withJSONObject: row, options: [])
            let decodedObject = try JSONDecoder().decode(T.self, from: jsonData)
            results.append(decodedObject)
        }
        return results
    }
    
    public func forget(id: Int64) throws {
        let deleteSQL = "DELETE FROM \(tableName) WHERE id = ?;"
        let statement = try database.prepareStatement(deleteSQL)
        try statement.bind(value: id, at: 1)
        try statement.execute()
    }
}

// MARK: History Extensions
extension SQLTable {
    
    private func generateInsertHistorySQL(for id: Int64) -> String {
        return "INSERT INTO \(tableName)_history SELECT *, datetime('now') FROM \(tableName) WHERE id = ?;"
    }
    
    private func createHistoryTable() throws {
        let createHistoryTableString = "CREATE TABLE IF NOT EXISTS \(tableName)_history AS SELECT *, NULL AS timestamp FROM \(tableName) WHERE 0;"
        try database.execute(sql: createHistoryTableString)
    }
    
    public func recall<T: Decodable>(as type: T.Type = T.self, asof: Date, where condition: String? = nil, limit: Int? = nil) throws -> [T] {
        let dateFormatter = ISO8601DateFormatter()
        let asofString = dateFormatter.string(from: asof)
        
        var query = "SELECT * FROM \(tableName)"
        query += " WHERE timestamp <= '\(asofString)'"
        
        if let condition = condition {
            query += " AND \(condition)"
        }
        query += " ORDER BY timestamp DESC"
        
        if let limit = limit {
            query += " LIMIT \(limit)"
        }
        query += ";"
        
        return try read(as: type, with: query)
    }
}

// MARK: SQLTable.Name
extension SQLTable {
    public struct Name: TypedString, Sendable {
        var rawValue: String
        public init(stringLiteral value: StringLiteralType) {
            rawValue = value
        }
        public init(_ name: String) {
            rawValue = name
        }
        public var description: String { rawValue }
    }
}
