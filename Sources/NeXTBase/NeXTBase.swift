/*
 * NeXTBase
 * Copyright (C) 2024 WildThink, Inc - All Rights Reserved
 */

 @_exported import SQLite3
import Foundation
import Observation

@Observable
public class NeXTBase: @unchecked Sendable {
    public enum Option: Int32 { case read, read_write }
    @ObservationIgnored public private(set) var ref: OpaquePointer?
    public internal(set) var lastUpdated: Date?
    let configuration: Configuration

    var tables: [SQLTable]
    
    
    public init(
        path: String = ":memory:",
        options: Option = .read_write,
        create: Bool = true,
        configuration: Configuration? = nil
    ) throws {
        tables = []
        self.configuration = configuration ?? .init()
        
        try checkError {
            var opt = options == .read_write ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY
            if create { opt = (opt | SQLITE_OPEN_CREATE) }
            return sqlite3_open_v2(path, &ref, opt, nil)
        }
        if let auth = self.configuration.authorizor {
            auth.register(in: self)
        }
    }
    
    deinit {
        sqlite3_close(ref)
    }
}

public extension NeXTBase {
    
    func clearAuthorizer() {
        sqlite3_set_authorizer(ref, nil, nil)
    }
    
   func table(_ named: SQLTable.Name) -> SQLTable {
        if let t = tables.first(where: {$0.tableName == named} ) {
            return t
        }
        let nt = SQLTable(database: self, tableName: named)
        tables.append(nt)
        return nt
    }
    
    func execute(sql: String) throws {
        try checkError {
            sqlite3_exec(ref, sql, nil, nil, nil)
        }
    }
    
    func prepareStatement(_ sql: String) throws -> SQLStatement {
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
    
    func read(_ sql: String, call: (SQLStatement) throws -> Void) throws {
        let statement: SQLStatement = try prepareStatement(sql)
        try execute(sql: "START TRANSACTION;")
        // Since we are read-only
        while try statement.step() {
            try call(statement)
        }
        try execute(sql: "ROLLBACK TRANSACTION;")
    }
    
    // MARK: Combine
//    var _events: CurrentValueSubject<NeXTEvent,Never> = .init(.neXtBaseWillCommit)
//    public lazy var events: AnyPublisher<NeXTEvent,Never> = _events.eraseToAnyPublisher()
//    public func post(_ event: NeXTEvent) { _events.send(event) }
}

// MARK: Combine Event Publisher
import Combine

public struct NeXTEvent: Sendable {
    public let name: String
//    public let timestamp: Date = Date()
//    public private(set) weak var db: NeXTBase?
}

public extension NeXTEvent {
    static let neXtBaseWillCommit  = NeXTEvent(name: "neXtBaseWillCommit")
    static let neXtBaseDidRollback = NeXTEvent(name: "neXtBaseDidRollback")
}

// MARK: Database Conguration
public extension NeXTBase {
    struct Configuration {
        var name: String
        var authorizor: StatementAuthorizer?
        
        init(name: String = "default", authorizor: StatementAuthorizer? = nil) {
            self.name = name
            self.authorizor = authorizor
        }
    }
}

// MARK: Read/Write through to Tables
public extension NeXTBase {
    
    func read<T: Decodable>(
    id: Int64, from table: SQLTable.Name,
    as type: T.Type = T.self
    ) throws -> T? {
        let table = self.table(table)
        return try table.read(as: T.self, where: "id = \(id) LIMIT 1").first
    }

    func read<T: Decodable>(
        from table: SQLTable.Name,
        as type: T.Type = T.self,
        where condition: String? = nil,
        limit: Int? = nil
    ) throws -> [T] {
        let table = self.table(table)
        return try table.read(as: T.self, where: condition, limit: limit)
    }
    
    func write<T: Codable>(_ nob: T, to: SQLTable.Name) throws {
        let table = self.table(to)
        try table.createOrUpdateTable(for: T.self)
        try table.write(nob)
    }
}

#if canImport(TabularData)
import TabularData

public extension NeXTBase {
    
    func dataFrame(from table: SQLTable.Name, limit: Int = 0) throws -> DataFrame {
        let stmt = try prepareStatement("SELECT * FROM \(table) LIMIT \(limit)")
        return try DataFrame(statement: stmt)
    }
    
    func write(dataFrame: DataFrame, to table: SQLTable.Name, create: Bool) throws {
        try dataFrame.writeSQL(connection: self, table: table.rawValue, createTable: create)
    }
}

#endif

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
