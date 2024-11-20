//
//  SQLColumn.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import Foundation

public struct SQLColumn {
    public var name: String
    public var definition: String { sqlType.decl }
    public var sqlType: SQLStorageType
    public var dataType: Codable.Type?
    
    //    public var encode: (Codable) -> SQLStorageType
    //    public var decode: (SQLStorageType) -> Codable
    
    public func encode(_ it: Encodable) throws -> SQLStorageType {
        throw SQLError(code: 0, "encode() NOT implemented")
    }
    
    public func decode(_ it: Any) throws -> Any {
        func _decode<D:Decodable>(_ type: D.Type) -> (Any, Error?) {
            do {
                guard let data = it as? Data
                else { return (it, nil) }
                let nob = try JSONDecoder().decode(D.self, from: data)
                return (nob, nil)
            } catch {
                return (Optional<Any>.none as Any, error)
            }
        }
        if let dataType {
            let (value, error) = _openExistential(dataType, do: _decode)
            if let error { throw error }
            return value
        }
        return it
    }
    
    public var declaration: String {
        return "'\(name)' \(sqlType.decl)"
    }
    
    public func format() -> String {
        if let dt = dataType {
            return "'\(name)' \(sqlType.decl) -> \(String(describing: dt))"
        } else {
            return "'\(name)' \(sqlType) -> ?"
        }
    }
}

extension SQLColumn: CustomStringConvertible {
    public var description: String { format() }
}

public extension SQLColumn {
    
    init<D>(name: String, dataType: D.Type)
    where D: Codable {
        self.name = name
        self.sqlType = .storageType(for: dataType)
        self.dataType = dataType
    }
    
    init(at ndx: Int32, in stmt: SQLStatement, dataType: Codable.Type? = nil) {
        self.name = stmt.columnName(at: ndx) ?? "<column>"
        self.sqlType =  stmt.columnType(at: ndx)
        self.dataType = dataType
    }
}
