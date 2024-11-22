//
//  SQLStorageType.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 10/15/24.
//
import Foundation

public enum SQLStorageType: Sendable {
    case primaryIntegerKey
    case integer, text, float, blob, null
    var decl: String {
        switch self {
            case .primaryIntegerKey: "INTEGER PRIMARY KEY"
            case .integer: "INTEGER"
            case .blob: "BLOB"
            case .float : "FLOAT"
            case .null: "NULL"
            case .text: "TEXT"
        }
    }
    
    var sqlType: Any.Type {
        switch self {
            case .primaryIntegerKey: Int64.self
            case .integer: Int64.self
            case .blob: Data.self
            case .float: Double.self
            case .text: String.self
            case .null: Optional<String>.self
        }
    }
    
    static func storageType(for any: Any.Type) -> SQLStorageType {
        switch any {
            case is any FixedWidthInteger.Type: .integer
            case is any BinaryFloatingPoint.Type: .float
            case is String.Type: .text
            case is String.SubSequence.Type: .text
            case is Data.Type : .blob
            case let opt as AnyOptional.Type: storageType(for: opt.wrappedType)
            case is Codable.Type: .blob
            default:
                    .null
        }
    }
}
