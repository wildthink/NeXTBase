//
//  File.swift
//  NeXTBase
//
//  Created by Jason Jobe on 11/21/24.
//

import Foundation

protocol TypedString:
    ExpressibleByStringLiteral, CustomStringConvertible,
    Hashable, Equatable, Sendable {}


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
