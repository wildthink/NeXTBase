//
//  StatementAuthorizer.swift
//  NeXTBase
//
//  Created by Jason Jobe on 11/21/24.
//

#if canImport(string_h)
import string_h
#elseif os(Linux)
import Glibc
#elseif os(macOS) || os(iOS) || os(watchOS) || os(tvOS) || os(visionOS)
import Darwin
#endif
// Referencing github.com/groue/GRDB.swift/GRDB/Core/StatementAuthorizer.swift

/// `StatementAuthorizer` provides information about compiled database
/// statements, and prevents the truncate optimization when row deletions are
/// observed by transaction observers.
///
/// <https://www.sqlite.org/c3ref/set_authorizer.html>
/// <https://www.sqlite.org/lang_delete.html#the_truncate_optimization>
final class StatementAuthorizer {

    /// Registers the authorizer with `sqlite3_set_authorizer`.
    func register(in database: SQLDatabase) {
        let authorizerP = Unmanaged.passUnretained(self).toOpaque()
        sqlite3_set_authorizer(
            database.ref,
            { (authorizerP, actionCode, cString1, cString2, cString3, cString4) in
                Unmanaged<StatementAuthorizer>
                    .fromOpaque(authorizerP.unsafelyUnwrapped)
                    .takeUnretainedValue()
                    .authorize(actionCode, cString1, cString2, cString3, cString4)
            },
            authorizerP)
    }
    
    /// Reset before compiling a new statement
    func reset() {
    }
    
    private func authorize(
        _ actionCode: CInt,
        _ cString1: UnsafePointer<CChar>?,
        _ cString2: UnsafePointer<CChar>?,
        _ cString3: UnsafePointer<CChar>?,
        _ cString4: UnsafePointer<CChar>?)
    -> CInt
    {
        switch actionCode {
        // Schema locked
        case SQLITE_DROP_TABLE, SQLITE_DROP_VTABLE,
             SQLITE_DROP_INDEX,
             SQLITE_DROP_VIEW,
             SQLITE_DROP_TRIGGER,
             SQLITE_ALTER_TABLE:
            return SQLITE_OK
            
        case SQLITE_ATTACH, SQLITE_DETACH,
                SQLITE_CREATE_INDEX, SQLITE_CREATE_TABLE:
                return SQLITE_OK
        
        // Mostly TEMP Table
        case SQLITE_DROP_TEMP_INDEX, SQLITE_DROP_TEMP_TRIGGER,
             SQLITE_DROP_TEMP_VIEW, SQLITE_DROP_TEMP_TABLE,
             SQLITE_CREATE_TEMP_INDEX, SQLITE_CREATE_TEMP_TABLE,
             SQLITE_CREATE_TEMP_TRIGGER, SQLITE_CREATE_TEMP_VIEW,
             SQLITE_CREATE_TRIGGER, SQLITE_CREATE_VIEW,
             SQLITE_CREATE_VTABLE:
            return SQLITE_OK
            
        case SQLITE_READ:
//            guard let tableName = cString1.map(String.init) else { return SQLITE_OK }
//            guard let columnName = cString2.map(String.init) else { return SQLITE_OK }
            return SQLITE_OK
            
        case SQLITE_INSERT:
//            guard let tableName = cString1.map(String.init) else { return SQLITE_OK }
            return SQLITE_OK
            
        case SQLITE_DELETE:
             guard let cString1 else { return SQLITE_OK }
            
            // Deletions from sqlite_master and sqlite_temp_master are not like
            // other deletions: `sqlite3_update_hook` does not notify them, and
            // they are prevented when the truncate optimization is disabled.
            // Let's always authorize such deletions by returning SQLITE_OK:
            guard strcmp(cString1, "sqlite_master") != 0 else { return SQLITE_OK }
            guard strcmp(cString1, "sqlite_temp_master") != 0 else { return SQLITE_OK }
            return SQLITE_OK

        case SQLITE_UPDATE:
//            guard let tableName = cString1.map(String.init) else { return SQLITE_OK }
//            guard let columnName = cString2.map(String.init) else { return SQLITE_OK }
            return SQLITE_OK
            
        case SQLITE_TRANSACTION:
             return SQLITE_OK
            
        case SQLITE_SAVEPOINT:
             return SQLITE_OK
            
        case SQLITE_FUNCTION:
            return SQLITE_OK
            
        default:
            return SQLITE_OK
        }
    }
}
