//
//  SQLEvents.swift
//  NeXTBase
//
//  Created by Jason Jobe on 11/25/24.
//
import Foundation

/// Possible types of row changes.
public enum RowChangeType: Sendable {
    /// A row was inserted.
    case insert
    /// A row was deleted.
    case delete
    /// A row was updated.
    case update
    /// Something happened but we don't know what
    case unknown
}

public struct RowUpdate: Sendable {
    var rowid: Int64
    var op: RowChangeType
}

typealias UnsafeString = UnsafePointer<CChar>

/// Set the `verbose` flag provide the database and table name to the callback.
/// The default is false to avoid the String creation for each operation.
/// In either case, when an Update Callback it is installed, the database.lastUpdated
/// Date will be updated.
public final class HookBox {
    public typealias Callback = (Int64, RowChangeType, String?, String?) -> Void
    weak var ndb: NeXTBase?
    let fn: Callback
    var verbose: Bool
    
    init(verbose: Bool = false, _ fn: @escaping Callback) {
        self.verbose = verbose
        self.fn = fn
    }
    
    func callAsFunction(rowid: Int64, op: Int32, db: UnsafeString?, table: UnsafeString?) {
        ndb?.lastUpdated = Date()
        if verbose {
            let d_name = if let db { String(utf8String: db) } else { String?.none }
            let t_name = if let table { String(utf8String: table) } else { String?.none }
            fn(rowid, RowChangeType(op), d_name, t_name)
        } else {
            fn(rowid, RowChangeType(op), nil, nil)
        }
    }
}

public extension HookBox {
    
    /// The `standard` update only sets the db.lastUpdated so Observers
    /// are informed of a datbase-wide commit
    static var standard: HookBox {
        HookBox { (_, _, _, _) in
        }
    }

    static var abbreviatedPrint: HookBox {
        HookBox { (row, op, _, _) in
            print(row, op)
        }
    }

    static var debug: HookBox {
        HookBox(verbose: true) { (row, op, db, table) in
            print(row, op, db ?? "main", table ?? "<table>")
        }
    }

}

extension NeXTBase {
    
    public func setUpdateHook(_ hook: HookBox) {
        hook.ndb = self
        let context = Unmanaged.passRetained(hook).toOpaque()
        if let old = sqlite3_update_hook(ref, { context, op, database_name, table_name, rowid in
            guard let context = context else { return }
            let callback = Unmanaged<HookBox>.fromOpaque(context).takeUnretainedValue()
            callback(rowid: rowid, op: op, db: database_name, table: table_name)
        }, context) {
            Unmanaged<HookBox>.fromOpaque(old).release()
        }
    }
    
    /// Removes the update hook.
    @discardableResult
    public func removeUpdateHook() -> AnyObject? {
        guard let old = sqlite3_update_hook(ref, nil, nil)
        else { return nil }
        let oldCallback = Unmanaged<AnyObject>.fromOpaque(old).takeRetainedValue()
        if oldCallback is HookBox {
            print("all good")
        } else {
            print("Callback type mismatch or not found.")
        }
        return oldCallback
    }
}

private extension RowChangeType {
    /// Convenience initializer for conversion of `SQLITE_` values.
    ///
    /// - parameter operation: The second argument to the callback function passed to `sqlite3_update_hook()`.
    init(_ operation: Int32) {
        switch operation {
        case SQLITE_INSERT:
            self = .insert
        case SQLITE_DELETE:
            self = .delete
        case SQLITE_UPDATE:
            self = .update
        default:
            self = .unknown
        }
    }
}

func castUnsafePointer<A: AnyObject>(
    _ mp: UnsafeMutableRawPointer?,
    as t: A.Type = A.self
) -> A? {
    guard let mp else { return nil }
    return mp.assumingMemoryBound(to: AnyObject.self).pointee as? A
}
