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

public final class HookBox {
    weak var ndb: NeXTBase?
    var fn: (RowUpdate) -> Void
    
    public init(ndb: NeXTBase, fn: @escaping (RowUpdate) -> Void) {
        self.ndb = ndb
        self.fn = fn
    }
    
    func callAsFunction(op: Int32, db: UnsafeString?, table: UnsafeString?, rowid: Int64) {
        //        let tn = String(utf8String: table.unsafelyUnwrapped).unsafelyUnwrapped
        fn(RowUpdate(rowid: rowid, op: RowChangeType(op)))
    }
}

public extension HookBox {
    static func debug(db: NeXTBase) -> HookBox {
        HookBox(ndb: db) {
            print($0)
        }
    }
}

extension NeXTBase {
    
    public func setUpdateHook(_ hook: HookBox? = nil) {
        let hook = hook ?? .debug(db: self)
        let context = Unmanaged.passRetained(hook).toOpaque()
        if let old = sqlite3_update_hook(ref, { context, op, database_name, table_name, rowid in
            guard let context = context else { return }
            let callback = Unmanaged<HookBox>.fromOpaque(context).takeUnretainedValue()
            callback(op: op, db: database_name, table: table_name, rowid: rowid)
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
