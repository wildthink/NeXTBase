//
//  SQLFunctions.swift
//  NeXTBase
//
//  Created by Jason Jobe on 1/11/25.
//


//
// Copyright © 2015 - 2024 Stephen F. Booth <me@sbooth.org>
// Part of https://github.com/sbooth/Pipeline
// MIT license
//

import Foundation
import SQLite3

// Shims - jmj
public typealias DatabaseValue = Any
public typealias Connection = NeXTBase

func SQLiteValue(_ value: OpaquePointer?) -> Any? {
    let type = sqlite3_value_type(value)
    return switch type {
    case SQLITE_INTEGER:
        sqlite3_value_int64(value)
    case SQLITE_FLOAT:
        sqlite3_value_double(value)
    case SQLITE_TEXT:
        String(cString: sqlite3_value_text(value))
    case SQLITE_BLOB:
        Data(bytes: sqlite3_value_blob(value), count: Int(sqlite3_value_bytes(value)))
    case SQLITE_NULL:
        nil
    default:
        fatalError("Unknown SQLite value type \(type) encountered")
    }
}

typealias SQLiteError = SQLError
extension Connection {
    var databaseConnection: OpaquePointer { self.ref! }
}

extension SQLError {
    init(_ msg: String, takingErrorCodeFromDatabaseConnection: OpaquePointer) {
        fatalError()
    }
}
// End Shims

/// A custom SQL function.
///
/// - parameter values: The SQL function parameters.
///
/// - throws: `Error`.
///
/// - returns: The result of applying the function to `values`.
public typealias SQLFunction = (_ values: [DatabaseValue]) throws -> DatabaseValue

/// Custom SQL function flags.
///
/// - seealso: [Function Flags](https://www.sqlite.org/c3ref/c_deterministic.html)
public struct SQLFunctionFlags: OptionSet, Sendable {
	public let rawValue: Int
	public init(rawValue: Int) {
		self.rawValue = rawValue
	}

	/// The function gives the same output when the input parameters are the same.
	public static let deterministic = SQLFunctionFlags(rawValue: 1 << 0)
	/// The function may only be invoked from top-level SQL, and cannot be used in views or triggers.
	/// nor in schema structures such as `CHECK` constraints, `DEFAULT` clauses, expression indexes, partial indexes, or generated columns.
	public static let directOnly = SQLFunctionFlags(rawValue: 1 << 1)
	/// Indicates to SQLite that a function may call `sqlite3_value_subtype()` to inspect the sub-types of its arguments.
	public static let subtype = SQLFunctionFlags(rawValue: 1 << 2)
	/// The function is unlikely to cause problems even if misused.
	/// An innocuous function should have no side effects and should not depend on any values other than its input parameters.
	public static let innocuous = SQLFunctionFlags(rawValue: 1 << 3)
	/// Indicates to SQLite that a function may call `sqlite3_result_subtype()` to to cause a sub-type to be associated with its result.
	public static let resultSubtype = SQLFunctionFlags(rawValue: 1 << 4)
}

/// A custom SQL aggregate function.
public protocol SQLAggregateFunction {
	/// Invokes the aggregate function for one or more values in a row.
	///
	/// - parameter values: The SQL function parameters.
	///
	/// - throws: `Error`.
	func step(_ values: [DatabaseValue]) throws

	/// Returns the current value of the aggregate function.
	///
	/// - note: This should also reset any function context to defaults.
	///
	/// - throws: `Error`.
	///
	/// - returns: The current value of the aggregate function.
	func final() throws -> DatabaseValue
}

/// A custom SQL aggregate window function.
public protocol SQLAggregateWindowFunction: SQLAggregateFunction {
	/// Invokes the inverse aggregate function for one or more values in a row.
	///
	/// - parameter values: The SQL function parameters.
	///
	/// - throws: `Error`.
	func inverse(_ values: [DatabaseValue]) throws

	/// Returns the current value of the aggregate window function.
	///
	/// - throws: `Error`.
	///
	/// - returns: The current value of the aggregate window function.
	func value() throws -> DatabaseValue
}

extension Connection {
	/// Adds a custom SQL scalar function.
	///
	/// For example, a localized uppercase scalar function could be implemented as:
	/// ```swift
	/// try database.addFunction("localizedUppercase", arity: 1) { values in
	///     let value = values.first.unsafelyUnwrapped
	///     switch value {
	///     case .text(let s):
	///         return .text(s.localizedUppercase())
	///     default:
	///         return value
	///     }
	/// }
	/// ```
	///
	/// - parameter name: The name of the function.
	/// - parameter arity: The number of arguments the function accepts.
	/// - parameter flags: Flags affecting the function's use by SQLite.
	/// - parameter block: A closure that returns the result of applying the function to the supplied arguments.
	///
	/// - throws: An error if the SQL scalar function couldn't be added.
	///
	/// - seealso: [Create Or Redefine SQL Functions](https://sqlite.org/c3ref/create_function.html)
	public func addFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ block: @escaping SQLFunction) throws {
		let function_ptr = UnsafeMutablePointer<SQLFunction>.allocate(capacity: 1)
		function_ptr.initialize(to: block)
		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_function_v2(databaseConnection, name, Int32(arity), function_flags, function_ptr, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { SQLiteValue($0.unsafelyUnwrapped) }
			do {
                set_sqlite3_result(sqlite_context, value: try function_ptr.pointee(arguments as [DatabaseValue]))
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, nil, nil, { context in
			let function_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLFunction.self)
			function_ptr.deinitialize(count: 1)
			function_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL scalar function \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Adds a custom SQL aggregate function.
	///
	/// For example, an integer sum aggregate function could be implemented as:
	/// ```swift
	/// class IntegerSumAggregateFunction: SQLAggregateFunction {
	///     func step(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum += i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func final() throws -> DatabaseValue {
	///         defer {
	///             sum = 0
	///         }
	///         return SQLiteValue(sum)
	///     }
	///
	///     var sum: Int64 = 0
	/// }
	/// ```
	///
	/// - parameter name: The name of the aggregate function.
	/// - parameter arity: The number of arguments the function accepts.
	/// - parameter flags: Flags affecting the function's use by SQLite.
	/// - parameter aggregateFunction: An object defining the aggregate function.
	///
	/// - throws:  An error if the SQL aggregate function can't be added.
	///
	/// - seealso: [Create Or Redefine SQL Functions](https://sqlite.org/c3ref/create_function.html)
	public func addAggregateFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ function: SQLAggregateFunction) throws {
		// function must live until the xDelete function is invoked
		let context_ptr = UnsafeMutablePointer<SQLAggregateFunction>.allocate(capacity: 1)
		context_ptr.initialize(to: function)
		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_function_v2(databaseConnection, name, Int32(arity), function_flags, context_ptr, nil, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			let function = context_ptr.pointee
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { SQLiteValue($0.unsafelyUnwrapped) }
			do {
                try function.step(arguments as [DatabaseValue])
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			let function = context_ptr.pointee
			do {
				set_sqlite3_result(sqlite_context, value: try function.final())
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { context in
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateFunction.self)
			context_ptr.deinitialize(count: 1)
			context_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL aggregate function \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Adds a custom SQL aggregate window function.
	///
	/// For example, an integer sum aggregate window function could be implemented as:
	/// ```swift
	/// class IntegerSumAggregateWindowFunction: SQLAggregateWindowFunction {
	///     func step(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum += i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func inverse(_ values: [DatabaseValue]) throws {
	///         let value = values.first.unsafelyUnwrapped
	///         switch value {
	///             case .integer(let i):
	///                 sum -= i
	///             default:
	///                 throw DatabaseError("Only integer values supported")
	///         }
	///     }
	///
	///     func value() throws -> DatabaseValue {
	///         return SQLiteValue(sum)
	///     }
	///
	///     func final() throws -> DatabaseValue {
	///         defer {
	///             sum = 0
	///         }
	///         return SQLiteValue(sum)
	///     }
	///
	///     var sum: Int64 = 0
	/// }
	/// ```
	///
	/// - parameter name: The name of the aggregate window function.
	/// - parameter arity: The number of arguments the function accepts.
	/// - parameter flags: Flags affecting the function's use by SQLite.
	/// - parameter aggregateWindowFunction: An object defining the aggregate window function.
	///
	/// - throws:  An error if the SQL aggregate window function can't be added.
	///
	/// - seealso: [User-Defined Aggregate Window Functions](https://sqlite.org/windowfunctions.html#udfwinfunc)
	public func addAggregateWindowFunction(_ name: String, arity: Int = -1, flags: SQLFunctionFlags = [.deterministic, .directOnly], _ function: SQLAggregateWindowFunction) throws {
		let context_ptr = UnsafeMutablePointer<SQLAggregateWindowFunction>.allocate(capacity: 1)
		context_ptr.initialize(to: function)
		let function_flags = SQLITE_UTF8 | flags.asSQLiteFlags()
		guard sqlite3_create_window_function(databaseConnection, name, Int32(arity), function_flags, context_ptr, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { SQLiteValue($0.unsafelyUnwrapped) }
			do {
                try function.step(arguments as [DatabaseValue])
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee
			do {
				set_sqlite3_result(sqlite_context, value: try function.final())
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee
			do {
				set_sqlite3_result(sqlite_context, value: try function.value())
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { sqlite_context, argc, argv in
			let context = sqlite3_user_data(sqlite_context)
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			let function = context_ptr.pointee
			let args = UnsafeBufferPointer(start: argv, count: Int(argc))
			let arguments = args.map { SQLiteValue($0.unsafelyUnwrapped) }
			do {
                try function.inverse(arguments as [DatabaseValue])
			} catch let error {
				sqlite3_result_error(sqlite_context, "\(error)", -1)
			}
		}, { context in
			let context_ptr = context.unsafelyUnwrapped.assumingMemoryBound(to: SQLAggregateWindowFunction.self)
			context_ptr.deinitialize(count: 1)
			context_ptr.deallocate()
		}) == SQLITE_OK else {
			throw SQLiteError("Error adding SQL aggregate window function \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}

	/// Removes a custom SQL scalar, aggregate, or window function.
	///
	/// - parameter name: The name of the custom SQL function.
	/// - parameter arity: The number of arguments the custom SQL function accepts.
	///
	/// - throws: An error if the SQL function couldn't be removed.
	public func removeFunction(_ name: String, arity: Int = -1) throws {
		guard sqlite3_create_function_v2(databaseConnection, name, Int32(arity), SQLITE_UTF8, nil, nil, nil, nil, nil) == SQLITE_OK else {
			throw SQLiteError("Error removing SQL function \"\(name)\"", takingErrorCodeFromDatabaseConnection: databaseConnection)
		}
	}
}

private extension SQLFunctionFlags {
	/// Returns the value of `self` using SQLite's flag values.
	func asSQLiteFlags() -> Int32 {
		var flags: Int32 = 0
		if contains(.deterministic) {
			flags |= SQLITE_DETERMINISTIC
		}
		if contains(.directOnly) {
			flags |= SQLITE_DIRECTONLY
		}
		if contains(.subtype) {
			flags |= SQLITE_SUBTYPE
		}
		if contains(.innocuous) {
			flags |= SQLITE_INNOCUOUS
		}
//		if contains(.resultSubtype) {
//			flags |= SQLITE_RESULT_SUBTYPE
//		}
		return flags
	}
}

/// An `sqlite3_context *` object.
///
/// - seealso: [SQL Function Context Object](https://sqlite.org/c3ref/context.html)
typealias SQLiteContext = OpaquePointer

/// Passes `value` to the appropriate `sqlite3_result` function.
///
/// - parameter sqlite_context: An `sqlite3_context *` object.
/// - parameter value: The value to pass.
// jmj
func set_sqlite3_result(_ sqlite_context: SQLiteContext!, value: DatabaseValue) {
    switch value {
    case let i as any FixedWidthInteger:
        sqlite3_result_int64(sqlite_context, Int64(i))
    case let r as Double:
        sqlite3_result_double(sqlite_context, r)
    case let t as String:
        sqlite3_result_text(sqlite_context, t, -1, Connection.transientStorage)
    case let b as Data:
        b.withUnsafeBytes {
            sqlite3_result_blob(sqlite_context, $0.baseAddress, Int32($0.count), Connection.transientStorage)
        }
    default:
        sqlite3_result_null(sqlite_context)
    }
}

extension Connection {
    /// The content pointer is constant and will never change.
    ///
    /// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
    static let staticStorage = unsafeBitCast(0, to: sqlite3_destructor_type.self)
    
    /// The content will likely change in the near future and that SQLite should make its own private copy of the content before returning.
    ///
    /// - seealso: [Constants Defining Special Destructor Behavior](https://sqlite.org/c3ref/c_static.html)
    static let transientStorage = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
}

//func set_sqlite3_result(_ sqlite_context: SQLiteContext!, value: DatabaseValue) {
//	switch value {
//	case .integer(let i):
//		sqlite3_result_int64(sqlite_context, i)
//	case .real(let r):
//		sqlite3_result_double(sqlite_context, r)
//	case .text(let t):
//		sqlite3_result_text(sqlite_context, t, -1, SQLite.transientStorage)
//	case .blob(let b):
//		b.withUnsafeBytes {
//			sqlite3_result_blob(sqlite_context, $0.baseAddress, Int32($0.count), SQLite.transientStorage)
//		}
//	case .null:
//		sqlite3_result_null(sqlite_context)
//	}
//}

/// An `sqlite3_value *` object.
///
/// - seealso: [Obtaining SQL Values](https://sqlite.org/c3ref/value_blob.html)
//typealias SQLiteValue = OpaquePointer

// jmj
//extension DatabaseValue {
//	/// Creates an instance containing `value`.
//	///
//	/// - parameter value: An `sqlite3_value *` object.
//	init(_ value: SQLiteValue) {
//		let type = sqlite3_value_type(value)
//		switch type {
//		case SQLITE_INTEGER:
//			self = .integer(sqlite3_value_int64(value))
//		case SQLITE_FLOAT:
//			self = .real(sqlite3_value_double(value))
//		case SQLITE_TEXT:
//			self = .text(String(cString: sqlite3_value_text(value)))
//		case SQLITE_BLOB:
//			self = .blob(Data(bytes: sqlite3_value_blob(value), count: Int(sqlite3_value_bytes(value))))
//		case SQLITE_NULL:
//			self = .null
//		default:
//			fatalError("Unknown SQLite value type \(type) encountered")
//		}
//	}
//}
