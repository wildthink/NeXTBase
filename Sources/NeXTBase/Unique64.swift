//
//  Unique64.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/17/24.
//

import Foundation

public protocol SystemEntity: Identifiable where ID == Int64 {}

public extension Identifiable where ID == Int64 {
    @_disfavoredOverload
    static func eid(tag: Int16 = 0) -> Int64 {
        return Unique64.shared.next(tag: tag)
    }
}

/**
 •    Date.timeIntervalSinceReferenceDate provides the time in seconds as a Double.
 •    Multiply the interval to scale it to the desired precision (e.g., microseconds).
 •    Convert it to an Int64, ensuring that the lower 16 bits are cleared by masking or shifting.
 */
public struct Unique64: Sendable {
    private var last: Int64 = 0
    private let lock = NSRecursiveLock() // Ensure thread safety
    
    /// Returns the current time as a 64-bit integer with lower 16 bits set to zero.
    func now() -> Int64 {
        // Get the current time in seconds since reference date
        let interval = Date.timeIntervalSinceReferenceDate
        // Convert to microseconds and clear lower 16 bits
        let scaledInterval = Int64(interval * 1_000_000) & ~0xFFFF
        return scaledInterval
    }
    
    /// Generates the next unique 64-bit value with a 16-bit tag.
    public mutating func next(tag: Int16 = 0) -> Int64 {
        lock.lock() // Begin critical section
        defer { lock.unlock() } // Ensure lock is released
        
        // Generate the base time value
        var next = now()
        // Ensure the sequence is monotonically increasing
        while !(last < next) {
            // Resolve collision by incrementing 17th bit
            // next = next.increment(bit: 16)
            next = next + (1 << 16)
        }
        // Update last value
        last = next
        // Add the tag to the lower 16 bits
        return last | Int64(tag)
    }
}

public extension Unique64 {
    nonisolated(unsafe)
    static var shared = Unique64()
}

/**
 Explanation
 
 1.   date Property:
 •    Masks out the lower 16 bits using self & ~0xFFFF to isolate the timestamp.
 •    Converts the remaining timestamp (in microseconds) back to seconds by dividing by 1,000,000.
 •    Uses the timeIntervalSinceReferenceDate to create a Date object.
 •    Includes a sanity check to ensure the time interval is valid (non-negative).
 
 2.   tag16 Property:
 •    Extracts the lower 16 bits using self & 0xFFFF and converts the result to Int16.
 */
public extension Int64 {
    /// Extracts the `Date` component from a `Unique64`-formatted value.
    ///
    /// Assumes the value is encoded as microseconds since the reference date with the lower 16 bits reserved for the tag.
    var date: Date? {
        let timestamp = self & ~0xFFFF // Mask out the lower 16 bits to get the timestamp
        let timeInterval = Double(timestamp) / 1_000_000 // Convert from microseconds to seconds
        guard timeInterval >= 0 else { return nil } // Sanity check for a valid time interval
        return Date(timeIntervalSinceReferenceDate: timeInterval)
    }
    
    /// Extracts the 16-bit tag from a `Unique64`-formatted value.
    ///
    /// Assumes the lower 16 bits represent the tag.
    var tag16: Int16 {
        return Int16(self & 0xFFFF) // Extract the lower 16 bits
    }
    
    init(date: Date, tag: Int16) {
        let interval = date.timeIntervalSinceReferenceDate
        let scaledInterval = Int64(interval * 1_000_000) & ~0xFFFF
        self = scaledInterval | Int64(tag)
    }
}
