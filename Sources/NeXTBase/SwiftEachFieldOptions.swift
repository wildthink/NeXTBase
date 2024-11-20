//
//  EachFieldOptions.swift
//  NeXTBase
//
//  Created by Jason Jobe on 9/3/24.
//  Created by Dave DeLong on 6/8/24.
//  https://github.com/davedelong/orm/blob/main/Sources/ORM/Internals/ForEachField.swift
//

import Foundation

public func fields(of type: Any.Type) -> Array<(String, AnyKeyPath)> {
    _openExistential(type, do: fields(of:))
}

public func fields<T>(of type: T.Type = T.self) -> Array<(String, PartialKeyPath<T>)> {
    var all = Array<(String, PartialKeyPath<T>)>()
    enumerateFields(of: type, using: {
        all.append(($0, $1))
    })
    return all
}

public func enumerateFields<T>(of type: T.Type = T.self, using block: (String, PartialKeyPath<T>) -> Void) {
    var options: EachFieldOptions = [.ignoreUnknown]
    _ = forEachFieldWithKeyPath(of: type, options: &options, body: { label, keyPath in
        let string = String(cString: label)
        block(string, keyPath)
        return true
    })
}

public extension AnyKeyPath {
    var erasedRootType: any Any.Type { type(of: self).rootType }
    var erasedValueType: any Any.Type { type(of: self).valueType }
}

internal struct EachFieldOptions: OptionSet {
    static let classType = Self(rawValue: 1 << 0)
    static let ignoreUnknown = Self(rawValue: 1 << 1)
    
    let rawValue: UInt32
    
    init(rawValue: UInt32) {
        self.rawValue = rawValue
    }
}

@discardableResult
@_silgen_name("$ss24_forEachFieldWithKeyPath2of7options4bodySbxm_s01_bC7OptionsVSbSPys4Int8VG_s07PartialeF0CyxGtXEtlF")
private func forEachFieldWithKeyPath<Root>(
    of type: Root.Type,
    options: inout EachFieldOptions,
    body: (UnsafePointer<CChar>, PartialKeyPath<Root>) -> Bool
) -> Bool
