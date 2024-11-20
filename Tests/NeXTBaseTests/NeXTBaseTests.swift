//
//  NeXTBaseTests.swift
//  NeXTBase
//
//  Copyright (C) 2024 WildThink, Inc - All Rights Reserved
//  Created by Jason Jobe on 11/19/24.
//
import Testing
@testable import NeXTBase
import Foundation
import TabularData

@Test func example() async throws {
    // Write your test here and use APIs like `#expect(...)` to check expected conditions.
    let rep = try testDB()
    var rlines = [String]()
    rep.enumerateLines { line, stop in
        rlines.append(line)
    }
    for (a, b) in zip(rlines, expected_report) {
//        print("\(a) =? \(b)")
        #expect(a == b)
    }
}

let expected_report: [String] = [
    #"Person(id: 1, name: "Jane")"#,
    #"Person(id: 2, name: "Judy")"#,
    #"Person(id: 3, name: "Leroy")"#,
    #"PersonII(id: 1, name: "Jane", tag: Optional("tagged"))"#,
    #"PersonII(id: 2, name: "Judy", tag: Optional(""))"#,
    #"BEFORE FORGET"#,
    // TODO: Properly compare as Dictionary
//    #"["id": 1, "name": "Jane", "tag": "tagged"]"#,
//    #"["id": 2, "name": "Judy", "tag": ""]"#,
//    #"["id": 3, "name": "Leroy", "tag": "kid"]"#,
//    #"AFTER FORGET"#,
]

struct Person: Codable {
    var id: Int
    var name: String
}

struct PersonII: Codable {
    var id: Int
    var name: String
    var tag: String?
}

func testDB() throws -> String {
    var result = ""
    func report(_ arg: Any) {
        Swift.print(arg, to: &result)
    }
    
    let db = try SQLDatabase(path: ":memory:")
    let t = SQLTable(database: db, tableName: "people")
    
    try t.createOrUpdateTable(for: Person.self)
    let p = Person(id: 1, name: "Jane")
    try t.write(p)
    
    try t.createOrUpdateTable(for: PersonII.self)
    let p2 = PersonII(id: 1, name: "George", tag: "tagged")
    try t.write(p2)
    try t.write(p)
    try t.write(Person(id: 2, name: "Judy"))
    try t.write(PersonII(id: 3, name: "Leroy", tag: "kid"))
    
    let p3: [Person] = try t.read()
    for p in p3 { report(p) }
    
    let p4: [PersonII] = try t.read(limit: 2)
    for p in p4 { report(p) }

    let stmt = try db.prepareStatement("SELECT * FROM people")
    report("BEFORE FORGET")
    let df = try DataFrame(statement: stmt)
    print(df)

    try t.forget(id: 1)

    report("AFTER FORGET")
    try stmt.reset()
    let df2 = try DataFrame(statement: stmt)
    print(df2)

    return result
}

func unique64Check() {
    
    var uniqueGenerator = Unique64()
    
    let zeroTag = uniqueGenerator.next(tag: 0)
    
    let first = uniqueGenerator.next(tag: 42)
    let second = uniqueGenerator.next(tag: 1)
    
    print("Now   ", Date())
    print("Tag 0 ", zeroTag.date!, "0\(zeroTag.tag16)", zeroTag)
    print(String(zeroTag, radix: 16))
    
    print("first ", first.date!, first.tag16, first)
    print(String(first, radix: 16))
    
    print("second", second.date!, "0\(second.tag16)", second)
    print(String(second, radix: 16))
    
    print("first < second", first < second)
}
