# NeXTBase
Why Yet Another SQLite Wrapper?

Traditional Object-Relational Models (ORM) pretty much 
insist on a single Object class/type definition and that 
there be a 1-to-1 relationship it and a particular table in 
your database. In support of this, most frameworks provide
automatic class/type generation with an incredibly annoying
and timeconsuming cascade effect on the rest of the code
base. In addition, requiring a single common class to
interact with the database creates considerable impacts
across all modules, regardless of what columns/attributes
that module might need; not every features need all the details.

NeXTBase does not have this requirement. Rather, it uses 
the structure and types of the type being used to discern 
what columns are expected or needed for the given table.
Missing columns are added using an "ALTER TABLE .. ADD COLUMN"
as needed. There are, of course, important conventions, but
strong type safety is easily maintained.

NOTE: This auto table creation and alteration can be disabled
by configuration when you need to lock-down the schema for
performance and/or security reasons.

Suppose we have the following types where we expect that both
can reference the same semantic entity from a SQL table.

```swift
struct Profile: Codable, Identifiable {
    var id: Int64
    var name: String
}

struct ProfileDetails: Codable, Identifiable {
    var id: Int64
    var name: String
    var location: Place?
  	var details: String
}
```

This is how easy it can be.

```swift
extension SQLTable.Name {
    static let profiles: Self = "profiles"
}

let db = try SQLDatabase()
let table = db.table(named: "profiles")

let profile = Profile(id: 1, name: "Jane")
let details = ProfileDetails(id: 1, name: "Jane", ...)

try? table.write(profile, to: .profiles)
try? table.write(profileDetails, to: .profiles)
// What just happened? It just works!

let p: Profile? = db.read(id: 1, from: .profiles)
let d: ProfileDetail? = db.select(id: 1, from: .profiles)
// What would you expect? It just works!
```

NeXTBase also supports reading and writing of Tabular
DataFrames where they are supported.

```swift
    let df = try db.dataFrame(from: .profiles, limit: 10)
    print(df)
```

provides output like this

```
┏━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┓
┃   ┃ id      ┃ name     ┃ tag      ┃
┃   ┃ <Int64> ┃ <String> ┃ <String> ┃
┡━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━┩
│ 0 │ 1       │ Jane     │ nil      │
│ 1 │ 2       │ George   │ tagged   │
└───┴─────────┴──────────┴──────────┘
2 rows, 3 columns
```

### What about MVVM, etc?

The short answer is, NeXTBase plays well here.

The longer answer requires a deconstruction of the "Model". As used, the term "model" conflates "schema" with "Source of Truth" (SoT). The Schema **only** specifies the structure of the domain, whereas the SoT is the instantiation of values used for calculation and presentation. The important distinction is NeXTBase favors using Swift structs (and classes) as a facet; its structure defining  a particular reified subset of the overall object graph as specified by the schema; something akin to a Relational Database "View", if you will. In MVVM terms, in the example above, both the `Profile` and `ProfileDetails` naturally provide a View-Model for use in the defining feature or module.

### Unique64

Unique64 provides a monotonically increasing Int64 value
sequence generator that encorporates the current DateTime
with a user defined Int16 tag in the lower bits. Incorporating
the DateTime ensures unique ids from session to session. 
Unlike UUIDs, the sequencing is a feature for database performance.

While not a strict requirement for using NeXTBase, it can
enable the expectation and use of a globally unique Int64
identifier for the records/entities in your database.

```swift

// More realisticaly, you should provide your own
// SystemEntity protocol that redirects eid() to
// use your own sequencer

struct Profile: Codable, Identifiable {
    var id: Int64 = eid() // <- Unique64.shared.next()
    var name: String
}

```

### Action Items

- [ ] Docc Documentation
- [ ] Proper Test Cases
- [ ] An interpolated SQLString
- [ ] Integrate with [Pipeline](https://github.com/sbooth/Pipeline)
- [ ] Integrate with a customized [CSQLite](https://github.com/sbooth/CSQLite)
- [ ] Connection Queue
- [ ] Connection Pool (write + multiple readers)

### Backlog

- [ ] SQLite + Usearch
- [ ] DuckDB integration

### Resources

- ByteCast X - SwiftUI Table with Dynamic Columns | JSON/CSV Tabular Data Frame
    - https://www.youtube.com/watch?v=T1QfB_9rMa0
    - https://github.com/alfianlosari/xca-bytes-cast
    - https://dev.to/canopassoftware/how-to-create-dynamic-table-view-in-swiftui-j1k
    - https://codewithchris.com/swiftui-table/
    
