import LeanPq

open LeanPq
open LeanPq.Extern
open LeanPq.Syntax

/-- Helper to convert IO errors to LeanPq.Error for EIO composition. -/
def IO.toLeanPqEIO (action : IO α) : EIO LeanPq.Error α :=
  action.toEIO (fun e => LeanPq.Error.otherError (toString e))

-- Example 1: pq! macro — the primary query-building interface
def examplePqMacro : IO Unit := do
  -- Define a schema (compile-time verified)
  let products : TableSchema :=
    { name := "example_products"
      columns := [
        { name := "id", type := .serial, nullable := false },
        { name := "name", type := .text, nullable := false },
        { name := "price", type := .numeric (some 10) (some 2), nullable := false },
        { name := "in_stock", type := .boolean, nullable := false }
      ] }

  IO.println "-- pq! macro renders SQL with parameterized values --"

  -- SELECT all
  let (sql, params) := (pq! select products).render
  IO.println s!"  {sql}  params={params}"

  -- SELECT specific columns with WHERE
  let (sql, params) := (pq! select products [name, price] | price > "5.00").render
  IO.println s!"  {sql}  params={params}"

  -- SELECT with ORDER BY and LIMIT
  let (sql, params) := (pq! select products | in_stock = true orderby price desc limit 5).render
  IO.println s!"  {sql}  params={params}"

  -- INSERT
  let (sql, params) := (pq! insert products [name, price, in_stock] ["Widget", "9.99", true]).render
  IO.println s!"  {sql}  params={params}"

  -- UPDATE with WHERE
  let (sql, params) := (pq! update products [price := "12.99"] | name = "Widget").render
  IO.println s!"  {sql}  params={params}"

  -- DELETE with WHERE
  let (sql, params) := (pq! delete products | name = "Widget").render
  IO.println s!"  {sql}  params={params}"

  -- DDL
  let (sql, _) := (pq! create products).render
  IO.println s!"  {sql}"
  let (sql, _) := (pq! drop_if_exists products).render
  IO.println s!"  {sql}"

-- Example 2: pq! with PqM monad — full CRUD against a real database
def examplePqMacroLive : IO Unit :=
  PqM.withConnectionIO (perm := .admin) "host=localhost port=5432 dbname=postgres user=postgres password=test" do
    let schema : TableSchema :=
      { name := "example_pq_macro"
        columns := [
          { name := "id", type := .serial, nullable := false },
          { name := "name", type := .text, nullable := false },
          { name := "email", type := .text, nullable := false }
        ] }

    let _ ← PqM.execQuery (pq! drop_if_exists schema)
    let _ ← PqM.execQuery (pq! create schema)
    let _ ← PqM.execQuery (pq! insert schema [name, email] ["Alice", "alice@example.com"])
    let _ ← PqM.execQuery (pq! insert schema [name, email] ["Bob", "bob@example.com"])

    let rows ← PqM.query (pq! select schema)
    PqM.liftIO (IO.println s!"  All users: {rows}")

    let rows ← PqM.query (pq! select schema [name] | name = "Alice")
    PqM.liftIO (IO.println s!"  Alice: {rows}")

    let _ ← PqM.execQuery (pq! update schema [email := "bob2@example.com"] | name = "Bob")
    let rows ← PqM.query (pq! select schema | name = "Bob")
    PqM.liftIO (IO.println s!"  Bob updated: {rows}")

    let _ ← PqM.execQuery (pq! delete schema | name = "Bob")
    let rows ← PqM.query (pq! select schema)
    PqM.liftIO (IO.println s!"  After delete: {rows}")

    let _ ← PqM.execQuery (pq! drop_if_exists schema)

-- Example 3: Raw SQL escape hatch — for complex queries not covered by pq!
def exampleRawSQL : IO Unit :=
  PqM.withConnectionIO "host=localhost port=5432 dbname=postgres user=postgres password=test" do
    let _ ← PqM.execAdmin "DROP TABLE IF EXISTS example_raw;"
    let _ ← PqM.execAdmin "CREATE TABLE example_raw (id serial PRIMARY KEY, name text, score int);"
    let _ ← PqM.execModify "INSERT INTO example_raw (name, score) VALUES ('Alice', 100), ('Bob', 85);"

    let result ← PqM.execSelect "SELECT name, score FROM example_raw ORDER BY score DESC;"
    let rows ← PqM.fetchAll result
    PqM.liftIO (IO.println s!"  Raw SQL result: {rows}")

    let _ ← PqM.execAdmin "DROP TABLE example_raw;"

def main : IO Unit := do
  IO.println "=== Example 1: pq! macro (compile-time only) ==="
  examplePqMacro

  IO.println "\n=== Example 2: pq! macro with live database ==="
  examplePqMacroLive

  IO.println "\n=== Example 3: Raw SQL escape hatch ==="
  exampleRawSQL

  IO.println "\nAll examples done."
