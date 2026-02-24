import LeanPq

open LeanPq
open LeanPq.Extern
open LeanPq.Syntax

/-- Helper to convert IO errors to LeanPq.Error for EIO composition. -/
def IO.toLeanPqEIO (action : IO α) : EIO LeanPq.Error α :=
  action.toEIO (fun e => LeanPq.Error.otherError (toString e))

-- Example 1: Low-level API (PqExec)
def exampleLowLevel : EIO LeanPq.Error Unit := do
  let conninfo := "host=localhost port=5432 dbname=postgres user=postgres password=test"
  let conn ← PqConnectDb conninfo
  let connStatus ← PqStatus conn
  IO.toLeanPqEIO (IO.println s!"connection status: {connStatus}")
  let result ← PqExec conn "SELECT 1 AS num, 'hello' AS greeting;"
  let resStatus ← PqResultStatus result
  IO.toLeanPqEIO (IO.println s!"Result status: {resStatus}")

  let nrows ← PqNtuples result
  let ncols ← PqNfields result
  IO.toLeanPqEIO (IO.println s!"Rows: {nrows}, Cols: {ncols}")

  for row in [0:nrows.toNat] do
    for col in [0:ncols.toNat] do
      let value ← PqGetvalue result (Int.ofNat row) (Int.ofNat col)
      let fname ← PqFname result (Int.ofNat col)
      IO.toLeanPqEIO (IO.println s!"  {fname} = {value}")

-- Example 2: PqM monad with permission tracking
def exampleMonad : IO Unit :=
  PqM.withConnectionIO "host=localhost port=5432 dbname=postgres user=postgres password=test" do
    -- This is a PqM .admin context (highest permission)
    let _ ← PqM.execAdmin "DROP TABLE IF EXISTS example_users;"
    let _ ← PqM.execAdmin "CREATE TABLE example_users (id serial PRIMARY KEY, name text, email text);"

    -- Data-modifying operations lift into admin context
    let _ ← PqM.execModify "INSERT INTO example_users (name, email) VALUES ('Alice', 'alice@example.com');"
    let _ ← PqM.execModify "INSERT INTO example_users (name, email) VALUES ('Bob', 'bob@example.com');"

    -- Read-only operations also lift into admin context
    let result ← PqM.execSelect "SELECT * FROM example_users;"
    let rows ← PqM.fetchAll result
    PqM.liftIO (IO.println s!"Users: {rows}")

    -- Cleanup
    let _ ← PqM.execAdmin "DROP TABLE example_users;"
    pure ()

-- Example 3: Type-safe queries with schema verification
def exampleTypeSafe : IO Unit := do
  -- Define a schema (compile-time)
  let products : TableSchema :=
    { name := "example_products"
      columns := [
        { name := "id", type := .serial, nullable := false },
        { name := "name", type := .text, nullable := false },
        { name := "price", type := .numeric (some 10) (some 2), nullable := false }
      ] }

  -- Render queries — all string values become $N parameters
  let (createSql, _) := (Query.createTable products).render
  IO.println s!"CREATE: {createSql}"

  let insertQ := Query.insert products ["name", "price"] [.litStr "Widget", .litStr "9.99"]
  let (insertSql, insertParams) := insertQ.render
  IO.println s!"INSERT: {insertSql}"
  IO.println s!"  params: {insertParams}"

  let selectQ := Query.select products .all
    (some (.binOp .gt (.col "price" (by decide)) (.litStr "5.00")))
  let (selectSql, selectParams) := selectQ.render
  IO.println s!"SELECT: {selectSql}"
  IO.println s!"  params: {selectParams}"

  let deleteQ := Query.delete products
    (some (.binOp .eq (.col "name" (by decide)) (.litStr "Widget")))
  let (deleteSql, deleteParams) := deleteQ.render
  IO.println s!"DELETE: {deleteSql}"
  IO.println s!"  params: {deleteParams}"

  -- Syntax macro examples
  let macroQ := pq_select_all! products
  let (macroSql, macroParams) := macroQ.render
  IO.println s!"pq_select_all!: {macroSql}"
  IO.println s!"  params: {macroParams}"

  let macroQ2 := pq_select! products [name, price]
  let (macroSql2, macroParams2) := macroQ2.render
  IO.println s!"pq_select! [name, price]: {macroSql2}"
  IO.println s!"  params: {macroParams2}"

def main : IO Unit := do
  IO.println "=== Example 3: Type-safe query rendering ==="
  exampleTypeSafe

  IO.println "\n=== Example 1: Low-level API ==="
  let _ ← exampleLowLevel.toIO (fun e => IO.Error.otherError 0 (toString e))

  IO.println "\n=== Example 2: PqM monad ==="
  exampleMonad

  IO.println "\nAll examples done."
