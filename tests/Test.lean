import LeanPq
import LeanPq.Monad
import LeanPq.Query
import LeanPq.Schema
import LeanPq.Syntax

import Tests.DataType
import Tests.Schema
import Tests.Monad
import Tests.Async

open LeanPq
open Extern

def conninfo := "host=localhost port=5432 user=postgres password=test dbname=postgres"

/-! ## Helper: assert with descriptive failure -/

def assertEq [BEq α] [ToString α] (label : String) (actual expected : α) : EIO LeanPq.Error Unit :=
  if actual == expected then pure ()
  else throw (.otherError s!"{label}: expected '{expected}', got '{actual}'")

def assertTrue (label : String) (cond : Bool) : EIO LeanPq.Error Unit :=
  if cond then pure ()
  else throw (.otherError s!"{label}: expected true, got false")

/-! ## 1. Basic Connection -/

def testConnect : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let status ← PqStatus conn
  assertEq "connection status" status .connectionOk
  let db ← PqDb conn
  assertEq "database name" db "postgres"
  let user ← PqUser conn
  assertEq "user name" user "postgres"

/-! ## 2. Connection info functions -/

def testConnectionInfo : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let host ← PqHost conn
  assertTrue "host is set" (!host.isEmpty)
  let port ← PqPort conn
  assertEq "port" port "5432"
  let _proto ← PqProtocolVersion conn
  let _ver ← PqServerVersion conn
  let txStatus ← PqTransactionStatus conn
  assertEq "transaction status" txStatus .idle

/-! ## 3. Simple exec (DDL + DML) -/

def testExec : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  -- Create table
  let _ ← PqExec conn "DROP TABLE IF EXISTS test_exec;"
  let createRes ← PqExec conn
    "CREATE TABLE test_exec (id SERIAL PRIMARY KEY, name TEXT NOT NULL, value INTEGER);"
  let createStatus ← PqResultStatus createRes
  assertEq "create table status" createStatus .commandOk

  -- Insert rows
  let insRes ← PqExec conn "INSERT INTO test_exec (name, value) VALUES ('alpha', 10), ('beta', 20);"
  let insStatus ← PqResultStatus insRes
  assertEq "insert status" insStatus .commandOk
  let affected ← PqCmdTuples insRes
  assertEq "rows inserted" affected "2"

  -- Select back
  let selRes ← PqExec conn "SELECT name, value FROM test_exec ORDER BY id;"
  let selStatus ← PqResultStatus selRes
  assertEq "select status" selStatus .tuplesOk
  let nrows ← PqNtuples selRes
  assertEq "row count" nrows 2
  let ncols ← PqNfields selRes
  assertEq "col count" ncols 2

  -- Check field names
  let col0 ← PqFname selRes 0
  assertEq "col 0 name" col0 "name"
  let col1 ← PqFname selRes 1
  assertEq "col 1 name" col1 "value"

  -- Check values
  let v00 ← PqGetvalue selRes 0 0
  assertEq "row 0 col 0" v00 "alpha"
  let v01 ← PqGetvalue selRes 0 1
  assertEq "row 0 col 1" v01 "10"
  let v10 ← PqGetvalue selRes 1 0
  assertEq "row 1 col 0" v10 "beta"
  let v11 ← PqGetvalue selRes 1 1
  assertEq "row 1 col 1" v11 "20"

  -- Cleanup
  let _ ← PqExec conn "DROP TABLE test_exec;"

/-! ## 4. Parameterized queries (PqExecParams) -/

def testExecParams : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let _ ← PqExec conn "DROP TABLE IF EXISTS test_params;"
  let _ ← PqExec conn
    "CREATE TABLE test_params (id SERIAL PRIMARY KEY, name TEXT, value INTEGER);"

  -- Insert with params
  let insRes ← PqExecParams conn
    "INSERT INTO test_params (name, value) VALUES ($1, $2)"
    2 #[0, 0] #["hello", "42"] #[0, 0] #[0, 0] 0
  let insStatus ← PqResultStatus insRes
  assertEq "param insert status" insStatus .commandOk

  -- Select with params
  let selRes ← PqExecParams conn
    "SELECT name, value FROM test_params WHERE name = $1"
    1 #[0] #["hello"] #[0] #[0] 0
  let selStatus ← PqResultStatus selRes
  assertEq "param select status" selStatus .tuplesOk
  let nrows ← PqNtuples selRes
  assertEq "param select rows" nrows 1
  let name ← PqGetvalue selRes 0 0
  assertEq "param select name" name "hello"
  let value ← PqGetvalue selRes 0 1
  assertEq "param select value" value "42"

  let _ ← PqExec conn "DROP TABLE test_params;"

/-! ## 5. Prepared statements -/

def testPrepared : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let _ ← PqExec conn "DROP TABLE IF EXISTS test_prepared;"
  let _ ← PqExec conn
    "CREATE TABLE test_prepared (id SERIAL PRIMARY KEY, label TEXT);"

  -- Prepare
  let prepRes ← PqPrepare conn "ins_label" "INSERT INTO test_prepared (label) VALUES ($1)" 1 #[0]
  let prepStatus ← PqResultStatus prepRes
  assertEq "prepare status" prepStatus .commandOk

  -- Execute prepared multiple times
  for label in ["one", "two", "three"] do
    let execRes ← PqExecPrepared conn "ins_label" 1 #[label] #[0] #[0] 0
    let execStatus ← PqResultStatus execRes
    assertEq s!"exec prepared '{label}'" execStatus .commandOk

  -- Verify
  let selRes ← PqExec conn "SELECT label FROM test_prepared ORDER BY id;"
  let nrows ← PqNtuples selRes
  assertEq "prepared rows" nrows 3
  let v0 ← PqGetvalue selRes 0 0
  assertEq "prepared val 0" v0 "one"
  let v2 ← PqGetvalue selRes 2 0
  assertEq "prepared val 2" v2 "three"

  let _ ← PqExec conn "DROP TABLE test_prepared;"

/-! ## 6. NULL handling -/

def testNulls : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let _ ← PqExec conn "DROP TABLE IF EXISTS test_nulls;"
  let _ ← PqExec conn "CREATE TABLE test_nulls (id SERIAL, val TEXT);"
  let _ ← PqExec conn "INSERT INTO test_nulls (val) VALUES ('present'), (NULL);"

  let res ← PqExec conn "SELECT val FROM test_nulls ORDER BY id;"
  let isNull0 ← PqGetisnull res 0 0
  assertEq "row 0 not null" isNull0 0
  let isNull1 ← PqGetisnull res 1 0
  assertEq "row 1 is null" isNull1 1

  let _ ← PqExec conn "DROP TABLE test_nulls;"

/-! ## 7. Escape functions -/

def testEscape : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let escaped ← PqEscapeLiteral conn "it's a test"
  -- PqEscapeLiteral wraps in quotes and escapes; result should be longer than raw input
  assertTrue "escape non-empty" (!escaped.isEmpty)
  assertTrue "escape wraps input" (escaped.length > "it's a test".length)
  let ident ← PqEscapeIdentifier conn "my table"
  -- PqEscapeIdentifier wraps in double quotes
  assertTrue "identifier non-empty" (!ident.isEmpty)
  assertTrue "identifier wraps input" (ident.length > "my table".length)

/-! ## 8. Query seeded data (from init.sql) -/

def testSeededData : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  -- Query employees table (seeded by init.sql)
  let res ← PqExec conn "SELECT name, department, salary FROM employees WHERE active = true ORDER BY name;"
  let status ← PqResultStatus res
  assertEq "seeded select status" status .tuplesOk
  let nrows ← PqNtuples res
  assertEq "active employees" nrows 4

  let name0 ← PqGetvalue res 0 0
  assertEq "first active employee" name0 "Alice"

  -- Query departments
  let deptRes ← PqExec conn "SELECT name, budget FROM departments ORDER BY name;"
  let deptRows ← PqNtuples deptRes
  assertEq "department count" deptRows 3

/-! ## 9. PqM monad with real DB -/

def testPqMBasic : IO Unit :=
  PqM.withConnectionIO conninfo do
    -- Admin: create table
    let _ ← PqM.execAdmin "DROP TABLE IF EXISTS test_pqm;"
    let _ ← PqM.execAdmin "CREATE TABLE test_pqm (id SERIAL PRIMARY KEY, name TEXT);"
    -- Data-altering: insert
    let _ ← PqM.execModify "INSERT INTO test_pqm (name) VALUES ('from_pqm');"
    -- Read: select
    let res ← PqM.execSelect "SELECT name FROM test_pqm;"
    let rows ← PqM.fetchAll res
    let expected := [["from_pqm"]]
    if rows != expected then
      throw (LeanPq.Error.otherError s!"PqM basic: expected {expected}, got {rows}")
    -- Cleanup
    let _ ← PqM.execAdmin "DROP TABLE test_pqm;"

/-! ## 10. PqM transactions -/

def testPqMTransaction : IO Unit :=
  PqM.withConnectionIO conninfo do
    let _ ← PqM.execAdmin "DROP TABLE IF EXISTS test_tx;"
    let _ ← PqM.execAdmin "CREATE TABLE test_tx (id SERIAL, val INT);"

    -- Successful transaction
    PqM.withTransaction do
      let _ ← PqM.execModify "INSERT INTO test_tx (val) VALUES (1);"
      let _ ← PqM.execModify "INSERT INTO test_tx (val) VALUES (2);"
      pure ()

    let res ← PqM.execSelect "SELECT count(*) FROM test_tx;"
    let rows ← PqM.fetchAll res
    if rows != [["2"]] then
      throw (LeanPq.Error.otherError s!"Transaction commit: expected [[\"2\"]], got {rows}")

    -- Failed transaction (should rollback)
    try
      PqM.withTransaction do
        let _ ← PqM.execModify "INSERT INTO test_tx (val) VALUES (3);"
        throw (LeanPq.Error.otherError "intentional rollback")
    catch _ => pure ()

    -- Count should still be 2
    let res2 ← PqM.execSelect "SELECT count(*) FROM test_tx;"
    let rows2 ← PqM.fetchAll res2
    if rows2 != [["2"]] then
      throw (LeanPq.Error.otherError s!"Transaction rollback: expected [[\"2\"]], got {rows2}")

    let _ ← PqM.execAdmin "DROP TABLE test_tx;"

/-! ## 11. Type-safe Query API with real DB -/

def testQueryAPI : IO Unit :=
  PqM.withConnectionIO (perm := .admin) conninfo do
    let schema : TableSchema :=
      { name := "test_query_api"
        columns := [
          { name := "id", type := .serial, nullable := false },
          { name := "name", type := .text, nullable := false },
          { name := "score", type := .integer, nullable := true }
        ] }

    -- Create table via Query API
    let _ ← PqM.execQuery (Query.createTable schema)

    -- Insert via Query API
    let _ ← PqM.execQuery (Query.insert schema ["name", "score"] [.litStr "Ada", .litInt 100])
    let _ ← PqM.execQuery (Query.insert schema ["name", "score"] [.litStr "Grace", .litInt 95])
    let _ ← PqM.execQuery (Query.insert schema ["name", "score"] [.litStr "Margaret", .litInt 99])

    -- Select all
    let res ← PqM.execQuery (Query.select schema .all)
    let rows ← PqM.fetchAll res
    if rows.length != 3 then
      throw (LeanPq.Error.otherError s!"Query API select all: expected 3 rows, got {rows.length}")

    -- Select with WHERE
    let res2 ← PqM.execQuery (Query.select schema .all
      (some (.binOp .gt (.col "score" (by decide)) (.litInt 97))))
    let rows2 ← PqM.fetchAll res2
    if rows2.length != 2 then
      throw (LeanPq.Error.otherError s!"Query API select where: expected 2 rows, got {rows2.length}")

    -- Delete with WHERE
    let _ ← PqM.execQuery (Query.delete schema
      (some (.binOp .eq (.col "name" (by decide)) (.litStr "Grace"))))

    let res3 ← PqM.execQuery (Query.select schema .all)
    let rows3 ← PqM.fetchAll res3
    if rows3.length != 2 then
      throw (LeanPq.Error.otherError s!"Query API after delete: expected 2 rows, got {rows3.length}")

    -- Drop table via Query API
    let _ ← PqM.execQuery (Query.dropTable "test_query_api")

/-! ## 12. Various data types round-trip -/

def testDataTypes : EIO LeanPq.Error Unit := do
  let conn ← PqConnectDb conninfo
  let _ ← PqExec conn "DROP TABLE IF EXISTS test_types;"
  let _ ← PqExec conn "CREATE TABLE test_types (
    t_bool BOOLEAN,
    t_int INTEGER,
    t_bigint BIGINT,
    t_float DOUBLE PRECISION,
    t_numeric NUMERIC(10,2),
    t_text TEXT,
    t_varchar VARCHAR(100),
    t_date DATE,
    t_uuid UUID,
    t_json JSONB
  );"

  let _ ← PqExec conn "INSERT INTO test_types VALUES (
    true, 42, 9999999999, 3.14, 123.45,
    'hello world', 'varchar val', '2024-06-15',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{\"key\": \"value\"}'
  );"

  let res ← PqExec conn "SELECT * FROM test_types;"
  let status ← PqResultStatus res
  assertEq "types select" status .tuplesOk

  let ncols ← PqNfields res
  assertEq "types col count" ncols 10

  let boolVal ← PqGetvalue res 0 0
  assertEq "bool value" boolVal "t"
  let intVal ← PqGetvalue res 0 1
  assertEq "int value" intVal "42"
  let bigintVal ← PqGetvalue res 0 2
  assertEq "bigint value" bigintVal "9999999999"
  let textVal ← PqGetvalue res 0 5
  assertEq "text value" textVal "hello world"
  let dateVal ← PqGetvalue res 0 7
  assertEq "date value" dateVal "2024-06-15"

  let _ ← PqExec conn "DROP TABLE test_types;"

/-! ## 13. Concurrent queries (async) -/

def testConcurrentQueries : IO Unit :=
  PqM.withConnectionIO (perm := .readOnly) conninfo do
    let actions : List (PqM .readOnly (List (List String))) := [
      do let r ← PqM.execSelect "SELECT 'a' AS val;"; PqM.fetchAll r,
      do let r ← PqM.execSelect "SELECT 'b' AS val;"; PqM.fetchAll r,
      do let r ← PqM.execSelect "SELECT 'c' AS val;"; PqM.fetchAll r
    ]
    let results ← PqM.concurrent conninfo actions
    if results.length != 3 then
      throw (LeanPq.Error.otherError s!"concurrent: expected 3 results, got {results.length}")
    if results != [[["a"]], [["b"]], [["c"]]] then
      throw (LeanPq.Error.otherError s!"concurrent: unexpected results {results}")

/-! ## 14. Spawn and await -/

def testSpawnAndAwait : IO Unit :=
  PqM.withConnectionIO (perm := .readOnly) conninfo do
    let task ← PqM.spawnOnNewConn conninfo do
      let r ← PqM.execSelect "SELECT 42 AS answer;"
      PqM.fetchAll r
    -- Do some local work while background task runs
    let localRes ← PqM.execSelect "SELECT 1 AS local;"
    let localRows ← PqM.fetchAll localRes
    if localRows != [["1"]] then
      throw (LeanPq.Error.otherError s!"spawn local: expected [[\"1\"]], got {localRows}")
    -- Now await the background result
    let bgRows ← PqM.await task
    if bgRows != [["42"]] then
      throw (LeanPq.Error.otherError s!"spawn bg: expected [[\"42\"]], got {bgRows}")

/-! ## 15. Both concurrent -/

def testBothConcurrent : IO Unit :=
  PqM.withConnectionIO (perm := .readOnly) conninfo do
    let (ra, rb) ← PqM.both conninfo
      (do let r ← PqM.execSelect "SELECT 'left' AS side;"; PqM.fetchAll r)
      (do let r ← PqM.execSelect "SELECT 'right' AS side;"; PqM.fetchAll r)
    if ra != [["left"]] then
      throw (LeanPq.Error.otherError s!"both left: expected [[\"left\"]], got {ra}")
    if rb != [["right"]] then
      throw (LeanPq.Error.otherError s!"both right: expected [[\"right\"]], got {rb}")

/-! ## Test runner -/

structure TestResult where
  name : String
  passed : Bool
  error : Option String := none

def runTest (name : String) (test : IO Unit) : IO TestResult := do
  try
    test
    return { name, passed := true }
  catch e =>
    return { name, passed := false, error := some (toString e) }

def runEIOTest (name : String) (test : EIO LeanPq.Error Unit) : IO TestResult :=
  runTest name (test.toIO (fun e => IO.Error.otherError 0 (toString e)))

def main : IO UInt32 := do
  IO.println "=== lean-pq Test Suite ==="
  IO.println ""

  let tests : List (IO TestResult) := [
    runEIOTest "Connect"           testConnect,
    runEIOTest "Connection info"   testConnectionInfo,
    runEIOTest "Simple exec"       testExec,
    runEIOTest "Exec params"       testExecParams,
    runEIOTest "Prepared stmts"    testPrepared,
    runEIOTest "NULL handling"     testNulls,
    runEIOTest "Escape functions"  testEscape,
    runEIOTest "Seeded data"       testSeededData,
    runTest    "PqM basic"         testPqMBasic,
    runTest    "PqM transactions"  testPqMTransaction,
    runTest    "Query API"         testQueryAPI,
    runEIOTest "Data types"        testDataTypes,
    runTest    "Concurrent queries" testConcurrentQueries,
    runTest    "Spawn and await"   testSpawnAndAwait,
    runTest    "Both concurrent"   testBothConcurrent
  ]

  let mut passed := 0
  let mut failed := 0
  let mut failures : List TestResult := []

  for testIO in tests do
    let result ← testIO
    if result.passed then
      IO.println s!"  PASS  {result.name}"
      passed := passed + 1
    else
      IO.println s!"  FAIL  {result.name}: {result.error.getD "unknown error"}"
      failed := failed + 1
      failures := failures ++ [result]

  IO.println ""
  IO.println s!"Results: {passed} passed, {failed} failed, {passed + failed} total"

  if failed > 0 then
    IO.println ""
    IO.println "Failures:"
    for f in failures do
      IO.println s!"  - {f.name}: {f.error.getD ""}"
    return 1
  else
    IO.println "All tests passed!"
    return 0
