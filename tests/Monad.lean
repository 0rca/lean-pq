/-
Compile-time tests for Permission tracking and PqM monad.
-/
import LeanPq.Monad
import LeanPq.Query

open LeanPq

namespace Tests.Monad

-- Test: Permission ordering
example : Permission.readOnly ≤ Permission.readOnly := by decide
example : Permission.readOnly ≤ Permission.dataAltering := by decide
example : Permission.readOnly ≤ Permission.admin := by decide
example : Permission.dataAltering ≤ Permission.dataAltering := by decide
example : Permission.dataAltering ≤ Permission.admin := by decide
example : Permission.admin ≤ Permission.admin := by decide

-- Test: Permission non-ordering (uncomment to verify compile-time errors)
-- example : Permission.admin ≤ Permission.readOnly := by decide         -- ERROR
-- example : Permission.dataAltering ≤ Permission.readOnly := by decide  -- ERROR
-- example : Permission.admin ≤ Permission.dataAltering := by decide     -- ERROR

-- Test: PqM .readOnly allows SELECT
def readOnlyProgram : PqM .readOnly Extern.PGresult :=
  PqM.execSelect "SELECT 1"

-- Test: PqM .dataAltering allows INSERT
def dataAlteringProgram : PqM .dataAltering Extern.PGresult :=
  PqM.execModify "INSERT INTO t VALUES (1)"

-- Test: PqM .admin allows DDL
def adminProgram : PqM .admin Extern.PGresult :=
  PqM.execAdmin "CREATE TABLE t (id INT)"

-- Test: readOnly lifts into dataAltering
def liftReadToData : PqM .dataAltering Extern.PGresult := do
  let _ ← PqM.execSelect "SELECT 1"
  PqM.execModify "INSERT INTO t VALUES (1)"

-- Test: readOnly lifts into admin
def liftReadToAdmin : PqM .admin Extern.PGresult := do
  let _ ← PqM.execSelect "SELECT 1"
  PqM.execAdmin "DROP TABLE t"

-- Test: dataAltering lifts into admin
def liftDataToAdmin : PqM .admin Extern.PGresult := do
  let _ ← PqM.execModify "INSERT INTO t VALUES (1)"
  PqM.execAdmin "DROP TABLE t"

-- Test: PqM .readOnly does NOT allow INSERT (uncomment to verify)
-- def readOnlyReject : PqM .readOnly Extern.PGresult :=
--   PqM.execModify "INSERT INTO t VALUES (1)"  -- ERROR: type mismatch

-- Test: PqM .readOnly does NOT allow DDL (uncomment to verify)
-- def readOnlyRejectAdmin : PqM .readOnly Extern.PGresult :=
--   PqM.execAdmin "DROP TABLE t"  -- ERROR: type mismatch

-- Test: withTransaction composes
def transactionProgram : PqM .dataAltering Unit :=
  PqM.withTransaction do
    let _ ← PqM.execModify "INSERT INTO t VALUES (1)"
    let _ ← PqM.execModify "INSERT INTO t VALUES (2)"
    pure ()

-- Test: execQuery with permission proof
section ExecQueryPermission
  open LeanPq

  def testSchema : TableSchema :=
    { name := "t", columns := [{ name := "id", type := .integer, nullable := false }] }

  -- SELECT in readOnly context — proof auto-discharged
  def selectInReadOnly : PqM .readOnly Extern.PGresult :=
    PqM.execQuery (Query.select testSchema .all)

  -- INSERT in dataAltering context — proof auto-discharged
  def insertInDataAltering : PqM .dataAltering Extern.PGresult :=
    PqM.execQuery (Query.insert testSchema ["id"] [.litInt 1])

  -- CREATE TABLE in admin context — proof auto-discharged
  def createInAdmin : PqM .admin Extern.PGresult :=
    PqM.execQuery (Query.createTable testSchema)

  -- SELECT in admin context — proof auto-discharged (readOnly ≤ admin)
  def selectInAdmin : PqM .admin Extern.PGresult :=
    PqM.execQuery (Query.select testSchema .all)

  -- DROP TABLE in readOnly context — DOES NOT COMPILE (uncomment to verify)
  -- def dropInReadOnly : PqM .readOnly Extern.PGresult :=
  --   PqM.execQuery (Query.dropTable "t")  -- ERROR: proof obligation fails

end ExecQueryPermission

end Tests.Monad
