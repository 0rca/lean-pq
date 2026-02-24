/-
Compile-time tests for async support types and permission gating.
-/
import LeanPq.Async

open LeanPq

namespace Tests.Async

-- Test: PqTask type construction
def testPqTaskType : PqTask String := ⟨Task.pure (.ok "hello")⟩

-- Test: spawnOnNewConn in readOnly context
def testSpawnReadOnly : PqM .readOnly (PqTask (List (List String))) :=
  PqM.spawnOnNewConn "host=localhost" do
    let res ← PqM.execSelect "SELECT 1"
    PqM.fetchAll res

-- Test: spawnOnNewConn from admin context spawning a readOnly task
def testSpawnCrossPermission : PqM .admin (PqTask (List (List String))) :=
  PqM.spawnOnNewConn "host=localhost" (perm := .readOnly) do
    let res ← PqM.execSelect "SELECT 1"
    PqM.fetchAll res

-- Test: await in readOnly context
def testAwait (task : PqTask String) : PqM .readOnly String :=
  PqM.await task

-- Test: concurrent with readOnly actions
def testConcurrentType : PqM .admin (List (List (List String))) :=
  PqM.concurrent "host=localhost" [
    do let r ← PqM.execSelect "SELECT 1"; PqM.fetchAll r,
    do let r ← PqM.execSelect "SELECT 2"; PqM.fetchAll r
  ]

-- Test: both with readOnly actions
def testBothType : PqM .admin (List (List String) × List (List String)) :=
  PqM.both "host=localhost"
    (do let r ← PqM.execSelect "SELECT 1"; PqM.fetchAll r)
    (do let r ← PqM.execSelect "SELECT 2"; PqM.fetchAll r)

-- Test: background preserves permission
def testBackground : PqM .readOnly (PqTask (List (List String))) :=
  PqM.background do
    let res ← PqM.execSelect "SELECT 1"
    PqM.fetchAll res

-- Test: permission gating preserved — cannot spawn admin action from readOnly
-- (uncomment to verify compile-time error)
-- def testSpawnAdminFromReadOnly : PqM .readOnly (PqTask Extern.PGresult) :=
--   PqM.spawnOnNewConn "host=localhost" (PqM.execAdmin "DROP TABLE t")

end Tests.Async
