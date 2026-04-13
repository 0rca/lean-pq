/-
Permission-tracking monad for PostgreSQL operations.

`PqM perm α` is a reader monad carrying a `Handle` that tracks the permission
level at the type level. Operations are gated by permission:
- `readOnly`:     SELECT queries
- `dataAltering`: INSERT, UPDATE, DELETE
- `admin`:        CREATE, DROP, ALTER, etc.

Lower-permission computations automatically lift into higher-permission contexts
via `MonadLift`, but the reverse is a compile-time error.
-/
import LeanPq.Error
import LeanPq.Extern

namespace LeanPq

/-- Permission levels for database operations, ordered from least to most privileged. -/
inductive Permission where
  | readOnly
  | dataAltering
  | admin
  deriving BEq, DecidableEq, Repr, Inhabited

namespace Permission

/-- Total order: readOnly ≤ dataAltering ≤ admin -/
def le : Permission → Permission → Bool
  | .readOnly, _ => true
  | .dataAltering, .dataAltering | .dataAltering, .admin => true
  | .admin, .admin => true
  | _, _ => false

instance : LE Permission where
  le p1 p2 := p1.le p2 = true

instance (p1 p2 : Permission) : Decidable (p1 ≤ p2) :=
  inferInstanceAs (Decidable (p1.le p2 = true))

end Permission

/-- Typeclass witnessing that permission `p1` is at most `p2`.
    Instances are defined for all 6 valid pairs so typeclass resolution works. -/
class PermLE (p1 p2 : Permission) where
  proof : p1.le p2 = true

instance : PermLE .readOnly .readOnly where proof := rfl
instance : PermLE .readOnly .dataAltering where proof := rfl
instance : PermLE .readOnly .admin where proof := rfl
instance : PermLE .dataAltering .dataAltering where proof := rfl
instance : PermLE .dataAltering .admin where proof := rfl
instance : PermLE .admin .admin where proof := rfl

/-- A reader-like monad carrying a database `Handle` with compile-time permission tracking. -/
structure PqM (perm : Permission) (α : Type) where
  run : Handle → EIO LeanPq.Error α

namespace PqM

@[inline] def pure' (a : α) : PqM perm α :=
  ⟨fun _ => Pure.pure a⟩

@[inline] def bind' (m : PqM perm α) (f : α → PqM perm β) : PqM perm β :=
  ⟨fun conn => do
    let a ← m.run conn
    (f a).run conn⟩

instance : Monad (PqM perm) where
  pure := PqM.pure'
  bind := PqM.bind'

instance : MonadLift (EIO LeanPq.Error) (PqM perm) where
  monadLift action := ⟨fun _ => action⟩

/-- Lift a readOnly computation into a dataAltering context. -/
instance : MonadLift (PqM .readOnly) (PqM .dataAltering) where
  monadLift m := ⟨fun conn => m.run conn⟩

/-- Lift a readOnly computation into an admin context. -/
instance : MonadLift (PqM .readOnly) (PqM .admin) where
  monadLift m := ⟨fun conn => m.run conn⟩

/-- Lift a dataAltering computation into an admin context. -/
instance : MonadLift (PqM .dataAltering) (PqM .admin) where
  monadLift m := ⟨fun conn => m.run conn⟩

instance : MonadExceptOf LeanPq.Error (PqM perm) where
  throw e := ⟨fun _ => throw e⟩
  tryCatch m handler := ⟨fun conn =>
    tryCatch (m.run conn) (fun e => (handler e).run conn)⟩

/-- Lift an IO action into PqM, converting IO errors to LeanPq.Error. -/
def liftIO (action : IO α) : PqM perm α :=
  ⟨fun _ => action.toEIO (fun e => LeanPq.Error.otherError (toString e))⟩

/-- Get the underlying connection handle. -/
def getConn : PqM perm Handle :=
  ⟨fun conn => Pure.pure conn⟩

-- Permission-gated query execution

/-- Execute a read-only SQL query (SELECT). -/
def execSelect (sql : String) : PqM .readOnly Extern.PGresult :=
  ⟨fun conn => Extern.PqExec conn sql⟩

/-- Execute a data-modifying SQL query (INSERT, UPDATE, DELETE). -/
def execModify (sql : String) : PqM .dataAltering Extern.PGresult :=
  ⟨fun conn => Extern.PqExec conn sql⟩

/-- Execute an administrative SQL query (CREATE, DROP, ALTER, etc.). -/
def execAdmin (sql : String) : PqM .admin Extern.PGresult :=
  ⟨fun conn => Extern.PqExec conn sql⟩

/-- Execute a parameterized query (read-only). -/
def execParamsSelect (sql : String) (paramTypes : Array Oid) (paramValues : Array String)
    : PqM .readOnly Extern.PGresult :=
  ⟨fun conn => Extern.PqExecParams conn sql (Int.ofNat paramValues.size)
    paramTypes paramValues (paramValues.map (fun _ => 0)) (paramValues.map (fun _ => 0)) 0⟩

/-- Execute a parameterized query (data-modifying). -/
def execParamsModify (sql : String) (paramTypes : Array Oid) (paramValues : Array String)
    : PqM .dataAltering Extern.PGresult :=
  ⟨fun conn => Extern.PqExecParams conn sql (Int.ofNat paramValues.size)
    paramTypes paramValues (paramValues.map (fun _ => 0)) (paramValues.map (fun _ => 0)) 0⟩

/-- Execute a parameterized query requesting binary format results (read-only). -/
def execParamsSelectBinary (sql : String) (paramTypes : Array Oid) (paramValues : Array String)
    : PqM .readOnly Extern.PGresult :=
  ⟨fun conn => Extern.PqExecParams conn sql (Int.ofNat paramValues.size)
    paramTypes paramValues (paramValues.map (fun _ => 0)) (paramValues.map (fun _ => 0)) 1⟩

/-- Execute a parameterized query requesting binary format results (data-modifying). -/
def execParamsModifyBinary (sql : String) (paramTypes : Array Oid) (paramValues : Array String)
    : PqM .dataAltering Extern.PGresult :=
  ⟨fun conn => Extern.PqExecParams conn sql (Int.ofNat paramValues.size)
    paramTypes paramValues (paramValues.map (fun _ => 0)) (paramValues.map (fun _ => 0)) 1⟩

/-- Run a computation inside a transaction. -/
def withTransaction (body : PqM perm α) : PqM perm α := ⟨fun conn => do
  let _ ← Extern.PqExec conn "BEGIN"
  try
    let result ← body.run conn
    let _ ← Extern.PqExec conn "COMMIT"
    Pure.pure result
  catch e =>
    let _ ← Extern.PqExec conn "ROLLBACK"
    throw e⟩

/-- Fetch all results from a PGresult as a list of rows (each row is a list of strings). -/
def fetchAll (result : Extern.PGresult) : PqM perm (List (List String)) := ⟨fun _ => do
  let nrows ← Extern.PqNtuples result
  let ncols ← Extern.PqNfields result
  let mut rows : List (List String) := []
  for row in [0:nrows.toNat] do
    let mut cols : List String := []
    for col in [0:ncols.toNat] do
      let value ← Extern.PqGetvalue result (Int.ofNat row) (Int.ofNat col)
      cols := cols ++ [value]
    rows := rows ++ [cols]
  Pure.pure rows⟩

/-- Fetch all results as raw byte arrays. For use with binary format queries.
    Each row is an array of (isNull, bytes) pairs. -/
def fetchAllBytes (result : Extern.PGresult) : PqM perm (Array (Array (Option ByteArray))) := ⟨ fun _ => do
  let nrows ← Extern.PqNtuples result
  let ncols ← Extern.PqNfields result
  let mut rows := Array.mkEmpty nrows.toNat

  for row in 0 ... nrows do
    let mut cols := Array.mkEmpty ncols.toNat

    for col in 0 ... ncols do
      let isNull ← Extern.PqGetisnull result row col
      if isNull != 0 then
        cols := cols.push none
      else
        let bytes ← Extern.PqGetvalueBytes result row col
        cols := cols.push (some bytes)
    rows := rows.push cols
  return rows⟩

/-- Connect and run a PqM computation. -/
def withConnection (conninfo : String) (body : PqM perm α) : EIO LeanPq.Error α := do
  let conn ← Extern.PqConnectDb conninfo
  body.run conn

/-- Connect and run a PqM computation, converting to IO. -/
def withConnectionIO (conninfo : String) (body : PqM perm α) : IO α := do
  let result ← (withConnection conninfo body).toIO (fun e => IO.Error.otherError 0 (toString e))
  Pure.pure result

end PqM
end LeanPq
