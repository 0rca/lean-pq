/-
Async support for PostgreSQL operations using Lean's Task-based concurrency.

Each concurrent query runs on its own connection (libpq connections are not
thread-safe). The key Lean primitive is:

  EIO.asTask : EIO ε α → BaseIO (Task (Except ε α))
-/
import LeanPq.Error
import LeanPq.Extern
import LeanPq.Monad

namespace LeanPq

/-- A handle to a pending database operation running on a background thread. -/
structure PqTask (α : Type) where
  task : Task (Except LeanPq.Error α)

namespace PqM

/-- Spawn a PqM action on a new background connection.
    The action opens its own connection using `conninfo`, runs on a separate
    Lean task, and returns a `PqTask` that can be awaited later.
    The caller's connection is untouched. -/
def spawnOnNewConn (conninfo : String) (action : PqM perm α) : PqM perm' (PqTask α) :=
  ⟨fun _ => do
    let task ← (do
      let conn ← Extern.PqConnectDb conninfo
      action.run conn
    ).asTask
    pure ⟨task⟩⟩

/-- Block until a background task completes, re-raising any error. -/
def await (task : PqTask α) : PqM perm α :=
  ⟨fun _ => do
    match task.task.get with
    | .ok a => pure a
    | .error e => throw e⟩

/-- Run N actions concurrently, each on its own connection.
    Returns results in the same order as the input list. -/
def concurrent (conninfo : String) (actions : List (PqM perm α)) : PqM perm' (List α) :=
  ⟨fun _ => do
    -- Spawn all tasks
    let tasks ← actions.mapM fun action => do
      let task ← (do
        let conn ← Extern.PqConnectDb conninfo
        action.run conn
      ).asTask
      pure (PqTask.mk task)
    -- Await all results in order
    tasks.mapM fun task => do
      match task.task.get with
      | .ok a => pure a
      | .error e => throw e⟩

/-- Run two actions concurrently on separate connections, return both results. -/
def both (conninfo : String) (a : PqM perm α) (b : PqM perm β) : PqM perm' (α × β) :=
  ⟨fun _ => do
    let taskA ← (do
      let conn ← Extern.PqConnectDb conninfo
      a.run conn
    ).asTask
    let taskB ← (do
      let conn ← Extern.PqConnectDb conninfo
      b.run conn
    ).asTask
    let ra := taskA.get
    let rb := taskB.get
    match ra, rb with
    | .ok va, .ok vb => pure (va, vb)
    | .error e, _ => throw e
    | _, .error e => throw e⟩

/-- Spawn an action on the current connection as a background task.
    **Unsafe**: the caller must not use the connection until the task is awaited. -/
def background (action : PqM perm α) : PqM perm (PqTask α) :=
  ⟨fun conn => do
    let task ← (action.run conn).asTask
    pure ⟨task⟩⟩

end PqM

/-- Standalone helper: run N PqM actions concurrently, each on its own connection.
    Converts the final result to IO. -/
def withConcurrentIO (conninfo : String) (actions : List (PqM perm α)) : IO (List α) := do
  let eio : EIO LeanPq.Error (List α) := do
    let tasks ← actions.mapM fun action => do
      let task ← (do
        let conn ← Extern.PqConnectDb conninfo
        action.run conn
      ).asTask
      pure (PqTask.mk task)
    tasks.mapM fun task => do
      match task.task.get with
      | .ok a => pure a
      | .error e => throw e
  eio.toIO (fun e => IO.Error.otherError 0 (toString e))

end LeanPq
