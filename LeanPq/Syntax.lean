/-
Syntax macros for type-safe SQL queries.

Provides helper macros that generate `Query` values with compile-time
column verification.
-/
import LeanPq.Query
import Lean

namespace LeanPq.Syntax

open Lean

/-- `pq_col! colname` expands to `Expr.col "colname" (by decide)` -/
macro "pq_col! " i:ident : term =>
  `(LeanPq.Expr.col $(quote (toString i.getId)) (by decide))

/-- `pq_select_all! table` — select all columns -/
macro "pq_select_all! " t:ident : term =>
  `(LeanPq.Query.select $t .all)

/-- `pq_select! table [col1, col2, ...]` — select specific columns -/
syntax "pq_select! " ident " [" ident,* "]" : term

macro_rules
  | `(pq_select! $t:ident [$cols:ident,*]) => do
    let names : Array (TSyntax `term) := cols.getElems.map fun c =>
      quote (toString c.getId)
    let listSyntax ← `([$names,*])
    `(LeanPq.Query.select $t (.specific $listSyntax (by decide)))

/-- `pq_delete! table` — delete all rows -/
macro "pq_delete! " t:ident : term =>
  `(LeanPq.Query.delete $t)

/-- `pq_create! schema` — create table from schema -/
macro "pq_create! " t:ident : term =>
  `(LeanPq.Query.createTable $t)

/-- `pq_drop! tableName` — drop table -/
macro "pq_drop! " t:ident : term =>
  `(LeanPq.Query.dropTable $(quote (toString t.getId)))

/-- `pq_drop_if_exists! tableName` — drop table if exists -/
macro "pq_drop_if_exists! " t:ident : term =>
  `(LeanPq.Query.dropTable $(quote (toString t.getId)) true)

end LeanPq.Syntax
