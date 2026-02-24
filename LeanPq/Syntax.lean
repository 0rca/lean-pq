/-
Syntax macros for type-safe SQL queries.

Provides the unified `pq!` macro that generates `Query` values with
compile-time column verification.
-/
import LeanPq.Query
import Lean

namespace LeanPq.Syntax

open Lean

/-! ## Expression syntax category -/

declare_syntax_cat pq_expr

-- Atoms
syntax ident                              : pq_expr  -- column ref, or true/false/null
syntax str                                : pq_expr  -- string literal
syntax num                                : pq_expr  -- numeric literal

-- Grouping
syntax "(" pq_expr ")"                    : pq_expr

-- Unary
syntax:60 "!" pq_expr:60                  : pq_expr  -- NOT

-- Null checks (postfix-style)
syntax:55 pq_expr:55 " is_null"           : pq_expr
syntax:55 pq_expr:55 " is_not_null"       : pq_expr

-- Comparisons (prec 50)
syntax:50 pq_expr:51 " = " pq_expr:51    : pq_expr
syntax:50 pq_expr:51 " != " pq_expr:51   : pq_expr
syntax:50 pq_expr:51 " > " pq_expr:51    : pq_expr
syntax:50 pq_expr:51 " < " pq_expr:51    : pq_expr
syntax:50 pq_expr:51 " >= " pq_expr:51   : pq_expr
syntax:50 pq_expr:51 " <= " pq_expr:51   : pq_expr
syntax:50 pq_expr:51 " like " pq_expr:51 : pq_expr
syntax:50 pq_expr:51 " ilike " pq_expr:51 : pq_expr

-- Logical (prec 40 and 30)
syntax:40 pq_expr:40 " && " pq_expr:41   : pq_expr  -- AND
syntax:30 pq_expr:30 " || " pq_expr:31   : pq_expr  -- OR

/-! ## Elaboration: pq_expr → Expr term -/

/-- Translate a `pq_expr` syntax node into a Lean `term` building an `Expr`. -/
partial def elabPqExpr : TSyntax `pq_expr → MacroM (TSyntax `term)
  -- identifier: check for special names true/false/null, otherwise column ref
  | `(pq_expr| $i:ident) => do
    let name := toString i.getId
    if name == "true" then
      `(LeanPq.Expr.litBool Bool.true)
    else if name == "false" then
      `(LeanPq.Expr.litBool Bool.false)
    else if name == "null" then
      `(LeanPq.Expr.litNull)
    else do
      let nameLit : TSyntax `term := quote name
      `(LeanPq.Expr.col $nameLit (by decide))
  -- string literal
  | `(pq_expr| $s:str) =>
    `(LeanPq.Expr.litStr $s)
  -- numeric literal
  | `(pq_expr| $n:num) =>
    `(LeanPq.Expr.litInt $n)
  -- grouping
  | `(pq_expr| ($e:pq_expr)) => elabPqExpr e
  -- NOT
  | `(pq_expr| ! $e:pq_expr) => do
    let e' ← elabPqExpr e
    `(LeanPq.Expr.not $e')
  -- null checks
  | `(pq_expr| $e:pq_expr is_null) => do
    let e' ← elabPqExpr e
    `(LeanPq.Expr.isNull $e')
  | `(pq_expr| $e:pq_expr is_not_null) => do
    let e' ← elabPqExpr e
    `(LeanPq.Expr.isNotNull $e')
  -- comparisons
  | `(pq_expr| $l:pq_expr = $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .eq $l' $r')
  | `(pq_expr| $l:pq_expr != $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .neq $l' $r')
  | `(pq_expr| $l:pq_expr > $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .gt $l' $r')
  | `(pq_expr| $l:pq_expr < $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .lt $l' $r')
  | `(pq_expr| $l:pq_expr >= $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .ge $l' $r')
  | `(pq_expr| $l:pq_expr <= $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .le $l' $r')
  | `(pq_expr| $l:pq_expr like $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .like $l' $r')
  | `(pq_expr| $l:pq_expr ilike $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .ilike $l' $r')
  -- logical
  | `(pq_expr| $l:pq_expr && $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .and $l' $r')
  | `(pq_expr| $l:pq_expr || $r:pq_expr) => do
    let l' ← elabPqExpr l; let r' ← elabPqExpr r
    `(LeanPq.Expr.binOp .or $l' $r')
  | _ => Macro.throwUnsupported

/-! ## Assignment syntax: col := expr -/

declare_syntax_cat pq_assign
syntax ident " := " pq_expr : pq_assign

/-! ## ORDER BY item syntax -/

declare_syntax_cat pq_order_item
syntax ident           : pq_order_item   -- default ASC
syntax ident " asc"    : pq_order_item
syntax ident " desc"   : pq_order_item

/-- Translate a pq_order_item to a `(String × SortDir)` term. -/
def elabOrderItem : TSyntax `pq_order_item → MacroM (TSyntax `term)
  | `(pq_order_item| $i:ident) => do
    let nameLit : TSyntax `term := quote (toString i.getId)
    `(($nameLit, LeanPq.SortDir.asc))
  | `(pq_order_item| $i:ident asc) => do
    let nameLit : TSyntax `term := quote (toString i.getId)
    `(($nameLit, LeanPq.SortDir.asc))
  | `(pq_order_item| $i:ident desc) => do
    let nameLit : TSyntax `term := quote (toString i.getId)
    `(($nameLit, LeanPq.SortDir.desc))
  | _ => Macro.throwUnsupported

/-! ## The unified `pq!` macro -/

-- SELECT variants
syntax "pq! " "select " ident
  ("[" ident,* "]")?
  ("|" pq_expr)?
  ("orderby" pq_order_item,*)?
  ("limit" num)? : term

-- INSERT
syntax "pq! " "insert " ident "[" ident,* "]" "[" pq_expr,* "]" : term

-- UPDATE
syntax "pq! " "update " ident "[" pq_assign,* "]" ("|" pq_expr)? : term

-- DELETE
syntax "pq! " "delete " ident ("|" pq_expr)? : term

-- DDL
syntax "pq! " "create " ident : term
syntax "pq! " "drop " ident : term
syntax "pq! " "drop_if_exists " ident : term

/-! ## Macro rules -/

-- SELECT
macro_rules
  | `(pq! select $t:ident $[ [$cols:ident,*] ]? $[ | $cond:pq_expr ]? $[ orderby $obs:pq_order_item,* ]? $[ limit $lim:num ]?) => do
    -- Columns
    let colsTerm ← match cols with
      | some cs => do
        let names : Array (TSyntax `term) := cs.getElems.map fun c => quote (toString c.getId)
        let listSyntax ← `([$names,*])
        `(LeanPq.SelectColumns.specific $listSyntax (by decide))
      | none => `(LeanPq.SelectColumns.all)
    -- WHERE
    let whereTerm ← match cond with
      | some c => do
        let e ← elabPqExpr c
        `(some $e)
      | none => `(none)
    -- ORDER BY
    let orderTerm ← match obs with
      | some items => do
        let elems ← items.getElems.mapM elabOrderItem
        `([$elems,*])
      | none => `([])
    -- LIMIT
    let limitTerm ← match lim with
      | some n => `(some $n)
      | none => `(none)
    `(LeanPq.Query.select $t $colsTerm $whereTerm $orderTerm $limitTerm)

-- INSERT
macro_rules
  | `(pq! insert $t:ident [$cols:ident,*] [$vals:pq_expr,*]) => do
    let names : Array (TSyntax `term) := cols.getElems.map fun c => quote (toString c.getId)
    let valTerms ← vals.getElems.mapM elabPqExpr
    `(LeanPq.Query.insert $t [$names,*] [$valTerms,*])

-- UPDATE
macro_rules
  | `(pq! update $t:ident [$assigns:pq_assign,*] $[ | $cond:pq_expr ]?) => do
    let assignTerms ← assigns.getElems.mapM fun a => do
      match a with
      | `(pq_assign| $i:ident := $e:pq_expr) => do
        let name := toString i.getId
        let nameLit : TSyntax `term := quote name
        let val ← elabPqExpr e
        `(LeanPq.Assignment.mk $nameLit $val (by decide))
      | _ => Macro.throwUnsupported
    let whereTerm ← match cond with
      | some c => do
        let e ← elabPqExpr c
        `(some $e)
      | none => `(none)
    `(LeanPq.Query.update $t [$assignTerms,*] $whereTerm)

-- DELETE
macro_rules
  | `(pq! delete $t:ident $[ | $cond:pq_expr ]?) => do
    let whereTerm ← match cond with
      | some c => do
        let e ← elabPqExpr c
        `(some $e)
      | none => `(none)
    `(LeanPq.Query.delete $t $whereTerm)

-- DDL
macro_rules
  | `(pq! create $t:ident) => `(LeanPq.Query.createTable $t)

macro_rules
  | `(pq! drop $t:ident) =>
    `(LeanPq.Query.dropTable ($t).name)

macro_rules
  | `(pq! drop_if_exists $t:ident) =>
    `(LeanPq.Query.dropTable ($t).name Bool.true)

end LeanPq.Syntax
