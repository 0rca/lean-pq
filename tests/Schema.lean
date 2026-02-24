/-
Compile-time tests for Schema, Query rendering, and Permission classification.
-/
import LeanPq.Schema
import LeanPq.Query
import LeanPq.Syntax

open LeanPq
open DataType

namespace Tests.Schema

-- Test schema definition
def users : TableSchema :=
  { name := "users"
    columns := [
      { name := "id", type := .serial, nullable := false },
      { name := "name", type := .text, nullable := false },
      { name := "email", type := .text, nullable := false },
      { name := "age", type := .integer, nullable := true },
      { name := "active", type := .boolean, nullable := false }
    ] }

-- Test: valid column references compile
example : users.hasCol "id" := by decide
example : users.hasCol "name" := by decide
example : users.hasCol "email" := by decide
example : users.hasCol "age" := by decide
example : users.hasCol "active" := by decide

-- Test: invalid column reference does NOT compile (uncomment to verify)
-- example : users.hasCol "nonexistent" := by decide  -- ERROR: proof fails

-- Test: column type lookup (using BEq since DataType lacks DecidableEq for recursive variants)
#eval do
  assert! users.colType "id" == some DataType.serial
  assert! users.colType "name" == some DataType.text
  assert! users.colType "nonexistent" == none
  IO.println "colType tests passed"

-- Test: column names
example : users.columnNames = ["id", "name", "email", "age", "active"] := by decide

-- Test: Expr with valid column reference compiles
def testExpr : Expr users.columns :=
  .col "name" (by decide)

-- Test: BinOp expression with valid columns
def testWhereExpr : Expr users.columns :=
  .binOp .gt (.col "age" (by decide)) (.litInt 18)

-- Test: Query rendering produces correct parameterized SQL
section QueryRendering

  def selectAllQuery := Query.select users .all
  #eval do
    let (sql, params) := selectAllQuery.render
    IO.println s!"SQL: {sql}"
    IO.println s!"Params: {params}"

  def selectWithWhere := Query.select users .all
    (some (.binOp .eq (.col "name" (by decide)) (.litStr "Alice")))
  #eval do
    let (sql, params) := selectWithWhere.render
    IO.println s!"SQL: {sql}"
    IO.println s!"Params: {params}"

  def insertQuery := Query.insert users
    ["name", "email", "age"]
    [.litStr "Bob", .litStr "bob@example.com", .litInt 25]
  #eval do
    let (sql, params) := insertQuery.render
    IO.println s!"SQL: {sql}"
    IO.println s!"Params: {params}"

  def deleteWithWhere := Query.delete users
    (some (.binOp .eq (.col "id" (by decide)) (.litInt 1)))
  #eval do
    let (sql, params) := deleteWithWhere.render
    IO.println s!"SQL: {sql}"
    IO.println s!"Params: {params}"

  def createTableQuery := Query.createTable users
  #eval do
    let (sql, _) := createTableQuery.render
    IO.println sql

  def dropQuery := Query.dropTable "users" true
  #eval do
    let (sql, _) := dropQuery.render
    IO.println sql

end QueryRendering

-- Test: Permission level classification
section PermissionLevel

  example : (Query.select users .all).permissionLevel = .readOnly := by rfl
  example : (Query.insert users ["name"] [.litStr "x"]).permissionLevel = .dataAltering := by rfl
  example : (Query.update users [] none).permissionLevel = .dataAltering := by rfl
  example : (Query.delete users none).permissionLevel = .dataAltering := by rfl
  example : (Query.createTable users).permissionLevel = .admin := by rfl
  example : (Query.dropTable "t").permissionLevel = .admin := by rfl

end PermissionLevel

-- Test: syntax macros with valid schema
section SyntaxMacro
  open LeanPq.Syntax

  def macroSelectAll := pq_select_all! users
  def macroSelectCols := pq_select! users [name, email]
  def macroDelete := pq_delete! users
  def macroCreate := pq_create! users
  def macroDrop := pq_drop! users
  def macroDropIfExists := pq_drop_if_exists! users

  #eval do
    let (sql, params) := macroSelectAll.render
    IO.println s!"pq_select_all!: {sql} | params: {params}"

  #eval do
    let (sql, params) := macroSelectCols.render
    IO.println s!"pq_select!: {sql} | params: {params}"

  #eval do
    let (sql, params) := macroDropIfExists.render
    IO.println s!"pq_drop_if_exists!: {sql} | params: {params}"

end SyntaxMacro

end Tests.Schema
