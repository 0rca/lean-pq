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

-- Test: pq! macro
section PqMacro
  open LeanPq.Syntax

  -- SELECT all
  def pqSelectAll := pq! select users
  #eval do
    let (sql, params) := pqSelectAll.render
    assert! sql == "SELECT * FROM users"
    assert! params == #[]
    IO.println s!"pq! select: {sql}"

  -- SELECT specific columns
  def pqSelectCols := pq! select users [name, email]
  #eval do
    let (sql, params) := pqSelectCols.render
    assert! sql == "SELECT name, email FROM users"
    assert! params == #[]
    IO.println s!"pq! select cols: {sql}"

  -- SELECT with WHERE
  def pqSelectWhere := pq! select users | age > 18
  #eval do
    let (sql, params) := pqSelectWhere.render
    assert! sql == "SELECT * FROM users WHERE (age > $1)"
    assert! params == #["18"]
    IO.println s!"pq! select where: {sql} | params: {params}"

  -- SELECT with columns + WHERE
  def pqSelectColsWhere := pq! select users [name] | age > 18 && active = true
  #eval do
    let (sql, params) := pqSelectColsWhere.render
    assert! sql == "SELECT name FROM users WHERE ((age > $1) AND (active = TRUE))"
    assert! params == #["18"]
    IO.println s!"pq! select cols+where: {sql} | params: {params}"

  -- SELECT with ORDER BY + LIMIT
  def pqSelectOrderLimit := pq! select users | age > 18 orderby name limit 10
  #eval do
    let (sql, params) := pqSelectOrderLimit.render
    assert! sql == "SELECT * FROM users WHERE (age > $1) ORDER BY name ASC LIMIT 10"
    assert! params == #["18"]
    IO.println s!"pq! select order+limit: {sql} | params: {params}"

  -- SELECT with ORDER BY desc
  def pqSelectOrderDesc := pq! select users orderby age desc
  #eval do
    let (sql, _) := pqSelectOrderDesc.render
    assert! sql == "SELECT * FROM users ORDER BY age DESC"
    IO.println s!"pq! select order desc: {sql}"

  -- INSERT
  def pqInsert := pq! insert users [name, email] ["Alice", "alice@example.com"]
  #eval do
    let (sql, params) := pqInsert.render
    assert! sql == "INSERT INTO users (name, email) VALUES ($1, $2)"
    assert! params == #["Alice", "alice@example.com"]
    IO.println s!"pq! insert: {sql} | params: {params}"

  -- UPDATE with WHERE
  def pqUpdate := pq! update users [name := "Bob", age := 30] | id = 1
  #eval do
    let (sql, params) := pqUpdate.render
    IO.println s!"pq! update: {sql} | params: {params}"

  -- DELETE with WHERE
  def pqDelete := pq! delete users | name = "Grace"
  #eval do
    let (sql, params) := pqDelete.render
    assert! sql == "DELETE FROM users WHERE (name = $1)"
    assert! params == #["Grace"]
    IO.println s!"pq! delete: {sql} | params: {params}"

  -- DELETE all
  def pqDeleteAll := pq! delete users
  #eval do
    let (sql, params) := pqDeleteAll.render
    assert! sql == "DELETE FROM users"
    assert! params == #[]
    IO.println s!"pq! delete all: {sql}"

  -- DDL: create
  def pqCreate := pq! create users
  #eval do
    let (sql, _) := pqCreate.render
    IO.println s!"pq! create: {sql}"

  -- DDL: drop
  def pqDrop := pq! drop users
  #eval do
    let (sql, _) := pqDrop.render
    assert! sql == "DROP TABLE users"
    IO.println s!"pq! drop: {sql}"

  -- DDL: drop_if_exists
  def pqDropIfExists := pq! drop_if_exists users
  #eval do
    let (sql, _) := pqDropIfExists.render
    assert! sql == "DROP TABLE IF EXISTS users"
    IO.println s!"pq! drop_if_exists: {sql}"

  -- Expression operators: OR
  def pqOrExpr := pq! select users | name = "Alice" || name = "Bob"
  #eval do
    let (sql, params) := pqOrExpr.render
    IO.println s!"pq! or: {sql} | params: {params}"

  -- Expression operators: NOT
  def pqNotExpr := pq! select users | !active = true
  #eval do
    let (sql, _) := pqNotExpr.render
    IO.println s!"pq! not: {sql}"

  -- Expression operators: is_null
  def pqIsNull := pq! select users | age is_null
  #eval do
    let (sql, _) := pqIsNull.render
    assert! sql == "SELECT * FROM users WHERE (age IS NULL)"
    IO.println s!"pq! is_null: {sql}"

  -- Expression operators: is_not_null
  def pqIsNotNull := pq! select users | age is_not_null
  #eval do
    let (sql, _) := pqIsNotNull.render
    assert! sql == "SELECT * FROM users WHERE (age IS NOT NULL)"
    IO.println s!"pq! is_not_null: {sql}"

  -- Expression operators: like
  def pqLike := pq! select users | name like "A%"
  #eval do
    let (sql, params) := pqLike.render
    assert! sql == "SELECT * FROM users WHERE (name LIKE $1)"
    assert! params == #["A%"]
    IO.println s!"pq! like: {sql} | params: {params}"

  -- Expression operators: ilike
  def pqIlike := pq! select users | name ilike "%alice%"
  #eval do
    let (sql, params) := pqIlike.render
    assert! sql == "SELECT * FROM users WHERE (name ILIKE $1)"
    assert! params == #["%alice%"]
    IO.println s!"pq! ilike: {sql} | params: {params}"

  -- Permission levels via pq!
  example : (pq! select users).permissionLevel = .readOnly := by rfl
  example : (pq! insert users [name] ["x"]).permissionLevel = .dataAltering := by rfl
  example : (pq! update users [name := "y"]).permissionLevel = .dataAltering := by rfl
  example : (pq! delete users).permissionLevel = .dataAltering := by rfl
  example : (pq! create users).permissionLevel = .admin := by rfl
  example : (pq! drop users).permissionLevel = .admin := by rfl
  example : (pq! drop_if_exists users).permissionLevel = .admin := by rfl

end PqMacro

end Tests.Schema
