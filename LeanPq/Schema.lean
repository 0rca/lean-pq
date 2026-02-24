/-
Schema definitions for compile-time table schema verification.
-/
import LeanPq.DataType

namespace LeanPq

/-- A single column in a table schema. -/
structure Column where
  name : String
  type : DataType
  nullable : Bool := true
  deriving Repr, Inhabited

instance : BEq Column where
  beq a b := a.name == b.name && a.type == b.type && a.nullable == b.nullable

/-- A table schema describing name and columns. -/
structure TableSchema where
  name : String
  columns : List Column
  deriving Repr, Inhabited

instance : BEq TableSchema where
  beq a b := a.name == b.name && a.columns == b.columns

/-- Proposition: the schema has a column with the given name.
    Decidable via `by decide` so the elaborator can verify column existence at compile time. -/
def TableSchema.hasCol (s : TableSchema) (colName : String) : Prop :=
  s.columns.any (fun c => c.name == colName) = true

instance (s : TableSchema) (colName : String) : Decidable (s.hasCol colName) :=
  inferInstanceAs (Decidable (_ = true))

/-- Look up the type of a column by name (returns Option since column may not exist). -/
def TableSchema.colType (s : TableSchema) (colName : String) : Option DataType :=
  (s.columns.find? (fun c => c.name == colName)).map Column.type

/-- Look up whether a column is nullable. -/
def TableSchema.colNullable (s : TableSchema) (colName : String) : Option Bool :=
  (s.columns.find? (fun c => c.name == colName)).map Column.nullable

/-- Get the list of column names. -/
def TableSchema.columnNames (s : TableSchema) : List String :=
  s.columns.map Column.name

end LeanPq
