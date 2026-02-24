# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**lean-pq** is a Lean 4 library providing bindings to PostgreSQL's `libpq` C client library via Lean's FFI. It enables Lean programs to connect to and query PostgreSQL databases.

- Lean version: `leanprover/lean4:v4.24.0` (pinned in `lean-toolchain`)
- Build system: Lake
- No external Lean package dependencies

## Build & Test Commands

```bash
lake build          # Build library + extern static lib (default targets)
lake test           # Run test suite (requires running PostgreSQL)
lake exe examples   # Run examples
lake clean          # Clean build artifacts
```

### Prerequisites

- `libpq` must be installed (`libpq-dev` on Ubuntu, `libpq` via Homebrew on macOS)
- `pkg-config` must be available (used to locate libpq headers and link flags)

### Test Database

Tests require a PostgreSQL instance on `localhost:5432` with user `postgres`, password `test`:

```bash
docker compose -f Tests/docker-compose.yml up -d
```

## Architecture

### FFI Pattern (core design)

The library follows a two-layer FFI pattern:

1. **Lean declarations** (`LeanPq/Extern.lean`): Opaque types and `@[extern "lean_pq_<name>"]` function declarations. All FFI functions return `EIO LeanPq.Error T`.
2. **C implementations** (`LeanPq/extern.c`): Corresponding `LEAN_EXPORT lean_obj_res lean_pq_<name>(...)` functions wrapping libpq calls.

Key conventions:
- Opaque `Handle` wraps `PGconn*` with a Lean external class and finalizer (`PQfinish`)
- Opaque `PGresult` wraps `PGresult*` with finalizer (`PQclear`)
- Lean inductive types for enums (`ConnStatus`, `ExecStatus`, `PGTransactionStatus`) must have constructors ordered to match C enum ordinals exactly — the C side returns raw integers
- Error handling uses `LeanPq.Error` (defined in `LeanPq/Error.lean`) with `lean_io_result_mk_ok`/`lean_io_result_mk_error`

### Module Layout

- `LeanPq.lean` — Root module, re-exports `DataType` and `Extern`
- `LeanPq/DataType.lean` — Pure Lean inductive type modeling all PostgreSQL data types (Chapter 8 of PG docs)
- `LeanPq/Extern.lean` — All `@[extern]` opaque declarations + status enum types
- `LeanPq/Error.lean` — `LeanPq.Error` inductive type
- `LeanPq/extern.c` — C FFI implementations (~700 lines)
- `LeanPq/Table.lean` — Placeholder for future higher-level table abstraction (currently empty)

### Build Configuration (`lakefile.lean`)

Platform-aware linking: macOS uses `pkg-config --libs libpq`, Linux uses `ldconfig` to locate `libpq.so`. The C file is compiled into `libextern.a` via a custom `extern_lib` target with `pkg-config --cflags libpq` for include paths.
