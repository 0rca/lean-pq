import Lake
open System Lake DSL

-- Phase 2a: Unified linking strategy — try pkg-config first (works on Alpine, Arch, Fedora, NixOS),
-- fall back to ldconfig, then plain -lpq
def linkArgsLinux : IO (Array String) := do
  try
    let libs ← IO.Process.run { cmd := "pkg-config", args := #["--libs", "libpq"] }
    let libsStr := libs.trimAscii.toString
    if !libsStr.isEmpty then
      let libsArr := libsStr.splitOn.toArray.filter (· ≠ "")
      -- ld.lld (used by Lean's bundled clang) may not search system library paths,
      -- so explicitly add -L<libdir> when pkg-config omits it
      if !libsArr.any (·.startsWith "-L") then
        let libdir ← IO.Process.run { cmd := "pkg-config", args := #["--variable=libdir", "libpq"] }
        let libdirStr := libdir.trimAscii.toString
        if !libdirStr.isEmpty then return #[s!"-L{libdirStr}"] ++ libsArr
      return libsArr
  catch _ => pure ()
  try
    let p ← IO.Process.run { cmd := "/bin/sh", args := #["-c", "ldconfig -p | grep -m 1 libpq | awk '{ print $4 }'"]}
    if !p.trimAscii.toString.isEmpty then return #[p.trimAscii.toString]
  catch _ => pure ()
  return #["-lpq"]

def linkArgsDarwin : IO (Array String) := do
  let output ← IO.Process.run {
    cmd := "pkg-config"
    args := #["--libs", "libpq"]
  }
  return output.trimAscii.toString.splitOn.toArray.filter (· ≠ "")

def linkArgs : Array String := run_io do
  if System.Platform.isOSX then
    return <- linkArgsDarwin
  else
    return <- linkArgsLinux

package leanPq where
  version := v!"0.1.0"
  moreLinkArgs := linkArgs

def buildType := match get_config? buildType with | some "debug" => Lake.BuildType.debug | _ => Lake.BuildType.release

@[default_target]
lean_lib LeanPq

lean_lib Tests

-- @[default_target]
lean_exe examples {
  root := `Examples
}

@[test_driver]
lean_exe tests {
  root := `Tests.Test
}

def traceArgs : FetchM (Array String) := do
  let output ← IO.Process.run {
    cmd := "pkg-config"
    args := #["--cflags", "libpq"]
  }
  logInfo s!"traceArgs: {output}"
  return (output.trimAscii.toString.splitOn.toArray.filter (· ≠ ""))

target extern_o pkg : FilePath := do
  let LeanPq_extern_c := pkg.dir / "LeanPq" / "extern.c"
  let LeanPq_extern_o := pkg.buildDir / "LeanPq" / "extern.o"
  IO.FS.createDirAll LeanPq_extern_o.parent.get!
  let lean_dir := (← getLeanIncludeDir).toString
  let trace_args ← traceArgs
  buildO LeanPq_extern_o (← inputTextFile LeanPq_extern_c) (#["-I", lean_dir]++trace_args) #["-fPIC"]

@[default_target]
extern_lib extern pkg := do
  let name := nameToStaticLib "extern"
  let LeanPq_extern_o <- extern_o.fetch
  buildStaticLib (pkg.sharedLibDir / name) #[LeanPq_extern_o]
