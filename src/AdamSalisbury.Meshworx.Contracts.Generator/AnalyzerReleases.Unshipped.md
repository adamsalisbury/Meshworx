; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/AnalyzerReleases.md

### New Rules

Rule ID | Category | Severity | Notes
--------|--------------------|----------|-------------------------------------------------------------
MESH001 | Meshworx.Contracts | Error | Contract method must return Task or Task<T>
MESH002 | Meshworx.Contracts | Error | Contract method parameter cannot be ref, out or in
MESH003 | Meshworx.Contracts | Error | Contract method cannot be generic
MESH004 | Meshworx.Contracts | Error | Contract method names must be unique
MESH005 | Meshworx.Contracts | Error | Contract may only declare methods
MESH006 | Meshworx.Contracts | Error | CancellationToken must be the last parameter
