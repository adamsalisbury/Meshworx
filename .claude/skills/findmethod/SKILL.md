---
name: findmethod
description: Finds all methods by name in the codebase and returns fully qualified namespaces. Use when asked to locate a method.
---

Search the entire codebase for all methods named `$ARGUMENTS`.

For each match, return the fully qualified namespace path in the format:
`Namespace.SubNamespace.ClassName.MethodName`

List all matches, one per line. No other output.