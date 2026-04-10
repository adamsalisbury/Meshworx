---
name: commitstaged
description: Generate a commit message based on the currently staged files and commit. Use to commit staged changes.
---

Analyse the currently staged files and produce a commit message detailing the changes made, and if you have context about the work completed, give an explanation for the changes.

Choose a prefix from the below table, which best describes the purpose of the changes in this commit.
| Prefix | Purpose |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, whitespace — no logic change |
| `refactor` | Code change that's neither fix nor feature |
| `perf` | Performance improvement |
| `test` | Adding or correcting tests |
| `chore` | Build process, tooling, dependencies |
| `ci` | CI/CD configuration |
| `revert` | Reverts a previous commit |
| `build` | Build system changes (webpack, npm, etc.) |

Do not perform any mutating git operations, this is an entirely read-only function.

The message must be written in British English, and should be entirely lower case.

Once you have generated the commit message, perform the commit.

If the commit is unsuccessful, return `UNSUCCESSFUL ` + [Short explanation]
If the commit is successful, return `SUCCESSFUL ` + [commit_hash]

Return nothing else.