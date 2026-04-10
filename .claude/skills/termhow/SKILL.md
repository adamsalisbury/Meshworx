---
name: termhow
description: Returns instructions explaining how to complete the described task within a POSIX terminal environment. Use when unsure how to complete a task in terminal.
---

Return instructions on how to achieve the following task in an POSIX terminal environment: `$ARGUMENTS`

For further context, assume Ubuntu 24.04 server. 

You may query the system to understand its characteristics or currently available tools, for example - `uname --all`.

The instuctions should be accompanied by an example. 

Do not output in MarkDown, output using plain text, as a typical POXIX terminal tool would do.

Spelling should always be British English.

Return nothing more than the instructions and an example.