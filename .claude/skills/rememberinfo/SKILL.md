---
name: rememberinfo
description: Remembers a piece of information. Use when you want to persist something outside of Claude's current context.
---

Rewrite the piece of infomration `$ARGUMENTS` into clear British English and store in ./memory/info_yyyy-MM-dd_HH:mm:ss.md.
Before writing to the file, you will normalise the text. This is a process where you fix any spelling mistakes, grammatical errors, and supplement the text with any additional information that is relevant.

If the directory ./memory does not exist in the current working directory, you are to create it.

Store in the following format:
 - Markdown
 - Template:
  # Original Text
  [Put the originally supplied argument here]

  # Normalised Text
  [Put the reworded, formatted, spelling corrected, grammatically corrected text here. Suppliment with your understanding of the current context].
  

After the text have been reviewed, edited and saved, return 'Info saved'. No other output.