---
name: recallinfo
description: Recalls stored information from memory. Use when asked to remember, retrieve, or look up something previously saved.
---

Search all `.md` files in `./memory/` for content relevant to `$ARGUMENTS`.

Steps:
1. If `./memory/` does not exist or is empty, return `No memory found.` and stop.
2. Scan the `# Normalised Text` section of each file for relevance to the query.
3. Return only the matches, formatted as:

---
**File:** `info_yyyy-MM-dd_HH:mm:ss.md`
**Recorded:** [datetime from filename]
**Summary:** [one-sentence summary of the normalised content]
**Detail:** [full Normalised Text content]
---

If multiple matches are found, order by filename descending (most recent first).
If no relevant files are found, return `No matching memory found for: [query].`

No other output.