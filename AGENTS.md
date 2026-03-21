For all PR comments and explanations: ≤10 words total.

Do not put any headings in comments. Try to use a single short sentence.

Commit message rules:
Always replace merge message
Extract PR number
Output exactly: Merge PR <number>
No other text

# Code Modification Rules

- Preserve all existing formatting, spacing, and alignment by default
- Only modify lines that are strictly required to implement the requested change
- Do not reformat unchanged lines
- Do not normalize spacing or indentation outside edited lines
- When editing a line, preserve its original alignment as much as possible
- Treat formatting as immutable unless the user explicitly requests formatting changes
- Do not modify whitespace on unchanged lines under any circumstance