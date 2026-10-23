import re, sys

path = "static/js/crm.js"
lines = open(path, encoding="utf-8").read().splitlines(keepends=False)

def indent_of(text):
    return len(text) - len(text.lstrip())

# Helpers the stock block leans on must live in the outer scope, or moving it out
# of the analytics module would break them.
needed = ("function setBusy", "function postJson", "function escapeHtml", "function money")
for needle in needed:
    hit = next((i for i, l in enumerate(lines, 1) if needle in l), None)
    where = "MISSING" if hit is None else f"line {hit}, indent {indent_of(lines[hit-1])}"
    print(f"  {needle:<24} {where}")

start = next(i for i, l in enumerate(lines) if "----- stock --" in l)
end = next(i for i, l in enumerate(lines) if i > start and "function updateFulfilment(" in l)
print()
print(f"stock block: lines {start+1}..{end} ({end-start} lines), first indent "
      f"{indent_of(lines[start])}, last indent {indent_of(lines[end-1])}")

block = lines[start:end]
# Dedent by two spaces: the block was written at the analytics module's indent.
moved = [(line[2:] if line.startswith("  ") else line) for line in block]

rest = lines[:start] + lines[end:]
# Place it as a sibling of the analytics module, just after that module closes.
close = next(i for i, l in enumerate(rest) if i > 200 and l.rstrip() == "  })();")
print(f"analytics module closes at line {close+1} of the trimmed file")
out = rest[:close+1] + [""] + moved + rest[close+1:]
open(path, "w", encoding="utf-8", newline="\n").write("\n".join(out) + "\n")
print("moved")
