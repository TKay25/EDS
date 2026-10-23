path = "templates/crm_portal.html"
lines = open(path, encoding="utf-8").read().splitlines()

start = next(i for i, l in enumerate(lines) if "{# Stock is kept per shop" in l)
end = next(i for i, l in enumerate(lines) if i > start and "{# One order in detail:" in l)
block = lines[start:end]
print(f"stock panel: lines {start+1}..{end} ({len(block)} lines)")
print(f"  first: {block[0].strip()[:60]}")
print(f"  last : {block[-1].strip()[:60]}")

rest = lines[:start] + lines[end:]

# Place it as its own tabpanel beside the other views, not inside analytics.
products = next(i for i, l in enumerate(rest) if 'id="crmViewProducts"' in l)
section_start = next(i for i in range(products, 0, -1)
                     if rest[i].strip().startswith("<section"))
print(f"products section starts at line {section_start+1} of the trimmed file")

wrapped = ["",
           "    <!-- =========================================================== stock -->",
           '    <section class="crm-view" id="crmViewStock" data-crm-view="stock"',
           '             role="tabpanel" aria-labelledby="crmTabStock">'] + block + ["    </section>", ""]
out = rest[:section_start] + wrapped + rest[section_start:]
open(path, "w", encoding="utf-8", newline="\n").write("\n".join(out) + "\n")
print("moved into its own section")
