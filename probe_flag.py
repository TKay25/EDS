import re
for path in ("static/js/crm.js", "static/js/portal.js"):
    src = open(path, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", " ", src, flags=re.S)
    src = re.sub(r"(?m)//.*$", " ", src)
    for needle in ("analytics", "description", "how", "name", "status", "badges", "label"):
        for m in re.finditer(r"(?<![.\w$])" + needle + r"\s*\(", src):
            line = src.count("\n", 0, m.start()) + 1
            snippet = src[max(0, m.start() - 45):m.end() + 25].replace("\n", " ")
            print(path + ":" + str(line) + "  ..." + snippet.strip() + "...")
