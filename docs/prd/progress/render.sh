#!/usr/bin/env bash
# Render docs/prd/progress/<task>.md to a one-file HTML page for
# `superset pages publish`. Prints the output directory.
#
#   docs/prd/progress/render.sh <task> [out-dir]
#
# Markdown and Mermaid render in the browser (marked + mermaid from
# cdn.jsdelivr). Style rules: docs/prd/diagram-style.md.
set -euo pipefail
task=${1:?usage: render.sh <task> [out-dir]}
src="$(cd "$(dirname "$0")" && pwd)/$task.md"
[ -f "$src" ] || { echo "render.sh: no such file: $src" >&2; exit 2; }
out=${2:-${TMPDIR:-/tmp}/k2-dist-pages/$task}
mkdir -p "$out"

python3 -I - "$src" "$out/index.html" "$task" <<'PY'
import json, sys
src, dst, task = sys.argv[1], sys.argv[2], sys.argv[3]
md = open(src, encoding="utf-8").read()
# JSON-encode, and keep "</" from closing the script tag early.
payload = json.dumps(md).replace("</", "<\\/")
page = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>k2-dist: %(task)s</title>
<style>
:root { --bg:#f8fafc; --panel:#ffffff; --ink:#0f172a; --muted:#475569; --line:#e2e8f0; --accent:#2563eb; }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) { --bg:#0b1220; --panel:#111a2e; --ink:#e2e8f0; --muted:#94a3b8; --line:#1e293b; --accent:#60a5fa; }
}
:root[data-theme="dark"] { --bg:#0b1220; --panel:#111a2e; --ink:#e2e8f0; --muted:#94a3b8; --line:#1e293b; --accent:#60a5fa; }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font:16px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif; padding:24px 16px 64px; }
main { max-width: 1040px; margin: 0 auto; }
h1,h2,h3 { line-height:1.2; } a { color:var(--accent); }
code, pre { font-family: ui-monospace,"SF Mono",Menlo,monospace; font-size:.88em; }
pre { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:12px 14px; overflow-x:auto; }
table { border-collapse:collapse; width:100%%; display:block; overflow-x:auto; }
th,td { border-bottom:1px solid var(--line); padding:6px 10px; text-align:left; vertical-align:top; }
.mermaid { background:#ffffff; border:1px solid var(--line); border-radius:12px; padding:12px; overflow-x:auto; margin:12px 0; }
</style></head>
<body><main id="doc"></main>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs";
const md = %(payload)s;
const doc = document.getElementById("doc");
doc.innerHTML = marked.parse(md);
doc.querySelectorAll("pre code.language-mermaid").forEach((c) => {
  const div = document.createElement("div");
  div.className = "mermaid";
  div.textContent = c.textContent;
  c.parentElement.replaceWith(div);
});
mermaid.initialize({ startOnLoad: false, theme: "base", securityLevel: "strict",
  themeVariables: { fontFamily: "system-ui, sans-serif", primaryColor: "#dbeafe", primaryBorderColor: "#2563eb", lineColor: "#64748b" } });
await mermaid.run({ querySelector: ".mermaid" });
</script></body></html>
""" % {"task": task, "payload": payload}
open(dst, "w", encoding="utf-8").write(page)
PY
echo "$out"
