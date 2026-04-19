import re

with open("app/web/routes.py", "r") as f:
    content = f.read()

# Match _page_context( not followed by `request,`
# Wait, let's just do a simpler replace.

content = content.replace('_page_context(\n            "index"', '_page_context(\n            request, "index"')
content = content.replace('_page_context(\n            "analysis"', '_page_context(\n            request, "analysis"')
content = content.replace('_page_context(\n            "assets"', '_page_context(\n            request, "assets"')
content = content.replace('_page_context(\n            "playground"', '_page_context(\n            request, "playground"')
content = content.replace('_page_context(\n            "preprocess"', '_page_context(\n            request, "preprocess"')
content = content.replace('_page_context(\n            "settings"', '_page_context(\n            request, "settings"')
content = content.replace('_page_context("settings"', '_page_context(request, "settings"')

with open("app/web/routes.py", "w") as f:
    f.write(content)

print("Patched routes.py 2")
