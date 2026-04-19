import re

with open("app/web/routes.py", "r") as f:
    content = f.read()

# I want to find all occurrences of `_page_context(` and if the next non-whitespace is a quote, replace it with `request, `
content = re.sub(r'_page_context\(\s*(["\'])', r'_page_context(request, \1', content)

with open("app/web/routes.py", "w") as f:
    f.write(content)

print("Patched routes.py 3")
