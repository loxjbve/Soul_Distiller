import re

with open("app/web/routes.py", "r") as f:
    content = f.read()

# 1. Add get_locale and the set-locale route
# Find where router = APIRouter() is
router_pattern = r"(router = APIRouter\(\)\n)"

route_addition = """
from fastapi.responses import RedirectResponse

def get_locale(request: Request) -> str:
    return request.cookies.get("locale", DEFAULT_LOCALE)

@router.get("/set-locale")
def set_locale(locale: str, next: str = "/"):
    response = RedirectResponse(url=next)
    response.set_cookie(key="locale", value=locale, max_age=31536000)
    return response

"""

content = content.replace("router = APIRouter()\n", "router = APIRouter()\n" + route_addition)

# 2. Update _page_context
# def _page_context(page_name: str, **kwargs: Any) -> dict[str, Any]:
#    return {
#        "locale": DEFAULT_LOCALE,
#        "ui": page_strings(page_name),
#        **kwargs,
#    }
old_page_context = """def _page_context(page_name: str, **kwargs: Any) -> dict[str, Any]:
    return {
        "locale": DEFAULT_LOCALE,
        "ui": page_strings(page_name),
        **kwargs,
    }"""
new_page_context = """def _page_context(request: Request, page_name: str, **kwargs: Any) -> dict[str, Any]:
    locale = get_locale(request)
    return {
        "locale": locale,
        "ui": page_strings(page_name, locale),
        **kwargs,
    }"""
content = content.replace(old_page_context, new_page_context)

# 3. Update all _page_context calls
# _page_context("something", ...) -> _page_context(request, "something", ...)
content = re.sub(r"_page_context\((['\"][\w]+['\"])", r"_page_context(request, \1", content)

# 4. Update _project_context to accept request
old_project_context_def = """def _project_context(session: Session, project_id: str, *, document_limit: int = 20, document_offset: int = 0) -> dict[str, Any]:"""
new_project_context_def = """def _project_context(request: Request, session: Session, project_id: str, *, document_limit: int = 20, document_offset: int = 0) -> dict[str, Any]:"""
content = content.replace(old_project_context_def, new_project_context_def)

# Update the calls to _project_context
content = content.replace("_project_context(session, project_id)", "_project_context(request, session, project_id)")

# In _project_context, update page_strings("project")
content = re.sub(r'"locale": DEFAULT_LOCALE,\s*"ui_strings": page_strings\("project"\),', r'"locale": get_locale(request),\n                "ui_strings": page_strings("project", get_locale(request)),', content)

# 5. In preprocess route (around line 742), update page_strings("preprocess")
# We need to find the preprocess view function to see what context it has
# It's inside `def preprocess(...)`
# We'll just replace `page_strings("preprocess")` with `page_strings("preprocess", get_locale(request))`
content = re.sub(r'"locale": DEFAULT_LOCALE,\s*"ui_strings": page_strings\("preprocess"\),', r'"locale": get_locale(request),\n        "ui_strings": page_strings("preprocess", get_locale(request)),', content)


with open("app/web/routes.py", "w") as f:
    f.write(content)

print("Patched routes.py")
