import json
from app.llm.client import parse_json_response

print(parse_json_response("just some text", fallback=True))
print(parse_json_response("```json\n{ \"a\": 1 }\n```", fallback=True))
print(parse_json_response("here is a partial json: { \"summary\": \"hello\", ", fallback=True))
