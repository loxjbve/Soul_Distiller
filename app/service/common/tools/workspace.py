from __future__ import annotations


def build_tool_schemas() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "list_project_documents",
                "description": "List project documents that are available in the current workspace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Optional filename/title filter."},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 12},
                    },
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "read_project_documents",
                "description": "Read extracted text for one or more project documents.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "document_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                            "maxItems": 12,
                        },
                        "max_chars_per_doc": {
                            "type": "integer",
                            "minimum": 200,
                            "maximum": 12000,
                            "default": 4000,
                        },
                        "include_metadata": {"type": "boolean", "default": True},
                    },
                    "required": ["document_ids"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_project_documents",
                "description": "Search project documents and return relevant snippets.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query."},
                        "document_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                            "maxItems": 24,
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 6},
                    },
                    "required": ["query"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_python_transform",
                "description": "Run Python code in the session workspace to transform text and generate files.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "intent": {"type": "string", "description": "Short human-readable task description."},
                        "python_code": {"type": "string", "description": "Executable Python code."},
                        "input_document_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                            "maxItems": 12,
                        },
                        "input_artifact_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                            "maxItems": 12,
                        },
                        "expected_output_files": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1,
                            "maxItems": 12,
                        },
                    },
                    "required": ["intent", "python_code"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_session_artifacts",
                "description": "List generated files already created in this preprocess session.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                    },
                    "additionalProperties": False,
                },
            },
        },
    ]
