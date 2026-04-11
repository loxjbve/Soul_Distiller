import json
from pathlib import Path
from app.pipeline.extractors import extract_text

def test_jsonl_extraction():
    content = '{"text": "line 1", "author": "Alice"}\n{"text": "line 2", "author": "Bob"}\nNot a JSON line\n{"text": "line 3"}'.encode('utf-8')
    filename = "test.jsonl"
    
    result = extract_text(filename, content)
    
    print(f"Raw text:\n{result.raw_text}")
    print(f"Clean text:\n{result.clean_text}")
    print(f"Metadata: {result.metadata}")
    print(f"Segments count: {len(result.segments)}")
    for i, segment in enumerate(result.segments):
        print(f"Segment {i}: {segment.text}")

if __name__ == "__main__":
    test_jsonl_extraction()
