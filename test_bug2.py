from test_bug import chunk_segments
from app.schemas import ExtractedSegment

text = "a" * 250 + "\n\n" + "b" * 1000 + "\n" + "c" * 1000
segments = [ExtractedSegment(text=text, metadata={})]
chunks = chunk_segments(segments, chunk_size=1800, overlap=300)
print(f"Total chunks: {len(chunks)}")
if len(chunks) > 0:
    for i, c in enumerate(chunks):
        print(f"Chunk {i}: {len(c.content)}")
