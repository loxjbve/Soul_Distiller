from app.pipeline.chunking import chunk_segments
from app.schemas import ExtractedSegment

# A long text with \n\n every 100 chars
text = ("x" * 98 + "\n\n") * 1000  # 100,000 chars
segments = [ExtractedSegment(text=text, metadata={})]
chunks = chunk_segments(segments, chunk_size=1200, overlap=200)
for i in range(5):
    print(len(chunks[i].content))
print(f"Total chunks: {len(chunks)}")
