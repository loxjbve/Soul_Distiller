from __future__ import annotations

from app.schemas import ChunkPayload, ExtractedSegment
from app.utils.text import token_count


def chunk_segments(
    segments: list[ExtractedSegment],
    *,
    chunk_size: int = 1800,
    overlap: int = 300,
) -> list[ChunkPayload]:
    chunks: list[ChunkPayload] = []
    global_offset = 0
    chunk_index = 0
    for segment in segments:
        text = segment.text.strip()
        if not text:
            continue
        start = 0
        while start < len(text):
            hard_end = min(start + chunk_size, len(text))
            end = _find_boundary(text, start, hard_end)
            if end <= start:
                end = hard_end
            chunk_text = text[start:end].strip()
            if chunk_text:
                chunks.append(
                    ChunkPayload(
                        chunk_index=chunk_index,
                        content=chunk_text,
                        start_offset=global_offset + start,
                        end_offset=global_offset + end,
                        page_number=segment.metadata.get("page_number"),
                        token_count=token_count(chunk_text),
                        metadata=segment.metadata.copy(),
                    )
                )
                chunk_index += 1
            if end >= len(text):
                break
            start = max(end - overlap, start + 1)
        global_offset += len(text) + 2
    return chunks


def _find_boundary(text: str, start: int, hard_end: int) -> int:
    if hard_end >= len(text):
        return len(text)
    for marker in ("\n\n", "\n", "。", ".", "!", "?", "！", "？", "；", ";", " "):
        index = text.rfind(marker, start, hard_end)
        if index != -1 and index > start + 200:
            return index + len(marker)
    return hard_end
