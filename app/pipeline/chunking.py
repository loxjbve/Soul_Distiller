from __future__ import annotations

from app.schemas import ChunkPayload, ExtractedSegment
from app.utils.text import token_count


def chunk_segments(
    segments: list[ExtractedSegment],
    *,
    chunk_size: int = 1200,
    overlap: int = 200,
) -> list[ChunkPayload]:
    # Merge segments with identical metadata to prevent excessive tiny chunks
    grouped_segments: list[dict] = []
    for segment in segments:
        text = segment.text.strip()
        if not text:
            continue
        if grouped_segments and grouped_segments[-1]["metadata"] == segment.metadata:
            grouped_segments[-1]["texts"].append(text)
        else:
            grouped_segments.append({"metadata": segment.metadata.copy(), "texts": [text]})

    merged_segments: list[ExtractedSegment] = [
        ExtractedSegment(text="\n\n".join(group["texts"]), metadata=group["metadata"])
        for group in grouped_segments
    ]

    chunks: list[ChunkPayload] = []
    global_offset = 0
    chunk_index = 0
    for segment in merged_segments:
        text = segment.text.strip()
        if not text:
            continue
        start = 0
        while start < len(text):
            hard_end = min(start + chunk_size, len(text))
            end = _find_boundary(text, start, hard_end, overlap)
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
                        token_count=len(chunk_text) // 2,  # Use rough estimate for speed instead of full token_count
                        metadata=segment.metadata.copy(),
                    )
                )
                chunk_index += 1
            if end >= len(text):
                break
            start = max(end - overlap, start + 1)
        global_offset += len(text) + 2
    return chunks


def _find_boundary(text: str, start: int, hard_end: int, overlap: int) -> int:
    if hard_end >= len(text):
        return len(text)
        
    min_advance = max(50, (hard_end - start) // 10)
    min_index = start + overlap + min_advance
    
    if min_index >= hard_end:
        min_index = start + overlap
        
    # Phase 1: Try to find a marker in the preferred range
    for marker in ("\n\n", "\n", "。", ".", "!", "?", "！", "？", "；", ";", " "):
        index = text.rfind(marker, min_index, hard_end)
        if index != -1:
            return index + len(marker)
            
    # Phase 2: If no marker found in preferred range, try finding ANY marker > start + overlap
    if min_index > start + overlap:
        for marker in ("\n\n", "\n", "。", ".", "!", "?", "！", "？", "；", ";", " "):
            index = text.rfind(marker, start + overlap, min_index)
            if index != -1:
                return index + len(marker)
                
    return hard_end
