from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from docx import Document as DocxDocument
from pypdf import PdfReader

from app.schemas import ExtractedSegment, ExtractionResult
from app.utils.text import guess_language, normalize_whitespace


class ExtractionError(Exception):
    """Base extraction error."""


class UnsupportedDocumentError(ExtractionError):
    """Raised when a file extension is not supported."""


class UnsupportedScannedPdfError(ExtractionError):
    """Raised when PDF appears to be image-only."""


MEANINGLESS_JSON_KEYS = {"id", "uuid", "hash", "etag", "checksum", "token"}


def extract_text(filename: str, content: bytes) -> ExtractionResult:
    extension = Path(filename).suffix.lower()
    if extension in {".html", ".htm"}:
        return _extract_html(content)
    if extension == ".json":
        return _extract_json(content)
    if extension == ".docx":
        return _extract_docx(content)
    if extension == ".pdf":
        return _extract_pdf(content)
    if extension in {".txt", ".md", ".log"}:
        return _extract_textual(content)
    raise UnsupportedDocumentError(f"Unsupported document type: {extension}")


def _extract_html(content: bytes) -> ExtractionResult:
    decoded = _decode_text(content)
    soup = BeautifulSoup(decoded, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    blocks: list[str] = []
    for tag in soup.find_all(["h1", "h2", "h3", "p", "li", "blockquote"]):
        text = tag.get_text(" ", strip=True)
        if text:
            blocks.append(text)
    title = soup.title.get_text(strip=True) if soup.title else None
    raw_text = "\n".join(blocks) or soup.get_text("\n", strip=True)
    segments = [ExtractedSegment(text=normalize_whitespace(block), metadata={}) for block in blocks if block.strip()]
    if not segments and raw_text.strip():
        segments = [ExtractedSegment(text=normalize_whitespace(raw_text), metadata={})]
    clean_text = normalize_whitespace("\n\n".join(segment.text for segment in segments))
    return ExtractionResult(
        raw_text=raw_text,
        clean_text=clean_text,
        title=title,
        author_guess=None,
        created_at_guess=None,
        language=guess_language(clean_text),
        metadata={"block_count": len(segments), "format": "html"},
        segments=segments,
    )


def _extract_json(content: bytes) -> ExtractionResult:
    decoded = _decode_text(content)
    payload = json.loads(decoded)
    lines: list[str] = []
    _flatten_json(payload, [], lines)
    raw_text = "\n".join(lines)
    clean_text = normalize_whitespace(raw_text)
    segments = [ExtractedSegment(text=line, metadata={}) for line in lines if line.strip()]
    return ExtractionResult(
        raw_text=raw_text,
        clean_text=clean_text,
        title=None,
        author_guess=None,
        created_at_guess=None,
        language=guess_language(clean_text),
        metadata={"line_count": len(lines), "format": "json"},
        segments=segments,
    )


def _extract_docx(content: bytes) -> ExtractionResult:
    doc = DocxDocument(io.BytesIO(content))
    parts: list[str] = []
    title = doc.core_properties.title or None
    author = doc.core_properties.author or None
    created_at_guess = None
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text:
            parts.append(text)
            if not title and paragraph.style and "heading" in paragraph.style.name.lower():
                title = text
    for table in doc.tables:
        rows = []
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells if cell.text.strip()]
            if cells:
                rows.append(" | ".join(cells))
        if rows:
            parts.extend(rows)
    raw_text = "\n".join(parts)
    segments = [ExtractedSegment(text=normalize_whitespace(part), metadata={}) for part in parts if part.strip()]
    clean_text = normalize_whitespace("\n\n".join(segment.text for segment in segments))
    return ExtractionResult(
        raw_text=raw_text,
        clean_text=clean_text,
        title=title,
        author_guess=author,
        created_at_guess=created_at_guess,
        language=guess_language(clean_text),
        metadata={"part_count": len(segments), "format": "docx"},
        segments=segments,
    )


def _extract_pdf(content: bytes) -> ExtractionResult:
    reader = PdfReader(io.BytesIO(content))
    page_segments: list[ExtractedSegment] = []
    for index, page in enumerate(reader.pages, start=1):
        text = normalize_whitespace(page.extract_text() or "")
        if text:
            page_segments.append(ExtractedSegment(text=text, metadata={"page_number": index}))
    if not page_segments:
        raise UnsupportedScannedPdfError("PDF contains no extractable text. OCR is not supported in v1.")
    raw_text = "\n\n".join(segment.text for segment in page_segments)
    clean_text = normalize_whitespace(raw_text)
    metadata = {"page_count": len(reader.pages), "format": "pdf", "digital_text_pages": len(page_segments)}
    return ExtractionResult(
        raw_text=raw_text,
        clean_text=clean_text,
        title=None,
        author_guess=None,
        created_at_guess=None,
        language=guess_language(clean_text),
        metadata=metadata,
        segments=page_segments,
    )


def _extract_textual(content: bytes) -> ExtractionResult:
    decoded = _decode_text(content)
    raw_text = decoded
    paragraphs = [normalize_whitespace(chunk) for chunk in re.split(r"\n\s*\n", decoded) if chunk.strip()]
    if not paragraphs:
        paragraphs = [normalize_whitespace(decoded)]
    clean_text = normalize_whitespace("\n\n".join(paragraphs))
    segments = [ExtractedSegment(text=paragraph, metadata={}) for paragraph in paragraphs if paragraph.strip()]
    return ExtractionResult(
        raw_text=raw_text,
        clean_text=clean_text,
        title=paragraphs[0][:80] if paragraphs else None,
        author_guess=None,
        created_at_guess=None,
        language=guess_language(clean_text),
        metadata={"paragraph_count": len(segments), "format": "text"},
        segments=segments,
    )


def _decode_text(content: bytes) -> str:
    encodings = ["utf-8", "utf-8-sig", "gb18030", "big5", "latin-1"]
    for encoding in encodings:
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


def _flatten_json(value: Any, path: list[str], lines: list[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_string = str(key)
            if key_string.lower() in MEANINGLESS_JSON_KEYS:
                continue
            _flatten_json(child, [*path, key_string], lines)
        return
    if isinstance(value, list):
        for index, child in enumerate(value[:50]):
            _flatten_json(child, [*path, str(index)], lines)
        return
    if value is None:
        return
    if isinstance(value, str):
        text = normalize_whitespace(value)
        if not text:
            return
    else:
        text = str(value)
    key_path = ".".join(path)
    lines.append(f"{key_path}: {text}" if key_path else text)
