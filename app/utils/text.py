from __future__ import annotations

import math
import re
from collections import Counter
from typing import Iterable


WORD_RE = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def guess_language(text: str) -> str:
    if not text.strip():
        return "unknown"
    cjk = sum(1 for char in text if "\u4e00" <= char <= "\u9fff")
    ascii_chars = sum(1 for char in text if char.isascii() and char.isalpha())
    total = max(len(text), 1)
    if cjk / total > 0.1:
        return "zh"
    if ascii_chars / total > 0.2:
        return "en"
    return "unknown"


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in WORD_RE.findall(text)]


def token_count(text: str) -> int:
    return len(tokenize(text))


def cosine_similarity(left: Iterable[float], right: Iterable[float]) -> float:
    left_list = list(left)
    right_list = list(right)
    if not left_list or not right_list or len(left_list) != len(right_list):
        return 0.0
    dot = sum(a * b for a, b in zip(left_list, right_list))
    left_norm = math.sqrt(sum(a * a for a in left_list))
    right_norm = math.sqrt(sum(b * b for b in right_list))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def top_terms(text: str, limit: int = 8) -> list[str]:
    tokens = [token for token in tokenize(text) if len(token) > 1]
    counts = Counter(tokens)
    return [term for term, _ in counts.most_common(limit)]
