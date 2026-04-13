from __future__ import annotations

import json
import logging
import re
import shutil
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

DEFAULT_VECTOR_PROVIDERS: tuple[str, ...] = ("faiss", "chroma", "memory")
VECTOR_PROVIDER_CHOICES = {"auto", "faiss", "chroma", "memory"}

EMBEDDING_DIMENSIONS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
    "text-embedding-qwen3-embedding-8b": 1024,
}


def get_embedding_dimension(model: str | None) -> int:
    if model:
        model_lower = model.lower()
        for key, dim in EMBEDDING_DIMENSIONS.items():
            if key.lower() in model_lower or model_lower in key.lower():
                return dim
        if "qwen3" in model_lower or "qwen" in model_lower:
            return 1024
        if "bge" in model_lower:
            return 1024
        if "e5" in model_lower:
            return 1024
    return 1536


def normalize_vector_provider(provider: str | None) -> str:
    normalized = (provider or "auto").strip().lower() or "auto"
    if normalized not in VECTOR_PROVIDER_CHOICES:
        raise ValueError(f"Unsupported vector store provider: {provider}")
    return normalized


def model_key_for(model: str | None) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (model or "default").strip().lower()).strip("-")
    return cleaned or "default"


class ReadWriteLock:
    def __init__(self) -> None:
        self._condition = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False

    @contextmanager
    def read_lock(self) -> Iterator[None]:
        with self._condition:
            while self._writer:
                self._condition.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write_lock(self) -> Iterator[None]:
        with self._condition:
            while self._writer or self._readers > 0:
                self._condition.wait()
            self._writer = True
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


@dataclass(frozen=True, slots=True)
class StoreCacheKey:
    project_id: str
    provider: str
    model_key: str


@dataclass(slots=True)
class VectorStoreResolution:
    store: VectorStore | None
    backend: str
    provider: str
    model: str | None
    model_key: str
    available: bool
    error: str | None = None
    degraded_reason: str | None = None

    def to_trace(self) -> dict[str, object]:
        return {
            "vector_store_backend": self.backend,
            "vector_store_provider": self.provider,
            "vector_store_model": self.model,
            "vector_store_available": self.available,
            "vector_store_error": self.error,
            "semantic_degraded": not self.available,
            "degraded_reason": self.degraded_reason if not self.available else None,
        }


@dataclass(slots=True)
class VectorStoreBatch:
    ids: list[str]
    vectors: list[list[float]]
    payloads: list[dict[str, Any]] | None = None


class VectorStore(ABC):
    @abstractmethod
    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        pass

    @abstractmethod
    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def delete(self, ids: list[str]) -> None:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def save(self) -> None:
        pass


class InMemoryVectorStore(VectorStore):
    def __init__(self) -> None:
        self._vectors: dict[str, list[float]] = {}
        self._payloads: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        with self._lock:
            for index, id_ in enumerate(ids):
                self._vectors[id_] = vectors[index]
                if payloads:
                    self._payloads[id_] = payloads[index]

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        from app.utils.text import cosine_similarity

        with self._lock:
            items = list(self._vectors.items())
            payloads = dict(self._payloads)
        scored = []
        for id_, vec in items:
            score = cosine_similarity(query_vector, vec)
            scored.append({"id": id_, "score": score, **payloads.get(id_, {})})
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:top_k]

    def delete(self, ids: list[str]) -> None:
        with self._lock:
            for id_ in ids:
                self._vectors.pop(id_, None)
                self._payloads.pop(id_, None)

    def count(self) -> int:
        with self._lock:
            return len(self._vectors)

    def save(self) -> None:
        pass


class FAISSVectorStore(VectorStore):
    def __init__(self, index_path: Path, dimension: int = 1536) -> None:
        self.index_path = index_path
        self.dimension = dimension
        self._index = None
        self._id_to_idx: dict[str, int] = {}
        self._idx_to_id: dict[int, str] = {}
        self._payloads: dict[str, dict[str, Any]] = {}
        self._rw_lock = ReadWriteLock()
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_or_create()

    @property
    def _meta_path(self) -> Path:
        return self.index_path.with_suffix(".meta.json")

    def _load_or_create(self) -> None:
        import faiss

        self._id_to_idx = {}
        self._idx_to_id = {}
        self._payloads = {}
        if self.index_path.exists():
            try:
                self._index = faiss.read_index(str(self.index_path))
                if self._meta_path.exists():
                    with open(self._meta_path, "r", encoding="utf-8") as handle:
                        meta = json.load(handle)
                    self._id_to_idx = {str(key): int(value) for key, value in meta.get("id_to_idx", {}).items()}
                    self._idx_to_id = {int(key): str(value) for key, value in meta.get("idx_to_id", {}).items()}
                    self._payloads = {
                        str(key): dict(value) for key, value in meta.get("payloads", {}).items() if isinstance(value, dict)
                    }
                return
            except Exception:
                logger.warning("Failed to load FAISS index at %s, recreating it.", self.index_path, exc_info=True)
        self._index = faiss.IndexFlatIP(self.dimension)

    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        import faiss
        import numpy as np

        if not ids or not vectors:
            return
        vectors_arr = np.array(vectors, dtype=np.float32)
        if vectors_arr.ndim != 2:
            raise ValueError("FAISS expects a 2D array of vectors.")
        with self._rw_lock.write_lock():
            if self._index is None:
                self._index = faiss.IndexFlatIP(vectors_arr.shape[1])
            if self._index.ntotal == 0 and vectors_arr.shape[1] != self._index.d:
                self.dimension = vectors_arr.shape[1]
                self._index = faiss.IndexFlatIP(self.dimension)
            elif vectors_arr.shape[1] != self._index.d:
                raise ValueError(f"FAISS dimension mismatch: expected {self._index.d}, got {vectors_arr.shape[1]}")

            faiss.normalize_L2(vectors_arr)
            start_idx = self._index.ntotal
            for index, id_ in enumerate(ids):
                self._id_to_idx[id_] = start_idx + index
                self._idx_to_id[start_idx + index] = id_
                if payloads:
                    self._payloads[id_] = payloads[index]
            self._index.add(vectors_arr)

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        import faiss
        import numpy as np

        q = np.array([query_vector], dtype=np.float32)
        if q.ndim != 2:
            raise ValueError("FAISS expects a single query vector.")
        with self._rw_lock.read_lock():
            if self._index is None or self._index.ntotal == 0:
                return []
            if q.shape[1] != self._index.d:
                raise ValueError(f"FAISS query dimension mismatch: expected {self._index.d}, got {q.shape[1]}")
            faiss.normalize_L2(q)
            distances, indices = self._index.search(q, min(top_k, self._index.ntotal))
            results = []
            for dist, idx in zip(distances[0], indices[0]):
                if idx < 0:
                    break
                id_ = self._idx_to_id.get(int(idx))
                if not id_:
                    continue
                results.append({"id": id_, "score": float(dist), **self._payloads.get(id_, {})})
            return results

    def delete(self, ids: list[str]) -> None:
        if not ids:
            return
        with self._rw_lock.write_lock():
            for id_ in ids:
                self._payloads.pop(id_, None)

    def count(self) -> int:
        with self._rw_lock.read_lock():
            return self._index.ntotal if self._index is not None else 0

    def save(self) -> None:
        import faiss

        with self._rw_lock.write_lock():
            if self._index is None:
                return
            faiss.write_index(self._index, str(self.index_path))
            with open(self._meta_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "id_to_idx": self._id_to_idx,
                        "idx_to_id": self._idx_to_id,
                        "payloads": self._payloads,
                    },
                    handle,
                )


class ChromaVectorStore(VectorStore):
    def __init__(self, persist_dir: Path, collection_name: str = "corpus") -> None:
        import chromadb

        self.persist_dir = persist_dir
        self.collection_name = collection_name
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._client = chromadb.PersistentClient(path=str(persist_dir))
        self._collection = self._client.get_or_create_collection(name=collection_name)

    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        if not ids or not vectors:
            return
        docs = [json.dumps(payload, ensure_ascii=False) for payload in (payloads or [{}] * len(ids))]
        self._collection.add(ids=ids, embeddings=vectors, documents=docs)

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        results = self._collection.query(query_embeddings=[query_vector], n_results=top_k)
        ids = results.get("ids", [[]])[0]
        distances = results.get("distances", [[]])[0]
        documents = results.get("documents", [[]])[0]
        items = []
        for index, id_ in enumerate(ids):
            distance = float(distances[index]) if index < len(distances) else 0.0
            document = documents[index] if index < len(documents) else None
            payload = json.loads(document) if isinstance(document, str) and document else {}
            items.append({"id": id_, "score": 1 - distance, **payload})
        return items

    def delete(self, ids: list[str]) -> None:
        if ids:
            self._collection.delete(ids=ids)

    def count(self) -> int:
        return self._collection.count()

    def save(self) -> None:
        pass


class VectorStoreManager:
    def __init__(
        self,
        data_dir: Path,
        default_model: str | None = None,
        *,
        default_provider: str = "auto",
        allow_memory_fallback: bool = False,
    ) -> None:
        self.data_dir = data_dir
        self.default_model = default_model
        self.default_provider = normalize_vector_provider(default_provider)
        self.allow_memory_fallback = allow_memory_fallback
        self._stores: dict[StoreCacheKey, VectorStore] = {}
        self._dirty_keys: set[StoreCacheKey] = set()
        self._diagnosed_backends: set[tuple[str, str, str, bool, str | None]] = set()
        self._lock = threading.Lock()

    def get_store(self, project_id: str, provider: str = "auto", model: str | None = None) -> VectorStore:
        resolution = self.resolve_store(project_id, provider=provider, model=model, allow_memory=True)
        if not resolution.store or not resolution.available:
            error_text = resolution.error or resolution.degraded_reason or "Vector store is unavailable."
            raise RuntimeError(error_text)
        return resolution.store

    def resolve_store(
        self,
        project_id: str,
        provider: str = "auto",
        model: str | None = None,
        *,
        allow_memory: bool | None = None,
    ) -> VectorStoreResolution:
        normalized_provider = normalize_vector_provider(provider or self.default_provider)
        effective_model = (model or self.default_model or "").strip() or None
        model_key = model_key_for(effective_model)
        allow_memory_fallback = self.allow_memory_fallback if allow_memory is None else allow_memory

        if normalized_provider == "auto":
            candidates = [item for item in DEFAULT_VECTOR_PROVIDERS if item != "memory" or allow_memory_fallback]
        else:
            candidates = [normalized_provider]

        errors: list[str] = []
        for candidate in candidates:
            resolution = self._get_or_create_store(project_id, provider=candidate, model=effective_model)
            if resolution.available:
                self._log_resolution(resolution)
                return resolution
            if resolution.error:
                errors.append(f"{candidate}: {resolution.error}")
            self._log_resolution(resolution)

        backend = normalized_provider if normalized_provider != "auto" else "disabled"
        degraded_reason = "vector_store_unavailable"
        error_text = "; ".join(errors) or None
        resolution = VectorStoreResolution(
            store=None,
            backend=backend,
            provider=normalized_provider,
            model=effective_model,
            model_key=model_key,
            available=False,
            error=error_text,
            degraded_reason=degraded_reason,
        )
        self._log_resolution(resolution)
        return resolution

    def rebuild_project(
        self,
        project_id: str,
        batches_by_model: dict[str, VectorStoreBatch],
        *,
        provider: str = "auto",
        allow_memory: bool | None = None,
    ) -> dict[str, VectorStoreResolution]:
        self.clear_project(project_id)
        results: dict[str, VectorStoreResolution] = {}
        for model, batch in batches_by_model.items():
            if not batch.ids or not batch.vectors:
                continue
            resolution = self.resolve_store(project_id, provider=provider, model=model, allow_memory=allow_memory)
            results[model] = resolution
            if not resolution.store or not resolution.available:
                continue
            store = resolution.store
            batch_size = 1000
            for index in range(0, len(batch.ids), batch_size):
                store.add(
                    ids=batch.ids[index:index + batch_size],
                    vectors=batch.vectors[index:index + batch_size],
                    payloads=batch.payloads[index:index + batch_size] if batch.payloads else None,
                )
            store.save()
        return results

    def clear_project(self, project_id: str) -> None:
        with self._lock:
            keys = [key for key in self._stores if key.project_id == project_id]
            for key in keys:
                self._stores.pop(key, None)
                self._dirty_keys.discard(key)
        project_dir = self.data_dir / "vectors" / project_id
        if project_dir.exists():
            shutil.rmtree(project_dir, ignore_errors=True)

    def delete_store(self, project_id: str, provider: str | None = None, model: str | None = None) -> None:
        normalized_provider = normalize_vector_provider(provider) if provider else None
        model_key = model_key_for(model) if model is not None else None
        with self._lock:
            keys = [
                key
                for key in self._stores
                if key.project_id == project_id
                and (normalized_provider is None or key.provider == normalized_provider)
                and (model_key is None or key.model_key == model_key)
            ]
            for key in keys:
                self._stores.pop(key, None)
                self._dirty_keys.discard(key)

    def mark_dirty(self, project_id: str, provider: str = "auto", model: str | None = None) -> None:
        normalized_provider = normalize_vector_provider(provider or self.default_provider)
        cache_key = StoreCacheKey(project_id=project_id, provider=normalized_provider, model_key=model_key_for(model))
        with self._lock:
            self._dirty_keys.add(cache_key)

    def save_project(self, project_id: str) -> None:
        with self._lock:
            keys = [key for key in self._dirty_keys if key.project_id == project_id]
            stores = [(key, self._stores.get(key)) for key in keys]
        for key, store in stores:
            if store is None:
                continue
            store.save()
            with self._lock:
                self._dirty_keys.discard(key)

    def save_all(self) -> None:
        with self._lock:
            dirty_keys = list(self._dirty_keys)
            stores = [(key, self._stores.get(key)) for key in dirty_keys]
        for key, store in stores:
            if store is None:
                continue
            store.save()
            with self._lock:
                self._dirty_keys.discard(key)

    def _get_or_create_store(self, project_id: str, *, provider: str, model: str | None) -> VectorStoreResolution:
        normalized_provider = normalize_vector_provider(provider)
        model_key = model_key_for(model)
        cache_key = StoreCacheKey(project_id=project_id, provider=normalized_provider, model_key=model_key)
        with self._lock:
            existing = self._stores.get(cache_key)
        if existing is not None:
            return VectorStoreResolution(
                store=existing,
                backend=normalized_provider,
                provider=normalized_provider,
                model=model,
                model_key=model_key,
                available=True,
            )

        try:
            store = self._create_store(project_id, provider=normalized_provider, model=model, model_key=model_key)
        except Exception as exc:
            return VectorStoreResolution(
                store=None,
                backend=normalized_provider,
                provider=normalized_provider,
                model=model,
                model_key=model_key,
                available=False,
                error=_format_exception(exc),
                degraded_reason="vector_store_unavailable",
            )

        with self._lock:
            existing = self._stores.setdefault(cache_key, store)
        return VectorStoreResolution(
            store=existing,
            backend=normalized_provider,
            provider=normalized_provider,
            model=model,
            model_key=model_key,
            available=True,
        )

    def _create_store(self, project_id: str, *, provider: str, model: str | None, model_key: str) -> VectorStore:
        model_dir = self.data_dir / "vectors" / project_id / model_key
        dimension = get_embedding_dimension(model)
        if provider == "faiss":
            import faiss  # noqa: F401

            return FAISSVectorStore(model_dir / "index.faiss", dimension=dimension)
        if provider == "chroma":
            import chromadb  # noqa: F401

            return ChromaVectorStore(model_dir, collection_name=f"{project_id}-{model_key}")
        if provider == "memory":
            return InMemoryVectorStore()
        raise ValueError(f"Unsupported vector store provider: {provider}")

    def _log_resolution(self, resolution: VectorStoreResolution) -> None:
        marker = (
            resolution.provider,
            resolution.backend,
            resolution.model_key,
            resolution.available,
            resolution.error,
        )
        with self._lock:
            if marker in self._diagnosed_backends:
                return
            self._diagnosed_backends.add(marker)
        if resolution.available:
            logger.info(
                "Vector store backend resolved: provider=%s backend=%s model=%s",
                resolution.provider,
                resolution.backend,
                resolution.model or "default",
            )
        else:
            logger.warning(
                "Vector store backend unavailable: provider=%s backend=%s model=%s reason=%s error=%s",
                resolution.provider,
                resolution.backend,
                resolution.model or "default",
                resolution.degraded_reason,
                resolution.error,
            )


def _format_exception(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text
    return exc.__class__.__name__
