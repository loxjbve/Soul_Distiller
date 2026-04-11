from __future__ import annotations

import json
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

EMBEDDING_DIMENSIONS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


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
            for i, id_ in enumerate(ids):
                self._vectors[id_] = vectors[i]
                if payloads:
                    self._payloads[id_] = payloads[i]

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        from app.utils.text import cosine_similarity
        with self._lock:
            items = list(self._vectors.items())
        scored = []
        for id_, vec in items:
            score = cosine_similarity(query_vector, vec)
            payload = self._payloads.get(id_, {})
            scored.append({"id": id_, "score": score, **payload})
        scored.sort(key=lambda x: x["score"], reverse=True)
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
        self._lock = threading.Lock()
        self._load_or_create()

    def _load_or_create(self) -> None:
        import faiss
        if self.index_path.exists():
            try:
                self._index = faiss.read_index(str(self.index_path))
                meta_path = self.index_path.with_suffix(".meta.json")
                if meta_path.exists():
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                        self._id_to_idx = meta.get("id_to_idx", {})
                        self._idx_to_id = {int(k): v for k, v in meta.get("idx_to_id", {}).items()}
                        self._payloads = meta.get("payloads", {})
            except Exception:
                self._index = faiss.IndexFlatIP(self.dimension)
        else:
            self._index = faiss.IndexFlatIP(self.dimension)

    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        import faiss
        with self._lock:
            start_idx = self._index.ntotal
            for i, id_ in enumerate(ids):
                self._id_to_idx[id_] = start_idx + i
                self._idx_to_id[start_idx + i] = id_
                if payloads:
                    self._payloads[id_] = payloads[i]
            vectors_arr = faiss.normalize_L2s(vectors)
            self._index.add(vectors_arr)

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        import faiss
        with self._lock:
            q = faiss.normalize_L2s([query_vector])
            distances, indices = self._index.search(q, min(top_k, self._index.ntotal))
            results = []
            for dist, idx in zip(distances[0], indices[0]):
                if idx < 0:
                    break
                id_ = self._idx_to_id.get(int(idx))
                if id_:
                    payload = self._payloads.get(id_, {})
                    results.append({"id": id_, "score": float(dist), **payload})
        return results

    def delete(self, ids: list[str]) -> None:
        pass

    def count(self) -> int:
        with self._lock:
            return self._index.ntotal if self._index else 0

    def save(self) -> None:
        import faiss
        with self._lock:
            if self._index is not None:
                faiss.write_index(self._index, str(self.index_path))
                meta_path = self.index_path.with_suffix(".meta.json")
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump({
                        "id_to_idx": self._id_to_idx,
                        "idx_to_id": self._idx_to_id,
                        "payloads": self._payloads,
                    }, f)


class ChromaVectorStore(VectorStore):
    def __init__(self, persist_dir: Path, collection_name: str = "corpus") -> None:
        import chromadb
        self.persist_dir = persist_dir
        self.collection_name = collection_name
        self._client = chromadb.PersistentClient(path=str(persist_dir))
        self._collection = self._client.get_or_create_collection(name=collection_name)

    def add(self, ids: list[str], vectors: list[list[float]], payloads: list[dict[str, Any]] | None = None) -> None:
        docs = []
        for p in (payloads or [{}] * len(ids)):
            docs.append(json.dumps(p, ensure_ascii=False))
        self._collection.add(ids=ids, embeddings=vectors, documents=docs)

    def search(self, query_vector: list[float], top_k: int = 5) -> list[dict[str, Any]]:
        results = self._collection.query(query_embeddings=[query_vector], n_results=top_k)
        items = []
        for i, (ids, distances, documents) in enumerate(zip(
            results.get("ids", [[]])[0],
            results.get("distances", [[]])[0],
            results.get("documents", [[]])[0],
        )):
            payload = json.loads(documents[i]) if documents else {}
            items.append({"id": ids, "score": 1 - distances[i], **payload})
        return items

    def delete(self, ids: list[str]) -> None:
        self._collection.delete(ids=ids)

    def count(self) -> int:
        return self._collection.count()

    def save(self) -> None:
        pass


class VectorStoreManager:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._stores: dict[str, VectorStore] = {}
        self._lock = threading.Lock()

    def get_store(self, project_id: str, provider: str = "auto") -> VectorStore:
        with self._lock:
            if project_id in self._stores:
                return self._stores[project_id]
            store = self._create_store(project_id, provider)
            self._stores[project_id] = store
            return store

    def _create_store(self, project_id: str, provider: str) -> VectorStore:
        project_dir = self.data_dir / "vectors" / project_id
        project_dir.mkdir(parents=True, exist_ok=True)
        if provider == "faiss":
            return FAISSVectorStore(project_dir / "index.faiss", dimension=1536)
        elif provider == "chroma":
            return ChromaVectorStore(project_dir, collection_name=project_id)
        elif provider == "memory":
            return InMemoryVectorStore()
        else:
            try:
                import chromadb
                return ChromaVectorStore(project_dir, collection_name=project_id)
            except ImportError:
                try:
                    import faiss
                    return FAISSVectorStore(project_dir / "index.faiss", dimension=1536)
                except ImportError:
                    return InMemoryVectorStore()

    def delete_store(self, project_id: str) -> None:
        with self._lock:
            if project_id in self._stores:
                del self._stores[project_id]

    def save_all(self) -> None:
        with self._lock:
            for store in self._stores.values():
                store.save()
