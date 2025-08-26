import json
import uuid
from typing import Dict, List, Optional, Tuple, Iterator
from ..storage.lsm import LSMEngine
from .wrapper import DocumentWrapper
from .schema import Schema


class Collection:
    def __init__(self, engine: LSMEngine, name: str, schema: Optional[Schema] = None):
        self.engine = engine
        self.prefix = f"{name}:"
        self.schema = schema

    def _k(self, doc_id: str) -> str:
        return self.prefix + doc_id

    def _to_stored(self, doc: dict) -> str:
        if self.schema:
            self.schema.validate(doc)
        return json.dumps(doc)

    def _from_stored(self, raw: str) -> dict:
        return json.loads(raw)

    def insert(self, doc: dict, doc_id: str = None, collections: dict = None) -> str:
        """
        Insert a new document into the collection.
        
        :param doc: the document to insert
        :param doc_id: optional document ID (auto-generated if None)
        :param collections: optional dict of collection_name -> Collection, used for 'ref' validation
        """
        if doc_id is None:
            doc_id = str(uuid.uuid4())

        # Gather existing documents for unique validation
        existing = []
        if self.schema and any("unique" in rules for rules in self.schema.fields.values()):
            existing = [d._doc for d in self.find_all()]

        # Ensure collections dict exists for ref validation
        collections = collections or {}

        # Validate the document against schema (type, unique, enum, length, numeric, refs)
        if self.schema:
            self.schema.validate(doc, existing_docs=existing, collections=collections)

        # Store the document
        self.engine.put(self._k(doc_id), self._to_stored(doc))

        # Register unique fields in schema
        if self.schema:
            self.schema.register(doc)

        return doc_id

    def get(self, doc_id: str) -> Optional[dict]:
        raw = self.engine.get(self._k(doc_id))
        return self._from_stored(raw) if raw is not None else None

    def update(self, doc_id: str, updates: dict, collections: dict = None):
        """
        Update fields of a document (merge semantics).
        
        :param doc_id: ID of the document to update
        :param updates: dict of fields to update
        :param collections: optional dict of collection_name -> Collection for 'ref' validation
        """
        # Fetch current document
        cur = self.get(doc_id)
        if cur is None:
            raise KeyError(f"{doc_id} not found")

        # Merge updates
        if self.model_cls:
            # If using a model/dataclass
            base = cur.__dict__.copy()
            base.update(updates)
            new_doc = self.model_cls(**base)
        else:
            base = cur.copy()
            base.update(updates)
            new_doc = base

        # Gather existing docs for unique validation (exclude current doc)
        existing = []
        if self.schema and any("unique" in rules for rules in self.schema.fields.values()):
            existing = [d._doc for d in self.find_all() if d._id != doc_id]

        # Ensure collections dict exists
        collections = collections or {}

        # Validate updated document against schema
        if self.schema:
            self.schema.validate(new_doc, existing_docs=existing, collections=collections)

        # Store updated document
        self.engine.put(self._k(doc_id), self._to_stored(new_doc))

        # Register unique fields
        if self.schema:
            self.schema.register(new_doc)

    def _scan_docs(self) -> Iterator[Tuple[str, dict]]:
        seen = set()

        for k, v in self.engine.memtable.items():
            if not k.startswith(self.prefix) or v is None:
                continue
            doc_id = k[len(self.prefix):]
            doc = self._from_stored(v)
            yield doc_id, doc
            seen.add(doc_id)

        for sst in reversed(self.engine.sstables):
            with open(sst.data_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    k = rec["key"]
                    if not k.startswith(self.prefix):
                        continue
                    doc_id = k[len(self.prefix):]
                    if doc_id in seen:
                        continue
                    v = rec.get("value")
                    if v is None:
                        continue
                    doc = self._from_stored(v)
                    yield doc_id, doc
                    seen.add(doc_id)

    def find_all(self, filter=None, limit=None) -> List[DocumentWrapper]:
        results = []
        for doc_id, doc in self._scan_docs():
            if self._matches(doc, filter):
                results.append(DocumentWrapper(doc_id, doc))
                if limit and len(results) >= limit:
                    break
        return results

    def find_one(self, filter=None) -> Optional[DocumentWrapper]:
        docs = self.find_all(filter=filter, limit=1)
        return docs[0] if docs else None

    def find(self, filter=None, limit=None) -> List[DocumentWrapper]:
        return self.find_all(filter=filter, limit=limit)

    def _matches(self, doc, filter):
        if filter is None:
            return True

        for field, cond in filter.items():
            val = doc.get(field)
            if isinstance(cond, dict):
                for op, cmp_val in cond.items():
                    if op == "$eq" and not val == cmp_val:
                        return False
                    if op == "$gt" and not val > cmp_val:
                        return False
                    if op == "$gte" and not val >= cmp_val:
                        return False
                    if op == "$lt" and not val < cmp_val:
                        return False
                    if op == "$lte" and not val <= cmp_val:
                        return False
            else:
                if val != cond:
                    return False
        return True
