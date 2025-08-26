from typing import Dict

from .collection import Collection
from ..storage.lsm import LSMEngine


class DocumentStore:
    def __init__(self, data_dir: str):
        self.engine = LSMEngine(data_dir)
        self.collections: Dict[str, Collection] = {}


    def collection(self, name: str, schema=None) -> Collection:
        if name not in self.collections:
            self.collections[name] = Collection(self.engine, name, schema)
        return self.collections[name]


    def compact(self):
        self.engine.compact()


    def close(self):
        self.engine.close()