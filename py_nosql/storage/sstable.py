import json

from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional

class SSTable:
    INDEX_SAMPLE = 0

    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.index_path = data_path.with_suffix(data_path.suffix + ".idx")
        self.index = Dict[str, int] = {}
        if self.index_path.exists():
            with open(self.index_path, 'r', encoding="utf-8") as f:
                self.index = json.load(f)

    @staticmethod
    def write(data_path: Path, items: Iterable[Tuple[str, Optional[str]]]):
        items = sorted(items, key=lambda kv: kv[0])
        index = Dict[str, int] = {}
        with open(data_path, 'w', encoding='utf-8') as f:
            for i, (k, v) in enumerate(items):
                pos = f.tell()
                rec = {"key": k, "value": v}
                f.write(json.dumps(rec) + "\n")
                if i % SSTable.INDEX_SAMPLE == 0:
                    index[k] = pos
        with open(data_path.with_suffix(data_path.suffix + ".idx"), "w", encoding="utf-8") as idxf:
            json.dump(index, idxf)
        return SSTable(data_path)
    
    def _scan_from(self, start_offset: int) -> Iterable[Tuple[str, Optional[str]]]:
        with open(self.data_path, 'r', encoding='utf-8') as f:
            f.seek(start_offset)
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                yield rec["key"], rec.get("value")

    def get(self, key: str) -> Optional[Optional[str]]:
        candidates = [k for k in self.index.keys() if k <= key]
        if candidates:
            start_key = max(candidates)
            start_offset = self.index[start_key]
        else:
            start_offset = 0
            for k, v in self._scan_from(start_offset):
                if k == key:
                    return v
                if k > key:
                    return None
            return None
