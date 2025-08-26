import os
import time
import json

from pathlib import Path
from typing import Dict, Optional, List

from .wal import WAL
from .sstable import SSTable

class LSMEngine:
    def __init__(self, data_dir: str, memtable_limit: int = 2000):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.wal = WAL(self.dir / 'wal.log')
        # recover memtable from WAL
        self.memtable: Dict[str, Optional[str]] = self.wal.replay()
        self.memtable_limit = memtable_limit
        self.sstables: List[SSTable] = []
        for fp in sorted(self.dir.glob('sst_*.jsonl')):
            self.sstables.append(SSTable(fp))


    def put(self, key: str, value: str):
        self.wal.append_put(key, value)
        self.memtable[key] = value
        if len(self.memtable) >= self.memtable_limit:
            self.flush()


    def delete(self, key: str):
        self.wal.append_del(key)
        self.memtable[key] = None
        if len(self.memtable) >= self.memtable_limit:
            self.flush()


    def get(self, key: str) -> Optional[str]:
        if key in self.memtable:
            return self.memtable[key]
        # search newest to oldest SSTable
        for sst in reversed(self.sstables):
            v = sst.get(key)
            if v is not None or v is None and self._exists_in_sstable(sst, key):
                return v
        return None


    def _exists_in_sstable(self, sst: SSTable, key: str) -> bool:
        # check presence by trying to locate neighbours; cheap linear fallback
        v = sst.get(key)
        return v is not None


    def flush(self):
        if not self.memtable:
            return
        ts = int(time.time() * 1000)
        path = self.dir / f'sst_{ts}.jsonl'
        sst = SSTable.write(path, self.memtable.items())
        self.sstables.append(sst)
        # reset WAL and clear memtable
        self.wal.reset()
        self.memtable.clear()


    def compact(self):
        """Merge all SSTables newest->oldest, discard tombstones and duplicates."""
        if not self.sstables:
            return
        merged: Dict[str, Optional[str]] = {}
        # newest wins
        for sst in reversed(self.sstables):
            with open(sst.data_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    k = rec["key"]
                    if k not in merged:
                        merged[k] = rec.get("value")
        # drop tombstones
        merged = {k: v for k, v in merged.items() if v is not None}
        # write a fresh SSTable and remove old ones
        ts = int(time.time() * 1000)
        new_path = self.dir / f'sst_{ts}_compacted.jsonl'
        new_sst = SSTable.write(new_path, merged.items())
        for sst in self.sstables:
            try:
                os.remove(sst.data_path)
            except FileNotFoundError:
                pass
            try:
                os.remove(sst.index_path)
            except FileNotFoundError:
                pass
        self.sstables = [new_sst]


    def close(self):
        self.flush()
        self.wal.close()