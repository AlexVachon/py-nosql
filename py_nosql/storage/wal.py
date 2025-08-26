import os
import json
import time

from pathlib import Path
from typing import Optional, Dict


class WAL:
    def __init__(self, path: Path):
        self.path = path
        self.f = open(self.path, 'a+', encoding="utf-8")

    def append_put(self, key: str, value: Optional[str]):
        rec = {"op": "put", "key": key, "value": value}
        self.f.write(json.dumps(rec) + "\n")
        self.f.flush()
        os.fsync(self.f.fileno())

    def replay(self) -> Dict[str, Optional[str]]:
        self.f.seek(0)
        mem: Dict[str, Optional[str]] = {}
        for line in self.f:
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec["op"] == "put":
                mem[rec["key"]] = rec.get("value")
            elif rec["op"] == "del":
                mem[rec["key"]] = None
        self.f.seek(0, os.SEEK_END)
        return mem
    
    def reset(self):
        self.f.close()

        ts = int(time.time() * 1000)
        archived = self.path.with_suffix(f".log.{ts}")
        os.replace(self.path, archived)
        self.f = open(self.path, "a+", encoding="utf-8")

    def close(self):
        self.f.close()