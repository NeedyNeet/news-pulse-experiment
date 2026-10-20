"""磁盘缓存 —— 作业明确要求"避免重复请求"。

key   = sha256(task_type + "|" + text[:500])
存储  = JSONL 追加写，重启不丢
命中  = 直接返回，不发 API 请求
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path

log = logging.getLogger("newspulse.ai.cache")


class ResponseCache:
    def __init__(self, cache_dir: str, filename: str = "ai_cache.jsonl"):
        self.path = Path(cache_dir) / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._mem: dict[str, str] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        loaded = 0
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    self._mem[rec["key"]] = rec["value"]
                    loaded += 1
                except Exception:  # noqa: BLE001 - 单行脏数据跳过
                    continue
        if loaded:
            log.info("💾 缓存已载入 %d 条: %s", loaded, self.path)

    @staticmethod
    def make_key(task: str, text: str) -> str:
        raw = f"{task}|{text[:500]}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._mem.get(key)

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._mem[key] = value
            try:
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")
            except Exception as e:  # noqa: BLE001 - 缓存写失败不阻断主流程
                log.warning("缓存写入失败: %s", e)

    def __len__(self) -> int:
        return len(self._mem)
