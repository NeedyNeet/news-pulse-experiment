"""新闻事件数据模型。

producer 发到 Kafka、streaming 消费、AI 富化、看板读取，
全链路共用这一个 schema。字段名与作业要求对齐：
title / content / publish_time / source / url
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class NewsEvent:
    """一条新闻的规范表示。"""

    title: str
    content: str
    publish_time: str          # ISO8601 UTC
    source: str
    url: str
    lang: str = "zh"
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ingested_at: str = field(default_factory=utc_now_iso)

    # AI 富化字段（由 ai/ 模块回写，producer 阶段留空）
    summary: str = ""
    sentiment: str = ""        # positive / negative / neutral
    entities: dict = field(default_factory=dict)
    analysis_status: str = ""  # ok / failed / pending

    MAX_CONTENT = 2000

    def __post_init__(self) -> None:
        self.title = (self.title or "").strip()
        self.content = (self.content or "").strip()[: self.MAX_CONTENT]
        self.source = (self.source or "").strip()
        self.url = (self.url or "").strip()
        self.lang = (self.lang or "zh").strip().lower() or "zh"

    @property
    def is_valid(self) -> bool:
        """至少要有标题和链接，否则不值得进管道。"""
        return bool(self.title) and bool(self.url)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> "NewsEvent":
        data = json.loads(raw)
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def from_rss_entry(cls, entry, source_name: str, lang: str) -> "NewsEvent | None":
        """从 feedparser 条目构造；解析失败返回 None。"""
        from .html_text import extract_text

        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link:
            return None

        # 正文优先用 content，其次 summary
        raw_body = ""
        content = entry.get("content") or []
        if content and isinstance(content, list):
            raw_body = (content[0].get("value") or "").strip()
        if not raw_body:
            raw_body = (entry.get("summary") or entry.get("description") or "").strip()

        published = entry.get("published") or entry.get("updated") or utc_now_iso()

        return cls(
            title=title,
            content=extract_text(raw_body),
            publish_time=published,
            source=source_name,
            url=link,
            lang=lang,
        )
