"""RSS 采集器。

并发策略采用生产级实践：
- 全局线程池控制总并发
- 按域名信号量限流，防止单站 429
- 单源失败只跳过，不中断整批
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
from urllib.parse import urlparse

import feedparser
import requests

from .html_text import extract_text
from .schemas import NewsEvent, utc_now_iso

log = logging.getLogger("newspulse.producer")

RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
}

DOWNLOAD_TIMEOUT = 15
FEED_RETRIES = 2


class _HostLimiter:
    """按域名限流：同一个 host 同时最多 N 个请求在飞。"""

    def __init__(self, per_host: int):
        self._per_host = per_host
        self._semas: dict[str, Semaphore] = {}
        self._lock = Lock()

    def get(self, host: str) -> Semaphore:
        with self._lock:
            if host not in self._semas:
                self._semas[host] = Semaphore(self._per_host)
            return self._semas[host]


def _download_feed(url: str, retries: int = FEED_RETRIES):
    """下载并解析一个 feed，失败重试。"""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=RSS_HEADERS, timeout=DOWNLOAD_TIMEOUT)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
            if parsed.entries:
                return parsed
            last_error = ValueError(getattr(parsed, "bozo_exception", None) or "未解析到条目")
        except Exception as e:  # noqa: BLE001 - 采集层必须兜住一切
            last_error = e
        log.warning("   [%s] 第 %d/%d 次失败: %s", url, attempt, retries, last_error)
        time.sleep(attempt)
    raise RuntimeError(str(last_error))


def fetch_feed(source: dict, max_entries: int, limiter: _HostLimiter) -> list[NewsEvent]:
    """抓取单个源，返回 NewsEvent 列表。失败返回空列表。"""
    url = source["url"]
    name = source.get("name") or urlparse(url).netloc
    lang = source.get("lang", "zh")
    host = urlparse(url).netloc

    sema = limiter.get(host)
    with sema:
        try:
            feed = _download_feed(url)
        except Exception as e:  # noqa: BLE001
            log.warning("   ✗ %s 抓取失败，跳过: %s", name, e)
            return []

        events: list[NewsEvent] = []
        for entry in feed.entries[:max_entries]:
            try:
                ev = NewsEvent.from_rss_entry(entry, source_name=name, lang=lang)
            except Exception as e:  # noqa: BLE001 - 单条脏数据不能拖垮整源
                log.warning("   [%s] 条目解析失败: %s", name, e)
                continue
            if ev and ev.is_valid:
                events.append(ev)

        log.info("   ✓ %-16s %d 条", name, len(events))
        return events


def fetch_all(sources: list[dict], max_entries: int, workers: int, per_host: int) -> list[NewsEvent]:
    """并发抓取所有源，汇总去重（按 URL）。"""
    limiter = _HostLimiter(per_host)
    workers = max(1, min(workers, len(sources)))
    log.info("📡 抓取 %d 个 RSS 源（并发 %d，同站最多 %d）", len(sources), workers, per_host)

    collected: list[NewsEvent] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_feed, s, max_entries, limiter): s for s in sources}
        for fut in as_completed(futures):
            src = futures[fut]
            try:
                collected.extend(fut.result())
            except Exception as e:  # noqa: BLE001
                log.warning("   ✗ %s 异常: %s", src.get("name"), e)

    # URL 归一化去重
    seen: set[str] = set()
    unique: list[NewsEvent] = []
    for ev in collected:
        key = ev.url.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(ev)

    log.info("✅ 共 %d 条（去重后 %d 条）", len(collected), len(unique))
    return unique
