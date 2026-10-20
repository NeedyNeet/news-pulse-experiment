"""静态数据集重放器 —— 作业要求的功能。

把 data/sample/*.jsonl 按指定速率发给 Kafka，模拟实时流。
RSS 抓取依赖网络且量不稳定，replay 保证 demo 一定能跑出数据。
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from .schemas import NewsEvent, utc_now_iso

log = logging.getLogger("newspulse.replay")


def load_jsonl(path: str | Path) -> list[NewsEvent]:
    """读取 JSONL 数据集，每行一个 NewsEvent。"""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"数据集不存在: {path}")

    events: list[NewsEvent] = []
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(NewsEvent.from_json(line))
            except Exception as e:  # noqa: BLE001
                log.warning("第 %d 行解析失败: %s", lineno, e)

    if not events:
        raise ValueError(f"数据集为空: {path}")

    log.info("📂 载入 %d 条: %s", len(events), path)
    return events


def replay(events: list[NewsEvent], send_fn, rate_per_sec: float = 5.0) -> int:
    """按速率逐条发送。send_fn(event) -> bool。"""
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    sent = 0
    for i, ev in enumerate(events, 1):
        if send_fn(ev):
            sent += 1
        if i % 10 == 0 or i == len(events):
            log.info("   重放进度 %d/%d", i, len(events))
        if interval:
            time.sleep(interval)
    return sent


def make_sample(path: str | Path, count: int = 60) -> None:
    """生成模拟新闻数据集（作业允许"模拟数据生成器"）。

    字段齐全 + 一定随机性，用于在无网络/RSS 失效时兜底。
    """
    import random

    topics = [
        ("AI 大模型发布新版本，推理成本下降 40%", "ai", "zh"),
        ("Major cloud provider announces data center expansion", "cloud", "en"),
        ("开源数据库项目发布 3.0，支持分布式事务", "oss", "zh"),
        ("Security researchers disclose critical vulnerability", "security", "en"),
        ("新能源汽车季度销量创新高，出口增长 25%", "auto", "zh"),
        ("New programming language gains traction among developers", "pl", "en"),
        ("量子计算原型机实现新突破", "quantum", "zh"),
        ("Tech giants face new antitrust scrutiny in Europe", "policy", "en"),
        ("芯片供应链调整，本土产能占比提升", "chip", "zh"),
        ("Study shows remote work productivity gains persist", "work", "en"),
    ]
    sources = ["模拟科技日报", "SimNews", "TechWire", "数码前线"]
    sentiments = ["positive", "negative", "neutral"]

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for i in range(count):
            title, tag, lang = random.choice(topics)
            ev = NewsEvent(
                title=f"{title} #{i}",
                content=(
                    f"这是一条模拟新闻正文，主题标签 {tag}。"
                    f"据模拟信源报道，相关技术指标在近期出现明显变化，"
                    f"业内分析人士认为这将影响未来数个季度的市场格局。"
                    f"编号 {i}，随机情感倾向 {random.choice(sentiments)}。"
                ),
                publish_time=utc_now_iso(),
                source=random.choice(sources),
                url=f"https://example.com/news/{i}",
                lang=lang,
            )
            f.write(ev.to_json() + "\n")
    log.info("📝 生成模拟数据集 %d 条 → %s", count, path)
