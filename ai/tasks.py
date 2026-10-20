"""三个 AI 任务：摘要 / 情感分类 / 实体抽取。

每个任务独立 prompt，输出严格约束（单标签或 JSON），
便于程序解析。解析失败标记 analysis_failed，不污染其他字段。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

log = logging.getLogger("newspulse.ai.tasks")

_SENTIMENT_RE = re.compile(r"positive|negative|neutral", re.I)
_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.I)


@dataclass
class AnalysisResult:
    summary: str = ""
    sentiment: str = ""
    entities: dict | None = None
    status: str = "pending"   # ok / failed

    def to_dict(self) -> dict:
        return {
            "summary": self.summary,
            "sentiment": self.sentiment,
            "entities": self.entities or {"persons": [], "orgs": [], "places": []},
            "analysis_status": self.status,
        }


# ── prompt 定义 ──

SUMMARIZE_SYSTEM = (
    "你是新闻编辑。用一句话概括给定新闻的核心内容，"
    "严格不超过 50 个汉字（英文则不超过 40 词）。"
    "只输出摘要本身，不要加引号、前缀或解释。"
)

SENTIMENT_SYSTEM = (
    "你是舆情分析引擎。判断给定新闻的情感倾向，"
    "只能从 positive / negative / neutral 中选一个。"
    "只输出这一个英文标签，不要任何其他文字。"
)

ENTITIES_SYSTEM = (
    "你是实体抽取引擎。从给定新闻中提取人物、组织、地点三类实体。"
    "严格输出 JSON，不要 markdown 代码块，不要其他文字：\n"
    '{"persons":[],"orgs":[],"places":[]}\n'
    "每个类别最多 5 个实体，没有就留空数组。"
)


def _clean(text: str) -> str:
    return _FENCE_RE.sub("", (text or "").strip()).strip()


def parse_summary(raw: str) -> str:
    s = _clean(raw)
    # 硬截断，防止模型不守 50 字约束
    return s[:120]


def parse_sentiment(raw: str) -> str:
    m = _SENTIMENT_RE.search(_clean(raw))
    if not m:
        raise ValueError(f"无法解析情感标签: {raw[:80]!r}")
    return m.group(0).lower()


def parse_entities(raw: str) -> dict:
    s = _clean(raw)
    data = json.loads(s)
    if not isinstance(data, dict):
        raise ValueError("实体结果不是 JSON object")
    return {
        "persons": [str(x)[:50] for x in (data.get("persons") or [])][:5],
        "orgs": [str(x)[:50] for x in (data.get("orgs") or [])][:5],
        "places": [str(x)[:50] for x in (data.get("places") or [])][:5],
    }


def analyze_one(client, cfg, cache, call_fn, title: str, content: str) -> AnalysisResult:
    """对一条新闻跑三个任务，逐个独立容错。"""
    text = f"标题：{title}\n正文：{content[:800]}"
    result = AnalysisResult()

    for task, system, parser in (
        ("summarize", SUMMARIZE_SYSTEM, parse_summary),
        ("sentiment", SENTIMENT_SYSTEM, parse_sentiment),
        ("entities", ENTITIES_SYSTEM, parse_entities),
    ):
        key = cache.make_key(task, text)
        cached = cache.get(key)
        if cached is not None:
            raw = cached
        else:
            try:
                raw = call_fn(client, cfg, system, text)
                cache.set(key, raw)
            except Exception as e:  # noqa: BLE001
                log.warning("   任务 %s 失败: %s", task, e)
                continue

        try:
            value = parser(raw)
        except Exception as e:  # noqa: BLE001
            log.warning("   任务 %s 解析失败: %s", task, e)
            continue

        if task == "summarize":
            result.summary = value
        elif task == "sentiment":
            result.sentiment = value
        else:
            result.entities = value

    result.status = "ok" if (result.summary or result.sentiment) else "failed"
    return result
