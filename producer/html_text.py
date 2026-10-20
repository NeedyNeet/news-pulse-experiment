"""HTML → 纯文本。

思路：
RSS 的 summary 字段常带 HTML 标签和实体，直接送给 LLM 会浪费 token
且干扰摘要质量。这里用 BeautifulSoup 剥掉标签，只留可见文字。
"""

from __future__ import annotations

import re
from html import unescape

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def extract_text(raw: str, limit: int = 2000) -> str:
    """把可能含 HTML 的字符串转成干净纯文本。"""
    if not raw:
        return ""

    text = _TAG_RE.sub(" ", raw)
    text = unescape(text)
    text = _WS_RE.sub(" ", text).strip()

    return text[:limit]
