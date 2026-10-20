"""AI 客户端（OpenAI 兼容协议）。

响应结构的防御性解析：
choices 为空 / content 为空时抛 RuntimeError 走重试，而不是 TypeError 崩掉。
"""

from __future__ import annotations

import logging
import time

import httpx
from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI

log = logging.getLogger("newspulse.ai.client")


class AIError(RuntimeError):
    """AI 调用失败（可重试）。"""


def create_client(cfg: dict) -> OpenAI:
    import os

    api_key = os.getenv(cfg["api_key_env"], "").strip()
    if not api_key:
        raise AIError(f"缺少 API Key：请设置环境变量 {cfg['api_key_env']}")
    return OpenAI(
        api_key=api_key,
        base_url=cfg["base_url"],
        timeout=httpx.Timeout(120.0, connect=30.0, read=120.0, write=30.0, pool=30.0),
        max_retries=0,  # 重试由上层统一控制
    )


def extract_completion_text(response) -> str:
    """从响应取正文；结构异常抛 AIError 让上层重试。"""
    choices = getattr(response, "choices", None)
    if not choices:
        err = getattr(response, "error", None)
        detail = ""
        if err is not None:
            msg = getattr(err, "message", None) or (
                err.get("message") if isinstance(err, dict) else None
            )
            detail = f"；API 返回: {msg}" if msg else ""
        raise AIError(f"响应中没有 choices 字段{detail}")

    message = getattr(choices[0], "message", None)
    content = getattr(message, "content", None) if message is not None else None
    if not content:
        raise AIError("响应中 message.content 为空")
    return content


def call_with_retry(client: OpenAI, cfg: dict, system: str, user: str) -> str:
    """带重试的调用。指数退避 2s/4s/8s。"""
    max_retries = int(cfg.get("max_retries", 3))
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=cfg["model"],
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.3,
                max_tokens=2000,
            )
            return extract_completion_text(resp)
        except (APITimeoutError, APIConnectionError, APIStatusError) as e:
            last_error = e
            log.warning("   AI 调用失败 (%d/%d): %s", attempt, max_retries, e)
            time.sleep(2 ** (attempt - 1))
        except AIError as e:
            last_error = e
            log.warning("   AI 响应异常 (%d/%d): %s", attempt, max_retries, e)
            time.sleep(2 ** (attempt - 1))

    raise AIError(f"AI 调用失败（重试 {max_retries} 次后）: {last_error}")
