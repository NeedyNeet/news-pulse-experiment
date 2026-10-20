"""Kafka producer。

用 kafka-python（纯 Python，无 C 依赖，Windows 省事）。
发送失败重试，指数退避。
"""

from __future__ import annotations

import json
import logging
import time

from kafka import KafkaProducer

from .schemas import NewsEvent

log = logging.getLogger("newspulse.kafka")


class NewsKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # 小 demo 不需要高吞吐，但要尽快让 consumer 看到
            linger_ms=0,
            acks="all",
            retries=3,
        )
        log.info("🔗 Kafka producer 已连接: %s (topic=%s)", bootstrap_servers, topic)

    def send(self, event: NewsEvent, retries: int = 3) -> bool:
        """发送单条，失败指数退避重试。"""
        payload = json.loads(event.to_json())
        # key 用 URL 哈希，保证同一 URL 落同一分区（便于按 URL 去重）
        key = str(hash(event.url) % 100000)

        for attempt in range(1, retries + 1):
            try:
                self._producer.send(self.topic, key=key, value=payload)
                return True
            except Exception as e:  # noqa: BLE001
                log.warning("   发送失败 (%d/%d): %s", attempt, retries, e)
                time.sleep(2 ** (attempt - 1))
        return False

    def flush_and_close(self) -> None:
        self._producer.flush()
        self._producer.close()
        log.info("🔌 Kafka producer 已关闭")

    # 兼容 send_fn(event) -> bool 签名
    __call__ = send
