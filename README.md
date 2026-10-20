# NewsPulse — 多源新闻实时流处理与智能舆情分析平台

从公开 RSS 源实时采集新闻，经 **Kafka** 消息队列缓冲，由 **Spark Structured Streaming** 做清洗、去重与实时聚合，落入 **Parquet 数据湖**；再用 **Spark SQL** 做批处理热点分析，最后调用 **大模型 API** 完成摘要、情感分类与实体抽取，通过 **Streamlit** 看板展示舆情概览。

---

## 系统架构

```
┌──────────────────────────────────────────────────────────────────────┐
│  producer/    RSS 采集 / 数据集重放  →  Kafka (topic: news.raw)       │
│               按域名限流并发 + 单源失败跳过                            │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ 3 partitions
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  streaming/   Spark Structured Streaming                              │
│               1. from_json 解析   2. 清洗去 HTML   3. URL 去重        │
│               4. 实时聚合（每分钟量/来源分布/关键词）  5. 写 Parquet   │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
┌──────────────────────────┐  ┌───────────────────────────────────────┐
│  batch/                  │  │  ai/                                  │
│  Spark SQL 热点识别       │  │  摘要 / 情感 / 实体                   │
│  关键词趋势 → SQLite     │  │  磁盘缓存 + 指数退避重试               │
└──────────────────────────┘  └──────────────────┬────────────────────┘
              │                                  │ 回写
              ▼                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│  dashboard/  Streamlit：实时趋势 / 情感分布 / 热点Top10 / 新闻+摘要    │
└──────────────────────────────────────────────────────────────────────┘
```

## 技术栈

| 层 | 组件 | 版本 |
|---|---|---|
| 消息队列 | Apache Kafka（KRaft 模式，无 ZooKeeper） | 3.9.0 |
| 流处理 | Spark Structured Streaming | 4.2.0 |
| 批处理 | Spark SQL | 4.2.0 |
| 存储 | Parquet（数据湖）+ SQLite（聚合结果） | — |
| AI | OpenAI 兼容 API（DeepSeek） | deepseek-chat |
| 看板 | Streamlit + Plotly | 1.66 |
| 运行时 | Python | 3.11 |

---

## 快速开始

> 所有命令都用 `run.ps1` 执行，它会自动处理 JDK 路径、Python 解释器、环境变量。
> **请用 `powershell`（5.1）运行，不要用 `pwsh`（7）**——后者对含 BOM 的脚本处理有差异。

### 1. 环境准备

```powershell
# Python 3.11 虚拟环境
uv venv --python 3.11
uv pip install -r requirements.txt --python .\.venv\Scripts\python.exe
```

安装 [JDK 17](https://adoptium.net/temurin/releases/?version=17)（Spark 4.x 必需），
装到 `C:\Program Files\Eclipse Adoptium\` 下即可，`run.ps1` 会自动识别。

### 2. 本地配置（一次性）

```powershell
Copy-Item config.local.example.ps1 config.local.ps1
```

编辑 `config.local.ps1`，**只有跑 AI 分析时才需要填 Key**：

```powershell
$NP_DEEPSEEK_API_KEY = "sk-你的key"   # https://platform.deepseek.com/api_keys
```

JDK 路径留空即可（会自动探测）。该文件已在 `.gitignore` 中。

### 3. 环境自检

```powershell
.\run.ps1 check
```

会检查 JDK 版本、Python、Kafka 格式化状态、API Key、关键文件，**首次使用务必先跑**。

### 4. 按顺序启动

```powershell
# 终端 1：Kafka broker（保持运行）
.\run.ps1 kafka

# 终端 2：创建 topic（broker 起来后执行一次即可）
.\run.ps1 topic

# 终端 3：Spark Streaming（保持运行）
.\run.ps1 stream

# 终端 4：生产数据（二选一）
.\run.ps1 produce     # 真实 RSS 采集
.\run.ps1 replay      # 重放模拟数据集（不依赖网络，保证 demo 可跑）

# 终端 5：AI 分析（需配置 Key）
.\run.ps1 ai 30

# 终端 6：批处理热点分析
.\run.ps1 batch

# 终端 7：可视化看板
.\run.ps1 dash
```

`.\run.ps1 dash` 会自动选择可用端口（8501 起，被占用则递增），并打印实际地址。

### 5. 手动执行（可选）

如果不想用 `run.ps1`，也可以直接调用 Python 模块，但需要自己设好环境变量：

```powershell
$env:JAVA_HOME   = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot"
$env:PATH        = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = ".\.venv\Scripts\python.exe"

.\.venv\Scripts\python.exe -m streaming.spark_consumer
```

> 注意用 `.venv\Scripts\python.exe`，而非系统 `python`——系统版本可能是 3.13+，
> pyspark 不兼容。三个 Spark 模块都会自动设置 `SPARK_HOME` / `HADOOP_HOME` / `winutils`。
---

## 模块说明

### producer/ — 数据采集

| 文件 | 职责 |
|---|---|
| `schemas.py` | `NewsEvent` 数据模型，全链路统一 schema |
| `html_text.py` | HTML → 纯文本（剥标签、解实体） |
| `rss_fetcher.py` | RSS 并发采集，**按域名限流** |
| `replay.py` | 静态数据集重放 + 模拟数据生成器 |
| `kafka_producer.py` | Kafka 发送，失败指数退避重试 |
| `run.py` | CLI 入口 |

**并发设计**：全局线程池（默认 4）+ 按域名信号量（默认同站 2 路）。
单源失败只跳过该源，不中断整批。

### streaming/ — 流处理

五段流水线，对应作业要求：

| 作业要求 | 实现 |
|---|---|
| 从 Kafka 消费 | `spark.readStream.format("kafka")` |
| 清洗 | `regexp_replace` 去 HTML、`to_timestamp` 解析时间 |
| 去重 | `withWatermark("10 minutes")` + `dropDuplicates(["url_key"])` |
| 实时统计 | 每分钟新闻量 / 各来源数 / 关键词频率 |
| 写入数据湖 | `writeStream.format("parquet")` + checkpoint |

**关键词提取**：标题小写 → 去标点 → 按空白切词 → 过滤长度 < 2 的词。

### batch/ — 批处理分析

`hotspot_job.py` 用 Spark SQL 读 Parquet：

- **热点识别**：同一标题在多源重复出现的次数（`GROUP BY title ORDER BY COUNT(*) DESC`）
- **关键词趋势**：标题切词计数 Top 30
- 结果写 SQLite 的 `hotspots` / `trends` 表

可挂 Windows 任务计划或 cron 实现每小时/每天定期执行。

### ai/ — 智能应用

| 文件 | 职责 |
|---|---|
| `client.py` | OpenAI 兼容客户端，**响应结构防御性解析** |
| `cache.py` | 磁盘缓存（JSONL，重启不丢） |
| `tasks.py` | 摘要 / 情感 / 实体三个任务的 prompt 与解析 |
| `batch_runner.py` | 批量执行，结果写回 Parquet |

三个任务：

| 任务 | 输出 | 约束 |
|---|---|---|
| 摘要生成 | ≤ 50 字 | 单句概括，硬截断 120 字兜底 |
| 情感分类 | positive / negative / neutral | 三选一，正则提取 |
| 实体抽取 | `{persons, orgs, places}` | 严格 JSON，每类最多 5 个 |

**作业三项要求的落实**：

- **缓存**：`sha256(task|text[:500])` 为 key，命中不发请求。二次运行零 API 调用。
- **批量**：`--limit N` 控制批量大小，逐条处理（单条失败不影响其他条）。
- **重试**：3 次指数退避（2s / 4s / 8s），网络异常与响应结构异常都纳入重试。

### storage/ — 存储层

| 数据 | 位置 | 格式 |
|---|---|---|
| 清洗后原始新闻 | `data/lake/news_raw/` | Parquet |
| 实时聚合（3 种） | `data/lake/agg_*/` | Parquet |
| AI 富化结果 | `data/lake/ai_enriched/` | Parquet |
| 批处理聚合 | `news_pulse.db` | SQLite |

### dashboard/ — 可视化看板

页面构成：

| 区块 | 图表/形式 | 数据源 |
|---|---|---|
| 顶部 hero | 渐变横幅 + 数据链路说明 | — |
| KPI 卡片 ×5 | 新闻总量 / AI 已分析 / 活跃来源 / 热点关键词 / 负面舆情 | 各湖表 + SQLite |
| 📈 实时新闻量趋势 | Plotly 面积图（平滑曲线 + 渐变填充） | `agg_minute` |
| 😊 情感分布 | Plotly 环形图（中心显示已分析总数） | `ai_enriched` |
| 📊 来源分布 | Plotly 横向条形图 | `agg_source` |
| 🔑 高频关键词 Top 12 | Plotly 横向条形图（**已过滤停用词**） | `agg_keyword` |
| 🔥 热点新闻 Top 10 | 卡片列表，前三名彩色排名徽章 | SQLite `hotspots` |
| 📋 新闻列表 | 卡片列表，带情感徽章 + AI 摘要 | `ai_enriched` |

**两个实现要点**：

1. **深浅色自适应** — 图表颜色由 `_is_dark()` 检测主题后设置（深色用纯白文字，背景 `#0e1117` 对比度约 19.8:1）；HTML 卡片走 CSS 变量。由于 Streamlit 切换主题**不会重跑脚本**，额外注入了一段 JS 监听主题变化并自动 reload，避免切换后配色错乱。
2. **停用词过滤** — `producer/stopwords.py` 维护约 340 个中英文虚词，`streaming`（Spark `isin` 下推）与 `dashboard`（Pandas 过滤）两层都生效，避免 `says`/`are`/`in` 这类词占据关键词榜。

> 数据读取优先用 **pyarrow** 直读 Parquet（毫秒级），读不到才回退 Spark —— 看板 5 秒刷新一次，不能每次都启 JVM。

每 5 秒轮询刷新（近实时，非毫秒级推送）。

---

## 配置

### 两级配置

| 文件 | 内容 | 是否入库 |
|---|---|---|
| `config.yaml` | 业务配置：Kafka 地址、Spark 内存、AI 模型、RSS 源列表、并发参数 | ✅ 提交 |
| `config.local.ps1` | 个人配置：JDK 路径、API Key（**全部可选**） | ❌ 已在 `.gitignore` |

`config.yaml` 主要项：

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "news.raw"
  partitions: 3

spark:
  driver_memory: "2g"          # 按机器内存调整，15G 机器用 2g 合适
  shuffle_partitions: 4        # 默认 200，小数据量纯浪费
  checkpoint_dir: "data/checkpoints"

ai:
  provider: "deepseek"
  base_url: "https://api.deepseek.com"
  model: "deepseek-chat"
  api_key_env: "DEEPSEEK_API_KEY"
  batch_size: 5
  max_retries: 3
  cache_dir: "data/cache"

sources:
  rss:
    workers: 4                 # 同时抓几个源
    per_host: 2                # 同域名最多几路（防 429）
    max_entries: 5             # 每源最多几条
    feeds:
      - { name: "BBC中文", url: "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml", lang: "zh" }
      - { name: "Solidot", url: "https://www.solidot.org/index.rss", lang: "zh" }
      # ... 共 5 个源
```

更换 AI 提供商：改 `provider` / `base_url` / `model` / `api_key_env`，代码无需改动（OpenAI 兼容协议）。
```

---

## 实现范围（对照作业要求）

| # | 作业模块 | 状态 | 说明 |
|---|---|---|---|
| 1 | 数据采集 | ✅ 完整 | RSS 实时采集 + JSONL 数据集重放，双通道 |
| 2 | 消息队列 | ✅ 完整 | Kafka 3.9 KRaft 模式，topic `news.raw` 3 分区 |
| 3 | 流处理 | ✅ 完整 | 清洗 / 去重 / 三项实时聚合 / 写数据湖 |
| 4 | 存储层 | 🟡 部分 | Parquet 数据湖 + SQLite 聚合库；**未用 MinIO** |
| 5 | 批处理分析 | ✅ 完整 | Spark SQL 热点识别 + 关键词趋势 |
| 6 | AI 智能应用 | ✅ 完整 | 摘要 / 情感 / 实体 + 缓存 / 批量 / 重试 |
| 7 | 可视化看板 | ✅ 完整 | Streamlit 四区块，5s 轮询刷新 |

### 关于存储层的说明（第 4 项）

作业原文为「使用 MinIO 作为对象存储，配合 Delta Lake 或 Parquet 格式存储」。
本项目采用**本地 Parquet**，理由：

1. Parquet 是作业明确认可的格式，且是 Delta Lake 的底层存储格式
2. 本机为 Windows 家庭版，无 Hyper-V，无法运行 Docker，MinIO 官方不提供 Windows 原生二进制
3. 数据量在 demo 规模下，本地文件系统与对象存储无性能差异

**迁移路径**：`storage/sink.py` 中 Parquet 读写已封装，改用 MinIO 只需将路径前缀换成 `s3a://` 并配置 Hadoop AWS connector，业务代码不变。

### 其他实现说明

- **Kafka 而非 Redpanda**：两者均为作业认可的选项。选 Kafka 是因为其提供官方 Windows 二进制与 KRaft 单机模式，在本机环境（无 Docker）下部署确定性更高。
- **Spark local 模式**：单机运行，通过 `spark.driver.memory` 控制资源。改为集群只需将 `.master("local[*]")` 换成 `spark://<master>:7077`。
- **情感/实体为作业自选项**，已完整实现并纳入缓存与重试机制。

---

## 项目结构

```
news-pulse/
├── producer/           # 数据采集（RSS + 重放）→ Kafka
│   └── stopwords.py    # 中英文停用词表（关键词过滤）
├── streaming/          # Spark Structured Streaming
├── batch/              # Spark SQL 批处理
├── ai/                 # 大模型智能应用
├── storage/            # Parquet + SQLite 封装、Spark 环境自检
├── dashboard/          # Streamlit 看板（深浅色自适应）
├── config/
│   └── kraft/
│       └── server.properties   # Kafka KRaft 配置
├── infra/
│   ├── kafka/          # Kafka 3.9.0 发行包
│   └── hadoop/bin/     # winutils.exe（Spark on Windows 必需）
├── data/
│   ├── sample/         # 模拟数据集（JSONL）
│   ├── lake/           # Parquet 数据湖
│   ├── cache/          # AI 响应缓存
│   ├── checkpoints/    # Spark Streaming checkpoint
│   └── kafka/          # Kafka 数据目录
├── run.ps1             # 一键启动脚本
├── config.yaml
├── requirements.txt
└── news_pulse.db       # SQLite 聚合结果
```

---

## 已验证的端到端链路

以下全部通过 `run.ps1` 实机跑通（Windows 11 + JDK 17 + Python 3.11 + Spark 4.2 + Kafka 3.9）：

| 阶段 | 命令 | 结果 |
|---|---|---|
| 环境自检 | `.\run.ps1 check` | ✅ JDK/Python/Kafka 全部识别正确 |
| Kafka broker 启动（KRaft） | `.\run.ps1 kafka` | ✅ 监听 9092，stderr 无报错 |
| Topic 创建 | `.\run.ps1 topic` | ✅ `news.raw` PartitionCount=3 |
| Producer 发送 | `.\run.ps1 replay` | ✅ 100/100 条写入 |
| Spark Streaming 消费 | `.\run.ps1 stream` | ✅ `Batch: 0` 起持续处理 |
| Parquet 落盘 | — | ✅ news_raw 22 文件 + 3 类聚合表 |
| 批处理分析 | `.\run.ps1 batch` | ✅ 热点 12 条 + 趋势词 30 个 → SQLite |
| AI 三任务 | `.\run.ps1 ai 30` | ✅ 12/12 成功（摘要/情感/实体） |
| AI 缓存命中 | 重复运行 | ✅ API 2.59s vs 缓存 <0.001s |
| 可视化看板 | `.\run.ps1 dash` | ✅ HTTP 200，四区块均有数据 |

数据规模：25 条真实 RSS 新闻（BBC中文/Solidot/TechCrunch/Ars Technica/InfoQ）+ 100 条模拟数据。

## Windows 环境踩坑记录（实机验证）

以下四个问题在实机部署中真实遇到并已解决，代码内均有对应处理，列出来供参考：

**1. `SPARK_HOME` 残留导致 PySpark 找不到 spark-submit**

系统级环境变量里残留了失效的 `SPARK_HOME=C:\spark`，PySpark 3.5+ 会优先采信它而非自动探测 venv 内位置，报 `FileNotFoundError`。
→ `storage/spark_env.py` 的 `fix_spark_env()` 强制把 `SPARK_HOME` 指向 `venv/Lib/site-packages/pyspark`。

**2. 缺 `winutils.exe` 导致写 Parquet 失败**

Spark 在 Windows 上做文件系统操作（写 Parquet、checkpoint、权限检查）需要 Hadoop 原生二进制，否则报 `HADOOP_HOME unset` 或 `UnsatisfiedLinkError`。
→ `infra/hadoop/bin/` 下放置 winutils.exe + hadoop.dll，`fix_spark_env()` 自动设置 `HADOOP_HOME` 与 PATH。

**3. Kafka connector 版本必须与 Spark 严格一致**

`spark-sql-kafka-0-10` 用了 `4.0.0` 而 Spark 是 `4.2.0`，运行时报 `NoSuchMethodError: SerializedOffset.<init>`。
→ 改为 `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`，与 `spark-sql_2.13-4.2.0` 对齐。

**4. Parquet sink 不支持 complete / update 输出模式**

聚合结果想落盘时直接报 `Invalid streaming output mode`。
→ 聚合查询改用 `foreachBatch`，在 micro-batch 内转成静态 DataFrame 写入，绕开该限制。

真实数据才暴露的坑：Spark 4.x 的 datetime pattern **不支持星期名 `EEE`**，需先剥掉 `Mon, ` 前缀；RSS 的 RFC-2822 与 ISO8601 带微秒时区（`2026-10-20T00:07:09.165523+00:00`）都要能解析，用 `coalesce` 多格式尝试，并关闭 `spark.sql.ansi.enabled` 让脏时间戳返回 NULL 而非挂掉整条流；`option("truncate", "40")` 应为布尔值；Python 正则量词 `{2,}` 经 PySpark 转 Java 正则会解析失败，改用 `length(col) >= 2`。

---

## 一键启动脚本

### 先配置本地环境（一次性）

```powershell
# 1. 复制配置模板
Copy-Item config.local.example.ps1 config.local.ps1

# 2. 用编辑器打开 config.local.ps1，按需填写：
#    - $NP_JAVA_HOME        JDK 17 路径（不填会自动扫描）
#    - $NP_DEEPSEEK_API_KEY DeepSeek Key（只有 AI 分析需要）
```

`config.local.ps1` 已在 `.gitignore` 中，不会提交到仓库。

也可以不建配置文件，改为在项目根目录放一个 `.env`：

```
DEEPSEEK_API_KEY=sk-xxxxxxxx
```

查找顺序：**已有环境变量 → 本项目 `.env` → `config.local.ps1`**。

### 环境自检

```powershell
.\run.ps1 check
```

会打印 JDK 版本、Python 版本、Kafka 是否已格式化、API Key 是否配置、关键文件是否齐全。
**首次使用建议先跑这个**，能提前发现环境问题。

### 可用命令

```powershell
.\run.ps1 check      # 环境自检（推荐先跑）
.\run.ps1 kafka      # 启动 Kafka broker（首次自动格式化）
.\run.ps1 topic      # 创建 topic news.raw（3 分区，broker 起来后执行）
.\run.ps1 stream     # 启动 Spark Streaming
.\run.ps1 produce    # 发真实 RSS 数据
.\run.ps1 replay     # 发模拟数据集（默认 100 条/秒，可传参调速率）
.\run.ps1 ai 30      # AI 三任务分析 30 条
.\run.ps1 batch      # 批处理热点分析
.\run.ps1 dash       # 启动 Streamlit 看板（端口自动选择）
.\run.ps1 sample 100 # 生成模拟数据
```

> **必须用项目内的 `.venv\Scripts\python.exe`（Python 3.11）。**
> 系统 Python 若是 3.13+，直接 `python -m ...` 会报 `No module named 'streaming'`。

---

## 已知限制

1. **无 MinIO** — 见上文存储层说明，本地 Parquet 替代，迁移路径明确
2. **看板为轮询而非推送** — 5 秒间隔，Streamlit 无原生 WebSocket 支持
3. **AI 分析为批式而非流式** — 在 Spark 流里调 LLM API 会因限流阻塞，故设计为旁路批处理
4. **内存需求** — Spark driver 默认 2G，全栈运行建议 ≥ 12G 内存

---

## 设计要点小结

本实现在若干工程细节上参考了生产环境的常见实践：

- **并发采集** — 全局线程池 + 按域名信号量限流，单源失败只跳过不中断
- **AI 响应防御** — `choices` / `message.content` 为空时抛可重试异常，而非直接崩溃
- **去重策略** — URL 归一化后精确去重（流式场景下比 AI 语义去重更可行）
- **热度信号** — 跨源重复度作为新闻热度指标
