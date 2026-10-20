# ============================================================
#  NewsPulse 本地配置
# ============================================================
#  使用方法：
#    1. 把本文件复制为 config.local.ps1（同目录）
#    2. 按下面说明填入你自己的路径和 API Key
#    3. 之后直接运行 run.ps1，会自动读取本配置
#
#  config.local.ps1 已被 .gitignore 忽略，不会提交到仓库，
#  可以放心填写。本模板（config.local.example.ps1）不要填真实 Key。
# ============================================================


# ── 1. JDK 17 路径（Spark 4.x 必需）─────────────────────────
#   不填则 run.ps1 会自动在 C:\Program Files\Eclipse Adoptium\ 下
#   查找 jdk-17* 目录。若自动查找失败，请手动填写，例如：
#   $NP_JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.13.11-hotspot"
#
#   下载地址：https://adoptium.net/temurin/releases/?version=17
$NP_JAVA_HOME = ""


# ── 2. DeepSeek API Key（AI 分析用）──────────────────────────
#   只有跑 AI 相关命令（.\run.ps1 ai）才需要，其他命令可留空。
#
#   获取方式：https://platform.deepseek.com/api_keys
#
#   注意：也可以不用改本文件，而是在项目根目录建一个 .env，
#   写入一行：DEEPSEEK_API_KEY=sk-xxxxxxxx
#   run.ps1 会按「环境变量 → .env → 本文件」的顺序查找。
$NP_DEEPSEEK_API_KEY = ""


# ── 3. 可选：Python 虚拟环境解释器路径 ──────────────────────
#   默认使用 .venv\Scripts\python.exe（由 uv venv --python 3.11 创建）。
#   若你的虚拟环境在别处，在这里覆盖：
#   $NP_PYTHON = "D:\somewhere\venv\Scripts\python.exe"
$NP_PYTHON = ""


# ── 4. 可选：Kafka 数据目录 ─────────────────────────────────
#   默认 data\kafka。若想放别的盘（例如 C 盘紧张），在这里覆盖：
#   $NP_KAFKA_DATA_DIR = "E:\newspulse-data\kafka"
$NP_KAFKA_DATA_DIR = ""
