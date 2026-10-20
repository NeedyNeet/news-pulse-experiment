# NewsPulse 一键启动（Windows PowerShell）
#
# 用法：
#   .\run.ps1 kafka      # 启动 Kafka broker（首次会自动格式化）
#   .\run.ps1 topic      # 创建 topic news.raw（3 分区，需 broker 已启动）
#   .\run.ps1 stream     # 启动 Spark Streaming（保持运行）
#   .\run.ps1 produce    # 发真实 RSS 数据
#   .\run.ps1 replay     # 发模拟数据集（默认 5 条/秒）
#   .\run.ps1 ai 30      # AI 三任务分析 30 条
#   .\run.ps1 batch      # 批处理热点分析
#   .\run.ps1 dash       # 启动 Streamlit 看板
#   .\run.ps1 sample 100 # 生成 100 条模拟数据
#   .\run.ps1 check      # 自检：检查 JDK / Python / Kafka 配置是否就绪
#
# 首次使用：
#   1. 复制 config.local.example.ps1 为 config.local.ps1
#   2. 在 config.local.ps1 里填入 JDK 17 路径和 DeepSeek API Key
#   3. 运行 .\run.ps1 check 验证
#   详细说明见 README.md「快速开始」

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Action,
    [Parameter(Position = 1)]
    [int]$Count = 100
)

$ErrorActionPreference = "Stop"

# ── 定位项目根目录（本脚本所在目录）──
$ROOT = $PSScriptRoot
Set-Location $ROOT

# ══════════════════ 读取本地配置 ══════════════════
# config.local.ps1 由用户自行创建（已被 .gitignore 忽略），
# 里面放 JDK 路径、API Key 等个人配置。模板见 config.local.example.ps1。
$LOCAL_CFG = Join-Path $ROOT "config.local.ps1"
if (Test-Path $LOCAL_CFG) {
    . $LOCAL_CFG
}

# ══════════════════ JDK 17 ══════════════════
function Resolve-JavaHome {
    # 1) 配置文件里显式指定（最高优先级）
    if ($NP_JAVA_HOME -and (Test-Path (Join-Path $NP_JAVA_HOME "bin\java.exe"))) {
        return $NP_JAVA_HOME
    }

    # 2) 自动扫描常见安装位置，只认 JDK 17+
    #    注意：不复用环境变量 JAVA_HOME —— 它常被其他软件设成旧版本（如 JDK 8），
    #    而 Spark 4.x 必须 JDK 17+，盲目采信会导致启动失败。
    $bases = @(
        "C:\Program Files\Eclipse Adoptium",
        "C:\Program Files\Java",
        "C:\Program Files\Amazon Corretto",
        "C:\Program Files\Microsoft",
        "C:\Program Files\Zulu"
    )
    $candidates = @()
    foreach ($b in $bases) {
        if (Test-Path $b) {
            Get-ChildItem $b -Directory -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -match 'jdk-?1[7-9]|jdk-?2\d|jre-?1[7-9]' } |
                ForEach-Object { $candidates += $_.FullName }
        }
    }
    # 17 优先，其余按路径排序（避免复杂 ScriptBlock 排序引发解析问题）
    $prefer17 = @($candidates | Where-Object { $_ -match '-17' } | Sort-Object)
    $others = @($candidates | Where-Object { $_ -notmatch '-17' } | Sort-Object)
    $ordered = @($prefer17) + @($others)
    if ($ordered.Count -gt 0) { return $ordered[0] }

    # 3) 退而求其次：环境变量 JAVA_HOME（调用方会再校验版本）
    if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
        return $env:JAVA_HOME
    }
    return $null
}

$javaHome = Resolve-JavaHome
if (-not $javaHome) {
    Write-Host ""
    Write-Host "❌ 未找到 JDK" -ForegroundColor Red
    Write-Host "   Spark 4.x 需要 JDK 17，请先安装：" -ForegroundColor Yellow
    Write-Host "   https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   装好后二选一：" -ForegroundColor Yellow
    Write-Host "   a) 复制 config.local.example.ps1 为 config.local.ps1，填写 `$NP_JAVA_HOME"
    Write-Host "   b) 设置用户环境变量 JAVA_HOME"
    Write-Host ""
    exit 1
}

$env:JAVA_HOME = $javaHome
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# 校验 Java 主版本（Spark 4.x 要求 17+）
# 注意：Java 8 的版本串是 "1.8.0_472"，要把 1.8 识别为 8 而不是 1
try {
    $verLine = (& "$env:JAVA_HOME\bin\java.exe" -version 2>&1 | Out-String).Trim()
    if ($verLine -match '"1\.(\d+)\.') { $major = [int]$Matches[1] }
    elseif ($verLine -match '"(\d+)') { $major = [int]$Matches[1] }
    else { $major = 0 }

    if ($major -gt 0 -and $major -lt 17) {
        Write-Host ""
        Write-Host "⚠ 检测到 Java $major，Spark 4.x 需要 Java 17 或更高" -ForegroundColor Yellow
        Write-Host "  当前: $env:JAVA_HOME" -ForegroundColor DarkGray
        Write-Host "  解决办法（二选一）：" -ForegroundColor Yellow
        Write-Host "   a) 安装 JDK 17 到 C:\Program Files\Eclipse Adoptium\（脚本会自动识别）"
        Write-Host "   b) 在 config.local.ps1 用 `$NP_JAVA_HOME 指向 JDK 17"
        Write-Host ""
    }
}
catch {
    # 版本解析失败不影响继续
}

# ══════════════════ Python 虚拟环境 ══════════════════
$PY = if ($NP_PYTHON -and (Test-Path $NP_PYTHON)) { $NP_PYTHON } else { "$ROOT\.venv\Scripts\python.exe" }
if (-not (Test-Path $PY)) {
    Write-Host ""
    Write-Host "❌ 找不到 Python 虚拟环境: $PY" -ForegroundColor Red
    Write-Host "   请先创建（需要 Python 3.11）：" -ForegroundColor Yellow
    Write-Host "     uv venv --python 3.11" -ForegroundColor Cyan
    Write-Host "     uv pip install -r requirements.txt --python .\.venv\Scripts\python.exe" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

$env:PYSPARK_PYTHON = $PY
$env:PYSPARK_DRIVER_PYTHON = $PY
$env:PYTHONIOENCODING = "utf-8"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
        # 控制台输出编码必须与 Python 一致，否则中文变乱码

# ══════════════════ API Key ══════════════════
# 查找顺序：已有环境变量 → 本项目 .env → config.local.ps1
if (-not $env:DEEPSEEK_API_KEY) {
    $envFile = Join-Path $ROOT ".env"
    if (Test-Path $envFile) {
        $line = Get-Content $envFile -ErrorAction SilentlyContinue |
            Where-Object { $_ -match '^\s*DEEPSEEK_API_KEY\s*=' } | Select-Object -First 1
        if ($line) {
            $env:DEEPSEEK_API_KEY = ($line -split '=', 2)[1].Trim().Trim('"').Trim("'")
        }
    }
}
if (-not $env:DEEPSEEK_API_KEY -and $NP_DEEPSEEK_API_KEY) {
    $env:DEEPSEEK_API_KEY = $NP_DEEPSEEK_API_KEY.Trim()
}

$KAFKA = "$ROOT\infra\kafka\kafka_2.13-3.9.0"
$JAVA = "$env:JAVA_HOME\bin\java.exe"
$KCP = "$KAFKA\libs\*"
# 日志目录独立，避免与运行中的 broker 抢同一文件（log4j rename 失败）
$KAFKA_LOG_DIR = "$ROOT\logs\kafka"
New-Item -ItemType Directory -Force -Path $KAFKA_LOG_DIR | Out-Null
$KLG = @("-Dlog4j.configuration=file:$KAFKA\config\log4j.properties", "-Dkafka.logs.dir=$KAFKA_LOG_DIR")

# Kafka 数据目录：默认 data\kafka，可通过 $NP_KAFKA_DATA_DIR 覆盖
$KAFKA_DATA_DIR = if ($NP_KAFKA_DATA_DIR) { $NP_KAFKA_DATA_DIR } else { "$ROOT\data\kafka" }
$META_FILE = Join-Path $KAFKA_DATA_DIR "meta.properties"

Write-Host ""
Write-Host "=== NewsPulse · action=$Action ===" -ForegroundColor Cyan
Write-Host "    root: $ROOT"
Write-Host "    py  : $PY"
Write-Host "    java: $env:JAVA_HOME"

switch ($Action.ToLower()) {

    "check" {
        Write-Host "🔎 环境自检" -ForegroundColor Cyan
        Write-Host ("  JDK 17       : {0}" -f $env:JAVA_HOME)
        $prevEAP = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        $jv = ((& "$env:JAVA_HOME\bin\java.exe" -version 2>&1 | Out-String).Trim() -replace "^.*?(:|>) ", "").Split("`n")[0].Trim()
        Write-Host ("  Java 版本    : {0}" -f $jv)
        $pv = (& $PY --version 2>&1 | Out-String).Trim()
        $ErrorActionPreference = $prevEAP
        Write-Host ("  Python       : {0}" -f $pv)
        Write-Host ("  Kafka 数据   : {0}" -f $KAFKA_DATA_DIR)
        Write-Host ("  已格式化     : {0}" -f $(if (Test-Path $META_FILE) { "是" } else { "否（首次运行 .\run.ps1 kafka 会自动格式化）" }))

        if ($env:DEEPSEEK_API_KEY) {
            $masked = $env:DEEPSEEK_API_KEY.Substring(0, [Math]::Min(6, $env:DEEPSEEK_API_KEY.Length)) + "****"
            Write-Host ("  DeepSeek Key : {0} (已配置)" -f $masked) -ForegroundColor Green
        }
        else {
            Write-Host "  DeepSeek Key : 未配置（仅 .\run.ps1 ai 需要）" -ForegroundColor Yellow
        }

        if (Test-Path $LOCAL_CFG) {
            Write-Host ("  本地配置     : {0}" -f $LOCAL_CFG) -ForegroundColor Green
        }
        else {
            Write-Host "  本地配置     : 未创建（可选，见 config.local.example.ps1）" -ForegroundColor DarkGray
        }

        $missing = @()
        foreach ($p in @(
            @{n = "config.yaml"; ok = (Test-Path "$ROOT\config.yaml") },
            @{n = "Kafka 发行包"; ok = (Test-Path "$KAFKA\libs") },
            @{n = "winutils.exe"; ok = (Test-Path "$ROOT\infra\hadoop\bin\winutils.exe") }
        )) {
            if (-not $p.ok) { $missing += $p.n }
        }
        if ($missing.Count -gt 0) {
            Write-Host ("  ⚠ 缺少: {0}" -f ($missing -join ", ")) -ForegroundColor Yellow
        }
        else {
            Write-Host "  ✅ 关键文件齐全" -ForegroundColor Green
        }
        Write-Host ""
    }

    "kafka" {
        # 生成临时配置：把 log.dirs 指向 $KAFKA_DATA_DIR（支持自定义盘符）
        $propsSrc = "$ROOT\config\kraft\server.properties"
        $propsRun = "$ROOT\config\kraft\.server.runtime.properties"
        $dataDirFwd = $KAFKA_DATA_DIR -replace '\\', '/'
        (Get-Content $propsSrc) -replace '^log\.dirs=.*', "log.dirs=$dataDirFwd" |
            Set-Content $propsRun -Encoding UTF8
        New-Item -ItemType Directory -Force -Path $KAFKA_DATA_DIR | Out-Null

        # 检查是否已格式化
        if (-not (Test-Path $META_FILE)) {
            Write-Host "首次运行：格式化 KRaft 存储..." -ForegroundColor Yellow
            $id = & $JAVA @KLG -cp $KCP kafka.tools.StorageTool random-uuid 2>$null
            $id = ($id | Where-Object { $_ -match '^[A-Za-z0-9_-]{10,}$' } | Select-Object -Last 1).Trim()
            if (-not $id) { Write-Host "❌ 无法生成 cluster id" -ForegroundColor Red; exit 1 }
            Write-Host "   cluster id: $id"
            & $JAVA @KLG -cp $KCP kafka.tools.StorageTool format -t $id -c $propsRun --ignore-formatted
        }
        else {
            Write-Host "存储已格式化，跳过。" -ForegroundColor DarkGray
        }

        Write-Host "启动 broker（Ctrl+C 停止）..." -ForegroundColor Green
        Write-Host "   log.dirs = $KAFKA_DATA_DIR" -ForegroundColor DarkGray
        Write-Host "   提示：另开终端运行 .\run.ps1 topic 创建 3 分区 topic" -ForegroundColor DarkGray
        & $JAVA @KLG -cp $KCP kafka.Kafka $propsRun
    }

    "topic" {
        # 创建 news.raw（3 分区），需 broker 已启动
        Write-Host "创建 topic news.raw（3 分区）..."
        & $JAVA @KLG -cp $KCP org.apache.kafka.tools.TopicCommand `
            --create --if-not-exists --topic news.raw `
            --partitions 3 --replication-factor 1 `
            --bootstrap-server localhost:9092 2>&1 | Select-String -Pattern 'Created|already exists|Error' | ForEach-Object { $_.Line }
        Write-Host "当前 topic:"
        & $JAVA @KLG -cp $KCP org.apache.kafka.tools.TopicCommand `
            --describe --topic news.raw --bootstrap-server localhost:9092 2>&1 |
            Select-String -Pattern 'PartitionCount|Topic:' | ForEach-Object { "   " + $_.Line.Trim() }
    }

    "stream" {
        if (-not (Test-Path $META_FILE)) {
            Write-Host "❌ Kafka 未格式化，请先运行: .\run.ps1 kafka" -ForegroundColor Red
            exit 1
        }
        Write-Host "启动 Spark Streaming（Ctrl+C 停止）..." -ForegroundColor Green
        & $PY -m streaming.spark_consumer
    }

    "produce" {
        & $PY -m producer.run --source rss
    }

    "replay" {
        # 注意：switch 分支内不能声明 param()，速率复用顶层 $Count
        # 用法：.\run.ps1 replay 10  →  每秒 10 条
        & $PY -m producer.run --source replay --rate $Count
    }

    "ai" {
        if (-not $env:DEEPSEEK_API_KEY) {
            Write-Host "❌ 缺少 DEEPSEEK_API_KEY" -ForegroundColor Red
            Write-Host "   请在 config.local.ps1 里填 `$NP_DEEPSEEK_API_KEY" -ForegroundColor Yellow
            Write-Host "   或设置环境变量 `$env:DEEPSEEK_API_KEY" -ForegroundColor DarkGray
            exit 1
        }
        # 用 try/finally 抑制 Spark 关闭时 stderr 的 temp dir 噪音，不影响退出码判断
        $prev = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        & $PY -m ai.batch_runner --limit $Count 2>$null | Where-Object { $_ -notmatch '^(SUCCESS|成功): .*PID' }
        $rc = $LASTEXITCODE; $ErrorActionPreference = $prev; if ($rc -ne 0) { exit $rc }
    }

    "batch" {
        $prev2 = $ErrorActionPreference; $ErrorActionPreference = "Continue"
        & $PY -m batch.hotspot_job --top-n $Count 2>$null | Where-Object { $_ -notmatch '^(SUCCESS|成功): .*PID' }
        $rc2 = $LASTEXITCODE; $ErrorActionPreference = $prev2; if ($rc2 -ne 0) { exit $rc2 }
    }

    "dash" {
        # 端口被占用时自动往后找一个可用端口（默认 8501）
        $port = 8501
        while (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
            Write-Host "  端口 $port 已被占用，尝试 $($port + 1)..." -ForegroundColor Yellow
            $port++
            if ($port -gt 8600) { Write-Host "❌ 找不到可用端口" -ForegroundColor Red; exit 1 }
        }
        Write-Host "🌐 看板地址: http://localhost:$port" -ForegroundColor Green
        & $PY -m streamlit run dashboard\app.py --server.port $port --server.headless true --browser.gatherUsageStats false
    }

    "sample" {
        & $PY -m producer.run --gen-sample $Count
    }

    default {
        Write-Host "未知操作: $Action" -ForegroundColor Red
        Write-Host ""
        Write-Host "可用操作:"
        Write-Host "  kafka    启动 Kafka broker"
        Write-Host "  stream   启动 Spark Streaming"
        Write-Host "  produce  发真实 RSS 数据"
        Write-Host "  replay   发模拟数据集"
        Write-Host "  ai       AI 三任务分析"
        Write-Host "  batch    批处理热点分析"
        Write-Host "  dash     启动 Streamlit 看板"
        Write-Host "  sample   生成模拟数据"
        exit 1
    }
}
