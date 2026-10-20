"""Streamlit 可视化看板。

四个区块（作业要求）：
    1. 实时新闻量趋势（面积图）
    2. 情感分布（环形图）
    3. 热点新闻 Top10（卡片列表）
    4. 新闻列表 + AI 摘要（可展开卡片）

数据源：Parquet 数据湖 + SQLite 聚合结果。
近实时（轮询刷新），非毫秒级推送。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402

from producer.stopwords import filter_words  # noqa: E402
from storage.spark_env import spark_base_config  # noqa: E402
from storage.sink import SqliteSink  # noqa: E402

with (ROOT / "config.yaml").open(encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

LAKE = ROOT / CFG["storage"]["lake_dir"]
REFRESH_SEC = 5

# ── 主题配色 ──
COLORS = {
    "positive": "#22c55e",
    "neutral": "#64748b",
    "negative": "#ef4444",
    "primary": "#3b82f6",
    "accent": "#8b5cf6",
}
SENTIMENT_LABEL = {
    "positive": "正面",
    "neutral": "中性",
    "negative": "负面",
}

st.set_page_config(
    page_title="NewsPulse 舆情看板",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── 全局样式 ──
st.markdown(
    """
<style>
  /* 只隐藏页脚，保留右上角功能菜单（MainMenu / header）*/
  footer {visibility: hidden;}
  .block-container {padding-top: 1.6rem; padding-bottom: 2rem;}

  /* 顶部标题区 */
  .np-hero {
      background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 55%, #8b5cf6 100%);
      border-radius: 16px;
      padding: 26px 30px;
      margin-bottom: 22px;
      box-shadow: 0 8px 24px rgba(30,58,138,0.28);
  }
  .np-hero h1 {
      color: #fff; font-size: 30px; font-weight: 700;
      margin: 0 0 6px 0; letter-spacing: .5px;
  }
  .np-hero p {
      color: rgba(255,255,255,.85); font-size: 13px; margin: 0;
  }

  /* KPI 卡片 —— 颜色用 CSS 变量，深色主题下由下面的 :root 覆盖 */
  .np-kpi {
      background: var(--np-surface, #fff);
      border-radius: 14px; padding: 18px 20px;
      border: 1px solid var(--np-border, #e8edf5);
      box-shadow: 0 2px 10px rgba(15,23,42,.06);
      transition: transform .15s ease, box-shadow .15s ease;
  }
  .np-kpi:hover {
      transform: translateY(-3px);
      box-shadow: 0 8px 20px rgba(15,23,42,.12);
  }
  .np-kpi .label {
      font-size: 12px; color: var(--np-muted, #64748b); text-transform: uppercase;
      letter-spacing: .8px; font-weight: 600;
  }
  .np-kpi .value {
      font-size: 30px; font-weight: 700; color: var(--np-text, #0f172a);
      line-height: 1.2; margin-top: 6px;
  }
  .np-kpi .sub {font-size: 12px; color: var(--np-muted, #94a3b8); margin-top: 2px;}

  /* 区块标题 */
  .np-section {
      font-size: 17px; font-weight: 700; color: var(--np-text, #0f172a);
      margin: 26px 0 12px 0; padding-left: 11px;
      border-left: 4px solid #3b82f6;
  }

  /* 新闻卡片 */
  .np-card {
      background: var(--np-surface, #fff);
      border: 1px solid var(--np-border, #e8edf5);
      border-radius: 12px;
      padding: 14px 16px; margin-bottom: 10px;
      box-shadow: 0 1px 4px rgba(15,23,42,.04);
  }
  .np-card .np-title {
      font-weight: 600; font-size: 14px; color: var(--np-text, #0f172a);
      margin-bottom: 6px; line-height: 1.45;
  }
  .np-card .np-body {
      font-size: 13px; color: var(--np-body, #475569); line-height: 1.6;
  }
  .np-card .np-meta {
      font-size: 11px; color: var(--np-muted, #94a3b8); margin-top: 8px;
  }
  .np-card a {color: #60a5fa; text-decoration: none;}
  .np-card a:hover {text-decoration: underline;}

  /* 情感徽章 */
  .np-badge {
      display: inline-block; padding: 2px 9px; border-radius: 999px;
      font-size: 11px; font-weight: 600; color: #fff;
  }
  .np-rank {
      display: inline-block; width: 24px; height: 24px; line-height: 24px;
      text-align: center; border-radius: 7px; font-size: 12px;
      font-weight: 700; color: #fff; background: #cbd5e1; margin-right: 9px;
  }
  .np-rank-1 {background: #f59e0b;}
  .np-rank-2 {background: #94a3b8;}
  .np-rank-3 {background: #b45309;}
</style>
""",
    unsafe_allow_html=True,
)


def read_parquet(subdir: str) -> pd.DataFrame:
    """读数据湖里的 Parquet。

    优先用 pyarrow 直接读（毫秒级，不依赖 Spark/JVM），
    失败才回退到 Spark —— 看板每 5 秒刷新一次，不能每次都启 JVM。
    """
    path = LAKE / subdir
    if not path.exists():
        return pd.DataFrame()

    try:
        import pyarrow.parquet as pq

        return pq.read_table(str(path)).to_pandas()
    except Exception:  # noqa: BLE001
        pass

    try:
        spark = spark_base_config(CFG, "NewsPulse-Dash").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark.read.parquet(str(path)).limit(500).toPandas()
    except Exception as e:  # noqa: BLE001
        st.warning(f"读取 {subdir} 失败: {e}")
        return pd.DataFrame()


def _window_start(series: pd.Series) -> pd.Series:
    """把 Spark 的 window struct 列转成 datetime。

    agg_minute 的 w 列是 struct{start,end}。start 的整数单位可能是
    ns / us / ms / s，按量级自动判断，避免硬编码单位导致年份错乱。
    """

    def pick(v):
        return v.get("start") if isinstance(v, dict) else v

    numeric = pd.to_numeric(series.apply(pick), errors="coerce")
    finite = numeric.dropna()
    if finite.empty:
        return pd.to_datetime(numeric, errors="coerce")

    scale = finite.abs().max()
    unit = "ns" if scale > 1e17 else ("us" if scale > 1e14 else ("ms" if scale > 1e11 else "s"))
    return pd.to_datetime(numeric, unit=unit, errors="coerce")


def _badge(sentiment: str) -> str:
    c = COLORS.get(sentiment, "#94a3b8")
    label = SENTIMENT_LABEL.get(sentiment, sentiment or "未知")
    return f'<span class="np-badge" style="background:{c}">{label}</span>'


def kpi_card(label: str, value, sub: str = "") -> None:
    st.markdown(
        f"""<div class="np-kpi">
            <div class="label">{label}</div>
            <div class="value">{value}</div>
            <div class="sub">{sub}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def section(title: str) -> None:
    st.markdown(f'<div class="np-section">{title}</div>', unsafe_allow_html=True)


def _is_dark() -> bool:
    """检测 Streamlit 当前是否为深色主题。

    不同版本取值方式不同，逐个尝试，失败则假定浅色。
    """
    try:
        t = st.context.theme.type
        if t:
            return str(t).lower() == "dark"
    except Exception:  # noqa: BLE001
        pass
    try:
        b = st.get_option("theme.base")
        if b:
            return str(b).lower() == "dark"
    except Exception:  # noqa: BLE001
        pass
    return False


def _dark_layout(fig):
    """统一图表外观。

    关键：文字/标题/网格颜色必须跟随 Streamlit 主题。
    硬编码深色文字在深色背景下会看不见。
    top margin 也要留够，否则标题被裁掉。
    """
    dark = _is_dark()
    # 深色背景下用纯白 + 加粗，保证最大对比度（背景 #0e1117 接近纯黑）
    text_c = "#ffffff" if dark else "#475569"
    title_c = "#ffffff" if dark else "#0f172a"
    grid_c = "rgba(148,163,184,0.35)" if dark else "#f1f5f9"
    hover_bg = "#1e293b" if dark else "white"
    hover_fg = "#ffffff" if dark else "#0f172a"

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Microsoft YaHei, sans-serif", color=text_c, size=12),
        margin=dict(l=8, r=8, t=48, b=8),
        height=320,
        hoverlabel=dict(bgcolor=hover_bg, font_color=hover_fg, font_size=12),
        title=dict(
            font=dict(size=14, color=title_c, family="Microsoft YaHei, sans-serif"),
            x=0.01,
            xanchor="left",
        ),
    )
    fig.update_xaxes(
        gridcolor=grid_c,
        zeroline=False,
        title_font=dict(color=text_c),
        tickfont=dict(color=text_c),
    )
    fig.update_yaxes(
        gridcolor=grid_c,
        zeroline=False,
        title_font=dict(color=text_c),
        tickfont=dict(color=text_c),
    )
    return fig


# ── 深色主题下覆盖卡片配色变量 ──
# 图表颜色在 _dark_layout() 里按主题设置；HTML 卡片走这里的 CSS 变量。
# 必须放在 _is_dark() 定义之后调用。
# ── 按主题注入卡片配色变量 ──
# 必须两种模式都注入：Streamlit rerun 会累积 <style> 标签，
# 若只在深色时注入，切回浅色后旧的 --np-text:#ffffff 仍留在 DOM 里，
# 导致「白底白字」看不见。
if _is_dark():
    _theme_vars = """
  :root {
      --np-surface: #1e293b;
      --np-border: #334155;
      --np-text: #ffffff;
      --np-body: #e2e8f0;
      --np-muted: #cbd5e1;
  }
"""
else:
    _theme_vars = """
  :root {
      --np-surface: #ffffff;
      --np-border: #e8edf5;
      --np-text: #0f172a;
      --np-body: #475569;
      --np-muted: #94a3b8;
  }
"""
st.markdown(f"<style>{_theme_vars}</style>", unsafe_allow_html=True)

# ── 主题变化自动刷新 ──
# Streamlit 切换深浅色时只改前端 DOM，不会重跑脚本，
# 导致 Python 侧 _is_dark() 仍是旧值、注入旧配色（切主题后看不清）。
# 这里监听主题变化，自动 reload 让脚本按新主题重新渲染。
st.html(
    """
<script>
(function () {
  const P = window.parent;
  const readTheme = () => {
    const el = P.document.documentElement;
    const attr = el.getAttribute('data-theme')
      || P.document.body.getAttribute('data-theme') || '';
    if (attr) return attr;
    // 退化：按背景亮度判断
    const bg = getComputedStyle(P.document.body).backgroundColor || '';
    const m = bg.match(/\\d+/g);
    if (!m) return 'unknown';
    const [r, g, b] = m.map(Number);
    return (r * 0.299 + g * 0.587 + b * 0.114) > 128 ? 'light' : 'dark';
  };

  let last = readTheme();
  setInterval(() => {
    const now = readTheme();
    if (now !== last && now !== 'unknown') {
      last = now;
      P.location.reload();
    }
  }, 600);
})();
</script>
""",
    unsafe_allow_javascript=True,
)



# ══════════════════ 数据加载 ══════════════════
minute_df = read_parquet("agg_minute")
source_df = read_parquet("agg_source")
keyword_df = read_parquet("agg_keyword")
enriched = read_parquet("ai_enriched")
raw_df = read_parquet("news_raw")

try:
    sink = SqliteSink(ROOT / CFG["storage"]["sqlite_path"])
    hotspots = sink.fetch_hotspots(10)
except Exception:  # noqa: BLE001
    hotspots = []

# ══════════════════ 顶部 ══════════════════
total_news = int(len(raw_df)) if not raw_df.empty else 0
analyzed = int(len(enriched)) if not enriched.empty else 0
src_count = int(len(source_df)) if not source_df.empty else 0
kw_count = int(len(keyword_df)) if not keyword_df.empty else 0

neg_n = 0
if not enriched.empty and "sentiment" in enriched.columns:
    neg_n = int((enriched["sentiment"] == "negative").sum())

st.markdown(
    """<div class="np-hero">
        <h1>📰 NewsPulse · 多源新闻舆情看板</h1>
        <p>Kafka → Spark Structured Streaming → Parquet 数据湖 + SQLite 聚合 · AI 摘要/情感/实体</p>
    </div>""",
    unsafe_allow_html=True,
)

# ══════════════════ KPI 卡片 ══════════════════
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    kpi_card("新闻总量", total_news, "已入湖")
with c2:
    kpi_card("AI 已分析", analyzed, f"覆盖 {analyzed} 条")
with c3:
    kpi_card("活跃来源", src_count, "RSS 源")
with c4:
    kpi_card("热点关键词", kw_count, "去重后")
with c5:
    kpi_card("负面舆情", neg_n, "需关注" if neg_n else "无")

# ══════════════════ 1. 趋势 + 情感 ══════════════════
left, right = st.columns([1.55, 1])

with left:
    section("📈 实时新闻量趋势")
    if not minute_df.empty and "count" in minute_df.columns and "w" in minute_df.columns:
        p = minute_df.copy()
        p["window"] = _window_start(p["w"])
        p = p.dropna(subset=["window"]).sort_values("window")
        agg = p.groupby("window", as_index=False)["count"].sum()
        if not agg.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=agg["window"],
                    y=agg["count"],
                    mode="lines+markers",
                    fill="tozeroy",
                    line=dict(color=COLORS["primary"], width=2.5, shape="spline"),
                    marker=dict(size=7, color=COLORS["primary"]),
                    fillcolor="rgba(59,130,246,0.16)",
                    hovertemplate="%{x|%H:%M}<br>新闻量 %{y} 条<extra></extra>",
                    name="新闻量",
                )
            )
            fig.update_layout(
                title="每分钟新闻流入量",
                xaxis_title="时间",
                yaxis_title="新闻条数",
                showlegend=False,
            )
            st.plotly_chart(_dark_layout(fig), width="stretch")
            st.caption(f"共 {len(agg)} 个 1 分钟窗口 · 最新窗口 {int(agg['count'].iloc[-1])} 条")
        else:
            st.info("窗口时间解析失败")
    else:
        st.info("暂无聚合数据，请先启动 streaming 并跑 producer")

with right:
    section("😊 情感分布")
    if not enriched.empty and "sentiment" in enriched.columns:
        dist = enriched["sentiment"].fillna("unknown").value_counts()
        order = ["positive", "neutral", "negative"]
        vals = [int(dist.get(k, 0)) for k in order]
        labels = [SENTIMENT_LABEL[k] for k in order]
        cmap = [COLORS[k] for k in order]

        dark_now = _is_dark()
        fig2 = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=vals,
                    hole=0.62,
                    marker=dict(
                        colors=cmap,
                        line=dict(color="#0f172a" if dark_now else "#fff", width=2),
                    ),
                    textinfo="label+percent",
                    hovertemplate="%{label}: %{value} 条<extra></extra>",
                )
            ]
        )
        fig2.update_layout(
            title="AI 情感分类占比",
            showlegend=False,
            annotations=[
                dict(
                    text=f"<b>{sum(vals)}</b><br>已分析",
                    x=0.5,
                    y=0.5,
                    font=dict(size=17, color="#ffffff" if dark_now else "#0f172a"),
                    showarrow=False,
                )
            ],
        )
        st.plotly_chart(_dark_layout(fig2), width="stretch")
    else:
        st.info("暂无 AI 情感数据，请先跑 `python -m ai.batch_runner`")

# ══════════════════ 2. 来源 + 关键词 ══════════════════
l2, r2 = st.columns(2)

with l2:
    section("📊 来源分布")
    if not source_df.empty and "source" in source_df.columns:
        s = source_df.groupby("source", as_index=False)["count"].sum()
        s = s.sort_values("count", ascending=True).tail(10)
        fig3 = go.Figure(
            go.Bar(
                x=s["count"],
                y=s["source"],
                orientation="h",
                # 用固定蓝色而非 colorscale：Streamlit 渲染时会把 Series 型
                # marker.color 误判为单个颜色值（控制台报 "number 5" 并回退黑色）
                marker=dict(
                    color=COLORS["primary"],
                    line=dict(color="rgba(0,0,0,0)", width=0),
                ),
                hovertemplate="%{y}: %{x} 条<extra></extra>",
            )
        )
        fig3.update_layout(title="各来源新闻条数", xaxis_title="新闻条数", yaxis_title="")
        st.plotly_chart(_dark_layout(fig3), width="stretch")
    else:
        st.info("暂无来源数据")

with r2:
    section("🔑 高频关键词 Top 12")
    if not keyword_df.empty and "word" in keyword_df.columns:
        k = keyword_df.groupby("word", as_index=False)["count"].sum()
        # 过滤停用词：says / are / in / for 等虚词没有舆情价值
        k = filter_words(k, "word")
        k = k.sort_values("count", ascending=False).head(12).iloc[::-1]
        fig4 = go.Figure(
            go.Bar(
                x=k["count"],
                y=k["word"],
                orientation="h",
                marker=dict(color=COLORS["accent"]),
                hovertemplate="%{y}: %{x} 次<extra></extra>",
            )
        )
        fig4.update_layout(title="出现频率最高的实义词", xaxis_title="出现次数", yaxis_title="")
        st.plotly_chart(_dark_layout(fig4), width="stretch")
    else:
        st.info("暂无关键词数据")

# ══════════════════ 3. 热点新闻 Top 10 ══════════════════
section("🔥 热点新闻 Top 10")
if hotspots:
    for i, h in enumerate(hotspots, 1):
        title = str(h.get("title") or "")
        url = str(h.get("url") or "")
        mentions = h.get("mentions") or 0
        rank_cls = f"np-rank-{i}" if i <= 3 else ""
        st.markdown(
            f"""<div class="np-card">
                <div class="np-title">
                    <span class="np-rank {rank_cls}">{i}</span>{title}
                </div>
                <div class="np-meta">被 {mentions} 个来源报道 ·
                    <a href="{url}" target="_blank">查看原文 →</a>
                </div>
            </div>""",
            unsafe_allow_html=True,
        )
else:
    st.info("暂无热点数据，请先跑 `python -m batch.hotspot_job`")

# ══════════════════ 4. 新闻列表 ══════════════════
section("📋 新闻列表（含 AI 摘要）")
if not enriched.empty:
    cols = [c for c in ["title", "summary", "sentiment", "source", "url"] if c in enriched.columns]
    view = enriched[cols].head(30)
    for _, r in view.iterrows():
        title = str(r.get("title") or "")
        summ = str(r.get("summary") or "（无摘要）")
        senti = str(r.get("sentiment") or "")
        src = str(r.get("source") or "")
        url = str(r.get("url") or "")
        st.markdown(
            f"""<div class="np-card">
                <div class="np-title">{title} {_badge(senti)}</div>
                <div class="np-body">{summ}</div>
                <div class="np-meta">{src} ·
                    <a href="{url}" target="_blank">查看原文 →</a>
                </div>
            </div>""",
            unsafe_allow_html=True,
        )
else:
    st.info("暂无富化数据")

# ══════════════════ 刷新 ══════════════════
st.markdown(
    f'<div style="text-align:center;color:#94a3b8;font-size:12px;margin-top:18px;">'
    f"⏱ 每 {REFRESH_SEC} 秒自动刷新 · 近实时</div>",
    unsafe_allow_html=True,
)
time.sleep(REFRESH_SEC)
st.rerun()
