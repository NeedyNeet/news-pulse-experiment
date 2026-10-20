"""停用词过滤。

关键词频率统计里会混入大量无信息量的功能词：
    英文：yet / are / in / for / on / be / says ...
    中文：的 / 了 / 是 / 在 / 和 ...
这些词出现次数高但没有舆情价值，会挤占 Top N，需要过滤掉。
"""

from __future__ import annotations

import re

# 英文功能词（含常见新闻套话里的高频虚词）
EN_STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
    "during", "each", "few", "for", "from", "further", "had", "hadn't", "has",
    "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her",
    "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
    "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it",
    "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my",
    "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other",
    "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't",
    "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "will", "with", "won't",
    "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
    "yours", "yourself", "yourselves",
    # 新闻标题里常见但信息量低的动词/助词
    "says", "said", "say", "gets", "get", "got", "make", "makes", "made", "take",
    "takes", "taken", "use", "uses", "used", "using", "new", "now", "yet", "still",
    "also", "may", "might", "must", "one", "two", "three", "first", "last",
    "next", "way", "ways", "thing", "things", "lot", "help", "helps", "need",
    "needs", "want", "wants", "see", "sees", "look", "looks", "come", "comes",
    "go", "goes", "going", "know", "knows", "think", "thinks", "just", "like",
    "well", "even", "much", "many", "own", "back", "around", "another", "sure",
    # 指示代词与人称相关（信息量低）
    "someone", "somebody", "anyone", "everyone", "everything", "something",
    "us", "them", "him", "her", "it", "its", "their", "our", "your", "my",
    # 标题里常见的缩写/套话
    "psa", "faq", "vs", "etc", "via", "amp", "www", "http", "https", "com",
}

# 中文虚词
ZH_STOPWORDS = {
    "的", "了", "是", "在", "和", "与", "及", "或", "等", "就", "都", "而", "及",
    "其", "这", "那", "有", "也", "还", "又", "更", "最", "很", "太", "被", "把",
    "让", "给", "从", "对", "为", "以", "将", "并", "但", "却", "则", "即", "若",
    "之", "于", "所", "由", "该", "个", "们", "么", "什", "怎", "如", "上", "下",
    "中", "内", "外", "前", "后", "时", "地", "得", "着", "过", "来", "去", "说",
    "做", "用", "到", "能", "会", "要", "可以", "这个", "那个", "一个", "一种",
    "方面", "进行", "通过", "实现", "表示", "认为", "目前", "已经", "正在",
}

STOPWORDS = EN_STOPWORDS | ZH_STOPWORDS

_PURE_NUM = re.compile(r"^[\d\W_]+$")


def is_stopword(word: str | None) -> bool:
    """判断一个词是否应被过滤。"""
    if not word:
        return True
    w = str(word).strip().lower()
    if len(w) < 2:
        return True
    if w in STOPWORDS:
        return True
    if _PURE_NUM.match(w):
        return True
    return False


def filter_words(df, col: str = "word"):
    """对 DataFrame 按停用词表过滤（走 Pandas 而非 Spark UDF，简单可靠）。

    Args:
        df: 含关键词列的 DataFrame
        col: 关键词列名

    Returns:
        过滤后的 DataFrame
    """
    if df is None or df.empty or col not in df.columns:
        return df
    mask = ~df[col].astype(str).apply(is_stopword)
    return df[mask].copy()
