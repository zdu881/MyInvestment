from __future__ import annotations

"""Market intelligence collection and scoring helpers.

This module borrows two ideas from external projects without reusing their code:
- feed-centric collection and normalized event schema
- query -> collect -> analyze staged sentiment pipeline
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import requests

GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_TIMEOUT_SECONDS = 6.0
DEFAULT_MAX_EVENTS = 6
REQUEST_HEADERS = {
    "User-Agent": "MyInvestment/1.0 (+https://github.com/openai/codex-cli)",
}

CATEGORY_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "regulatory": ("处罚", "立案", "监管", "问询", "调查", "罚款", "警示函", "通报", "行政处罚"),
    "governance": ("减持", "质押", "冻结", "辞职", "失联", "违规担保", "占用资金", "实控人", "股东"),
    "earnings_stress": ("预亏", "亏损", "下修", "ST", "退市", "暴雷", "商誉减值", "营收下滑", "业绩预告"),
    "litigation_safety": ("诉讼", "仲裁", "事故", "停产", "召回", "爆炸", "伤亡", "安全事故", "环保处罚"),
    "operations": ("违约", "逾期", "停牌", "债务", "流拍", "破产", "重整", "失信", "抽检不合格"),
}
CATEGORY_LABELS = {
    "regulatory": "监管",
    "governance": "股东/治理",
    "earnings_stress": "业绩压力",
    "litigation_safety": "诉讼/安全",
    "operations": "经营异常",
}
HIGH_SEVERITY_KEYWORDS = {
    "立案",
    "退市",
    "ST",
    "处罚",
    "行政处罚",
    "诉讼",
    "仲裁",
    "事故",
    "停产",
    "爆炸",
    "伤亡",
    "违约",
    "逾期",
    "破产",
    "重整",
    "失信",
}
MEDIUM_SEVERITY_KEYWORDS = {
    "问询",
    "调查",
    "减持",
    "质押",
    "冻结",
    "辞职",
    "下修",
    "亏损",
    "预亏",
    "债务",
    "停牌",
}
SEVERITY_POINTS = {"low": 1.0, "medium": 2.0, "high": 3.0}
CATEGORY_PRIORITY = [
    "regulatory",
    "earnings_stress",
    "operations",
    "litigation_safety",
    "governance",
]


class MarketIntelligenceError(RuntimeError):
    """Raised when public intelligence sources cannot produce a usable result."""


@dataclass(frozen=True)
class NormalizedEvent:
    published_at: Optional[str]
    title: str
    link: str
    source: str
    query: str
    risk_category: str
    severity: str
    matched_keywords: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "published_at": self.published_at,
            "date": self.published_at[:10] if self.published_at else "N/A",
            "title": self.title,
            "headline": self.title,
            "link": self.link,
            "source": self.source,
            "query": self.query,
            "risk_category": self.risk_category,
            "type": self.risk_category,
            "severity": self.severity,
            "matched_keywords": list(self.matched_keywords),
            "is_duplicate": False,
        }


def build_search_queries(ticker: str, company_name: Optional[str] = None) -> List[str]:
    """Build a small set of public-news queries for one symbol."""
    candidates: List[str] = []
    normalized_name = (company_name or "").strip()
    if normalized_name:
        candidates.extend(
            [
                f"{ticker} {normalized_name}",
                f"{normalized_name} 股票",
                f"{normalized_name} 处罚 OR 立案 OR 调查 OR 减持 OR 诉讼 OR 预亏 OR 退市",
            ]
        )
    else:
        candidates.extend(
            [
                f"{ticker} A股",
                f"{ticker} 上市公司",
                f"{ticker} 处罚 OR 立案 OR 调查 OR 减持 OR 诉讼 OR 预亏 OR 退市",
            ]
        )

    seen = set()
    queries: List[str] = []
    for item in candidates:
        key = item.strip()
        if key and key not in seen:
            seen.add(key)
            queries.append(key)
    return queries


def fetch_google_news_rss(
    query: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> str:
    """Fetch Google News RSS for a query."""
    url = (
        f"{GOOGLE_NEWS_RSS_URL}?q={quote_plus(query)}"
        "&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    )
    client = session or requests
    response = client.get(url, timeout=timeout, headers=REQUEST_HEADERS)
    response.raise_for_status()
    return response.text


def parse_google_news_rss(xml_text: str, query: str) -> List[Dict[str, Any]]:
    """Parse RSS items into a minimal raw event list."""
    root = ET.fromstring(xml_text)
    events: List[Dict[str, Any]] = []

    for item in root.findall(".//item"):
        title_text = _extract_text(item, "title")
        source_text = _extract_source(item)
        clean_title = _clean_title(title_text, source_text)
        if not clean_title:
            continue
        events.append(
            {
                "published_at": _parse_pub_date(_extract_text(item, "pubDate")),
                "title": clean_title,
                "description": _extract_text(item, "description"),
                "link": _extract_text(item, "link"),
                "source": source_text or "Unknown",
                "query": query,
            }
        )
    return events


def build_market_intelligence_report(
    ticker: str,
    *,
    company_name: Optional[str] = None,
    session: Optional[requests.Session] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_events: int = DEFAULT_MAX_EVENTS,
) -> Dict[str, Any]:
    """Collect public news, normalize negative events, and return a risk summary."""
    queries = build_search_queries(ticker, company_name=company_name)
    raw_events: List[Dict[str, Any]] = []
    errors: List[str] = []
    successful_queries = 0

    for query in queries:
        try:
            xml_text = fetch_google_news_rss(query, session=session)
        except Exception as exc:
            errors.append(f"{query}: {exc}")
            continue
        successful_queries += 1
        raw_events.extend(parse_google_news_rss(xml_text, query))

    if successful_queries == 0:
        raise MarketIntelligenceError("公开新闻源检索失败，无法生成舆情结论")

    normalized_events = _normalize_negative_events(
        raw_events,
        lookback_days=lookback_days,
    )
    normalized_events.sort(key=_event_sort_key, reverse=True)
    selected_events = normalized_events[:max_events]

    categories = _ordered_unique(event.risk_category for event in selected_events)
    source_count = len({event.source for event in selected_events if event.source})
    risk_score = _compute_risk_score(selected_events, source_count=source_count)

    data: Dict[str, Any] = {
        "lookback_months": max(1, round(lookback_days / 30)),
        "query_terms": queries,
        "source_count": source_count,
        "categories": categories,
        "risk_score": risk_score,
        "negative_events": [event.to_dict() for event in selected_events],
        "conclusion": _build_conclusion(selected_events, categories, risk_score),
    }
    if errors:
        data["partial_errors"] = errors[:3]
    return data


def _extract_text(element: ET.Element, tag: str) -> str:
    child = element.find(tag)
    if child is None or child.text is None:
        return ""
    return unescape(child.text).strip()


def _extract_source(element: ET.Element) -> str:
    child = element.find("source")
    if child is None or child.text is None:
        return ""
    return unescape(child.text).strip()


def _clean_title(title: str, source: str) -> str:
    text = re.sub(r"\s+", " ", unescape(title or "")).strip()
    if source:
        suffix = f" - {source}"
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text


def _parse_pub_date(value: str) -> Optional[str]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def _normalize_negative_events(raw_events: Sequence[Dict[str, Any]], *, lookback_days: int) -> List[NormalizedEvent]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)
    normalized: List[NormalizedEvent] = []
    seen_keys = set()

    for item in raw_events:
        published_at = item.get("published_at")
        if published_at:
            try:
                parsed = datetime.fromisoformat(published_at)
                if parsed < cutoff:
                    continue
            except ValueError:
                pass

        category, severity, keywords = classify_negative_text(
            title=str(item.get("title", "")),
            description=str(item.get("description", "")),
        )
        if not category:
            continue

        key = _dedupe_key(str(item.get("link", "")), str(item.get("title", "")))
        if key in seen_keys:
            continue
        seen_keys.add(key)

        normalized.append(
            NormalizedEvent(
                published_at=published_at,
                title=str(item.get("title", "")).strip(),
                link=str(item.get("link", "")).strip(),
                source=str(item.get("source", "Unknown")).strip() or "Unknown",
                query=str(item.get("query", "")).strip(),
                risk_category=category,
                severity=severity,
                matched_keywords=tuple(keywords),
            )
        )
    return normalized


def classify_negative_text(title: str, description: str = "") -> Tuple[str, str, List[str]]:
    """Return (category, severity, matched_keywords) for a headline."""
    haystack = f"{title} {description}".strip()
    if not haystack:
        return "", "", []

    category_hits: Dict[str, List[str]] = {}
    all_keywords: List[str] = []
    for category, keywords in CATEGORY_KEYWORDS.items():
        hits = [keyword for keyword in keywords if keyword and keyword in haystack]
        if hits:
            category_hits[category] = hits
            all_keywords.extend(hits)

    if not category_hits:
        return "", "", []

    category = sorted(
        category_hits,
        key=lambda item: (-len(category_hits[item]), CATEGORY_PRIORITY.index(item)),
    )[0]
    matched_keywords = _ordered_unique(category_hits[category])
    severity = _derive_severity(_ordered_unique(all_keywords))
    return category, severity, matched_keywords


def _derive_severity(matched_keywords: Sequence[str]) -> str:
    if any(keyword in HIGH_SEVERITY_KEYWORDS for keyword in matched_keywords):
        return "high"
    if any(keyword in MEDIUM_SEVERITY_KEYWORDS for keyword in matched_keywords) or len(matched_keywords) >= 2:
        return "medium"
    return "low"


def _dedupe_key(link: str, title: str) -> str:
    normalized_link = link.strip().lower()
    normalized_title = re.sub(r"\s+", " ", title.strip().lower())
    return normalized_link or normalized_title


def _event_sort_key(event: NormalizedEvent) -> Tuple[int, float, str]:
    published_score = 0.0
    if event.published_at:
        try:
            published_score = datetime.fromisoformat(event.published_at).timestamp()
        except ValueError:
            published_score = 0.0
    return (int(SEVERITY_POINTS[event.severity]), published_score, event.title)


def _compute_risk_score(events: Sequence[NormalizedEvent], *, source_count: int) -> float:
    if not events:
        return 0.0

    weighted_sum = 0.0
    for event in events:
        weighted_sum += SEVERITY_POINTS[event.severity] * _recency_weight(event.published_at)

    category_bonus = len({event.risk_category for event in events}) * 1.0
    source_bonus = min(source_count, 3) * 0.4
    raw_score = (weighted_sum + category_bonus + source_bonus) * 5.0
    return round(min(100.0, raw_score), 1)


def _recency_weight(published_at: Optional[str]) -> float:
    if not published_at:
        return 0.8
    try:
        days = (datetime.now(timezone.utc) - datetime.fromisoformat(published_at)).days
    except ValueError:
        return 0.8
    if days <= 7:
        return 1.4
    if days <= 30:
        return 1.2
    if days <= 90:
        return 1.0
    return 0.7


def _build_conclusion(events: Sequence[NormalizedEvent], categories: Sequence[str], risk_score: float) -> str:
    if not events:
        return "近3个月未检索到明确负面舆情命中。"

    category_labels = [CATEGORY_LABELS.get(category, category) for category in categories[:3]]
    if risk_score >= 70:
        level = "偏高"
    elif risk_score >= 40:
        level = "中等"
    else:
        level = "可控"
    return (
        f"近3个月检索到 {len(events)} 条负面事件，主要集中在"
        f"{'、'.join(category_labels) or '综合风险'}，综合风险分 {risk_score:.1f}/100（{level}）。"
    )


def _ordered_unique(values: Iterable[str]) -> List[str]:
    seen = set()
    items: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            items.append(value)
    return items
