import json
import re
from html import unescape
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_html(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _parse_json_ld_blocks(html: str) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for match in re.finditer(
        r"<script[^>]*type=\"application/ld\+json\"[^>]*>(.*?)</script>",
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        raw = unescape(match.group(1)).strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            blocks.extend(item for item in parsed if isinstance(item, dict))
        elif isinstance(parsed, dict):
            blocks.append(parsed)
    return blocks


def _iter_json_ld_docs(blocks: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for block in blocks:
        graph = block.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                if isinstance(item, dict):
                    yield item
        else:
            yield block


def _get_type(doc: Dict[str, Any]) -> List[str]:
    doc_type = doc.get("@type")
    if isinstance(doc_type, list):
        return [str(item) for item in doc_type]
    if isinstance(doc_type, str):
        return [doc_type]
    return []


def _extract_breadcrumbs(docs: Iterable[Dict[str, Any]]) -> List[str]:
    for doc in docs:
        if "BreadcrumbList" in _get_type(doc):
            items = doc.get("itemListElement") or []
            breadcrumbs = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = None
                if "name" in item:
                    name = item.get("name")
                else:
                    item_data = item.get("item")
                    if isinstance(item_data, dict):
                        name = item_data.get("name")
                if isinstance(name, str):
                    breadcrumbs.append(name.strip())
            if breadcrumbs:
                return breadcrumbs
    return []


def _extract_product_name(docs: Iterable[Dict[str, Any]], html: str) -> Optional[str]:
    for doc in docs:
        if "Product" in _get_type(doc):
            name = doc.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
    og_match = re.search(
        r"<meta[^>]+property=\"og:title\"[^>]+content=\"(.*?)\"",
        html,
        re.IGNORECASE,
    )
    if og_match:
        return unescape(og_match.group(1)).strip()
    title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if title_match:
        return unescape(title_match.group(1)).strip()
    return None


def extract_metadata(url: str) -> Dict[str, Any]:
    html = fetch_html(url)
    blocks = _parse_json_ld_blocks(html)
    docs = list(_iter_json_ld_docs(blocks))
    breadcrumbs = _extract_breadcrumbs(docs)
    product_name = _extract_product_name(docs, html)

    categories = [crumb for crumb in breadcrumbs if crumb.lower() != "home"]
    if product_name and categories and categories[-1].lower() == product_name.lower():
        categories = categories[:-1]

    return {
        "url": url,
        "product_name": product_name,
        "categories": categories,
        "category_path": " > ".join(categories),
    }


def extract_metadata_table(urls: Iterable[str]) -> pd.DataFrame:
    records = [extract_metadata(url) for url in urls]
    max_depth = max((len(record["categories"]) for record in records), default=0)

    for record in records:
        categories = record["categories"]
        for idx in range(max_depth):
            key = f"category_level_{idx + 1}"
            record[key] = categories[idx] if idx < len(categories) else None

    return pd.DataFrame.from_records(records)


if __name__ == "__main__":
    SAMPLE_URLS = [
        "https://hedonism.co.uk/product/karuizawa-29-year-old-cask-7802-1984-whisky",
        "https://hedonism.co.uk/product/arran-19-year-old-private-cask-exclusive-hedonism-wines-2005-whisky",
        "https://hedonism.co.uk/product/ichiros-malt-hanyu-joker-whisky",
    ]
    table = extract_metadata_table(SAMPLE_URLS)
    print(table.to_markdown(index=False))
