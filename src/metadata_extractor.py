import json
import re
from pathlib import Path
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

REFERENCE_SCHEMA = {
    "url": (
        "https://hedonism.co.uk/product/karuizawa-29-year-old-cask-7802-1984-whisky"
        "?srsltid=AfmBOoq7PEAwIFLNM2b39lsyMXk8MUdeu5r5ictqzubxODFPnYdHF_J8"
    ),
    "hedonism_id": "HED89307",
    "name": "Karuizawa 29 Year Old Cask 7802 1984 Whisky",
    "type": "Single Malt Whisky",
    "total_production": "1 of 577 bottles",
    "region": "Nagano",
    "country": "Japan",
    "cask": "Oloroso Sherry",
    "distillery": "Karuizawa",
    "distributor": "No. 1 Drinks",
    "distillery_closed": "Yes",
    "description": (
        "An outstanding example of single malt whisky from the iconic Karuizawa "
        "Distillery, this special 1984 vintage has been aged for 29 years in a "
        "single Oloroso Sherry cask (#7802). Selected by Richard Keller, Marius "
        "Vestnes & Martin T. Smith, this expression was bottled in October 2014 "
        "for the Norwegian market."
    ),
}

TEST_URLS = [
    "https://hedonism.co.uk/product/ardbeg-23-year-old-single-sherry-butt-2394-committee-release-1976-whisky",
    "https://hedonism.co.uk/product/glendronach-43-year-old-px-butt-cask-706-batch-12-1972-whisky",
    "https://hedonism.co.uk/product/karuizawa-29-year-old-cask-7802-1984-whisky",
]


def fetch_html(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        if response.status_code != 403:
            raise
        normalized_url = re.sub(r"^https?://", "", url)
        fallback_url = f"https://r.jina.ai/http://{normalized_url}"
        fallback_headers = {
            **DEFAULT_HEADERS,
            "Referer": "https://hedonism.co.uk/",
        }
        fallback_response = requests.get(
            fallback_url,
            headers=fallback_headers,
            timeout=timeout,
        )
        fallback_response.raise_for_status()
        return fallback_response.text
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


def _extract_hed_code(value: Any) -> Optional[str]:
    if isinstance(value, str):
        match = re.search(r"\bHED\d{3,}\b", value, re.IGNORECASE)
        if match:
            return match.group(0).upper()
        return None
    if isinstance(value, list):
        for item in value:
            code = _extract_hed_code(item)
            if code:
                return code
    if isinstance(value, dict):
        for nested_value in value.values():
            code = _extract_hed_code(nested_value)
            if code:
                return code
    return None


def _extract_hed_id(docs: Iterable[Dict[str, Any]], html: str) -> Optional[str]:
    for doc in docs:
        if "Product" in _get_type(doc):
            for key in ("sku", "mpn", "productID", "productId", "id"):
                code = _extract_hed_code(doc.get(key))
                if code:
                    return code
            code = _extract_hed_code(doc.get("offers"))
            if code:
                return code
    return _extract_hed_code(html)


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
    hed_id = _extract_hed_id(docs, html)

    categories = [crumb for crumb in breadcrumbs if crumb.lower() != "home"]
    if product_name and categories and categories[-1].lower() == product_name.lower():
        categories = categories[:-1]
    if hed_id and hed_id not in categories:
        categories.append(hed_id)

    return {
        "url": url,
        "hed_id": hed_id,
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


def build_schema_example(
    url: str,
    reference_schema: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    schema = dict(reference_schema or REFERENCE_SCHEMA)
    metadata = extract_metadata(url)
    schema["url"] = metadata.get("url") or schema.get("url")
    schema["hedonism_id"] = metadata.get("hed_id") or schema.get("hedonism_id")
    schema["name"] = metadata.get("product_name") or schema.get("name")
    ordered_columns = list(schema.keys())
    return pd.DataFrame([schema], columns=ordered_columns)


if __name__ == "__main__":
    schema_table = build_schema_example(REFERENCE_SCHEMA["url"])
    test_table = extract_metadata_table(TEST_URLS)
    output_dir = Path("data/enrichment")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "metadata_schema_example.csv"
    schema_table.to_csv(output_path, index=False)
    test_output_path = output_dir / "metadata_test_urls.csv"
    test_table.to_csv(test_output_path, index=False)
