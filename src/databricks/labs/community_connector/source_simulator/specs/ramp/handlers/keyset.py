"""Ramp ``data``/``page.next`` keyset pagination simulator."""

import json
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.corpus import (
    apply_filters,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


def serve_list(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Serve a stable ID-ordered Ramp list page with a full continuation URL."""
    parsed = urlsplit(prep.url or "")
    query = {
        key: values[-1] for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
    }
    records = corpus.get(spec.corpus) or []
    if not isinstance(records, list):
        records = []
    records = apply_filters(records, spec.filters, query)
    records = sorted(records, key=lambda record: str(record.get("id", "")))

    page_size = _bounded_page_size(query.get("page_size"))
    offset = _offset_after(records, query.get("start"))
    page = records[offset : offset + page_size]
    next_offset = offset + len(page)
    next_url = None
    if page and next_offset < len(records):
        next_query = dict(query)
        next_query["start"] = str(page[-1]["id"])
        next_url = urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(next_query),
                "",
            )
        )

    payload = {"data": page, "page": {"next": next_url}}
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    record = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)


def _bounded_page_size(raw: str | None) -> int:
    try:
        value = int(raw) if raw is not None else DEFAULT_PAGE_SIZE
    except ValueError:
        value = DEFAULT_PAGE_SIZE
    return min(MAX_PAGE_SIZE, max(2, value))


def _offset_after(records: list[dict[str, Any]], start: str | None) -> int:
    if not start:
        return 0
    for index, record in enumerate(records):
        if str(record.get("id")) == start:
            return index + 1
    return len(records)
