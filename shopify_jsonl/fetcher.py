"""
Shopify Bulk Operation Fetcher.

Triggers a bulk operation via GraphQL, polls for completion, and downloads
the result JSONL. Designed for CLI use: blocks until done, prints progress,
writes the result to a local file.

Simplified from SnowPipe's BulkOperationManager: single operation only,
no concurrency or deferred pattern.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any

from shopify_jsonl.queries import products_query

logger = logging.getLogger(__name__)

DEFAULT_API_VERSION = "2026-01"
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_MAX_WAIT = 1200.0  # 20 minutes


class BulkOperationError(Exception):
    pass


def fetch_bulk_export(
    shop_domain: str,
    access_token: str,
    output_path: Path,
    *,
    include_inventory: bool = True,
    api_version: str = DEFAULT_API_VERSION,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    max_wait: float = DEFAULT_MAX_WAIT,
) -> int:
    """
    Trigger a Shopify products bulk operation, poll until done, download result.

    Returns the number of bytes written to *output_path*.
    Raises BulkOperationError on failures.
    """
    graphql_url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    # 1. Build query
    query = products_query(include_inventory=include_inventory)
    logger.info("Triggering bulk operation on %s (API %s)", shop_domain, api_version)

    # 2. Create the bulk operation.
    # GraphQL triple-quoted strings embed the inner query without escaping.
    mutation = (
        'mutation { bulkOperationRunQuery(query: """'
        + query.strip()
        + '""") { bulkOperation { id status } userErrors { field message } } }'
    )

    result = _graphql(graphql_url, headers, mutation)
    bulk_response = result.get("bulkOperationRunQuery", {})

    user_errors = bulk_response.get("userErrors", [])
    if user_errors:
        messages = [e.get("message", str(e)) for e in user_errors]
        raise BulkOperationError(f"Shopify rejected the bulk operation: {'; '.join(messages)}")

    operation = bulk_response.get("bulkOperation")
    if not operation or not operation.get("id"):
        raise BulkOperationError("No bulk operation returned. Check your access token and shop domain.")

    operation_id = operation["id"]
    logger.info("Bulk operation created: %s (status: %s)", operation_id, operation.get("status"))

    # 3. Poll for completion
    start = time.monotonic()
    poll_query = """
    {
      currentBulkOperation {
        id
        status
        objectCount
        fileSize
        url
        errorCode
      }
    }
    """

    while True:
        elapsed = time.monotonic() - start
        if elapsed > max_wait:
            raise BulkOperationError(
                f"Bulk operation did not complete within {max_wait:.0f}s. "
                f"The operation may still be running on Shopify's side. "
                f"Try increasing --max-wait or check your Shopify admin."
            )

        time.sleep(poll_interval)

        poll_result = _graphql(graphql_url, headers, poll_query)
        current = poll_result.get("currentBulkOperation")
        if not current:
            logger.debug("No currentBulkOperation in response, retrying")
            continue

        status = current.get("status", "UNKNOWN")
        object_count = current.get("objectCount", "?")
        file_size = current.get("fileSize")

        logger.info(
            "Status: %s | Objects: %s | File size: %s | Elapsed: %.0fs",
            status,
            object_count,
            _fmt_bytes(file_size),
            elapsed,
        )

        if status == "COMPLETED":
            download_url = current.get("url")
            if not download_url:
                raise BulkOperationError("Bulk operation completed but no download URL returned.")
            break

        if status in ("FAILED", "CANCELED", "EXPIRED"):
            error_code = current.get("errorCode", "unknown")
            raise BulkOperationError(f"Bulk operation {status}: error code {error_code}")

    # 4. Download the result
    logger.info("Downloading result to %s", output_path)
    bytes_written = _download(download_url, output_path)
    logger.info("Downloaded %s", _fmt_bytes(bytes_written))
    return bytes_written


def _graphql(url: str, headers: dict[str, str], query: str) -> dict[str, Any]:
    """Execute a GraphQL request and return the data payload."""
    body = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")[:500]
        raise BulkOperationError(f"GraphQL request failed ({e.code}): {error_body}") from e
    except urllib.error.URLError as e:
        raise BulkOperationError(f"Cannot reach {url}: {e.reason}") from e

    if raw.get("errors"):
        messages = [e.get("message", str(e)) for e in raw["errors"]]
        raise BulkOperationError(f"GraphQL errors: {'; '.join(messages)}")

    return raw.get("data", {})


def _download(url: str, output_path: Path) -> int:
    """Download a URL to a local file with retry. Returns bytes written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    retries = [2.0, 8.0, 30.0]
    last_error: Exception | None = None

    for attempt in range(len(retries) + 1):
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=120) as resp:
                with open(output_path, "wb") as f:
                    total = 0
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        total += len(chunk)
            return total
        except Exception as e:
            last_error = e
            if attempt < len(retries):
                delay = retries[attempt]
                logger.warning("Download attempt %d failed (%s), retrying in %.0fs", attempt + 1, e, delay)
                time.sleep(delay)

    raise BulkOperationError(f"Download failed after {len(retries) + 1} attempts: {last_error}")


def _fmt_bytes(n: Any) -> str:
    if n is None:
        return "unknown"
    try:
        size = int(n)
    except (ValueError, TypeError):
        return str(n)
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.1f} MB"
