"""
Shopify JSONL Parser.

Stream parser for Shopify bulk operation JSONL results. Handles parent-child
relationships via __parentId, normalizes heterogeneous GraphQL response shapes
into typed dataclass nodes, and skips malformed lines without stopping the stream.

Ported from `SnowPipe/src/server/plugins/sources/shopify/bulk/parsers/jsonl-parser.ts`.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import Any

from shopify_jsonl.types import (
    BulkCollectionNode,
    BulkDataNode,
    BulkImageNode,
    BulkInventoryLevelNode,
    BulkMetafieldNode,
    BulkProductNode,
    BulkVariantNode,
    FeaturedImage,
    InventoryItem,
    InventoryItemMeasurement,
    InventoryItemWeight,
    InventoryQuantity,
    Location,
    ParsedBulkData,
    ProductCategory,
    ProductStatus,
    ProductsCount,
    SelectedOption,
    Seo,
    VariantImage,
)

logger = logging.getLogger(__name__)

_GID_TAIL = re.compile(r"/(\d+)$")


def parse_jsonl_stream(lines: Iterable[str]) -> Iterator[BulkDataNode]:
    """
    Parse an iterable of JSONL text lines into a stream of typed nodes.

    Malformed or unrecognized lines are silently skipped (logged at DEBUG).
    Consumers should treat this as a best-effort stream: a single bad line
    never aborts parsing.

    Typical usage:
        with open("export.jsonl", encoding="utf-8") as f:
            for node in parse_jsonl_stream(f):
                ...
    """
    for line in lines:
        trimmed = line.strip()
        if not trimmed:
            continue
        node = parse_jsonl_line(trimmed)
        if node is not None:
            yield node


def parse_jsonl_line(line: str) -> BulkDataNode | None:
    """Parse a single JSONL line into a BulkDataNode, or None if unrecognized."""
    try:
        parsed: dict[str, Any] = json.loads(line)
    except json.JSONDecodeError:
        logger.debug("Failed to parse JSONL line: %s", line[:100])
        return None

    typename = parsed.get("__typename")

    # --- Explicit typename paths -------------------------------------------------
    if typename == "Product":
        return _normalize_product(parsed)
    if typename == "ProductVariant":
        return _normalize_variant(parsed)
    if typename == "InventoryLevel":
        return _normalize_inventory_level(parsed)
    if typename == "Image":
        return _normalize_image(parsed)
    if typename == "Collection":
        return _normalize_collection(parsed)
    if typename == "Metafield":
        return _normalize_metafield(parsed)

    # --- Structural inference (older exports may lack __typename) ----------------
    parent_id = parsed.get("__parentId")

    # Root product: has id, title, legacyResourceId, no parent, no sortOrder
    if (
        parsed.get("legacyResourceId")
        and parsed.get("title")
        and not parent_id
        and "sortOrder" not in parsed
    ):
        return _normalize_product(parsed)

    # Variant child: has SKU or price and a parent
    if parent_id and (
        parsed.get("price") is not None or parsed.get("inventoryQuantity") is not None
    ):
        return _normalize_variant(parsed)

    # InventoryLevel: legacy `available` + location, OR new `quantities` + location
    if parsed.get("location") and (
        parsed.get("available") is not None or parsed.get("quantities") is not None
    ):
        return _normalize_inventory_level(parsed)

    # Image child
    if parent_id and parsed.get("url") and "altText" in parsed and "price" not in parsed:
        return _normalize_image(parsed)

    # Collection (has productsCount and sortOrder)
    if "sortOrder" in parsed and "productsCount" in parsed:
        return _normalize_collection(parsed)

    # Metafield (namespace + key + value)
    if (
        parsed.get("namespace") is not None
        and parsed.get("key") is not None
        and parsed.get("value") is not None
    ):
        return _normalize_metafield(parsed)

    # Last-ditch inference for untyped child nodes
    if parsed.get("id") and parent_id:
        if parsed.get("price") is not None:
            return _normalize_variant(parsed)
        if parsed.get("available") is not None or parsed.get("quantities") is not None:
            return _normalize_inventory_level(parsed)
        if parsed.get("namespace") is not None and parsed.get("key") is not None:
            return _normalize_metafield(parsed)

    # API 2024-04+ InventoryLevel sometimes lacks id and typename entirely
    if parent_id and parsed.get("quantities") is not None and parsed.get("location"):
        return _normalize_inventory_level(parsed)

    return None


def build_parsed_data(lines: Iterable[str]) -> ParsedBulkData:
    """
    Read an entire JSONL export into an in-memory grouped structure.

    Useful for batch operations where you need all nodes available at once.
    For large files, prefer streaming via `parse_jsonl_stream` + the expander.
    """
    data = ParsedBulkData()
    for node in parse_jsonl_stream(lines):
        match node:
            case BulkProductNode():
                data.products[node.id] = node
            case BulkVariantNode():
                data.variants.setdefault(node.parent_id, []).append(node)
                # Note: inventory item to variant mapping is captured at expand time,
                # not here, since ParsedBulkData is a passive snapshot.
            case BulkInventoryLevelNode():
                data.inventory_levels.setdefault(node.parent_id, []).append(node)
            case BulkCollectionNode():
                data.collections[node.id] = node
            case BulkMetafieldNode():
                data.metafields.setdefault(node.parent_id, []).append(node)
            case BulkImageNode():
                # Images are consumed by the streaming expander, not stored in batch mode
                pass
    return data


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------


def _normalize_product(parsed: dict[str, Any]) -> BulkProductNode:
    return BulkProductNode(
        typename="Product",
        id=parsed["id"],
        legacy_resource_id=parsed.get("legacyResourceId") or _extract_id_from_gid(parsed["id"]),
        title=parsed.get("title", ""),
        handle=parsed.get("handle", ""),
        vendor=parsed.get("vendor") or "",
        product_type=parsed.get("productType") or "",
        status=_normalize_status(parsed.get("status", "ACTIVE")),
        tags=_normalize_tags_to_list(parsed.get("tags")),
        created_at=parsed.get("createdAt", ""),
        updated_at=parsed.get("updatedAt", ""),
        description_html=parsed.get("descriptionHtml"),
        online_store_url=parsed.get("onlineStoreUrl"),
        featured_image=_build_featured_image(parsed.get("featuredImage")),
        category=_build_category(parsed.get("category")),
        seo=_build_seo(parsed.get("seo")),
        published_at=parsed.get("publishedAt"),
        total_inventory=parsed.get("totalInventory"),
    )


def _normalize_variant(parsed: dict[str, Any]) -> BulkVariantNode:
    return BulkVariantNode(
        typename="ProductVariant",
        id=parsed["id"],
        legacy_resource_id=parsed.get("legacyResourceId") or _extract_id_from_gid(parsed["id"]),
        title=parsed.get("title") or "Default",
        sku=parsed.get("sku"),
        price=_as_str(parsed.get("price"), default="0.00"),
        compare_at_price=_as_str(parsed.get("compareAtPrice"), default=None),
        barcode=parsed.get("barcode"),
        inventory_quantity=parsed.get("inventoryQuantity"),
        selected_options=_build_selected_options(parsed.get("selectedOptions")),
        position=parsed.get("position"),
        taxable=parsed.get("taxable"),
        available_for_sale=parsed.get("availableForSale"),
        inventory_policy=parsed.get("inventoryPolicy"),
        created_at=parsed.get("createdAt"),
        updated_at=parsed.get("updatedAt"),
        image=_build_variant_image(parsed.get("image")),
        inventory_item=_build_inventory_item(parsed.get("inventoryItem")),
        parent_id=parsed["__parentId"],
    )


def _normalize_image(parsed: dict[str, Any]) -> BulkImageNode:
    return BulkImageNode(
        typename="Image",
        id=parsed.get("id") or "",
        url=parsed["url"],
        alt_text=parsed.get("altText"),
        parent_id=parsed["__parentId"],
    )


def _normalize_inventory_level(parsed: dict[str, Any]) -> BulkInventoryLevelNode:
    location_raw = parsed.get("location") or {}
    location = Location(
        id=location_raw.get("id", ""),
        name=location_raw.get("name") or "Unknown Location",
    )

    quantities_raw = parsed.get("quantities")
    if isinstance(quantities_raw, list):
        quantities = [
            InventoryQuantity(name=q.get("name", ""), quantity=q.get("quantity", 0) or 0)
            for q in quantities_raw
        ]
        return BulkInventoryLevelNode(
            typename="InventoryLevel",
            location=location,
            parent_id=parsed["__parentId"],
            quantities=quantities,
        )

    return BulkInventoryLevelNode(
        typename="InventoryLevel",
        location=location,
        parent_id=parsed["__parentId"],
        available=parsed.get("available") or 0,
    )


def _normalize_collection(parsed: dict[str, Any]) -> BulkCollectionNode:
    products_count_raw = parsed.get("productsCount") or {"count": 0}
    return BulkCollectionNode(
        typename="Collection",
        id=parsed["id"],
        legacy_resource_id=parsed.get("legacyResourceId") or _extract_id_from_gid(parsed["id"]),
        title=parsed.get("title", ""),
        handle=parsed.get("handle", ""),
        sort_order=parsed.get("sortOrder") or "BEST_SELLING",
        updated_at=parsed.get("updatedAt", ""),
        description_html=parsed.get("descriptionHtml"),
        template_suffix=parsed.get("templateSuffix"),
        image=_build_featured_image(parsed.get("image")),
        seo=_build_seo(parsed.get("seo")),
        products_count=ProductsCount(count=products_count_raw.get("count", 0) or 0),
    )


def _normalize_metafield(parsed: dict[str, Any]) -> BulkMetafieldNode:
    return BulkMetafieldNode(
        typename="Metafield",
        id=parsed["id"],
        namespace=parsed["namespace"],
        key=parsed["key"],
        value=parsed["value"],
        type=parsed.get("type", ""),
        created_at=parsed.get("createdAt", ""),
        updated_at=parsed.get("updatedAt", ""),
        description=parsed.get("description"),
        parent_id=parsed["__parentId"],
    )


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _extract_id_from_gid(gid: str) -> str:
    match = _GID_TAIL.search(gid or "")
    return match.group(1) if match else gid


def _normalize_status(status: Any) -> ProductStatus:
    upper = str(status or "ACTIVE").upper()
    if upper in ("ACTIVE", "ARCHIVED", "DRAFT"):
        return upper  # type: ignore[return-value]
    return "ACTIVE"


def _normalize_tags_to_list(tags: Any) -> list[str]:
    if isinstance(tags, list):
        return [str(t) for t in tags if t]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    return []


def _as_str(value: Any, *, default: str | None) -> str | None:
    if value is None:
        return default
    return str(value)


def _build_featured_image(raw: dict[str, Any] | None) -> FeaturedImage | None:
    if not raw or not raw.get("url"):
        return None
    return FeaturedImage(url=raw["url"])


def _build_category(raw: dict[str, Any] | None) -> ProductCategory | None:
    if not raw or not raw.get("fullName"):
        return None
    return ProductCategory(full_name=raw["fullName"])


def _build_seo(raw: dict[str, Any] | None) -> Seo | None:
    if not raw:
        return None
    return Seo(title=raw.get("title"), description=raw.get("description"))


def _build_selected_options(raw: Any) -> list[SelectedOption] | None:
    if not isinstance(raw, list):
        return None
    return [
        SelectedOption(name=o.get("name", ""), value=o.get("value", ""))
        for o in raw
        if isinstance(o, dict)
    ]


def _build_variant_image(raw: dict[str, Any] | None) -> VariantImage | None:
    if not raw or not raw.get("url"):
        return None
    return VariantImage(url=raw["url"], alt_text=raw.get("altText"))


def _build_inventory_item(raw: dict[str, Any] | None) -> InventoryItem | None:
    if not raw or not raw.get("id"):
        return None
    measurement = None
    measurement_raw = raw.get("measurement")
    if isinstance(measurement_raw, dict):
        weight_raw = measurement_raw.get("weight")
        weight = None
        if isinstance(weight_raw, dict) and "value" in weight_raw and "unit" in weight_raw:
            weight = InventoryItemWeight(
                value=float(weight_raw["value"]),
                unit=str(weight_raw["unit"]),
            )
        measurement = InventoryItemMeasurement(weight=weight)
    return InventoryItem(id=raw["id"], measurement=measurement)
