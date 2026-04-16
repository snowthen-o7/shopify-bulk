"""
Streaming Row Expander for Shopify Bulk JSONL.

Buffers ONE product and its children at a time, then yields flattened
dict rows before moving to the next product. Memory usage is O(1 product)
regardless of catalog size.

Shopify bulk JSONL arrives parent-first:
  Product A
    Variant A1
      InventoryLevel A1-L1
      InventoryLevel A1-L2
    Variant A2
      Image (variant-specific)
    Image (product-level)
  Product B
    ...

Ported from `SnowPipe/src/server/plugins/sources/shopify/bulk/parsers/streaming-expander.ts`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

from shopify_jsonl.types import (
    BulkDataNode,
    BulkImageNode,
    BulkInventoryLevelNode,
    BulkProductNode,
    BulkVariantNode,
)

_GID_TAIL = re.compile(r"/(\d+)$")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class ExpansionOptions:
    expand_variants: bool = True
    include_inventory: bool = True


@dataclass
class _BufferedProduct:
    product: BulkProductNode
    variants: list[BulkVariantNode] = field(default_factory=list)
    inventory_levels: dict[str, list[BulkInventoryLevelNode]] = field(default_factory=dict)
    images: list[BulkImageNode] = field(default_factory=list)


def expand_products(
    nodes: Iterable[BulkDataNode],
    options: ExpansionOptions | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Stream-expand parsed JSONL nodes into flat product rows.

    Yields one ``dict[str, Any]`` per variant (or per product when
    ``expand_variants=False``). Each dict has a stable set of keys for the
    product-level fields plus variant/inventory fields when present.

    Typical usage::

        from shopify_jsonl.parser import parse_jsonl_stream
        from shopify_jsonl.expander import expand_products

        with open("export.jsonl") as f:
            for row in expand_products(parse_jsonl_stream(f)):
                print(row["title"], row.get("variant_sku"))
    """
    if options is None:
        options = ExpansionOptions()

    current: _BufferedProduct | None = None

    for node in nodes:
        match node:
            case BulkProductNode(parent_id=None):
                if current is not None:
                    yield from _flush(current, options)
                current = _BufferedProduct(product=node)

            case BulkVariantNode() if current is not None:
                current.variants.append(node)

            case BulkImageNode() if current is not None:
                current.images.append(node)

            case BulkInventoryLevelNode() if current is not None:
                current.inventory_levels.setdefault(node.parent_id, []).append(node)

    if current is not None:
        yield from _flush(current, options)


def _flush(
    buf: _BufferedProduct,
    options: ExpansionOptions,
) -> Iterator[dict[str, Any]]:
    if options.expand_variants and buf.variants:
        for variant in buf.variants:
            yield _build_row(buf, variant, options)
    elif buf.variants:
        yield _build_row(buf, buf.variants[0], options)
    else:
        yield _build_row(buf, None, options)


def _build_row(
    buf: _BufferedProduct,
    variant: BulkVariantNode | None,
    options: ExpansionOptions,
) -> dict[str, Any]:
    product = buf.product

    featured_url = (
        (product.featured_image.url if product.featured_image else None)
        or (buf.images[0].url if buf.images else None)
    )
    additional_images = [
        img.url for img in buf.images if img.url != featured_url
    ]

    row: dict[str, Any] = {
        "id": (
            (variant.legacy_resource_id or _extract_id(variant.id))
            if variant
            else (product.legacy_resource_id or _extract_id(product.id))
        ),
        "title": product.title,
        "body_html": product.description_html or "",
        "vendor": product.vendor,
        "product_type": product.product_type,
        "handle": product.handle,
        "status": product.status.lower(),
        "tags": ", ".join(product.tags),
        "created_at": product.created_at,
        "updated_at": product.updated_at,
        "image_url": featured_url,
        "product_url": product.online_store_url,
        "product_category": product.category.full_name if product.category else None,
        "seo_title": product.seo.title if product.seo else None,
        "seo_description": product.seo.description if product.seo else None,
        "published_at": product.published_at,
        "total_inventory": product.total_inventory,
        "additional_image_links": ",".join(additional_images) if additional_images else None,
        "product_id": product.id,
        "product_legacy_id": product.legacy_resource_id,
    }

    if variant is None:
        return row

    variant_legacy_id = variant.legacy_resource_id or _extract_id(variant.id)
    try:
        row["variant_id"] = int(variant_legacy_id)
    except ValueError:
        row["variant_id"] = variant_legacy_id

    row["variant_title"] = variant.title
    row["variant_price"] = variant.price
    row["variant_sku"] = variant.sku or None
    row["variant_barcode"] = variant.barcode
    row["compare_at_price"] = variant.compare_at_price

    if variant.selected_options:
        for opt in variant.selected_options:
            key = _sanitize_name(opt.name)
            row[key] = opt.value

    measurement = (
        variant.inventory_item.measurement.weight
        if variant.inventory_item
        and variant.inventory_item.measurement
        and variant.inventory_item.measurement.weight
        else None
    )
    if measurement and measurement.value is not None:
        row["weight"] = str(measurement.value)
    if measurement and measurement.unit:
        row["weight_unit"] = measurement.unit.lower()

    row["variant_position"] = variant.position
    row["variant_taxable"] = variant.taxable
    row["variant_available_for_sale"] = variant.available_for_sale
    row["variant_inventory_policy"] = (
        variant.inventory_policy.lower() if variant.inventory_policy else None
    )
    row["variant_created_at"] = variant.created_at
    row["variant_updated_at"] = variant.updated_at
    row["variant_image_url"] = (
        variant.image.url if variant.image else featured_url
    )

    if options.include_inventory and variant.inventory_item and variant.inventory_item.id:
        levels = buf.inventory_levels.get(variant.inventory_item.id, [])
        total = 0
        for level in levels:
            qty = _get_available_quantity(level)
            total += qty
            loc_key = _sanitize_name(level.location.name)
            row[f"inventory_location_{loc_key}"] = qty
        row["inventory_total"] = total
        row["inventory_item_id"] = variant.inventory_item.id
        row["variant_inventory_quantity"] = total
    elif variant.inventory_quantity is not None:
        row["variant_inventory_quantity"] = variant.inventory_quantity
        row["inventory_total"] = variant.inventory_quantity

    return row


def _extract_id(gid: str) -> str:
    match = _GID_TAIL.search(gid or "")
    return match.group(1) if match else gid


def _sanitize_name(name: str) -> str:
    return _NON_ALNUM.sub("_", name.lower()).strip("_")


def _get_available_quantity(level: BulkInventoryLevelNode) -> int:
    if level.quantities:
        for q in level.quantities:
            if q.name == "available":
                return q.quantity
        return 0
    return level.available or 0
