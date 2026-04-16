"""
Tests for shopify_jsonl.expander.

Uses the same fixture as test_parser.py. Validates that the expander:
- Produces correct flat rows from a parsed node stream
- Handles products with/without variants
- Extracts dynamic option columns (Size, Color)
- Aggregates inventory by location
- Falls back to product image when variant has none
- Builds additional_image_links from buffered images
"""

from __future__ import annotations

from pathlib import Path

import pytest

from shopify_jsonl.expander import ExpansionOptions, expand_products
from shopify_jsonl.parser import parse_jsonl_stream

FIXTURE = Path(__file__).parent / "fixtures" / "sample_products.jsonl"


def _expand(
    expand_variants: bool = True,
    include_inventory: bool = True,
) -> list[dict]:
    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    nodes = parse_jsonl_stream(lines)
    options = ExpansionOptions(
        expand_variants=expand_variants,
        include_inventory=include_inventory,
    )
    return list(expand_products(nodes, options))


def test_row_count_with_variants_expanded() -> None:
    rows = _expand()
    # Product 111 has 2 variants, product 222 has 1 variant = 3 rows
    assert len(rows) == 3


def test_row_count_without_variant_expansion() -> None:
    rows = _expand(expand_variants=False)
    # 1 row per product = 2 rows
    assert len(rows) == 2


def test_product_level_fields() -> None:
    rows = _expand()
    row = rows[0]  # first variant of Snowboard
    assert row["title"] == "Snowboard"
    assert row["body_html"] == "<p>Cool board</p>"
    assert row["vendor"] == "Snowline"
    assert row["handle"] == "snowboard"
    assert row["product_type"] == "Apparel"
    assert row["status"] == "active"
    assert row["tags"] == "winter, board"
    assert row["product_url"] == "https://example.com/products/snowboard"
    assert row["product_category"] == "Sporting Goods > Winter Sports"
    assert row["seo_title"] == "Snowboard"
    assert row["total_inventory"] == 50
    assert row["product_id"].endswith("/111")
    assert row["product_legacy_id"] == "111"


def test_variant_level_fields() -> None:
    rows = _expand()
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    assert red["variant_price"] == "199.99"
    assert red["compare_at_price"] == "249.99"
    assert red["variant_barcode"] == "012345678905"
    assert red["variant_id"] == 1111
    assert red["variant_title"] == "Large / Red"
    assert red["variant_position"] == 1
    assert red["variant_taxable"] is True
    assert red["variant_available_for_sale"] is True
    assert red["variant_inventory_policy"] == "deny"


def test_id_is_variant_legacy_id() -> None:
    rows = _expand()
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    assert red["id"] == "1111"


def test_dynamic_option_columns() -> None:
    rows = _expand()
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    assert red["size"] == "Large"
    assert red["color"] == "Red"

    blue = next(r for r in rows if r.get("variant_sku") == "SNB-LG-BLU")
    assert blue["size"] == "Large"
    assert blue["color"] == "Blue"


def test_weight_extraction_from_inventory_item() -> None:
    rows = _expand()
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    assert red["weight"] == "2.5"
    assert red["weight_unit"] == "kilograms"


def test_inventory_aggregation_by_location() -> None:
    rows = _expand(include_inventory=True)
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    # Main Warehouse: available=15, Secondary: available=15 → total=30
    assert red["inventory_total"] == 30
    assert red["inventory_location_main_warehouse"] == 15
    assert red["inventory_location_secondary"] == 15
    assert red["variant_inventory_quantity"] == 30


def test_inventory_disabled_falls_back_to_variant_quantity() -> None:
    rows = _expand(include_inventory=False)
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    # With inventory disabled, should use the variant-level inventoryQuantity (30)
    assert red.get("inventory_total") == 30
    assert red.get("variant_inventory_quantity") == 30
    # No per-location keys
    assert "inventory_location_main_warehouse" not in red


def test_featured_image_fallback() -> None:
    rows = _expand()
    # Snowboard has a featured image set on the product
    red = next(r for r in rows if r.get("variant_sku") == "SNB-LG-RED")
    assert red["image_url"] == "https://cdn.example.com/snowboard.jpg"
    # Red variant has its own image
    assert red["variant_image_url"] == "https://cdn.example.com/snowboard-red.jpg"
    # Blue variant has NO variant image, falls back to featured
    blue = next(r for r in rows if r.get("variant_sku") == "SNB-LG-BLU")
    assert blue["variant_image_url"] == "https://cdn.example.com/snowboard.jpg"


def test_additional_image_links() -> None:
    rows = _expand()
    row = rows[0]
    # The fixture has 1 product-level image child (extra1.jpg). Featured is snowboard.jpg.
    # So additional should be extra1.jpg
    assert row["additional_image_links"] == "https://cdn.example.com/snowboard-extra1.jpg"


def test_gift_card_product_with_null_inventory() -> None:
    rows = _expand()
    gift = next(r for r in rows if r["title"] == "Gift Card")
    assert gift["variant_price"] == "25.00"
    assert gift["variant_sku"] is None
    assert gift.get("variant_inventory_quantity") is None
    assert gift["total_inventory"] is None


def test_product_without_variants() -> None:
    # Simulate a product with no variant children
    from shopify_jsonl.types import BulkProductNode

    product = BulkProductNode(
        typename="Product",
        id="gid://shopify/Product/999",
        legacy_resource_id="999",
        title="Solo Product",
        handle="solo",
        vendor="Test",
        product_type="Test",
        status="ACTIVE",
        tags=[],
        created_at="2026-01-01",
        updated_at="2026-01-01",
    )
    rows = list(expand_products([product]))
    assert len(rows) == 1
    assert rows[0]["title"] == "Solo Product"
    assert rows[0]["id"] == "999"
    assert "variant_id" not in rows[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
