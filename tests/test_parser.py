"""
Tests for shopify_jsonl.parser.

Uses the fixture at tests/fixtures/sample_products.jsonl which includes:
- 2 products (one with 2 variants + 1 image + 2 inventory levels + 1 metafield, one with 1 variant)
- 1 intentionally malformed line to verify graceful skipping
"""

from __future__ import annotations

from pathlib import Path

import pytest

from shopify_jsonl.parser import (
    build_parsed_data,
    parse_jsonl_line,
    parse_jsonl_stream,
)
from shopify_jsonl.types import (
    BulkCollectionNode,
    BulkImageNode,
    BulkInventoryLevelNode,
    BulkMetafieldNode,
    BulkProductNode,
    BulkVariantNode,
)


FIXTURE = Path(__file__).parent / "fixtures" / "sample_products.jsonl"


def _read_lines() -> list[str]:
    return FIXTURE.read_text(encoding="utf-8").splitlines()


def test_parse_jsonl_stream_yields_expected_node_types() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    type_counts: dict[str, int] = {}
    for node in nodes:
        type_counts[type(node).__name__] = type_counts.get(type(node).__name__, 0) + 1
    assert type_counts == {
        "BulkProductNode": 2,
        "BulkVariantNode": 3,
        "BulkImageNode": 1,
        "BulkInventoryLevelNode": 2,
        "BulkMetafieldNode": 1,
    }


def test_malformed_lines_are_skipped() -> None:
    # Fixture contains one bad line "invalid json line should be skipped gracefully"
    lines = _read_lines()
    valid_lines = [line for line in lines if line.strip()]
    assert any("invalid json line" in line for line in valid_lines), (
        "fixture should include a malformed line for this test"
    )
    # If parser didn't handle it gracefully, this would raise
    nodes = list(parse_jsonl_stream(lines))
    assert len(nodes) == 9  # 2+3+1+2+1


def test_empty_lines_are_skipped() -> None:
    lines = ["", "   ", "\n", '{"__typename":"Product","id":"x","legacyResourceId":"x","title":"t","handle":"t","vendor":"","productType":"","status":"ACTIVE","tags":[],"createdAt":"2026","updatedAt":"2026"}']
    nodes = list(parse_jsonl_stream(lines))
    assert len(nodes) == 1


def test_product_node_fields() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    product = next(n for n in nodes if isinstance(n, BulkProductNode) and n.id.endswith("/111"))
    assert product.title == "Snowboard"
    assert product.handle == "snowboard"
    assert product.vendor == "Snowline"
    assert product.status == "ACTIVE"
    assert product.tags == ["winter", "board"]
    assert product.featured_image is not None
    assert product.featured_image.url == "https://cdn.example.com/snowboard.jpg"
    assert product.category is not None
    assert product.category.full_name == "Sporting Goods > Winter Sports"
    assert product.seo is not None
    assert product.seo.title == "Snowboard"
    assert product.total_inventory == 50
    assert product.legacy_resource_id == "111"


def test_variant_node_fields() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    variants = [n for n in nodes if isinstance(n, BulkVariantNode)]
    red_variant = next(v for v in variants if v.sku == "SNB-LG-RED")
    assert red_variant.price == "199.99"
    assert red_variant.compare_at_price == "249.99"
    assert red_variant.barcode == "012345678905"
    assert red_variant.inventory_quantity == 30
    assert red_variant.parent_id.endswith("/111")
    assert red_variant.selected_options is not None
    names = [o.name for o in red_variant.selected_options]
    assert "Size" in names and "Color" in names
    assert red_variant.inventory_item is not None
    assert red_variant.inventory_item.id.endswith("/1001")
    assert red_variant.inventory_item.measurement is not None
    assert red_variant.inventory_item.measurement.weight is not None
    assert red_variant.inventory_item.measurement.weight.value == 2.5
    assert red_variant.inventory_item.measurement.weight.unit == "KILOGRAMS"


def test_inventory_level_uses_quantities_array() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    levels = [n for n in nodes if isinstance(n, BulkInventoryLevelNode)]
    main = next(lvl for lvl in levels if lvl.location.name == "Main Warehouse")
    assert main.quantities is not None
    assert main.available is None
    names = [q.name for q in main.quantities]
    assert "available" in names and "on_hand" in names


def test_image_node_has_alt_text_and_parent() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    images = [n for n in nodes if isinstance(n, BulkImageNode)]
    assert len(images) == 1
    assert images[0].url == "https://cdn.example.com/snowboard-extra1.jpg"
    assert images[0].alt_text == "Board in action"
    assert images[0].parent_id.endswith("/111")


def test_metafield_parsing() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    metafields = [n for n in nodes if isinstance(n, BulkMetafieldNode)]
    assert len(metafields) == 1
    mf = metafields[0]
    assert mf.namespace == "custom"
    assert mf.key == "fabric"
    assert mf.value == "nylon"
    assert mf.type == "single_line_text_field"
    assert mf.parent_id.endswith("/111")


def test_tags_string_to_list() -> None:
    # Gift card product has tags as a comma-separated string
    nodes = list(parse_jsonl_stream(_read_lines()))
    gift_card = next(n for n in nodes if isinstance(n, BulkProductNode) and n.title == "Gift Card")
    assert gift_card.tags == ["gifts", "extra"]


def test_variant_defaults_for_missing_fields() -> None:
    nodes = list(parse_jsonl_stream(_read_lines()))
    gift_variant = next(
        v for v in nodes if isinstance(v, BulkVariantNode) and v.title == "$25"
    )
    assert gift_variant.sku is None
    assert gift_variant.compare_at_price is None
    assert gift_variant.inventory_quantity is None
    assert gift_variant.price == "25.00"


def test_build_parsed_data_groups_by_parent() -> None:
    data = build_parsed_data(_read_lines())
    assert len(data.products) == 2
    product_ids = set(data.products.keys())
    assert any(pid.endswith("/111") for pid in product_ids)

    # Product 111 has 2 variants
    variants_by_product = {pid: len(vs) for pid, vs in data.variants.items()}
    assert any(vs_count == 2 for vs_count in variants_by_product.values())

    # Inventory item 1001 has 2 levels
    levels_by_item = {iid: len(lvls) for iid, lvls in data.inventory_levels.items()}
    assert any(lvls_count == 2 for lvls_count in levels_by_item.values())

    # Metafield count = 1
    assert sum(len(mfs) for mfs in data.metafields.values()) == 1


def test_parse_jsonl_line_returns_none_for_unrecognized() -> None:
    assert parse_jsonl_line('{"foo":"bar","not_a_node":true}') is None


def test_legacy_inventory_level_fallback() -> None:
    # API pre-2024-04 used a scalar `available` field instead of `quantities`
    line = '{"__typename":"InventoryLevel","available":42,"location":{"id":"gid://shopify/Location/1","name":"L"},"__parentId":"gid://shopify/InventoryItem/X"}'
    node = parse_jsonl_line(line)
    assert isinstance(node, BulkInventoryLevelNode)
    assert node.available == 42
    assert node.quantities is None


def test_collection_node_parsing() -> None:
    line = '{"__typename":"Collection","id":"gid://shopify/Collection/777","legacyResourceId":"777","title":"Featured","handle":"featured","descriptionHtml":null,"sortOrder":"MANUAL","templateSuffix":null,"updatedAt":"2026-01-01T00:00:00Z","image":null,"seo":null,"productsCount":{"count":17}}'
    node = parse_jsonl_line(line)
    assert isinstance(node, BulkCollectionNode)
    assert node.title == "Featured"
    assert node.handle == "featured"
    assert node.sort_order == "MANUAL"
    assert node.products_count.count == 17


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
