"""
Type definitions for Shopify bulk operation JSONL nodes.

Mirrors the TypeScript definitions in SnowPipe's `bulk-types.ts`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

ProductStatus = Literal["ACTIVE", "ARCHIVED", "DRAFT"]


@dataclass(slots=True)
class FeaturedImage:
    url: str


@dataclass(slots=True)
class ProductCategory:
    full_name: str


@dataclass(slots=True)
class Seo:
    title: str | None = None
    description: str | None = None


@dataclass(slots=True)
class SelectedOption:
    name: str
    value: str


@dataclass(slots=True)
class VariantImage:
    url: str
    alt_text: str | None = None


@dataclass(slots=True)
class InventoryItemWeight:
    value: float
    unit: str


@dataclass(slots=True)
class InventoryItemMeasurement:
    weight: InventoryItemWeight | None = None


@dataclass(slots=True)
class InventoryItem:
    id: str
    measurement: InventoryItemMeasurement | None = None


@dataclass(slots=True)
class Location:
    id: str
    name: str


@dataclass(slots=True)
class InventoryQuantity:
    """One entry in the `quantities` array (Shopify API 2024-04+)."""

    name: str  # 'available', 'on_hand', 'committed', 'incoming'
    quantity: int


@dataclass(slots=True)
class BulkProductNode:
    """Root-level product node from a bulk operation export."""

    typename: Literal["Product"]
    id: str
    legacy_resource_id: str
    title: str
    handle: str
    vendor: str
    product_type: str
    status: ProductStatus
    tags: list[str]
    created_at: str
    updated_at: str
    description_html: str | None = None
    online_store_url: str | None = None
    featured_image: FeaturedImage | None = None
    category: ProductCategory | None = None
    seo: Seo | None = None
    published_at: str | None = None
    total_inventory: int | None = None
    parent_id: None = None  # products have no parent


@dataclass(slots=True)
class BulkVariantNode:
    """ProductVariant child node. `parent_id` is the product GID."""

    typename: Literal["ProductVariant"]
    id: str
    legacy_resource_id: str
    title: str
    price: str
    parent_id: str
    sku: str | None = None
    compare_at_price: str | None = None
    barcode: str | None = None
    inventory_quantity: int | None = None
    selected_options: list[SelectedOption] | None = None
    position: int | None = None
    taxable: bool | None = None
    available_for_sale: bool | None = None
    inventory_policy: Literal["DENY", "CONTINUE"] | None = None
    created_at: str | None = None
    updated_at: str | None = None
    image: VariantImage | None = None
    inventory_item: InventoryItem | None = None


@dataclass(slots=True)
class BulkInventoryLevelNode:
    """
    InventoryLevel child node. `parent_id` is the InventoryItem GID.

    Shopify API 2024-04+ uses the `quantities` list. Pre-2024-04 uses `available`.
    This dataclass can hold either form; exactly one should be populated.
    """

    typename: Literal["InventoryLevel"]
    location: Location
    parent_id: str
    available: int | None = None
    quantities: list[InventoryQuantity] | None = None


@dataclass(slots=True)
class BulkImageNode:
    """Image child node. `parent_id` is the product GID."""

    typename: Literal["Image"]
    id: str
    url: str
    parent_id: str
    alt_text: str | None = None


@dataclass(slots=True)
class ProductsCount:
    count: int


@dataclass(slots=True)
class BulkCollectionNode:
    """Root-level collection node."""

    typename: Literal["Collection"]
    id: str
    legacy_resource_id: str
    title: str
    handle: str
    sort_order: str
    updated_at: str
    description_html: str | None = None
    template_suffix: str | None = None
    image: FeaturedImage | None = None
    seo: Seo | None = None
    products_count: ProductsCount = field(default_factory=lambda: ProductsCount(count=0))
    parent_id: None = None


@dataclass(slots=True)
class BulkCollectionProductNode:
    """Product-reference child under a Collection. `parent_id` is the Collection GID."""

    typename: Literal["Product"]
    id: str
    legacy_resource_id: str
    parent_id: str


@dataclass(slots=True)
class BulkMetafieldNode:
    """Metafield child node. `parent_id` is the product or variant GID."""

    typename: Literal["Metafield"]
    id: str
    namespace: str
    key: str
    value: str
    type: str
    created_at: str
    updated_at: str
    parent_id: str
    description: str | None = None


BulkDataNode = (
    BulkProductNode
    | BulkVariantNode
    | BulkInventoryLevelNode
    | BulkImageNode
    | BulkCollectionNode
    | BulkCollectionProductNode
    | BulkMetafieldNode
)


@dataclass(slots=True)
class ParsedBulkData:
    """Grouped result of reading an entire JSONL export into memory. Used by batch mode."""

    products: dict[str, BulkProductNode] = field(default_factory=dict)
    variants: dict[str, list[BulkVariantNode]] = field(default_factory=dict)
    inventory_levels: dict[str, list[BulkInventoryLevelNode]] = field(default_factory=dict)
    collections: dict[str, BulkCollectionNode] = field(default_factory=dict)
    metafields: dict[str, list[BulkMetafieldNode]] = field(default_factory=dict)
