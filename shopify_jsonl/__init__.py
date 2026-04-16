"""
shopify_jsonl: Process Shopify bulk operation JSONL exports.

Ported from the SnowPipe Shopify bulk parser (TypeScript).
"""

__version__ = "0.1.0"

from shopify_jsonl.parser import parse_jsonl_stream, parse_jsonl_line
from shopify_jsonl.types import (
    BulkProductNode,
    BulkVariantNode,
    BulkInventoryLevelNode,
    BulkImageNode,
    BulkCollectionNode,
    BulkMetafieldNode,
    InventoryQuantity,
    BulkDataNode,
    ParsedBulkData,
)

__all__ = [
    "__version__",
    "parse_jsonl_stream",
    "parse_jsonl_line",
    "BulkProductNode",
    "BulkVariantNode",
    "BulkInventoryLevelNode",
    "BulkImageNode",
    "BulkCollectionNode",
    "BulkMetafieldNode",
    "InventoryQuantity",
    "BulkDataNode",
    "ParsedBulkData",
]
