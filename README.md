# shopify-bulk

[![PyPI version](https://img.shields.io/pypi/v/shopify-bulk)](https://pypi.org/project/shopify-bulk/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-27%20passed-brightgreen)]()

A Python CLI and library for processing Shopify bulk operation JSONL exports. Handles the full lifecycle: trigger a bulk operation, poll for completion, download the result, parse the nested parent/child JSONL, and flatten it into clean CSV, JSON, or JSONL.

## Why this exists

Shopify's [Bulk Operations API](https://shopify.dev/docs/api/usage/bulk-operations/queries) returns data as JSONL where child records (variants, inventory levels, images) reference parents via `__parentId`. Every developer who uses bulk operations has to write custom code to reassemble this tree. There is no reusable library for it in Python, and Shopify's own Ruby SDK has had an [open issue requesting this utility since 2023](https://github.com/Shopify/shopify-api-ruby/issues/729).

This tool fills that gap.

## Install

```bash
pip install shopify-bulk
```

Or run directly from source:

```bash
git clone https://github.com/snowthen-o7/shopify-bulk.git
cd shopify-bulk
pip install -e .
```

## Quick start

### Fetch + process (full workflow)

```bash
# 1. Trigger a bulk operation on Shopify, poll until done, download the JSONL
shopify-bulk fetch --shop mystore.myshopify.com --token shpat_xxxxx -o export.jsonl

# 2. Parse the JSONL and flatten to CSV
shopify-bulk process export.jsonl -o catalog.csv
```

### Process an existing JSONL file

```bash
# CSV output (default)
shopify-bulk process export.jsonl -o products.csv

# JSON output
shopify-bulk process export.jsonl -f json -o products.json

# Use a preset config for curated field selection
shopify-bulk process export.jsonl -c products -o catalog.csv
shopify-bulk process export.jsonl -c inventory -o stock.csv

# Select specific fields only
shopify-bulk process export.jsonl --fields title,handle,variant_sku,variant_price,inventory_total
```

### Use as a Python library

```python
from shopify_jsonl.parser import parse_jsonl_stream
from shopify_jsonl.expander import expand_products

with open("export.jsonl") as f:
    for row in expand_products(parse_jsonl_stream(f)):
        print(row["title"], row.get("variant_sku"), row.get("inventory_total"))
```

## What it handles

- **Streaming parser.** Processes JSONL line by line. Never loads the full file into memory. Handles 50K+ product catalogs in seconds with constant memory usage.
- **Parent/child assembly.** Products, variants, inventory levels, and images are buffered one product at a time and flattened into output rows. The `__parentId` reassembly that everyone writes from scratch is built in.
- **Dynamic option columns.** Shopify's `selectedOptions` (Size, Color, Material, or whatever a store uses) become their own columns automatically.
- **Inventory aggregation by location.** Each warehouse/location gets its own column plus a total.
- **Both inventory API formats.** Legacy `available` field (pre-2024-04) and modern `quantities` array (2024-04+) are normalized transparently.
- **Image fallback.** Missing variant images fall back to the product's featured image, so every row has a usable image URL.
- **Structural inference.** Handles exports that lack `__typename` by inferring node types from their fields. Works with both modern and older Shopify API versions.
- **Preset configs.** Built-in YAML presets for common use cases (products, inventory). Write your own for custom field selection.

## Why not just use `jsonlines`?

The `jsonlines` Python package reads JSONL, but it gives you flat dictionaries with no awareness of Shopify's parent/child structure. You still have to:

1. Detect whether a line is a Product, Variant, InventoryLevel, or Image
2. Buffer children until the parent product completes
3. Assemble variants under their parent product
4. Aggregate inventory levels by location per variant
5. Extract dynamic option names into columns
6. Handle the two different inventory quantity formats
7. Fall back to parent images when variants have none

That is what this tool does. `jsonlines` is step 0. This tool is steps 1 through 7.

## Commands

### `shopify-bulk fetch`

Trigger a Shopify bulk operation, poll until done, download the result JSONL.

```
Options:
  --shop TEXT             Shopify store domain (required)
  --token TEXT            Admin API access token, shpat_... (required)
  -o, --output PATH      Output JSONL path (default: export.jsonl)
  --no-inventory         Skip inventory levels (faster for large catalogs)
  --api-version TEXT     Shopify API version (default: 2026-01)
  --max-wait FLOAT       Max seconds to wait (default: 1200)
  --poll-interval FLOAT  Seconds between status polls (default: 5)
  -v, --verbose          Debug logging
```

### `shopify-bulk process`

Parse a local JSONL file into CSV, JSON, or JSONL.

```
Options:
  -o, --output PATH              Output file path (default: stdout)
  -f, --format [csv|json|jsonl]  Output format (default: csv)
  -c, --config NAME              Preset config or path to .yaml
  --no-variants                  One row per product instead of per variant
  --no-inventory                 Skip per-location inventory breakdown
  --fields TEXT                  Comma-separated fields (overrides config)
  -v, --verbose                  Debug logging
```

### `shopify-bulk configs`

List available preset configs.

## Output fields

Each output row includes:

**Product-level:** id, title, body_html, vendor, product_type, handle, status, tags, image_url, product_url, product_category, seo_title, seo_description, published_at, total_inventory, additional_image_links, created_at, updated_at

**Variant-level:** variant_id, variant_title, variant_sku, variant_barcode, variant_price, compare_at_price, variant_image_url, weight, weight_unit, variant_position, variant_taxable, variant_available_for_sale, variant_inventory_policy

**Dynamic options:** size, color, material (or whatever option names the store uses)

**Inventory:** inventory_total, variant_inventory_quantity, inventory_location_{name}

## Getting a Shopify access token

The `fetch` command needs a Shopify Admin API access token (`shpat_...`):

1. In your Shopify admin, go to **Settings > Apps and sales channels > Develop apps**
2. Create a custom app
3. Under **Admin API access scopes**, enable `read_products` and `read_inventory`
4. Install the app and copy the **Admin API access token**

The `process` command works entirely offline and does not need a token.

## Contributing

Issues and pull requests welcome. To set up a dev environment:

```bash
git clone https://github.com/snowthen-o7/shopify-bulk.git
cd shopify-bulk
pip install -e ".[dev]"
pytest
```

## License

MIT. See [LICENSE](LICENSE).

---

Built by [SnowForge](https://snowforge.dev). For automated feed pipelines with scheduling, transforms, and multi-destination pushes, see [SnowPipe](https://pipe.snowforge.dev).
