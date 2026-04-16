# Quickstart

## 1. Install

```bash
cd shopify-jsonl-processor
pip install .
```

Or run without installing:

```bash
python -m shopify_jsonl process YOUR_FILE.jsonl
```

## 2. Process a JSONL Export

### CSV output (default)

```bash
shopify-bulk process export.jsonl -o products.csv
```

### JSON output

```bash
shopify-bulk process export.jsonl -f json -o products.json
```

### JSONL output (streaming, for piping)

```bash
shopify-bulk process export.jsonl -f jsonl | head -5
```

## 3. Use a Preset Config

```bash
# Full product catalog (best for Google Shopping / Meta feeds)
shopify-bulk process export.jsonl -c products -o catalog.csv

# Inventory snapshot (lean, stock-focused)
shopify-bulk process export.jsonl -c inventory -o stock.csv
```

## 4. Select Specific Fields

```bash
shopify-bulk process export.jsonl --fields title,handle,variant_sku,variant_price,inventory_total
```

## 5. Collapse Variants

One row per product instead of one row per variant:

```bash
shopify-bulk process export.jsonl --no-variants
```

## 6. Skip Inventory Breakdown

Omit per-location inventory columns:

```bash
shopify-bulk process export.jsonl --no-inventory
```

## Troubleshooting

### "No module named 'click'"

Install dependencies:

```bash
pip install click pyyaml
```

### "UnicodeDecodeError"

The JSONL file may not be UTF-8. Convert it first:

```bash
iconv -f ISO-8859-1 -t UTF-8 export.jsonl > export_utf8.jsonl
shopify-bulk process export_utf8.jsonl
```

### "0 rows written"

The input file may not be a Shopify bulk operation JSONL. Check that lines contain `"id": "gid://shopify/Product/..."` and `"__parentId"` fields. If lines look like `{"handle":"...","type":"product","input":{...}}`, that is a seed-data format, not a bulk operation output.

### Memory usage

The tool processes one product at a time regardless of file size. If you see high memory usage, check that your JSONL is in parent-first order (products before their variants). Out-of-order data may cause buffering.
