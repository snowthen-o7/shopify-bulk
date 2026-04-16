"""
CLI entry point for shopify-bulk.

Usage:
    shopify-bulk fetch --shop DOMAIN --token TOKEN [options]
    shopify-bulk process INPUT [options]
    shopify-bulk configs
    shopify-bulk --help
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import click

from shopify_jsonl.configs import load_config, list_configs
from shopify_jsonl.exporter import write_csv, write_json, write_jsonl
from shopify_jsonl.expander import ExpansionOptions, expand_products
from shopify_jsonl.parser import parse_jsonl_stream


@click.group()
@click.version_option(package_name="shopify-jsonl-processor")
def main() -> None:
    """shopify-bulk: Process Shopify bulk operation JSONL exports."""


@main.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), default=None, help="Output file path (default: stdout)")
@click.option("--format", "-f", "fmt", type=click.Choice(["csv", "json", "jsonl"]), default="csv", help="Output format")
@click.option("--config", "-c", "config_name", type=str, default=None, help="Named preset config (products, inventory) or path to .yaml")
@click.option("--no-variants", is_flag=True, default=False, help="One row per product instead of per variant")
@click.option("--no-inventory", is_flag=True, default=False, help="Skip per-location inventory aggregation")
@click.option("--fields", type=str, default=None, help="Comma-separated fields (overrides config fields)")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Enable verbose logging")
def process(
    input_file: str,
    output: str | None,
    fmt: str,
    config_name: str | None,
    no_variants: bool,
    no_inventory: bool,
    fields: str | None,
    verbose: bool,
) -> None:
    """Parse a local JSONL file into CSV, JSON, or JSONL."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    log = logging.getLogger("shopify_jsonl")

    # Load preset config if specified
    config = load_config(config_name) if config_name else None
    if config:
        log.info("Using config: %s", config.name)

    input_path = Path(input_file)
    log.info("Reading %s", input_path)
    start = time.perf_counter()

    # CLI flags override config; config provides defaults
    expand_variants = config.expand_variants if config else True
    include_inventory = config.include_inventory if config else True
    if no_variants:
        expand_variants = False
    if no_inventory:
        include_inventory = False

    options = ExpansionOptions(
        expand_variants=expand_variants,
        include_inventory=include_inventory,
    )

    # Field selection: --fields overrides config fields
    fieldnames: list[str] | None = None
    if fields:
        fieldnames = [f.strip() for f in fields.split(",") if f.strip()]
    elif config and config.fields:
        fieldnames = config.fields

    def row_stream():
        with open(input_path, encoding="utf-8") as f:
            yield from expand_products(parse_jsonl_stream(f), options)

    writer_map = {
        "csv": write_csv,
        "json": write_json,
        "jsonl": write_jsonl,
    }
    writer_fn = writer_map[fmt]

    output_path = Path(output) if output else None

    if output_path:
        with open(output_path, "w", encoding="utf-8", newline="") as out:
            if fmt == "csv" and fieldnames:
                count = write_csv(row_stream(), out, fieldnames=fieldnames)
            else:
                count = writer_fn(row_stream(), out)
    else:
        if fmt == "csv" and fieldnames:
            count = write_csv(row_stream(), sys.stdout, fieldnames=fieldnames)
        else:
            count = writer_fn(row_stream(), sys.stdout)

    elapsed = time.perf_counter() - start
    log.info("Wrote %d rows in %.2fs", count, elapsed)


@main.command()
@click.option("--shop", required=True, help="Shopify store domain (e.g. mystore.myshopify.com)")
@click.option("--token", required=True, help="Shopify Admin API access token (shpat_...)")
@click.option("--output", "-o", type=click.Path(), default="export.jsonl", help="Output JSONL file path (default: export.jsonl)")
@click.option("--no-inventory", is_flag=True, default=False, help="Skip inventory levels in the bulk query (faster for large catalogs)")
@click.option("--api-version", type=str, default="2026-01", help="Shopify API version (default: 2026-01)")
@click.option("--max-wait", type=float, default=1200.0, help="Max seconds to wait for completion (default: 1200)")
@click.option("--poll-interval", type=float, default=5.0, help="Seconds between status checks (default: 5)")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Enable debug logging")
def fetch(
    shop: str,
    token: str,
    output: str,
    no_inventory: bool,
    api_version: str,
    max_wait: float,
    poll_interval: float,
    verbose: bool,
) -> None:
    """Trigger a Shopify bulk operation, poll until done, download the JSONL result."""
    from shopify_jsonl.fetcher import fetch_bulk_export, BulkOperationError

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    output_path = Path(output)
    try:
        bytes_written = fetch_bulk_export(
            shop_domain=shop,
            access_token=token,
            output_path=output_path,
            include_inventory=not no_inventory,
            api_version=api_version,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )
        click.echo(f"Saved {output_path} ({bytes_written:,} bytes)")
        click.echo(f"Next: shopify-bulk process {output_path} -c products -o catalog.csv")
    except BulkOperationError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from e


@main.command("configs")
def list_configs_cmd() -> None:
    """List available preset configs."""
    configs = list_configs()
    if not configs:
        click.echo("No built-in configs found.")
        return
    click.echo("Available configs:")
    for name in configs:
        try:
            cfg = load_config(name)
            desc = cfg.description.strip().split("\n")[0][:80]
            click.echo(f"  {name:12s}  {desc}")
        except Exception:
            click.echo(f"  {name}")
