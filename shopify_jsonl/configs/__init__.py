"""
Preset config loader.

Resolves a config name (like "products") to its YAML file in this directory,
or loads an arbitrary YAML file from a user-supplied path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

_CONFIGS_DIR = Path(__file__).parent


@dataclass(slots=True)
class PresetConfig:
    name: str
    description: str = ""
    expand_variants: bool = True
    include_inventory: bool = True
    fields: list[str] = field(default_factory=list)


def load_config(name_or_path: str) -> PresetConfig:
    """
    Load a preset config by name (built-in) or by file path (custom).

    Built-in names: products, inventory, orders
    Custom: any path ending in .yaml or .yml
    """
    path = _resolve_path(name_or_path)
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return PresetConfig(
        name=raw.get("name", path.stem),
        description=raw.get("description", ""),
        expand_variants=raw.get("expand_variants", True),
        include_inventory=raw.get("include_inventory", True),
        fields=raw.get("fields") or [],
    )


def list_configs() -> list[str]:
    """Return names of all built-in configs."""
    return sorted(p.stem for p in _CONFIGS_DIR.glob("*.yaml"))


def _resolve_path(name_or_path: str) -> Path:
    # If it looks like a file path, use directly
    candidate = Path(name_or_path)
    if candidate.suffix in (".yaml", ".yml") and candidate.exists():
        return candidate

    # Otherwise treat as a built-in name
    builtin = _CONFIGS_DIR / f"{name_or_path}.yaml"
    if builtin.exists():
        return builtin

    available = list_configs()
    raise FileNotFoundError(
        f"Config '{name_or_path}' not found. "
        f"Built-in configs: {', '.join(available)}. "
        f"Or pass a path to a .yaml file."
    )
