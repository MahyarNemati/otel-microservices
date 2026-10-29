#!/usr/bin/env python3
"""Validate the stack's YAML with a loader that rejects duplicate keys.

yaml.safe_load accepts a duplicate mapping key and silently keeps the last one,
so a local "it parses" check passes while GitHub Actions and the OpenTelemetry
Collector both refuse the file. This loader raises instead.

Usage: python3 scripts/validate-yaml.py [file ...]
"""

from __future__ import annotations

import sys

import yaml


class StrictLoader(yaml.SafeLoader):
    """A SafeLoader that treats a duplicate mapping key as an error."""


def _no_duplicates(loader: StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    # flatten_mapping resolves YAML merge keys (`<<`), which docker-compose and
    # the collector config both use. Without it a merge key arrives as an
    # unconstructable tag.
    loader.flatten_mapping(node)
    mapping: dict = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicates)

DEFAULT_FILES = [
    "docker-compose.yml",
    "deploy/collector/config.yaml",
    "deploy/tempo/tempo.yaml",
    "deploy/loki/loki.yaml",
    "deploy/prometheus/prometheus.yml",
    "deploy/grafana/provisioning/datasources/datasources.yaml",
    "deploy/grafana/provisioning/dashboards/dashboards.yaml",
    ".github/workflows/ci.yml",
]


def main(argv: list[str]) -> int:
    files = argv[1:] or DEFAULT_FILES
    failed = False
    for path in files:
        try:
            with open(path, encoding="utf-8") as handle:
                yaml.load(handle, Loader=StrictLoader)
        except FileNotFoundError:
            print(f"MISSING  {path}")
            failed = True
        except Exception as err:  # noqa: BLE001 - report whatever the loader says
            print(f"INVALID  {path}\n         {err}")
            failed = True
        else:
            print(f"ok       {path}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
