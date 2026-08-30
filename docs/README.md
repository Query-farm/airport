<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# Airport Extension for DuckDB

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/airport.html)
[![v1.5 build](https://github.com/Query-farm/airport/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/airport/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **Airport** extension brings [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) support to [DuckDB](https://duckdb.org), enabling DuckDB to query, modify, and store data via Arrow Flight servers.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/airport](https://query.farm/products/extensions/airport)**

## Installation

```sql
INSTALL airport FROM community;
LOAD airport;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
