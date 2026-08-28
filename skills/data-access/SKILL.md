---
name: data-access
version: 0.1.0
description: Fetch an analysis dataset from an externally supplied query.
---

# Data Access

Use this Skill when an analysis requires data that is not already present in
the execution workspace.

The query is supplied by an external data-lake owner. Do not invent or alter
its business meaning. The initial `fetch_dataset` implementation is a
deterministic fake that creates representative sample data for development.

## Tool

- `fetch_dataset`: accepts the external query, dataset name, output format,
  and optional seed. It returns the generated path, schema, shape, and seed.
