---
name: data-inspection
version: 0.1.0
description: Inspect a tabular dataset before choosing an analysis method.
---

# Data Inspection

Use this Skill immediately after data acquisition or whenever the schema and
basic shape of an input dataset are unknown.

## Tool

- `inspect_dataset`: reads CSV or Parquet and returns schema, shape, a bounded
  sample, and memory usage.
