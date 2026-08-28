def inspect_dataset(path: str, sample_rows: int = 5) -> dict:
    """Return bounded structural metadata for a CSV or Parquet dataset."""
    from pathlib import Path

    import pandas as pd

    source = Path(path)
    if source.suffix.lower() == ".parquet":
        frame = pd.read_parquet(source)
    elif source.suffix.lower() == ".csv":
        frame = pd.read_csv(source)
    else:
        raise ValueError("Only CSV and Parquet datasets are supported")
    bounded_rows = max(1, min(sample_rows, 20))
    return {
        "path": str(source),
        "rows": len(frame),
        "columns": len(frame.columns),
        "schema": {name: str(dtype) for name, dtype in frame.dtypes.items()},
        "sample": frame.head(bounded_rows).to_dict(orient="records"),
        "memory_bytes": int(frame.memory_usage(deep=True).sum()),
    }
