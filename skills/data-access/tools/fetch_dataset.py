def fetch_dataset(
    query: str,
    dataset_name: str,
    output_format: str = "parquet",
    seed: int | None = None,
) -> dict:
    """Create deterministic development data for an external lake query."""
    import hashlib
    from pathlib import Path

    import numpy as np
    import pandas as pd

    if output_format not in {"csv", "parquet"}:
        raise ValueError("output_format must be csv or parquet")
    safe_name = "".join(
        char if char.isalnum() or char in {"-", "_"} else "_"
        for char in dataset_name
    ).strip("_")
    if not safe_name:
        raise ValueError("dataset_name must contain a safe character")
    digest = hashlib.sha256(query.encode("utf-8")).hexdigest()
    resolved_seed = seed if seed is not None else int(digest[:8], 16)
    rng = np.random.default_rng(resolved_seed)
    row_count = 500
    frame = pd.DataFrame(
        {
            "customer_id": np.arange(1, row_count + 1),
            "segment": rng.choice(["A", "B", "C"], row_count),
            "revenue": rng.gamma(4.0, 25.0, row_count).round(2),
            "orders": rng.poisson(3.0, row_count),
            "is_active": rng.choice([True, False], row_count),
        }
    )
    missing_rows = rng.choice(row_count, size=20, replace=False)
    frame.loc[missing_rows, "revenue"] = np.nan
    output_dir = Path("artifacts/datasets")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{safe_name}.{output_format}"
    if output_format == "parquet":
        frame.to_parquet(output_path, index=False)
    else:
        frame.to_csv(output_path, index=False)
    return {
        "path": str(output_path),
        "format": output_format,
        "rows": len(frame),
        "columns": len(frame.columns),
        "schema": {name: str(dtype) for name, dtype in frame.dtypes.items()},
        "seed": resolved_seed,
        "query_sha256": digest,
    }
