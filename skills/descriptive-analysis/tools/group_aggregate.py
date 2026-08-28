def group_aggregate(
    path: str,
    group_by: str,
    value: str,
    aggregation: str = "mean",
) -> dict:
    """Aggregate a numeric value by a categorical column."""
    from pathlib import Path

    import pandas as pd

    allowed = {"mean", "sum", "count", "median", "min", "max"}
    if aggregation not in allowed:
        raise ValueError(f"aggregation must be one of {sorted(allowed)}")
    source = Path(path)
    frame = (
        pd.read_parquet(source)
        if source.suffix.lower() == ".parquet"
        else pd.read_csv(source)
    )
    if group_by not in frame or value not in frame:
        raise ValueError("group_by and value must exist in the dataset")
    grouped = (
        frame.groupby(group_by, dropna=False)[value]
        .agg(aggregation)
        .reset_index()
    )
    return {
        "path": str(source),
        "group_by": group_by,
        "value": value,
        "aggregation": aggregation,
        "rows": grouped.to_dict(orient="records"),
    }
