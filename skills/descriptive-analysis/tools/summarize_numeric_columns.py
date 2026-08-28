def summarize_numeric_columns(
    path: str,
    columns: list[str] | None = None,
) -> dict:
    """Compute descriptive statistics for selected numeric columns."""
    from pathlib import Path

    import pandas as pd

    source = Path(path)
    frame = (
        pd.read_parquet(source)
        if source.suffix.lower() == ".parquet"
        else pd.read_csv(source)
    )
    numeric = frame.select_dtypes(include="number")
    if columns is not None:
        missing = sorted(set(columns) - set(numeric.columns))
        if missing:
            raise ValueError(f"Columns are not numeric or missing: {missing}")
        numeric = numeric[columns]
    summary = numeric.describe(percentiles=[0.25, 0.5, 0.75]).transpose()
    return {
        "path": str(source),
        "columns": summary.reset_index()
        .rename(columns={"index": "column"})
        .to_dict(orient="records"),
    }
