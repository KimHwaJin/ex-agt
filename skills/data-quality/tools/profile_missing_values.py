def profile_missing_values(path: str) -> dict:
    """Calculate missing-value counts and percentages by column."""
    from pathlib import Path

    import pandas as pd

    source = Path(path)
    frame = (
        pd.read_parquet(source)
        if source.suffix.lower() == ".parquet"
        else pd.read_csv(source)
    )
    missing = frame.isna().sum()
    percentages = missing.div(max(len(frame), 1)).mul(100)
    return {
        "path": str(source),
        "rows": len(frame),
        "columns": {
            name: {
                "missing_count": int(missing[name]),
                "missing_percent": round(float(percentages[name]), 4),
            }
            for name in frame.columns
        },
    }
