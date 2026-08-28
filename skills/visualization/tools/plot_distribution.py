def plot_distribution(path: str, column: str, bins: int = 30) -> dict:
    """Create a PNG histogram for one numeric column."""
    from pathlib import Path

    import matplotlib.pyplot as plt
    import pandas as pd

    source = Path(path)
    frame = (
        pd.read_parquet(source)
        if source.suffix.lower() == ".parquet"
        else pd.read_csv(source)
    )
    if column not in frame:
        raise ValueError(f"Column does not exist: {column}")
    bounded_bins = max(5, min(bins, 200))
    output_dir = Path("artifacts/plots")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{column}_distribution.png"
    figure, axis = plt.subplots(figsize=(8, 5))
    axis.hist(frame[column].dropna(), bins=bounded_bins)
    axis.set_title(f"Distribution of {column}")
    axis.set_xlabel(column)
    axis.set_ylabel("Count")
    figure.tight_layout()
    figure.savefig(output_path, dpi=144)
    plt.close(figure)
    return {
        "path": str(output_path),
        "column": column,
        "bins": bounded_bins,
    }
