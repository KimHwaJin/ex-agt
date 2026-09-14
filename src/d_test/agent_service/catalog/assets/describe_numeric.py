def describe_numeric(data, column="revenue"):
    """Describe finite numeric values in a named record column."""
    import math
    import statistics

    if data and not any(column in row for row in data):
        raise ValueError(f"Unknown column: {column}")
    values = [
        row[column]
        for row in data
        if type(row.get(column)) in (int, float) and math.isfinite(row[column])
    ]
    result = {
        "column": column,
        "count": len(values),
        "mean": statistics.mean(values) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }
    print(result)
    return result
