def inspect_data(data):
    """Inspect records without mutating them."""
    columns = sorted({key for row in data for key in row})
    result = {
        "rows": len(data),
        "columns": columns,
        "missing": {
            key: sum(row.get(key) is None for row in data) for key in columns
        },
    }
    print(result)
    return result
