def fetch_sample_data(query, rows=100, seed=42):
    """Fake lake download: create deterministic records, without network IO."""
    import random
    from datetime import date, timedelta

    rng = random.Random(seed)
    records = [
        {
            "date": str(date(2026, 1, 1) + timedelta(days=i % 30)),
            "category": rng.choice(["A", "B", "C"]),
            "revenue": round(rng.uniform(10, 200), 2),
        }
        for i in range(rows)
    ]
    print({"synthetic": True, "rows": len(records), "query": query})
    return records
