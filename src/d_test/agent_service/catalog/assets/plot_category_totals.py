def plot_category_totals(data, category="category", value="revenue"):
    """Return SVG and display inline when running inside Jupyter."""
    import html
    import math
    from collections import defaultdict

    totals = defaultdict(float)
    for row in data:
        number = row.get(value)
        if category not in row or type(number) not in (float, int):
            raise ValueError("Missing category or non-numeric value")
        if not math.isfinite(number) or number < 0:
            raise ValueError("This example chart requires finite values >= 0")
        totals[str(row[category])] += number
        if len(totals) > 30:
            raise ValueError("At most 30 categories are supported")
    scale = max(totals.values(), default=1) or 1
    height = max(60, 40 * len(totals))
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="720" height="{height}" viewBox="0 0 720 {height}">'
    ]
    for index, (label, total) in enumerate(sorted(totals.items())):
        y = index * 40
        parts.append(
            f'<text x="5" y="{y + 22}">{html.escape(label[:20])}</text>'
            f'<rect x="180" y="{y + 4}" width="{total / scale * 430}" '
            'height="28" fill="#3979c6"/>'
            f'<text x="620" y="{y + 22}">{total:.2f}</text>'
        )
    svg = "".join(parts) + "</svg>"
    try:
        # IPython is provided by Jupyter, not the Agent service environment.
        from IPython.display import (  # ty: ignore[unresolved-import]
            SVG,
            display,
        )
    except ImportError:
        print(svg)
    else:
        display(SVG(svg))
    return svg
