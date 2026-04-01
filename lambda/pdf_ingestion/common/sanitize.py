"""Sanitize canonical output for safe CSV storage."""


def sanitize_for_csv(data):
    """Recursively remove commas from all string values in dicts/lists."""
    if isinstance(data, dict):
        return {k: sanitize_for_csv(v) for k, v in data.items()}
    if isinstance(data, list):
        return [sanitize_for_csv(item) for item in data]
    if isinstance(data, str):
        return data.replace(",", "")
    return data
