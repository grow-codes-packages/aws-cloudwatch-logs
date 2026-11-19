"""CloudWatch Embedded Metric Format (EMF) documents (API.md §2.2).

An EMF document is a JSON log event that CloudWatch turns into a metric. The SDK only builds
and sends the document; metric extraction happens inside AWS.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from ._batching import now_ms, to_epoch_ms
from ._core import ValidationError

__all__ = ["DEFAULT_EMF_NAMESPACE", "EMF_UNITS", "build_emf"]

#: Units accepted by CloudWatch metrics.
EMF_UNITS: tuple[str, ...] = (
    "Seconds",
    "Microseconds",
    "Milliseconds",
    "Bytes",
    "Kilobytes",
    "Megabytes",
    "Gigabytes",
    "Terabytes",
    "Bits",
    "Kilobits",
    "Megabits",
    "Gigabits",
    "Terabits",
    "Percent",
    "Count",
    "Bytes/Second",
    "Kilobytes/Second",
    "Megabytes/Second",
    "Gigabytes/Second",
    "Terabytes/Second",
    "Bits/Second",
    "Kilobits/Second",
    "Megabits/Second",
    "Gigabits/Second",
    "Terabits/Second",
    "Count/Second",
    "None",
)

#: Default EMF namespace.
DEFAULT_EMF_NAMESPACE = "GrowSdk"


def _finite(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def build_emf(
    metric: str,
    value: float | Sequence[float],
    dimensions: Mapping[str, str] | None = None,
    *,
    namespace: str = DEFAULT_EMF_NAMESPACE,
    unit: str = "None",
    timestamp: datetime | int | None = None,
    properties: Mapping[str, str | int | float | bool] | None = None,
) -> dict[str, Any]:
    """Build an EMF document for one metric.

    ``value`` is a finite number or 1-100 finite numbers; ``dimensions`` (at most 30) become
    one dimension set; ``properties`` are extra top-level fields (searchable, not dimensions);
    ``timestamp`` defaults to now. Raises ``ValidationError`` on invalid input.
    """
    if not isinstance(metric, str) or not 1 <= len(metric) <= 1024:
        raise ValidationError("emf: metric name must be 1-1024 characters")
    if not isinstance(namespace, str) or not 1 <= len(namespace) <= 1024:
        raise ValidationError("emf: namespace must be 1-1024 characters")
    if unit not in EMF_UNITS:
        raise ValidationError(f"emf: unknown unit {unit}")
    single = _finite(value)
    values = [value] if single else list(value) if isinstance(value, Sequence) else []
    if not values or len(values) > 100 or not all(_finite(v) for v in values):
        raise ValidationError("emf: value must be a finite number or 1-100 finite numbers")
    dims = dict(dimensions or {})
    if len(dims) > 30:
        raise ValidationError("emf: at most 30 dimensions")
    for key, dim in dims.items():
        if not isinstance(key, str) or key in ("", "_aws", metric) or not isinstance(dim, str):
            raise ValidationError("emf: dimension names must be non-empty and values strings")
    ts = now_ms() if timestamp is None else to_epoch_ms(timestamp, "timestamp", "emf")
    doc: dict[str, Any] = {
        "_aws": {
            "Timestamp": ts,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [list(dims)],
                    "Metrics": [{"Name": metric, "Unit": unit}],
                }
            ],
        }
    }
    for key, prop in (properties or {}).items():
        if key in ("_aws", metric):
            raise ValidationError(f"emf: property {key} is reserved")
        doc[key] = prop
    doc.update(dims)
    doc[metric] = value if single else values
    return doc
