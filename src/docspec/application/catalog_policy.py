"""Small source-independent helpers shared by DocSpec catalog policies."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from urllib.parse import urlsplit

from docspec.domain.source_catalog import CatalogNormalizationField
from docspec.errors import IntegrityError

_RIN = re.compile(r"^[0-9]{4}-[A-Z][A-Z0-9]{3}$")
_UTC_INSTANT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def utf16_key(value: str) -> bytes:
    try:
        return value.encode("utf-16-be")
    except UnicodeEncodeError as error:
        raise IntegrityError("catalog policy text contains a lone Unicode surrogate") from error


def array_with_unparseable(value: object) -> tuple[list[Any], tuple[Any, ...]]:
    if value is None:
        return [], ()
    if isinstance(value, list):
        return value, ()
    return [], (value,)


def strings(value: object) -> tuple[list[str], tuple[Any, ...]]:
    values, rejected = array_with_unparseable(value)
    accepted: set[str] = set()
    unparseable = list(rejected)
    for item in values:
        if isinstance(item, str) and item:
            accepted.add(item)
        else:
            unparseable.append(item)
    return sorted(accepted, key=utf16_key), tuple(unparseable)


def text_value(value: object) -> tuple[str | None, tuple[Any, ...]]:
    if value is None:
        return None, ()
    if isinstance(value, str) and value.strip():
        return value, ()
    return None, (value,)


def iso_date(value: object) -> str | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    selected = value[:10]
    try:
        date.fromisoformat(selected)
    except ValueError:
        return None
    return selected


def date_value(value: object) -> tuple[str | None, tuple[Any, ...]]:
    if value is None:
        return None, ()
    normalized = iso_date(value)
    return (normalized, ()) if normalized is not None else (None, (value,))


def utc_instant_date_value(value: object) -> tuple[str | None, tuple[Any, ...]]:
    """Read a canonical second-precision UTC instant as its calendar date."""

    if value is None:
        return None, ()
    if not isinstance(value, str) or _UTC_INSTANT.fullmatch(value) is None:
        return None, (value,)
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return None, (value,)
    if parsed.isoformat().replace("+00:00", "Z") != value:
        return None, (value,)
    return parsed.date().isoformat(), ()


def normalized_rins(value: object) -> tuple[list[str], tuple[Any, ...]]:
    accepted: set[str] = set()
    values, rejected = strings(value)
    unparseable = list(rejected)
    for raw in values:
        normalized = unicodedata.normalize("NFKC", raw.strip()).upper()
        if _RIN.fullmatch(normalized):
            accepted.add(normalized)
        else:
            unparseable.append(raw)
    return sorted(accepted, key=utf16_key), tuple(unparseable)


def http_url(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = urlsplit(value)
    return value if parsed.scheme in {"http", "https"} and bool(parsed.netloc) else None


def http_url_value(value: object) -> tuple[str | None, tuple[Any, ...]]:
    if value is None:
        return None, ()
    normalized = http_url(value)
    return (normalized, ()) if normalized is not None else (None, (value,))


def normalization_field(
    normalized_field: str,
    source_paths: tuple[str, ...],
    value: Any,
    *,
    value_source: str = "source",
    unparseable_values: tuple[Any, ...] = (),
    present: bool | None = None,
) -> CatalogNormalizationField:
    is_present = bool(value) if present is None else present
    outcome = "unparseable" if unparseable_values else "normalized" if is_present else "absent"
    distinct_unparseable: list[Any] = []
    for raw in unparseable_values:
        if not any(raw == existing for existing in distinct_unparseable):
            distinct_unparseable.append(raw)
    return CatalogNormalizationField(
        normalized_field,
        source_paths,
        value_source,
        outcome,
        value,
        tuple(distinct_unparseable),
    )


#: URN prefixes reserved for a concept registry this repository does not own,
#: so a publisher's raw vocabulary can never mint an id that registry owns (D6).
_RESERVED_TOPIC_NAMESPACES = ("urn:ref:", "urn:refspec:")


def observed_topics(
    value: object,
    *,
    scheme: str,
    identity_fields: tuple[str, ...],
    label_fields: tuple[str, ...],
) -> tuple[dict[str, str], ...]:
    if scheme.startswith(_RESERVED_TOPIC_NAMESPACES):
        raise IntegrityError(f"observed topic scheme {scheme!r} claims a reserved concept namespace")
    result: dict[tuple[str, str], dict[str, str]] = {}
    values = value if isinstance(value, list) else []
    for raw in values:
        if isinstance(raw, str) and raw:
            identity = label = raw
        elif isinstance(raw, Mapping):
            label = next(
                (
                    raw.get(field)
                    for field in label_fields
                    if isinstance(raw.get(field), str) and raw.get(field)
                ),
                None,
            )
            identity = next(
                (
                    raw.get(field)
                    for field in identity_fields
                    if isinstance(raw.get(field), str) and raw.get(field)
                ),
                label,
            )
            if not isinstance(label, str) or not isinstance(identity, str):
                continue
        else:
            continue
        if identity.startswith(_RESERVED_TOPIC_NAMESPACES):
            raise IntegrityError(f"observed topic id {identity!r} claims a reserved concept namespace")
        result[(identity, label)] = {
            "observedTopicId": identity,
            "observedTopicScheme": scheme,
            "label": label,
        }
    return tuple(
        result[key]
        for key in sorted(result, key=lambda pair: tuple(utf16_key(part) for part in pair))
    )


__all__ = [
    "array_with_unparseable",
    "date_value",
    "http_url",
    "http_url_value",
    "normalization_field",
    "normalized_rins",
    "observed_topics",
    "strings",
    "text_value",
    "utc_instant_date_value",
    "utf16_key",
]
