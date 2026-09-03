"""Canonical JSON, immutable JSON values, digests, and DocSpec identities."""

from __future__ import annotations

import contextvars

import contextlib

import hashlib
import json
import math
import re
from collections.abc import Iterator, Iterable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, TypeAlias

from docspec.errors import IntegrityError

JSONScalar: TypeAlias = None | bool | int | str
JSONValue: TypeAlias = JSONScalar | tuple["JSONValue", ...] | Mapping[str, "JSONValue"]
JSONObject: TypeAlias = Mapping[str, JSONValue]

_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_URN_PART_RE = re.compile(r"^[a-z][a-z0-9-]*$")


def require_text(value: object, label: str) -> str:
    """Return one non-empty string or fail with a stable message."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def require_sha256(value: object, label: str = "digest") -> str:
    """Return one normalized ``sha256:`` digest."""

    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be sha256 followed by 64 lowercase hexadecimal characters")
    return value


def require_relative_path(value: object, label: str = "path") -> str:
    """Return a safe portable relative path."""

    text = require_text(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"{label} must be a contained relative path")
    return path.as_posix()


def closed_mapping(
    value: object,
    keys: Iterable[str],
    label: str,
    *,
    error: type[Exception] = IntegrityError,
) -> Mapping[str, Any]:
    """Return one mapping whose keys are exactly ``keys``, or refuse it.

    ``error`` names the boundary, not a second rule. A domain value object
    reading its own dict raises ``ValueError``; bytes admitted from outside fail
    closed with ``IntegrityError``; a profile raises ``ProfileError``. The check
    those boundaries share -- a mapping, and exactly these keys, no more and no
    fewer -- is written once, here.
    """

    if not isinstance(value, Mapping) or set(value) != set(keys):
        raise error(f"{label} has an invalid closed shape")
    return value


_TRUSTED_JSON_INPUT = contextvars.ContextVar("docspec_trusted_json_input", default=False)

#: The exact types canonical JSON decoding produces; see _canonical_plain_checked.
_PLAIN_JSON_TYPES = (dict, list, str, int, bool, type(None))


@contextlib.contextmanager
def trusted_json_input() -> Iterator[None]:
    """Freeze already-proven JSON by wrapping alone.

    Inside this context ``freeze_json`` skips the checks canonical parsing has
    already established for its input -- string keys, no duplicates, no
    floats, keys in canonical order -- and only makes the tree immutable. It
    is for values decoded from bytes a verifier has ALREADY admitted (a reader
    re-streaming a catalog it verified), never for arbitrary input.
    """

    token = _TRUSTED_JSON_INPUT.set(True)
    try:
        yield
    finally:
        _TRUSTED_JSON_INPUT.reset(token)


def _wrap_trusted(value: Any) -> JSONValue:
    if type(value) is dict:
        return MappingProxyType({key: _wrap_trusted(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_wrap_trusted(item) for item in value)
    return value


def freeze_json(value: Any, *, label: str = "value") -> JSONValue:
    """Return an immutable JSON value and reject ambiguous inputs."""

    if _TRUSTED_JSON_INPUT.get() and type(value) in _PLAIN_JSON_TYPES:
        return _wrap_trusted(value)
    return _freeze_checked(value, label)


def _freeze_checked(value: Any, label: str) -> JSONValue:
    """Freeze one value, dispatching on exact type before anything abstract.

    Same ordering argument as :func:`_canonical_plain_checked`: dict, str, int
    and list are almost every value in a record, and reaching them through
    ``is_dataclass``, ``Enum`` and the ``Mapping``/``Sequence`` abstract base
    classes made each one pay for checks that do not match. A profiled build
    put this function at 11.1M calls and 11.3 s of self time.

    Anything the fast path does not recognise falls through to the original
    checks, in the original order, with the original refusals.
    """

    kind = type(value)
    if kind is str or kind is int or kind is bool or value is None:
        return value
    if kind is dict:
        frozen: dict[str, JSONValue] = {}
        for key, item in value.items():
            if type(key) is not str and not isinstance(key, str):
                raise ValueError(f"{label} contains a non-string object key")
            frozen[key] = _freeze_checked(item, f"{label}.{key}")
        return MappingProxyType(dict(sorted(frozen.items())))
    if kind is list:
        return tuple(_freeze_checked(item, f"{label}[]") for item in value)

    if is_dataclass(value) and not isinstance(value, type):
        return _freeze_checked(asdict(value), label)
    if isinstance(value, Enum):
        return _freeze_checked(value.value, label)
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{label} contains a non-finite number")
        raise ValueError(f"{label} contains a floating-point number; use an integer unit or decimal string")
    if isinstance(value, Mapping):
        mapped: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{label} contains a non-string object key")
            if key in mapped:
                raise ValueError(f"{label} contains a duplicate key: {key}")
            mapped[key] = _freeze_checked(item, f"{label}.{key}")
        return MappingProxyType(dict(sorted(mapped.items())))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return tuple(_freeze_checked(item, f"{label}[]") for item in value)
    raise ValueError(f"{label} contains unsupported type {type(value).__name__}")


def thaw_json(value: JSONValue) -> Any:
    """Return a mutable JSON-shaped copy suitable for standard encoders."""

    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _canonical_plain(value: Any, *, label: str = "value") -> Any:
    """Validate one value against the identity rules, returning plain JSON types.

    Same rules and same refusals as :func:`freeze_json`, but it builds ordinary
    dicts and lists instead of ``MappingProxyType`` and tuples, because the one
    caller that needs them -- :func:`canonical_json_bytes` -- immediately threw
    the immutable copy away through :func:`thaw_json`. That pair walked every
    record twice and allocated two complete throwaway trees before the encoder
    walked it a third time; a profiled real-corpus catalog build spent about a
    quarter of its time here (24.8M calls each way, 355M ``isinstance`` calls
    across the walkers). Freezing is still what callers holding a value want --
    :func:`freeze_json` keeps them -- but encoding never did.

    Key order is left to ``json.dumps(sort_keys=True)``, which sorts by the same
    string comparison ``freeze_json`` used, so dropping the redundant sort here
    cannot move a byte.
    """

    if _TRUSTED_JSON_INPUT.get() and type(value) in _PLAIN_JSON_TYPES:
        # Canonical parsing already established string keys, no duplicates, no
        # floats and canonical order. Nothing is left to check or to copy.
        return value
    return _canonical_plain_checked(value, label)


def _canonical_plain_checked(value: Any, label: str) -> Any:
    """Walk one value, dispatching on exact type before anything abstract.

    Records are overwhelmingly str, dict, int and list. Reaching those through
    ``is_dataclass`` (a ``hasattr``), an ``Enum`` check and finally ``Mapping``
    and ``Sequence`` -- which are abstract base classes, so every test routes
    through ``abc.__instancecheck__`` -- made each value pay for six checks
    that do not match before the one that does. A profiled 40-release build
    spent 10.6 s in ``is_dataclass`` over 31.7M calls, 4.7 s in ABC dispatch
    over 20.3M, and 2.4 s reading a ContextVar per value; ``isinstance`` was
    188.8M calls and 15.6 s.

    Exact ``type(...) is`` tests come first, so those paths cost one pointer
    comparison each. Everything the fast path does not recognise -- dataclasses,
    Enums, str subclasses, tuples, custom Mappings -- falls through to the
    original checks, in the original order, with the original refusals. The
    ContextVar is read once by the caller rather than at every level.
    """

    kind = type(value)
    if kind is str or kind is int or kind is bool or value is None:
        return value
    if kind is dict:
        plain: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str and not isinstance(key, str):
                raise ValueError(f"{label} contains a non-string object key")
            plain[key] = _canonical_plain_checked(item, f"{label}.{key}")
        return plain
    if kind is list:
        return [_canonical_plain_checked(item, f"{label}[]") for item in value]

    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
        return _canonical_plain_checked(value, label)
    if isinstance(value, Enum):
        return _canonical_plain_checked(value.value, label)
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{label} contains a non-finite number")
        raise ValueError(f"{label} contains a floating-point number; use an integer unit or decimal string")
    if isinstance(value, Mapping):
        mapped: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{label} contains a non-string object key")
            if key in mapped:
                raise ValueError(f"{label} contains a duplicate key: {key}")
            mapped[key] = _canonical_plain_checked(item, f"{label}.{key}")
        return mapped
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return [_canonical_plain_checked(item, f"{label}[]") for item in value]
    raise ValueError(f"{label} contains unsupported type {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Encode one value with DocSpec's identity-bearing JSON rules."""

    plain = _canonical_plain(value)
    return json.dumps(plain, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def canonical_json_file_bytes(value: Any) -> bytes:
    """Encode a canonical JSON file with one trailing newline."""

    return canonical_json_bytes(value) + b"\n"


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def parse_canonical_json(data: bytes, *, label: str = "JSON", file_form: bool = True) -> JSONValue:
    """Parse exact canonical UTF-8 JSON and reject alternate encodings."""

    value = parse_closed_json(data, label=label)
    expected = canonical_json_file_bytes(value) if file_form else canonical_json_bytes(value)
    if data != expected:
        raise IntegrityError(f"{label} is not canonical JSON")
    return value


def parse_closed_json(data: bytes, *, label: str = "JSON") -> JSONValue:
    """Parse duplicate-safe finite UTF-8 JSON without imposing file formatting."""

    try:
        text = data.decode("utf-8")
        value = json.loads(text, object_pairs_hook=_closed_object, parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)))
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise IntegrityError(f"{label} is not valid closed UTF-8 JSON: {error}") from error
    return freeze_json(value, label=label)


def sha256_digest(data: bytes) -> str:
    """Return the normalized SHA-256 digest of exact bytes."""

    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def identity_digest(value: Any) -> str:
    """Digest one canonical identity-bearing value."""

    return sha256_digest(canonical_json_bytes(value))


class OrderedJsonSequenceDigester:
    """Incrementally digest one canonical JSON array with a single framing implementation."""

    __slots__ = ("_digest", "_finished", "_first", "_result")

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._first = True
        self._finished = False
        self._result: str | None = None

    def accept(self, value: Any) -> None:
        if self._finished:
            raise RuntimeError("ordered JSON sequence digest is already complete")
        if not self._first:
            self._digest.update(b",")
        self._digest.update(canonical_json_bytes(value))
        self._first = False

    def finish(self) -> str:
        if not self._finished:
            self._digest.update(b"]")
            self._result = f"sha256:{self._digest.hexdigest()}"
            self._finished = True
        assert self._result is not None
        return self._result


def ordered_json_sequence_digest(values: Iterable[Any]) -> str:
    """Digest a canonical JSON array without retaining all items in memory."""

    digest = OrderedJsonSequenceDigester()
    for value in values:
        digest.accept(value)
    return digest.finish()


def stable_urn(kind: str, value: Any, *, version: int = 1) -> str:
    """Create a content-derived DocSpec URN."""

    kind = require_text(kind, "identity kind")
    if _URN_PART_RE.fullmatch(kind) is None:
        raise ValueError("identity kind must use lowercase letters, digits, and hyphens")
    digest = identity_digest(value).removeprefix("sha256:")
    return f"urn:docspec:{kind}:v{version}:{digest}"
