"""Build and open one complete immutable DocSpec ``SourceCatalog`` snapshot."""

from __future__ import annotations

import hashlib
import collections
import heapq
import json
import os
import pickle
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from itertools import zip_longest
from typing import Any

import jsonschema

try:  # the compiled validator is the optional `fast` extra; the authority always works
    import jsonschema_rs
except ImportError:  # pragma: no cover - environment without the extra
    jsonschema_rs = None  # type: ignore[assignment]
from rulespec_artifacts import (
    ROOT_OBJECT_KEY,
    ArtifactInput,
    ArtifactPin,
    ArtifactVerificationError,
    FramedSection,
    MemberDescriptor,
    MemberManifestReference,
    MemberSource,
    Producer,
    Supersedes,
    VerifiedArtifact,
    admit_artifact,
    build_artifact_root,
    canonical_json_bytes,
    describe_member_from_receipt,
    framed_section_digest,
    iter_member_descriptors,
    parse_canonical_json,
    schema_bundle_digest,
    sha256_digest,
)

from docspec.domain.references import SourceCatalogRef
from docspec.adapters.framing import (
    FramedSectionHasher as _FramedSectionHasher,
    canonical_record_payload as _canonical_record_payload,
)
from docspec.domain.identity import require_sha256, require_text, trusted_json_input
from docspec.domain.storage import partition_bucket
from docspec.domain.source_catalog import (
    CatalogDisposition,
    SOURCE_CATALOG_ITEM_SCHEMA_ID,
    SOURCE_CATALOG_MAX_JOIN_IDS,
    SOURCE_CATALOG_POLICY_SCHEMA_ID,
    SOURCE_CATALOG_RECEIPT_SCHEMA_ID,
    SourceCatalogItem,
    source_catalog_schemas,
)
from docspec.errors import IntegrityError, LimitExceededError
from docspec.ports.source_catalog import (
    CatalogPolicyInputs,
    CatalogPolicyWorkspace,
    CatalogResumePoint,
    FRESH_BUILD,
    ImmutableSourceCatalogReader,
    LocatedSourceCatalogItem,
    SourceCatalogBlobSource,
    SourceCatalogPolicy,
    SourceCatalogSnapshot,
    SourceCatalogSnapshotSummary,
    SourceCatalogStore,
    SourceCatalogSuccession,
    SourceInputSelector,
    SourceNativeDescription,
    SourceNativeRecordSource,
    SourceNativeRow,
)

CATALOG_KIND = "docspec-source-catalog"
CATALOG_POLICY_KEY = "catalog-policy.json"
CATALOG_RECEIPT_KEY = "catalog-build-receipt.json"
CATALOG_MANIFEST_KEY = "manifests/catalog.json"
CATALOG_POLICY_ROLE = "catalog-policy"
CATALOG_ITEMS_ROLE = "source-items"
CATALOG_RECEIPT_ROLE = "catalog-build-receipt"
CATALOG_ITEMS_MEDIA_TYPE = "application/x-ndjson"
CATALOG_JSON_MEDIA_TYPE = "application/json"
CATALOG_POLICY_FORMAT = "docspec-catalog-policy"
CATALOG_RECEIPT_FORMAT = "docspec-source-catalog-build-receipt"
CATALOG_FORMAT_VERSION = "1.0"
MAX_CATALOG_ROW_BYTES = 4 * 1024 * 1024
MAX_SMALL_MEMBER_BYTES = 1024 * 1024
MAX_SOURCE_RENDITIONS_PER_RECORD = 1024
MAX_SOURCE_RENDITION_BYTES_PER_RECORD = 4 * 1024 * 1024
CATALOG_PARTITION_POLICY_ID = "urn:docspec:partition-policy:source-item-sha256:1"
CATALOG_PARTITION_POLICY_VERSION = "1.0.0"
CATALOG_PARTITION_BUCKET_COUNT = 64
_UNIVERSE_ACCOUNTING_NAMESPACE = "docspec-internal/universe"
_OUTPUT_ACCOUNTING_NAMESPACE = "docspec-internal/output"
_OUTPUT_PARTITION_NAMESPACE_PREFIX = "docspec-internal/output-partition/"
_SOURCE_ROW_NAMESPACE_PREFIX = "docspec-internal/source-rows/"
# Each of these is an integrity fingerprint over a derived, in-memory per-row
# projection (see `_derive_catalog`'s and `_derive_catalog_parallel`'s
# `diagnostics` dict below) -- not a reference to a published member. No
# member with this content is declared anywhere in the distribution; verifying
# one means recomputing it from the published `source-items` partitions, not
# dereferencing a blob. Full rationale is on the matching properties in
# `source_catalog.py`'s `source_catalog_schemas()` receipt schema.
_DIAGNOSTIC_DIGEST_FIELDS = (
    "normalizedFieldsDigest",
    "joinedFieldsDigest",
    "dispositionsDigest",
    "reasonsDigest",
    "interpretationsDigest",
    "renditionChoicesDigest",
)
_INTERPRETATION_KINDS = (
    "exact-join",
    "normalization",
    "rendition-preference",
    "sampling",
    "selection",
    "topic-recovery",
)

_CATALOG_SPEC_FIELDS = {
    "catalogId",
    "catalogSchemaDigest",
    "sourceSystemSetDigest",
    "sourceNativeSchemaSetDigest",
    "selectionPolicyId",
    "selectionPolicyVersion",
    "selectionPolicyDigest",
    "requestedUniverseSetDigest",
    "selectedSourceSetDigest",
    "catalogStateDigest",
}
_SOURCE_RECORD_FIELDS = {
    "sourceRecordId",
    "scopeId",
    "schemaName",
    "schemaVersion",
    "schemaDigest",
    "record",
    "fieldDiagnostics",
}
_SOURCE_RENDITION_REQUIRED_FIELDS = {
    "sourceRecordId",
    "renditionId",
    "sourceField",
    "locator",
    "mediaType",
    "expectedSha256",
    "expectedByteSize",
}
_SCHEMAS = source_catalog_schemas()
_POLICY_VALIDATOR = jsonschema.Draft202012Validator(_SCHEMAS["catalog-policy.schema.json"])
_RECEIPT_VALIDATOR = jsonschema.Draft202012Validator(_SCHEMAS["catalog-build-receipt.schema.json"])


def source_catalog_producer(
    *,
    implementation_id: str,
    verifier_id: str,
    verifier_version: str,
    verifier_implementation_id: str,
) -> Producer:
    """Validate standard immutable implementation identities at the outer edge."""

    return Producer.from_dict(
        {
            "product": "docspec",
            "implementationId": implementation_id,
            "verifierId": verifier_id,
            "verifierVersion": verifier_version,
            "verifierImplementationId": verifier_implementation_id,
        },
        path="source-catalog/producer",
    )


@dataclass(frozen=True, slots=True)
class SourceCatalogBuildRequest:
    catalog_id: str
    producer: Producer
    supersedes: Supersedes | None = None

    def __post_init__(self) -> None:
        require_text(self.catalog_id, "source catalog series catalog_id")
        if self.supersedes is not None:
            if not isinstance(self.supersedes, Supersedes):
                raise TypeError("source catalog supersedes must use Rulespec Supersedes")
            Supersedes.from_dict(self.supersedes.as_dict(), path="source-catalog/supersedes")
            require_text(self.supersedes.reason, "source catalog supersedes reason")


@dataclass(frozen=True, slots=True)
class SourceCatalogBuildResult:
    reference: SourceCatalogRef
    summary: SourceCatalogSnapshotSummary
    byte_measurements: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _CatalogPartition:
    partition_id: str
    member: MemberDescriptor

    def to_receipt(self) -> dict[str, object]:
        if self.member.blob_ref is None or self.member.record_count is None:
            raise ValueError("source-item partitions require blobRef and recordCount")
        return {
            "partitionId": self.partition_id,
            "blobRef": self.member.blob_ref,
            "byteSize": self.member.byte_size,
            "recordCount": self.member.record_count,
        }


def _partition_policy() -> dict[str, object]:
    identity = {
        "policyId": CATALOG_PARTITION_POLICY_ID,
        "policyVersion": CATALOG_PARTITION_POLICY_VERSION,
        "bucketCount": CATALOG_PARTITION_BUCKET_COUNT,
    }
    return {**identity, "policyDigest": sha256_digest(canonical_json_bytes(identity))}


def _partition_id(source_item_id: str) -> str:
    bucket = partition_bucket(source_item_id, CATALOG_PARTITION_BUCKET_COUNT)
    return f"{bucket:04d}"


def _partition_namespace(partition_id: str) -> str:
    return f"{_OUTPUT_PARTITION_NAMESPACE_PREFIX}{partition_id}"


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{label} must be an object")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise IntegrityError(f"{label} must be nonempty text")
    return value


def _utf16_key(value: str) -> bytes:
    """Use the shared artifact ordering rule for DocSpec-owned row keys."""

    try:
        return value.encode("utf-16-be")
    except UnicodeEncodeError as error:
        raise IntegrityError("catalog identity contains a lone Unicode surrogate") from error


def _require_interpretation_order(row: Mapping[str, Any]) -> None:
    kinds = tuple(value["interpretationKind"] for value in row["interpretations"])
    if kinds != _INTERPRETATION_KINDS:
        raise IntegrityError("source-catalog interpretations differ from the closed ordered kind set")


def _read_small(source: MemberSource, key: str) -> bytes:
    with source.open(key) as stream:
        payload = stream.read(MAX_SMALL_MEMBER_BYTES + 1)
    if len(payload) > MAX_SMALL_MEMBER_BYTES:
        raise LimitExceededError(f"{key} exceeds its {MAX_SMALL_MEMBER_BYTES}-byte limit")
    return payload


def _schema_error(validator: jsonschema.Draft202012Validator, value: object, label: str) -> None:
    try:
        validator.validate(value)
    except jsonschema.ValidationError as error:
        path = "/".join(str(part) for part in error.absolute_path) or "$"
        raise IntegrityError(f"{label} schema failure at {path}: {error.message}") from error


class _CompiledSchemaGate:
    """Fast-accept schema checking with python-jsonschema as the sole authority.

    The compiled validator only ever short-circuits ACCEPTANCE; every rejection
    is re-decided by ``jsonschema`` so refusal semantics and error text cannot
    drift behind a faster engine. The differential test pins the two validators
    to each other on this schema's shapes.
    """

    __slots__ = ("_authority", "_fast", "implementation_id")

    def __init__(self, schema: Mapping[str, Any]) -> None:
        self._authority = jsonschema.Draft202012Validator(schema)
        #: Which engine decides acceptance here. ``jsonschema-rs`` is a declared
        #: dependency, so the pure-Python value should never be seen in a
        #: released build -- and that is the point of naming it. Falling back
        #: costs ~116x on this schema (1,993 us/row against 17.1 us/row), which
        #: used to happen silently.
        self.implementation_id = "jsonschema-rs"
        if jsonschema_rs is None:
            self._fast = None
            self.implementation_id = "jsonschema"
            return
        try:
            self._fast = jsonschema_rs.validator_for(dict(schema))
        except Exception:  # noqa: BLE001 - an uncompilable schema falls back to the authority
            self._fast = None
            self.implementation_id = "jsonschema"

    def error(self, value: object, label: str) -> None:
        if self._fast is not None and self._fast.is_valid(value):
            return
        _schema_error(self._authority, value, label)


_ITEM_VALIDATOR = _CompiledSchemaGate(_SCHEMAS["source-item.schema.json"])


def source_item_validator_implementation() -> str:
    """Name the schema engine deciding acceptance for source-item rows."""

    return _ITEM_VALIDATOR.implementation_id


def _iter_partition_rows(
    blob_source: SourceCatalogBlobSource,
    partition: _CatalogPartition,
    *,
    validate: bool = True,
    with_raw: bool = False,
    as_dict: bool = False,
) -> Iterator[Any]:
    member = partition.member
    if member.blob_ref is None or member.record_count is None:
        raise IntegrityError("source-item partition descriptor requires blobRef and recordCount")
    with blob_source.open(member.blob_ref) as stream:
        yield from _iter_partition_stream(
            stream,
            partition_id=partition.partition_id,
            record_count=member.record_count,
            validate=validate,
            with_raw=with_raw,
            as_dict=as_dict,
        )


def _iter_partition_stream(
    stream: Any,
    *,
    partition_id: str,
    record_count: int,
    validate: bool,
    with_raw: bool,
    as_dict: bool = False,
) -> Iterator[Any]:
    previous: bytes | None = None
    count = 0
    while raw := stream.readline(MAX_CATALOG_ROW_BYTES + 2):
        if len(raw) > MAX_CATALOG_ROW_BYTES + 1:
            raise LimitExceededError("source-catalog row exceeds its byte limit")
        if not raw.endswith(b"\n"):
            raise IntegrityError("source-catalog rows must end with a newline")
        if validate:
            try:
                value = parse_canonical_json(
                    raw[:-1],
                    path=f"source-items/{partition_id}/{count}",
                )
            except ArtifactVerificationError as error:
                raise IntegrityError(
                    f"source-catalog row {count} is not canonical: {error}"
                ) from error
            _ITEM_VALIDATOR.error(value, f"source-catalog row {count}")
        else:
            # An unvalidated pass only re-reads bytes that a validated pass of
            # the same derivation (or the producer gate) proves canonical and
            # schema-conformant; plain parsing avoids re-serializing every row.
            value = json.loads(raw[:-1])
        if validate:
            _require_interpretation_order(value)
        if as_dict:
            source_item_id = value["sourceItemId"]
            if not isinstance(source_item_id, str):
                raise IntegrityError(
                    f"source-catalog row {count} is invalid: sourceItemId must be text"
                )
            item: Any = value
        else:
            try:
                if validate:
                    item = SourceCatalogItem.from_dict(value)
                else:
                    # The bytes were admitted by a verifier already (this is
                    # the unvalidated re-stream); construct by wrapping alone.
                    with trusted_json_input():
                        item = SourceCatalogItem.from_dict(value)
            except (TypeError, ValueError) as error:
                raise IntegrityError(f"source-catalog row {count} is invalid: {error}") from error
            source_item_id = item.source_item_id
        if _partition_id(source_item_id) != partition_id:
            raise IntegrityError("source-catalog row is stored in the wrong logical partition")
        key = _utf16_key(source_item_id)
        if previous is not None and key <= previous:
            raise IntegrityError("source-catalog partition rows must be strictly ordered and distinct")
        previous = key
        count += 1
        yield (item, raw[:-1]) if with_raw else item
    if count != record_count:
        raise IntegrityError("source-catalog row count differs from its partition descriptor")


def _iter_catalog_rows(
    blob_source: SourceCatalogBlobSource,
    partitions: Sequence[_CatalogPartition],
    expected_count: int,
    *,
    validate: bool = True,
    with_raw: bool = False,
    as_dict: bool = False,
) -> Iterator[Any]:
    rows = _iter_located_catalog_rows(
        blob_source,
        partitions,
        expected_count,
        validate=validate,
        with_raw=with_raw,
        as_dict=as_dict,
    )
    if with_raw:
        for located, raw in rows:
            yield located.item, raw
    else:
        for located in rows:
            yield located.item


def _iter_located_catalog_rows(
    blob_source: SourceCatalogBlobSource,
    partitions: Sequence[_CatalogPartition],
    expected_count: int,
    *,
    validate: bool = True,
    with_raw: bool = False,
    as_dict: bool = False,
) -> Iterator[Any]:
    """Attach each parsed row to its supplying partition without reparsing it."""

    streams = [
        iter(
            _iter_partition_rows(
                blob_source, partition, validate=validate, with_raw=with_raw, as_dict=as_dict
            )
        )
        for partition in partitions
    ]
    heap: list[tuple[bytes, int, Any]] = []
    previous: bytes | None = None
    count = 0
    try:
        def entry_id(entry: Any) -> str:
            row = entry[0] if with_raw else entry
            return row["sourceItemId"] if as_dict else row.source_item_id

        for index, stream in enumerate(streams):
            entry = next(stream, None)
            if entry is not None:
                heapq.heappush(heap, (_utf16_key(entry_id(entry)), index, entry))
        while heap:
            key, index, entry = heapq.heappop(heap)
            item = entry[0] if with_raw else entry
            if previous is not None and key <= previous:
                raise IntegrityError("source-catalog rows must be globally ordered and distinct")
            previous = key
            blob_ref = partitions[index].member.blob_ref
            if blob_ref is None:
                raise IntegrityError("source-item partition descriptor requires blobRef")
            count += 1
            if with_raw:
                yield LocatedSourceCatalogItem(item, blob_ref), entry[1]
            else:
                yield LocatedSourceCatalogItem(item, blob_ref)
            following = next(streams[index], None)
            if following is not None:
                heapq.heappush(heap, (_utf16_key(entry_id(following)), index, following))
    finally:
        for stream in streams:
            stream.close()
    if count != expected_count:
        raise IntegrityError("source-catalog row count differs from its partition descriptors")


def _framed_digest(domain: str, name: str, count: int, records: Iterable[object]) -> str:
    try:
        return framed_section_digest(domain, (FramedSection(name, count, records),))
    except (TypeError, ValueError) as error:
        raise IntegrityError(f"cannot compute {domain}: {error}") from error


def requested_universe_set_digest(
    count: int,
    sorted_source_item_ids: Iterable[str],
) -> str:
    """Digest one bounded, UTF-16-ordered requested-universe identity stream."""

    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("requested-universe count must be a non-negative integer")

    def records() -> Iterator[Mapping[str, str]]:
        previous: str | None = None
        for raw_identity in sorted_source_item_ids:
            identity = _text(raw_identity, "requested-universe sourceItemId")
            if previous is not None and _utf16_key(identity) <= _utf16_key(previous):
                raise IntegrityError("requested-universe identities must be sorted and distinct")
            previous = identity
            yield {"sourceItemId": identity}

    return _framed_digest("docspec-requested-universe-set/1", "members", count, records())


def selected_source_set_digest(
    count: int,
    sorted_members: Iterable[tuple[str, str]],
) -> str:
    """Digest one bounded, UTF-16-ordered selected source/document stream."""

    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("selected-source count must be a non-negative integer")

    def records() -> Iterator[Mapping[str, str]]:
        previous: tuple[bytes, bytes] | None = None
        for raw_source_item_id, raw_document_id in sorted_members:
            source_item_id = _text(raw_source_item_id, "selected-source sourceItemId")
            document_id = _text(raw_document_id, "selected-source documentId")
            key = (_utf16_key(source_item_id), _utf16_key(document_id))
            if previous is not None and key <= previous:
                raise IntegrityError("selected-source members must be sorted and distinct")
            previous = key
            yield {"sourceItemId": source_item_id, "documentId": document_id}

    return _framed_digest("docspec-selected-source-set/1", "members", count, records())


def _source_system_set_digest(descriptions: Sequence[SourceNativeDescription]) -> str:
    rows = tuple(
        sorted(
            (
                {
                    "sourceSystemId": value.source_system_id,
                    "sourceSystemVersion": value.source_system_version,
                    "logicalDigest": value.logical_id.rsplit(":", 1)[-1],
                    "sourceStateScope": value.source_state_scope,
                    "sourceStateDigest": value.source_state_digest,
                    "sourceNativeSchemaSetDigest": value.source_native_schema_set_digest,
                }
                for value in descriptions
            ),
            key=lambda value: (
                _utf16_key(value["sourceSystemId"]),
                _utf16_key(value["sourceSystemVersion"]),
                _utf16_key(value["logicalDigest"]),
            ),
        )
    )
    keys = [
        (value["sourceSystemId"], value["sourceSystemVersion"], value["logicalDigest"])
        for value in rows
    ]
    if len(keys) != len(set(keys)):
        raise IntegrityError("source-native inputs contain a duplicate logical source system")
    return _framed_digest("docspec-source-system-set/1", "sources", len(rows), rows)


def _source_schema_set_digest(descriptions: Sequence[SourceNativeDescription]) -> str:
    rows = tuple(
        sorted(
            (
                {
                    "sourceSystemId": value.source_system_id,
                    "sourceSystemVersion": value.source_system_version,
                    "sourceNativeSchemaSetDigest": value.source_native_schema_set_digest,
                }
                for value in descriptions
            ),
            key=lambda value: (
                _utf16_key(value["sourceSystemId"]),
                _utf16_key(value["sourceSystemVersion"]),
                _utf16_key(value["sourceNativeSchemaSetDigest"]),
            ),
        )
    )
    return _framed_digest("docspec-source-native-schema-set/1", "schemas", len(rows), rows)


def _source_rows(source: SourceNativeRecordSource) -> Iterator[tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]]:
    renditions = iter(source.iter_renditions())
    next_rendition = next(renditions, None)
    previous_record_id: str | None = None
    # Ordering keys, not the text: renditions are ordered across the whole
    # stream, so the previous key's encoding outlives the record it belonged to.
    previous_rendition_order: tuple[bytes, bytes] | None = None

    def checked_rendition(value: object) -> tuple[Mapping[str, Any], tuple[str, str]]:
        item = _mapping(value, "source-native rendition")
        fields = set(item)
        if fields != _SOURCE_RENDITION_REQUIRED_FIELDS:
            raise IntegrityError("source-native rendition has an invalid closed shape")
        key = (
            _text(item["sourceRecordId"], "source-native rendition sourceRecordId"),
            _text(item["renditionId"], "source-native rendition renditionId"),
        )
        _text(item["sourceField"], "source-native rendition sourceField")
        _text(item["mediaType"], "source-native rendition mediaType")
        locator = item["locator"]
        if locator is not None:
            _text(locator, "source-native rendition locator")
        expected_digest = item["expectedSha256"]
        if expected_digest is not None:
            try:
                require_sha256(expected_digest, "source-native rendition expectedSha256")
            except ValueError as error:
                raise IntegrityError(str(error)) from error
        expected_size = item["expectedByteSize"]
        if expected_size is not None and (
            isinstance(expected_size, bool)
            or not isinstance(expected_size, int)
            or expected_size < 0
        ):
            raise IntegrityError("source-native rendition expectedByteSize must be null or non-negative")
        return item, key

    for raw_record in source.iter_records():
        record = _mapping(raw_record, "source-native record")
        if set(record) != _SOURCE_RECORD_FIELDS:
            raise IntegrityError("source-native record has an invalid closed shape")
        record_id = _text(record["sourceRecordId"], "source-native sourceRecordId")
        _text(record["scopeId"], "source-native scopeId")
        _text(record["schemaName"], "source-native schemaName")
        _text(record["schemaVersion"], "source-native schemaVersion")
        try:
            require_sha256(record["schemaDigest"], "source-native schemaDigest")
        except ValueError as error:
            raise IntegrityError(str(error)) from error
        _mapping(record["record"], "source-native record payload")
        if not isinstance(record["fieldDiagnostics"], list):
            raise IntegrityError("source-native fieldDiagnostics must be an array")
        record_order = _utf16_key(record_id)
        if previous_record_id is not None and record_order <= _utf16_key(previous_record_id):
            raise IntegrityError("source-native records must be strictly ordered by sourceRecordId")
        previous_record_id = record_id
        selected: list[Mapping[str, Any]] = []
        selected_bytes = 0
        while next_rendition is not None:
            rendition, key = checked_rendition(next_rendition)
            key_order = (_utf16_key(key[0]), _utf16_key(key[1]))
            if previous_rendition_order is not None and key_order <= previous_rendition_order:
                raise IntegrityError("source-native renditions must be strictly ordered")
            if key_order[0] < record_order:
                raise IntegrityError("source-native rendition has no matching record")
            if key_order[0] > record_order:
                break
            previous_rendition_order = key_order
            if len(selected) >= MAX_SOURCE_RENDITIONS_PER_RECORD:
                raise LimitExceededError(
                    "source-native rendition count exceeds its per-record limit"
                )
            rendition_bytes = len(canonical_json_bytes(rendition))
            if selected_bytes + rendition_bytes > MAX_SOURCE_RENDITION_BYTES_PER_RECORD:
                raise LimitExceededError(
                    "source-native rendition bytes exceed their per-record limit"
                )
            selected_bytes += rendition_bytes
            selected.append(rendition)
            next_rendition = next(renditions, None)
        yield record, tuple(selected)
    if next_rendition is not None:
        raise IntegrityError("source-native rendition has no matching record")


_RESUME_NAMESPACE = "source-catalog/resume"


class _ResumeLedger:
    """Every resume point of one build, kept in the workspace it describes.

    A durable workspace outlives a killed process, and SQLite discards whatever
    followed the last commit when it is next opened, so this ledger is the whole
    of what a resumed build may trust: the identity the workspace was staged
    under (``open`` refuses any other), which inputs finished loading, whether
    the policy's pre-pass finished, and the last committed batch of staged
    items with the counts the receipt will need. Every mark commits, so the
    ledger never claims more than the file holds.

    A workspace with no ``commit`` cannot survive a kill; its ledger records
    nothing and reports a fresh build, so the temporary-workspace path is the
    build it always was.
    """

    def __init__(self, workspace: CatalogPolicyWorkspace) -> None:
        self._workspace = workspace
        self._commit = getattr(workspace, "commit", None)
        self.point = FRESH_BUILD
        self.cursor_state: Mapping[str, Any] | None = None
        self.staged_state: Mapping[str, Any] | None = None

    def _get(self, key: tuple[str, ...]) -> Mapping[str, Any] | None:
        return self._workspace.get(_RESUME_NAMESPACE, key)

    def _mark(self, key: tuple[str, ...], value: Mapping[str, Any]) -> None:
        if self._commit is None:
            return
        if self._get(key) is None:
            self._workspace.put(_RESUME_NAMESPACE, key, value)
        else:
            self._workspace.replace(_RESUME_NAMESPACE, key, value)
        self._commit()

    def open(self, identity: Mapping[str, Any]) -> CatalogResumePoint:
        """Bind the workspace to this build, or resume the one it already holds.

        ``identity`` is everything that decides the rows: catalog id, policy
        digest, schema digest, producer, and the inputs in the order given.
        Order matters because staged rows carry their source index.
        """

        if self._commit is None:
            return self.point
        stored = self._get(("build",))
        if stored is None:
            self._mark(("build",), identity)
            return self.point
        if canonical_json_bytes(stored) != canonical_json_bytes(identity):
            raise IntegrityError("catalog workspace was staged by a different build")
        self.staged_state = self._get(("staged",))
        self.cursor_state = None if self.staged_state is not None else self._get(("cursor",))
        state = self.staged_state if self.staged_state is not None else self.cursor_state
        self.point = CatalogResumePoint(
            indexed=self._get(("indexed",)) is not None,
            after=None if state is None else state["after"],
            selected_count=0 if state is None else int(state["selectedCount"]),
        )
        return self.point

    def input_loaded(self, source_index: int) -> bool:
        return self._commit is not None and self._get(("input", str(source_index))) is not None

    def mark_input(self, source_index: int, logical_id: str) -> None:
        self._mark(
            ("input", str(source_index)),
            {"sourceIndex": source_index, "logicalId": logical_id},
        )

    def mark_indexed(self) -> None:
        self._mark(("indexed",), {"indexed": True})

    def mark_cursor(self, state: Mapping[str, Any]) -> None:
        self._mark(("cursor",), state)

    def mark_staged(self, state: Mapping[str, Any]) -> None:
        self._mark(("staged",), state)


class _CatalogPolicyInputs:
    """Validate each selected source once and account for the complete universe."""

    def __init__(
        self,
        sources: Sequence[SourceNativeRecordSource],
        descriptions: Sequence[SourceNativeDescription],
        universe_inputs: Sequence[SourceInputSelector],
        workspace: CatalogPolicyWorkspace,
        policy: SourceCatalogPolicy | None = None,
        ledger: "_ResumeLedger | None" = None,
    ) -> None:
        self._policy = policy
        self._ledger = ledger if ledger is not None else _ResumeLedger(workspace)
        self._sources = tuple(sources)
        self._descriptions = tuple(descriptions)
        self._universe_inputs = tuple(universe_inputs)
        if not self._universe_inputs:
            raise ValueError("catalog policy must declare at least one universe input")
        if len(self._universe_inputs) != len(set(self._universe_inputs)):
            raise ValueError("catalog policy universe inputs must be distinct")
        self._workspace = workspace
        self._loaded = False
        self._opened: collections.Counter[SourceInputSelector] = collections.Counter()
        self._universe_passes = 0
        self._completed: set[SourceInputSelector] = set()


    @property
    def descriptions(self) -> tuple[SourceNativeDescription, ...]:
        return self._descriptions

    @property
    def resume(self) -> CatalogResumePoint:
        return self._ledger.point

    @staticmethod
    def _namespace(selector: SourceInputSelector) -> str:
        digest = sha256_digest(canonical_json_bytes(selector.to_dict()))
        return f"{_SOURCE_ROW_NAMESPACE_PREFIX}{digest}"

    def _load(self) -> None:
        if self._loaded:
            return
        for source_index, (source, description) in enumerate(
            zip(self._sources, self._descriptions, strict=True)
        ):
            # A resumed build does not read an input it already loaded: the
            # ledger bound this workspace to the same inputs in the same order,
            # so the staged rows are the bytes admission checked, read once,
            # earlier. Keyed by position, not logical id: two inputs may share
            # a logical id (the cross-file fixtures do) and each is its own read.
            if self._ledger.input_loaded(source_index):
                continue
            for record, renditions in _source_rows(source):
                selector = SourceInputSelector(
                    description.source_system_id,
                    description.source_system_version,
                    record["scopeId"],
                    record["schemaName"],
                    record["schemaVersion"],
                )
                namespace = self._namespace(selector)
                incoming = {
                    "sourceIndex": source_index,
                    "record": dict(record),
                    "renditions": [dict(value) for value in renditions],
                }
                try:
                    self._workspace.put(namespace, (record["sourceRecordId"],), incoming)
                except IntegrityError as error:
                    self._resolve_repeat(namespace, selector, incoming, error)
            self._ledger.mark_input(source_index, description.logical_id)
        self._loaded = True

    def _resolve_repeat(
        self,
        namespace: str,
        selector: SourceInputSelector,
        incoming: Mapping[str, Any],
        error: IntegrityError,
    ) -> None:
        """Let the policy own a repeated sourceRecordId, or keep the refusal.

        Reached only when ``put`` has already refused, so a corpus with no
        repeats pays nothing for this: the lookup and the resolution are on the
        exception path, not per row. Measured over the 670 non-Federal-Register
        catalog-A releases, exactly two of 2,221,713 records reach it.

        A policy that does not implement ``resolve_source_record_collision``
        keeps the refusal it has today, unchanged and with the same message.
        The capability is read structurally rather than declared on the
        protocol because absence has to mean "refuse as before" for every
        policy that has not thought about it -- a default on the protocol would
        silently opt them all in.
        """

        resolver = getattr(self._policy, "resolve_source_record_collision", None)
        stored = self._workspace.get(namespace, (incoming["record"]["sourceRecordId"],))
        resolution = (
            None
            if resolver is None or stored is None
            else resolver(selector, stored, incoming)
        )
        if resolution is None:
            raise IntegrityError(
                "source-native inputs repeat a sourceRecordId for one policy selector"
            ) from error
        owner = dict(resolution.owner)
        discarded = dict(resolution.discarded)
        owner["discardedFilings"] = [
            *owner.get("discardedFilings", ()),
            {
                "reasonCode": resolution.reason_code,
                "reason": resolution.reason,
                "record": discarded["record"],
                "renditions": discarded["renditions"],
            },
        ]
        self._workspace.replace(namespace, (owner["record"]["sourceRecordId"],), owner)

    def _ensure_available(self, selector: SourceInputSelector) -> None:
        if not any(
            description.source_system_id == selector.source_system_id
            and description.source_system_version == selector.source_system_version
            for description in self._descriptions
        ):
            raise IntegrityError("catalog policy source input selector matched no source-native input")

    def _row(self, value: Mapping[str, Any]) -> SourceNativeRow:
        if set(value) - {"discardedFilings"} != {"sourceIndex", "record", "renditions"}:
            raise IntegrityError("catalog policy workspace source row has an invalid closed shape")
        source_index = value["sourceIndex"]
        if (
            isinstance(source_index, bool)
            or not isinstance(source_index, int)
            or source_index < 0
            or source_index >= len(self._descriptions)
        ):
            raise IntegrityError("catalog policy workspace source index is invalid")
        record = _mapping(value["record"], "catalog policy workspace source record")
        raw_renditions = value["renditions"]
        if not isinstance(raw_renditions, list):
            raise IntegrityError("catalog policy workspace renditions must be an array")
        renditions = tuple(
            _mapping(raw, "catalog policy workspace rendition") for raw in raw_renditions
        )
        # Absent is the overwhelming case -- all but two of 2,221,713 records in
        # the 670 non-Federal-Register catalog-A releases -- so the default has
        # to satisfy the same
        # check a present value does, not merely be falsy.
        discarded = value.get("discardedFilings", [])
        if not isinstance(discarded, list):
            raise IntegrityError("catalog policy workspace discarded filings must be an array")
        return SourceNativeRow(
            self._descriptions[source_index],
            record,
            renditions,
            tuple(
                _mapping(item, "catalog policy workspace discarded filing")
                for item in discarded
            ),
        )

    def _rows(
        self,
        selector: SourceInputSelector,
        *,
        after: str | None = None,
    ) -> Iterator[SourceNativeRow]:
        self._ensure_available(selector)
        if self._opened[selector] >= MAX_ORDERED_PASSES:
            raise IntegrityError(
                "catalog policy attempted to read one selected input more than twice"
            )
        self._opened[selector] += 1
        self._load()
        previous: str | None = None
        for value in self._workspace.iter_ordered(
            self._namespace(selector), after=None if after is None else (after,)
        ):
            row = self._row(value)
            if (
                row.description.source_system_id != selector.source_system_id
                or row.description.source_system_version != selector.source_system_version
                or row.record["scopeId"] != selector.scope_id
                or row.record["schemaName"] != selector.schema_name
                or row.record["schemaVersion"] != selector.schema_version
            ):
                raise IntegrityError("catalog policy workspace returned a row for another selector")
            source_item_id = row.record["sourceRecordId"]
            if previous is not None and _utf16_key(source_item_id) <= _utf16_key(previous):
                raise IntegrityError("catalog policy workspace source rows are not sorted and distinct")
            previous = source_item_id
            yield row
        self._completed.add(selector)

    def iter_universe_rows(self) -> Iterator[SourceNativeRow]:
        if self._universe_passes >= MAX_ORDERED_PASSES:
            raise IntegrityError("catalog policy attempted to read the universe more than twice")
        self._universe_passes += 1
        resume = self._ledger.point
        first_pass = self._universe_passes == 1
        # Accounting is a property of the universe, written by the first
        # ordered pass. A resumed run's first pass starts past the cursor, and
        # whether the rows beyond it are already accounted depends on the
        # policy: a two-pass policy accounted the whole universe in the
        # pre-pass it committed, a one-pass policy accounted exactly the rows
        # it staged. So a resumed pass checks before it writes, and only a
        # resumed pass pays that lookup.
        resuming = resume.indexed or resume.after is not None
        streams = [
            iter(self._rows(selector, after=resume.after))
            for selector in self._universe_inputs
        ]
        heap: list[tuple[bytes, int, SourceNativeRow]] = []
        try:
            for index, stream in enumerate(streams):
                row = next(stream, None)
                if row is not None:
                    heapq.heappush(
                        heap,
                        (_utf16_key(str(row.record["sourceRecordId"])), index, row),
                    )
            previous: str | None = None
            while heap:
                _, index, row = heapq.heappop(heap)
                source_item_id = str(row.record["sourceRecordId"])
                if previous is not None and _utf16_key(source_item_id) <= _utf16_key(previous):
                    raise IntegrityError(
                        "catalog policy universe sourceItemId values are not globally distinct"
                    )
                previous = source_item_id
                # Accounting records which ids the universe contained, which is
                # a property of the universe rather than of a pass over it. The
                # first pass establishes it; a later ordered read must not
                # write it again, and re-deriving the same rows is exactly what
                # the duplicate-key refusal is for.
                if first_pass and not (
                    resuming
                    and self._workspace.get(_UNIVERSE_ACCOUNTING_NAMESPACE, (source_item_id,))
                    is not None
                ):
                    self._workspace.put(
                        _UNIVERSE_ACCOUNTING_NAMESPACE,
                        (source_item_id,),
                        {"sourceItemId": source_item_id},
                    )
                yield row
                following = next(streams[index], None)
                if following is not None:
                    heapq.heappush(
                        heap,
                        (
                            _utf16_key(str(following.record["sourceRecordId"])),
                            index,
                            following,
                        ),
                    )
        finally:
            for stream in streams:
                stream.close()

    def iter_lookup_rows(self, selector: SourceInputSelector) -> Iterator[SourceNativeRow]:
        if selector in self._universe_inputs:
            raise IntegrityError("catalog policy lookup input must differ from its universe input")
        yield from self._rows(selector)

    def finish(self) -> None:
        if not self._universe_passes or not set(self._universe_inputs).issubset(self._completed):
            raise IntegrityError("catalog policy did not read every declared universe input")
        # Compares which inputs were opened against which were drained, not how
        # many times each was read: `_opened` counts passes now, and a second
        # ordered pass is permitted. An input opened and abandoned partway is
        # still the defect this catches.
        if set(self._opened) != self._completed:
            raise IntegrityError("catalog policy did not fully consume every selected source input")


def _policy_rows(
    sources: Sequence[SourceNativeRecordSource],
    descriptions: Sequence[SourceNativeDescription],
    policy: SourceCatalogPolicy,
    policy_digest: str,
    workspace: CatalogPolicyWorkspace,
    ledger: _ResumeLedger,
) -> Iterator[SourceCatalogItem]:
    inputs: CatalogPolicyInputs = _CatalogPolicyInputs(
        sources,
        descriptions,
        policy.universe_inputs,
        workspace,
        policy,
        ledger=ledger,
    )
    for item in policy.iter_items(inputs, workspace):
        for interpretation in item.interpretations:
            if (
                interpretation.get("policyId") != policy.policy_id
                or interpretation.get("policyVersion") != policy.policy_version
                or interpretation.get("policyDigest") != policy_digest
            ):
                raise IntegrityError("catalog interpretation differs from the installed policy pin")
        yield item
    inputs.finish()
    universe = workspace.iter_ordered(_UNIVERSE_ACCOUNTING_NAMESPACE)
    output = workspace.iter_ordered(_OUTPUT_ACCOUNTING_NAMESPACE)
    for expected, actual in zip_longest(universe, output):
        if expected != actual:
            raise IntegrityError("catalog policy output does not account for its complete universe")


class _DispositionTally:
    """Count rows by disposition and by (disposition, reasonCode).

    One tally serves the build, both derivations and their merge, so the two
    count sections a receipt reconciles come from one rule. It pickles across
    the parallel derivation's worker boundary as a plain object.
    """

    def __init__(self) -> None:
        self.dispositions = {value.value: 0 for value in CatalogDisposition}
        self.reasons: dict[tuple[str, str], int] = {}

    def add(self, disposition: str, reason_code: str | None) -> None:
        self.dispositions[disposition] += 1
        if reason_code is not None:
            key = (disposition, reason_code)
            self.reasons[key] = self.reasons.get(key, 0) + 1

    def merge(self, other: _DispositionTally) -> None:
        for name, value in other.dispositions.items():
            self.dispositions[name] += value
        for key, value in other.reasons.items():
            self.reasons[key] = self.reasons.get(key, 0) + value

    def to_state(self) -> dict[str, object]:
        return {
            "dispositions": dict(self.dispositions),
            "reasons": [[d, c, n] for (d, c), n in sorted(self.reasons.items())],
        }

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> _DispositionTally:
        tally = cls()
        tally.dispositions = {str(k): int(v) for k, v in state["dispositions"].items()}
        tally.reasons = {(str(d), str(c)): int(n) for d, c, n in state["reasons"]}
        return tally

    def reason_counts(self) -> list[dict[str, object]]:
        """Return the receipt's ``reasonCounts`` rows in their sealed order."""

        return [
            {"disposition": disposition, "reasonCode": reason_code, "count": count}
            for (disposition, reason_code), count in sorted(
                self.reasons.items(),
                key=lambda item: (_utf16_key(item[0][0]), _utf16_key(item[0][1])),
            )
        ]


def _reconcile_reason_counts(rows: Sequence[Mapping[str, Any]], counts: Mapping[str, int]) -> None:
    """Require ordered, distinct reason rows that sum to every non-selected bucket.

    The schema already closes each row's shape, keeps ``selected`` out of the
    enum and requires a positive count; this is the cross-section arithmetic
    the schema cannot express.
    """

    previous: tuple[bytes, bytes] | None = None
    totals = {name: 0 for name in counts}
    for row in rows:
        key = (_utf16_key(row["disposition"]), _utf16_key(row["reasonCode"]))
        if previous is not None and key <= previous:
            raise IntegrityError("catalog build receipt reason counts must be ordered and distinct")
        previous = key
        totals[row["disposition"]] += row["count"]
    for name, total in totals.items():
        if name != _SELECTED_DISPOSITION and total != counts[name]:
            raise IntegrityError(
                "catalog build receipt reason counts do not account for every non-selected row"
            )


class _CatalogRowPartitioner:
    def __init__(
        self,
        rows: Iterable[SourceCatalogItem],
        *,
        ledger: _ResumeLedger,
        batch_items: int,
    ) -> None:
        if batch_items < 1:
            raise ValueError("resume batch size must be at least one item")
        self._rows = rows
        self._ledger = ledger
        self._batch_items = batch_items
        self.item_count = 0
        self.tally = _DispositionTally()
        self.disposition_counts = self.tally.dispositions
        self.selected_count = 0
        self.partition_counts: dict[str, int] = {}
        self.last_item_id: str | None = None

    def state(self) -> dict[str, Any]:
        """Everything a resumed run restores instead of recomputing.

        The producer gate re-derives all of it from the staged rows before
        publication, so a stale or tampered state fails closed there.
        """

        return {
            "after": self.last_item_id,
            "itemCount": self.item_count,
            "selectedCount": self.selected_count,
            "partitionCounts": dict(self.partition_counts),
            **self.tally.to_state(),
        }

    def restore(self, state: Mapping[str, Any]) -> None:
        self.last_item_id = None if state["after"] is None else str(state["after"])
        self.item_count = int(state["itemCount"])
        self.selected_count = int(state["selectedCount"])
        self.partition_counts = {str(k): int(v) for k, v in state["partitionCounts"].items()}
        self.tally = _DispositionTally.from_state(state)
        self.disposition_counts = self.tally.dispositions

    def stage(self, workspace: CatalogPolicyWorkspace) -> None:
        previous: str | None = self.last_item_id
        started = False
        for item in self._rows:
            if not started:
                started = True
                # The policy's pre-pass is complete once it yields; commit it
                # on its own so a kill during the first batch resumes here.
                if not self._ledger.point.indexed:
                    self._ledger.mark_indexed()
            if previous is not None and _utf16_key(item.source_item_id) <= _utf16_key(previous):
                raise IntegrityError("catalog policy produced duplicate or out-of-order sourceItemId values")
            previous = item.source_item_id
            value = item.to_dict()
            _ITEM_VALIDATOR.error(value, f"source-catalog row {self.item_count}")
            _require_interpretation_order(value)
            payload = canonical_json_bytes(value)
            if len(payload) > MAX_CATALOG_ROW_BYTES:
                raise LimitExceededError("source-catalog row exceeds its byte limit")
            self.item_count += 1
            self.tally.add(item.disposition.value, item.selection.reason_code)
            if item.disposition is CatalogDisposition.SELECTED:
                self.selected_count += 1
            selected_partition = _partition_id(item.source_item_id)
            put_payload = getattr(workspace, "put_payload", None)
            if put_payload is not None:
                put_payload(
                    _partition_namespace(selected_partition),
                    (item.source_item_id,),
                    payload,
                )
            else:
                workspace.put(
                    _partition_namespace(selected_partition),
                    (item.source_item_id,),
                    value,
                )
            self.partition_counts[selected_partition] = self.partition_counts.get(selected_partition, 0) + 1
            # Accounting sits beside the payload so every commit holds both
            # for exactly the same items; a kill can never leave them apart.
            workspace.put(
                _OUTPUT_ACCOUNTING_NAMESPACE,
                (item.source_item_id,),
                {"sourceItemId": item.source_item_id},
            )
            self.last_item_id = item.source_item_id
            if self.item_count % self._batch_items == 0:
                self._ledger.mark_cursor(self.state())

    @staticmethod
    def chunks(workspace: CatalogPolicyWorkspace, partition_id: str) -> Iterator[bytes]:
        iter_payloads = getattr(workspace, "iter_payloads", None)
        if iter_payloads is not None:
            # The workspace stores exactly the canonical bytes stage() produced;
            # streaming them verbatim avoids a parse and a re-serialization per
            # row per read, and the artifact's own gates re-verify every row.
            for payload in iter_payloads(_partition_namespace(partition_id)):
                yield payload + b"\n"
            return
        for value in workspace.iter_ordered(_partition_namespace(partition_id)):
            yield canonical_json_bytes(value) + b"\n"


def _measure_blob(chunks: Iterable[bytes]) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_size = 0
    for chunk in chunks:
        if not isinstance(chunk, bytes):
            raise TypeError("source-catalog blob measurements require bytes")
        digest.update(chunk)
        byte_size += len(chunk)
    return "sha256:" + digest.hexdigest(), byte_size


_SELECTED_DISPOSITION = CatalogDisposition.SELECTED.value
#: Ordered reads one policy may make of the rows the loader staged. Source
#: artifacts are still read exactly once -- ``_load`` guarantees that
#: independently -- and this bounds only re-reads of builder-owned SQLite,
#: which cannot go stale under us. Two: one pass to build lookup indexes, one
#: to emit items.
MAX_ORDERED_PASSES = 2

_PARALLEL_ROW_THRESHOLD = 5_000
_MAX_DERIVE_WORKERS = 8
#: How long the worker probe waits before the derivation gives up on workers.
#: A healthy pool answers in 0.13 s with eight workers, since Pool starts them
#: lazily and the first one to come up replies, so this is ~460x headroom. It
#: is not a performance knob: it only elapses when workers cannot run at all,
#: and then the serial derivation still produces the identical result.
_PARALLEL_PROBE_TIMEOUT_SECONDS = 60.0


def _derive_worker_count(item_count: int, workers: int | None) -> int:
    if workers is not None:
        return max(1, workers)
    if item_count < _PARALLEL_ROW_THRESHOLD:
        return 1
    return max(1, min(_MAX_DERIVE_WORKERS, os.cpu_count() or 1))


def _row_digest_payloads(
    row: Mapping[str, Any],
    raw: bytes,
) -> tuple[
    bytes,
    bytes,
    bytes | None,
    bytes,
    bytes,
    bytes,
    list[bytes],
    list[tuple[bytes, str, str]],
    list[bytes],
]:
    """Build every framed payload one row contributes, in one place.

    The serial engine and the parallel workers both call this, so the bytes a
    digest consumes cannot depend on which path derived them.
    """

    source_item_id = row["sourceItemId"]
    disposition = row["selection"]["disposition"]
    selected_payload = (
        _canonical_record_payload({"sourceItemId": source_item_id, "documentId": row["documentId"]})
        if disposition == _SELECTED_DISPOSITION
        else None
    )
    joined: list[tuple[bytes, str, str]] = []
    for record in _joined_field_records_for(row):
        # Carry the identity and outcome out with the payload. The worker used
        # to recover joinId by json.loads-ing bytes it had just serialized from
        # a record that held it.
        joined.append(
            (_canonical_record_payload(record), str(record["outcome"]), str(record["joinId"]))
        )
    return (
        raw,
        _canonical_record_payload({"sourceItemId": source_item_id}),
        selected_payload,
        _canonical_record_payload({"sourceItemId": source_item_id, "disposition": disposition}),
        _canonical_record_payload(
            {"sourceItemId": source_item_id, "reason": row["selection"]["reason"]}
        ),
        _canonical_record_payload(_rendition_choice_record(row)),
        [_canonical_record_payload(r) for r in _normalized_field_records_for(row)],
        joined,
        [_canonical_record_payload(r) for r in _interpretation_records_for(row)],
    )


def _parallel_probe() -> bool:
    """A no-op worker task proving this interpreter can host spawned workers."""

    return True


def _derive_pool_context() -> Any:
    """The spawn context derive pools start from; a seam for tests.

    Named the way SpicySearch names the same seam in its snapshot build, so a
    test can observe what crosses the process boundary without reaching into
    :mod:`multiprocessing`.
    """

    import multiprocessing

    return multiprocessing.get_context("spawn")


def _derive_partition_worker(
    args: tuple[str, Any, int, bool, str],
) -> tuple[str, str, int, _DispositionTally, dict[str, dict[str, int]], int, int, int]:
    """Process one partition's rows in a subprocess and spill ordered payloads.

    Returns (partition_id, spill_path, row_count, tally,
    join_counts, normalized_count, joined_count, interpretation_count). The
    spill holds one pickled payload tuple per row, in partition order; global
    ordering across partitions is enforced by the parent's merge.

    ``args[1]`` is a duplicated descriptor for the partition blob, already
    opened through the parent's pinned blob source, so the rows arrive as a
    stream this worker reads at its own pace rather than as a bytes copy of the
    whole partition. See :func:`_derive_catalog_parallel` for why.
    """

    partition_id, blob, record_count, validate, spill_dir = args
    spill_path = os.path.join(spill_dir, f"{partition_id}.rows")
    tally = _DispositionTally()
    join_counts: dict[str, dict[str, int]] = {}
    normalized_count = 0
    joined_count = 0
    interpretation_count = 0
    rows = 0
    descriptor = blob.detach()
    # A duplicated descriptor shares its file offset with the one it came from,
    # so start from the beginning rather than from wherever the parent left it.
    os.lseek(descriptor, 0, os.SEEK_SET)
    with open(spill_path, "wb") as spill, os.fdopen(descriptor, "rb") as payload:
        for row, raw in _iter_partition_stream(
            payload,
            partition_id=partition_id,
            record_count=record_count,
            validate=validate,
            with_raw=True,
            as_dict=True,
        ):
            payloads = _row_digest_payloads(row, raw)
            tally.add(row["selection"]["disposition"], row["selection"]["reasonCode"])
            normalized_count += len(payloads[6])
            for _record_bytes, outcome, join_id in payloads[7]:
                joined_count += 1
                _accumulate_join_coverage(join_counts, {"joinId": join_id, "outcome": outcome})
            interpretation_count += len(payloads[8])
            pickle.dump(
                (_utf16_key(row["sourceItemId"]), payloads),
                spill,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            rows += 1
    return (
        partition_id,
        spill_path,
        rows,
        tally,
        join_counts,
        normalized_count,
        joined_count,
        interpretation_count,
    )


def _iter_spill(spill_path: str) -> Iterator[tuple[bytes, tuple[Any, ...]]]:
    with open(spill_path, "rb") as spill:
        while True:
            try:
                yield pickle.load(spill)
            except EOFError:
                return


@dataclass(frozen=True, slots=True)
class _DerivedCatalog:
    """Every digest and diagnostic one derivation pass proves about the rows.

    ``diagnostics`` holds ``joinCoverage`` plus the six
    ``_DIAGNOSTIC_DIGEST_FIELDS`` fingerprints; see that tuple's comment for
    why those six are not published-member references.
    """

    catalog_state_digest: str
    requested_universe_set_digest: str
    selected_source_set_digest: str
    disposition_counts: dict[str, int]
    reason_counts: list[dict[str, object]]
    diagnostics: dict[str, object]


def _derive_catalog(
    blob_source: SourceCatalogBlobSource,
    partitions: Sequence[_CatalogPartition],
    *,
    item_count: int,
    selected_count: int,
    validate_rows: bool = True,
    workers: int | None = None,
) -> _DerivedCatalog:
    """Derive the catalog's digests and diagnostics in two streamed passes.

    Pass one validates every row exactly once and feeds each fixed-count framed
    digest incrementally; the staged row bytes are proven canonical by the
    parse, and item round-tripping is byte-exact (pinned by test), so the state
    digest frames the raw row bytes instead of re-serializing. The three
    diagnostics whose framed sections declare data-dependent counts are counted
    in pass one and hashed in pass two, which re-reads rows without repeating
    the schema validation pass one already performed. The per-row ordering the
    old per-digest generators re-checked is enforced once, globally, by
    ``_iter_located_catalog_rows``.
    """

    worker_count = _derive_worker_count(item_count, workers)
    if worker_count > 1 and len(partitions) > 1:
        return _derive_catalog_parallel(
            blob_source,
            partitions,
            item_count=item_count,
            selected_count=selected_count,
            validate_rows=validate_rows,
            worker_count=worker_count,
        )
    state = _FramedSectionHasher("docspec-source-catalog-state/1", "sourceItems", item_count)
    requested = _FramedSectionHasher("docspec-requested-universe-set/1", "members", item_count)
    selected = _FramedSectionHasher("docspec-selected-source-set/1", "members", selected_count)
    dispositions = _FramedSectionHasher("docspec-catalog-dispositions/1", "records", item_count)
    reasons = _FramedSectionHasher("docspec-catalog-reasons/1", "records", item_count)
    rendition_choices = _FramedSectionHasher(
        "docspec-catalog-rendition-choices/1", "records", item_count
    )
    tally = _DispositionTally()
    join_counts: dict[str, dict[str, int]] = {}
    normalized_count = 0
    joined_count = 0
    interpretation_count = 0
    for row, raw in _iter_catalog_rows(
        blob_source, partitions, item_count, validate=validate_rows, with_raw=True, as_dict=True
    ):
        source_item_id = row["sourceItemId"]
        disposition = row["selection"]["disposition"]
        state.add_payload(raw)
        requested.add({"sourceItemId": source_item_id})
        tally.add(disposition, row["selection"]["reasonCode"])
        if disposition == _SELECTED_DISPOSITION:
            selected.add({"sourceItemId": source_item_id, "documentId": row["documentId"]})
        dispositions.add({"sourceItemId": source_item_id, "disposition": disposition})
        reasons.add({"sourceItemId": source_item_id, "reason": row["selection"]["reason"]})
        rendition_choices.add(_rendition_choice_record(row))
        for _record in _normalized_field_records_for(row):
            normalized_count += 1
        for record in _joined_field_records_for(row):
            joined_count += 1
            _accumulate_join_coverage(join_counts, record)
        for _record in _interpretation_records_for(row):
            interpretation_count += 1
    normalized = _FramedSectionHasher(
        "docspec-catalog-normalized-fields/1", "records", normalized_count
    )
    joined = _FramedSectionHasher("docspec-catalog-joined-fields/1", "records", joined_count)
    interpretations = _FramedSectionHasher(
        "docspec-catalog-interpretations/1", "records", interpretation_count
    )
    for row in _iter_catalog_rows(
        blob_source, partitions, item_count, validate=False, as_dict=True
    ):
        for record in _normalized_field_records_for(row):
            normalized.add(record)
        for record in _joined_field_records_for(row):
            joined.add(record)
        for record in _interpretation_records_for(row):
            interpretations.add(record)
    join_coverage = [
        {"joinId": join_id, **join_counts[join_id]}
        for join_id in sorted(join_counts, key=_utf16_key)
    ]
    return _DerivedCatalog(
        state.digest(),
        requested.digest(),
        selected.digest(),
        tally.dispositions,
        tally.reason_counts(),
        {
            "joinCoverage": join_coverage,
            "normalizedFieldsDigest": normalized.digest(),
            "joinedFieldsDigest": joined.digest(),
            "dispositionsDigest": dispositions.digest(),
            "reasonsDigest": reasons.digest(),
            "interpretationsDigest": interpretations.digest(),
            "renditionChoicesDigest": rendition_choices.digest(),
        },
    )


def _derive_catalog_parallel(
    blob_source: SourceCatalogBlobSource,
    partitions: Sequence[_CatalogPartition],
    *,
    item_count: int,
    selected_count: int,
    validate_rows: bool,
    worker_count: int,
) -> _DerivedCatalog:
    """Derive the same digests with per-partition workers and one ordered merge.

    Workers own every expensive per-row step -- parse, schema check, item
    construction, projection canonicalization -- and spill ordered digest-ready
    payloads built by the same helpers the serial engine uses. The parent sums
    the counters, initializes every framed hasher with known counts, and feeds
    them from one heap-merge of the ordered spills, so the byte stream each
    digest consumes is identical to the serial derivation's. When several
    partitions carry defects, which partition's refusal surfaces first may
    differ from the serial order; the refusals themselves are identical.

    Each task carries a *descriptor* for its partition blob, not the blob. The
    parent once read each partition whole and pickled those bytes to a worker,
    which windowed the number of partitions in flight but not their size:
    ``CATALOG_PARTITION_BUCKET_COUNT`` is a fixed 64, so a partition is always
    1/64th of the catalog and peak memory was a fixed *fraction* of the corpus
    rather than a bound on it -- 16 in the parent's queue plus one per worker.
    Measured, that put a 1.17 GB catalog's derivation at 1,956 MB across the
    process tree and a real 7.61 GB catalog's build at 2.78 GB, growing without
    limit. Streaming makes both ends O(one buffered read) instead.

    The descriptor is duplicated from an open the parent performed through its
    pinned blob source, so a worker inherits exactly the file the parent
    resolved and verified; handing over a path instead would reintroduce the
    re-resolution that :mod:`docspec.adapters.source_catalog_store` pins
    against. A sibling with the same problem, SpicySearch's snapshot build,
    bounds its worker arguments by shipping fixed-size row chunks instead;
    that also works, but DocSpec checks record count, strict ordering and
    bucket membership per partition in ``_iter_partition_stream``, and keeping
    the partition whole keeps those checks exactly as they were.
    """

    import tempfile
    from multiprocessing import reduction

    context = _derive_pool_context()
    ordered = sorted(partitions, key=lambda value: _utf16_key(value.partition_id))
    with tempfile.TemporaryDirectory(prefix="docspec-catalog-derive-") as spill_dir:

        def task_arguments() -> Iterator[tuple[str, Any, int, bool, str]]:
            for partition in ordered:
                member = partition.member
                if member.blob_ref is None or member.record_count is None:
                    raise IntegrityError(
                        "source-item partition descriptor requires blobRef and recordCount"
                    )
                with blob_source.open(member.blob_ref) as stream:
                    # DupFd duplicates the descriptor as it is constructed, so
                    # the pinned open can close here and the worker still holds
                    # the same file.
                    blob = reduction.DupFd(stream.fileno())
                yield (
                    partition.partition_id,
                    blob,
                    member.record_count,
                    validate_rows,
                    spill_dir,
                )

        summaries: list[tuple[str, str, int, dict[str, int], dict[str, dict[str, int]], int, int, int]] = []
        arguments = task_arguments()
        with context.Pool(worker_count) as pool:
            try:
                # Must be the timed asynchronous form. An interpreter whose
                # __main__ cannot be re-imported (frozen, embedded, stdin)
                # cannot host spawned workers -- but the child fails during its
                # own bootstrap, and Pool answers a dead worker by starting
                # another one, forever. The blocking pool.apply() therefore
                # never returns and never raises for exactly the case this
                # fallback exists to handle: measured, a derivation driven from
                # a stdin script span workers until it was killed. Only a
                # timeout can observe it.
                pool.apply_async(_parallel_probe).get(
                    timeout=_PARALLEL_PROBE_TIMEOUT_SECONDS
                )
            except Exception:  # noqa: BLE001 - workers are unavailable here;
                # the serial derivation is always available and identical.
                return _derive_catalog(
                    blob_source,
                    partitions,
                    item_count=item_count,
                    selected_count=selected_count,
                    validate_rows=validate_rows,
                    workers=1,
                )
            pending: list[Any] = []

            def submit_next() -> bool:
                try:
                    args = next(arguments)
                except StopIteration:
                    return False
                pending.append(pool.apply_async(_derive_partition_worker, (args,)))
                return True

            for _ in range(worker_count * 2):
                if not submit_next():
                    break
            while pending:
                summaries.append(pending.pop(0).get())
                submit_next()

        tally = _DispositionTally()
        join_counts: dict[str, dict[str, int]] = {}
        normalized_count = 0
        joined_count = 0
        interpretation_count = 0
        total_rows = 0
        for summary in sorted(summaries, key=lambda value: _utf16_key(value[0])):
            _, _, rows, partition_tally, partition_joins, n_count, j_count, i_count = summary
            total_rows += rows
            tally.merge(partition_tally)
            for join_id in sorted(partition_joins, key=_utf16_key):
                if join_id not in join_counts and len(join_counts) >= SOURCE_CATALOG_MAX_JOIN_IDS:
                    raise LimitExceededError(
                        "catalog join coverage exceeds its distinct-identity limit"
                    )
                target = join_counts.setdefault(
                    join_id,
                    {"eligible": 0, "matched": 0, "unmatched": 0, "nullResult": 0},
                )
                for name in target:
                    target[name] += partition_joins[join_id][name]
            normalized_count += n_count
            joined_count += j_count
            interpretation_count += i_count
        if total_rows != item_count:
            raise IntegrityError(
                "source-catalog row count differs from its partition descriptors"
            )

        state = _FramedSectionHasher("docspec-source-catalog-state/1", "sourceItems", item_count)
        requested = _FramedSectionHasher("docspec-requested-universe-set/1", "members", item_count)
        selected = _FramedSectionHasher("docspec-selected-source-set/1", "members", selected_count)
        dispositions = _FramedSectionHasher("docspec-catalog-dispositions/1", "records", item_count)
        reasons = _FramedSectionHasher("docspec-catalog-reasons/1", "records", item_count)
        rendition_choices = _FramedSectionHasher(
            "docspec-catalog-rendition-choices/1", "records", item_count
        )
        normalized = _FramedSectionHasher(
            "docspec-catalog-normalized-fields/1", "records", normalized_count
        )
        joined = _FramedSectionHasher("docspec-catalog-joined-fields/1", "records", joined_count)
        interpretations = _FramedSectionHasher(
            "docspec-catalog-interpretations/1", "records", interpretation_count
        )
        previous: bytes | None = None
        spill_streams = [_iter_spill(summary[1]) for summary in summaries]
        for key, payloads in heapq.merge(*spill_streams, key=lambda entry: entry[0]):
            if previous is not None and key <= previous:
                raise IntegrityError("source-catalog rows must be globally ordered and distinct")
            previous = key
            state.add_payload(payloads[0])
            requested.add_payload(payloads[1])
            if payloads[2] is not None:
                selected.add_payload(payloads[2])
            dispositions.add_payload(payloads[3])
            reasons.add_payload(payloads[4])
            rendition_choices.add_payload(payloads[5])
            for record_bytes in payloads[6]:
                normalized.add_payload(record_bytes)
            for record_bytes, _outcome, _join_id in payloads[7]:
                joined.add_payload(record_bytes)
            for record_bytes in payloads[8]:
                interpretations.add_payload(record_bytes)
        join_coverage = [
            {"joinId": join_id, **join_counts[join_id]}
            for join_id in sorted(join_counts, key=_utf16_key)
        ]
        return _DerivedCatalog(
            state.digest(),
            requested.digest(),
            selected.digest(),
            tally.dispositions,
            tally.reason_counts(),
            {
                "joinCoverage": join_coverage,
                "normalizedFieldsDigest": normalized.digest(),
                "joinedFieldsDigest": joined.digest(),
                "dispositionsDigest": dispositions.digest(),
                "reasonsDigest": reasons.digest(),
                "interpretationsDigest": interpretations.digest(),
                "renditionChoicesDigest": rendition_choices.digest(),
            },
        )


def _item_interpretations(item_dict: Mapping[str, Any], kind: str) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        value
        for value in item_dict["interpretations"]
        if value["interpretationKind"] == kind
    )


def _normalized_field_records_for(row: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    fields: list[Mapping[str, Any]] = []
    for interpretation in _item_interpretations(row, "normalization"):
        fields.extend(interpretation["result"]["fields"])
    previous_key: tuple[bytes, int] | None = None
    for field in sorted(fields, key=lambda value: _utf16_key(value["normalizedField"])):
        field_path = field["normalizedField"]
        for value_index, value in _indexed_values(field["value"]):
            key = (_utf16_key(field_path), value_index)
            if previous_key is not None and key <= previous_key:
                raise IntegrityError("normalized-field diagnostic keys must be ordered and distinct")
            previous_key = key
            yield {
                "sourceItemId": row["sourceItemId"],
                "fieldPath": field_path,
                "valueIndex": value_index,
                "value": value,
                "diagnostics": {
                    "outcome": field["outcome"],
                    "sourcePaths": field["sourcePaths"],
                    "unparseableValues": field["unparseableValues"],
                    "valueSource": field["valueSource"],
                },
            }


def _indexed_values(value: object) -> Iterator[tuple[int, object]]:
    if isinstance(value, list) and value:
        yield from enumerate(value)
        return
    yield 0, value


def _joined_field_records_for(row: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    joins: list[Mapping[str, Any]] = []
    for interpretation in _item_interpretations(row, "exact-join"):
        joins.extend(interpretation["result"]["joins"])
    previous_key: tuple[bytes, bytes, int] | None = None
    for join in sorted(joins, key=lambda value: _utf16_key(value["joinId"])):
        key = (_utf16_key(join["joinId"]), _utf16_key("matchedSourceRecordId"), 0)
        if previous_key is not None and key <= previous_key:
            raise IntegrityError("joined-field diagnostic keys must be ordered and distinct")
        previous_key = key
        yield {
            "sourceItemId": row["sourceItemId"],
            "joinId": join["joinId"],
            "outputPath": "matchedSourceRecordId",
            "valueIndex": 0,
            "value": join["matchedSourceRecordId"],
            "outcome": join["outcome"],
            "evidence": {
                "lookupScopeId": join["lookupScopeId"],
                "sourceField": join["sourceField"],
                "sourceValue": join["sourceValue"],
            },
        }


def _interpretation_records_for(row: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    by_kind: dict[str, list[Mapping[str, Any]]] = {}
    for interpretation in row["interpretations"]:
        by_kind.setdefault(interpretation["interpretationKind"], []).append(interpretation)
    for kind in sorted(by_kind, key=_utf16_key):
        for index, interpretation in enumerate(by_kind[kind]):
            yield {
                "sourceItemId": row["sourceItemId"],
                "interpretationKind": kind,
                "interpretationId": f"{index:04d}",
                "value": interpretation["result"],
                "diagnostics": {
                    "inputScopeIds": interpretation["inputScopeIds"],
                    "policyDigest": interpretation["policyDigest"],
                    "policyId": interpretation["policyId"],
                    "policyVersion": interpretation["policyVersion"],
                },
            }


def _rendition_choice_record(row: Mapping[str, Any]) -> Mapping[str, Any]:
    choices = _item_interpretations(row, "rendition-preference")
    if len(choices) != 1:
        raise IntegrityError("source-catalog row requires one rendition-preference interpretation")
    return {
        "sourceItemId": row["sourceItemId"],
        "selectedFamilyId": choices[0]["result"]["selectedFamilyId"],
        "candidateIds": [candidate["renditionId"] for candidate in row["candidateRenditions"]],
    }


def _accumulate_join_coverage(
    counts: dict[str, dict[str, int]],
    record: Mapping[str, Any],
) -> None:
    join_id = record["joinId"]
    if not isinstance(join_id, str):
        raise IntegrityError("catalog join identity must be text")
    if join_id not in counts and len(counts) >= SOURCE_CATALOG_MAX_JOIN_IDS:
        raise LimitExceededError("catalog join coverage exceeds its distinct-identity limit")
    selected = counts.setdefault(
        join_id,
        {"eligible": 0, "matched": 0, "unmatched": 0, "nullResult": 0},
    )
    outcome = record["outcome"]
    if outcome == "matched":
        selected["eligible"] += 1
        selected["matched"] += 1
    elif outcome == "no-match":
        selected["eligible"] += 1
        selected["unmatched"] += 1
    elif outcome == "not-stated":
        selected["nullResult"] += 1
    else:
        raise IntegrityError("catalog join outcome is not recognized")


def _source_catalog_succession(value: object) -> SourceCatalogSuccession:
    supersedes = Supersedes.from_dict(value, path="source-catalog/supersedes")
    return SourceCatalogSuccession(
        supersedes.logical_id,
        supersedes.artifact_digest,
        supersedes.reason,
    )


class SourceCatalogArtifactVerifier:
    """Check DocSpec meaning after Rulespec has checked generic structure."""

    def __init__(self, producer: Producer, blob_source: SourceCatalogBlobSource) -> None:
        self._producer = producer
        self._blob_source = blob_source
        self.summary: SourceCatalogSnapshotSummary | None = None
        self.partitions: tuple[_CatalogPartition, ...] = ()
        self.receipt: Mapping[str, Any] | None = None

    def __call__(self, artifact: VerifiedArtifact, source: MemberSource) -> None:
        root = artifact.root
        if root["kind"] != CATALOG_KIND:
            raise IntegrityError("source catalog reference names a different product kind")
        spec = _mapping(root["spec"], "source-catalog spec")
        if set(spec) != _CATALOG_SPEC_FIELDS:
            raise IntegrityError("source-catalog spec has an invalid closed shape")
        if not artifact.inputs or {value.role for value in artifact.inputs} != {"source-native"}:
            raise IntegrityError("source catalog must pin one or more source-native inputs")
        declared_members = tuple(iter_member_descriptors(artifact, source))
        local_members = {
            value.object_key: value for value in declared_members if value.object_key is not None
        }
        item_members = tuple(value for value in declared_members if value.role == CATALOG_ITEMS_ROLE)
        if set(local_members) != {CATALOG_POLICY_KEY, CATALOG_RECEIPT_KEY}:
            raise IntegrityError("source-catalog members differ from the DocSpec product view")
        if len(declared_members) != len(local_members) + len(item_members):
            raise IntegrityError("source-catalog member roles differ from the closed product role set")
        policy_member = local_members[CATALOG_POLICY_KEY]
        receipt_member = local_members[CATALOG_RECEIPT_KEY]
        if (
            policy_member.role != CATALOG_POLICY_ROLE
            or policy_member.media_type != CATALOG_JSON_MEDIA_TYPE
            or policy_member.schema_id != SOURCE_CATALOG_POLICY_SCHEMA_ID
            or policy_member.record_count is not None
            or receipt_member.role != CATALOG_RECEIPT_ROLE
            or receipt_member.media_type != CATALOG_JSON_MEDIA_TYPE
            or receipt_member.schema_id != SOURCE_CATALOG_RECEIPT_SCHEMA_ID
            or receipt_member.record_count is not None
        ):
            raise IntegrityError("source-catalog member descriptions are invalid")
        policy = parse_canonical_json(_read_small(source, CATALOG_POLICY_KEY), path=CATALOG_POLICY_KEY)
        receipt = parse_canonical_json(_read_small(source, CATALOG_RECEIPT_KEY), path=CATALOG_RECEIPT_KEY)
        _schema_error(_POLICY_VALIDATOR, policy, "catalog policy")
        _schema_error(_RECEIPT_VALIDATOR, receipt, "catalog build receipt")
        policy = _mapping(policy, "catalog policy")
        receipt = _mapping(receipt, "catalog build receipt")
        producer = _mapping(root["producer"], "source-catalog producer")
        expected_producer = self._producer.as_dict()
        if producer != expected_producer:
            raise IntegrityError("source-catalog producer differs from the installed implementation")
        comparisons = {
            "catalogId": "catalogId",
            "catalogSchemaDigest": "catalogSchemaDigest",
            "sourceSystemSetDigest": "sourceSystemSetDigest",
            "sourceNativeSchemaSetDigest": "sourceNativeSchemaSetDigest",
            "selectionPolicyId": "selectionPolicyId",
            "selectionPolicyVersion": "selectionPolicyVersion",
            "selectionPolicyDigest": "selectionPolicyDigest",
            "catalogStateDigest": "catalogStateDigest",
            "requestedUniverseSetDigest": "requestedUniverseSetDigest",
            "selectedSourceSetDigest": "selectedSourceSetDigest",
        }
        for receipt_field, spec_field in comparisons.items():
            if receipt[receipt_field] != spec[spec_field]:
                raise IntegrityError(f"catalog build receipt {receipt_field} differs from the root")
        if spec["catalogSchemaDigest"] != schema_bundle_digest(_SCHEMAS):
            raise IntegrityError("source catalog schema digest differs from the installed schema family")
        expected_inputs = [
            {"logicalId": value.logical_id, "artifactDigest": value.artifact_digest}
            for value in artifact.inputs
        ]
        if receipt["sourceNativeInputs"] != expected_inputs:
            raise IntegrityError("catalog build receipt source-native inputs differ from the root")
        if (
            policy["policyId"] != spec["selectionPolicyId"]
            or policy["policyVersion"] != spec["selectionPolicyVersion"]
            or sha256_digest(canonical_json_bytes(policy)) != spec["selectionPolicyDigest"]
        ):
            raise IntegrityError("catalog policy identity differs from the root")
        if (
            receipt["verifierId"] != producer["verifierId"]
            or receipt["verifierVersion"] != producer["verifierVersion"]
            or receipt["verifierImplementationId"] != producer["verifierImplementationId"]
        ):
            raise IntegrityError("catalog build receipt verifier differs from the root producer")
        if receipt["partitionPolicy"] != _partition_policy():
            raise IntegrityError("catalog build receipt partition policy differs from the installed policy")
        partition_rows = receipt["partitions"]
        partitions: list[_CatalogPartition] = []
        previous_partition: str | None = None
        members_by_ref = {value.blob_ref: value for value in item_members}
        if None in members_by_ref or len(members_by_ref) != len(item_members):
            raise IntegrityError("source-item members require distinct blobRef values")
        for raw_partition in partition_rows:
            partition = _mapping(raw_partition, "catalog receipt partition")
            partition_id = _text(partition["partitionId"], "catalog partitionId")
            if (
                previous_partition is not None
                and _utf16_key(partition_id) <= _utf16_key(previous_partition)
            ):
                raise IntegrityError("catalog receipt partitions must be ordered and distinct")
            previous_partition = partition_id
            if (
                len(partition_id) != 4
                or not partition_id.isascii()
                or not partition_id.isdigit()
                or not 0 <= int(partition_id) < CATALOG_PARTITION_BUCKET_COUNT
            ):
                raise IntegrityError("catalog receipt partitionId is outside the installed policy")
            member = members_by_ref.get(partition["blobRef"])
            if member is None:
                raise IntegrityError("catalog receipt partition has no matching source-items member")
            if (
                member.media_type != CATALOG_ITEMS_MEDIA_TYPE
                or member.schema_id != SOURCE_CATALOG_ITEM_SCHEMA_ID
                or member.record_count != partition["recordCount"]
                or member.byte_size != partition["byteSize"]
                or member.object_key is not None
                or member.sha256 is not None
            ):
                raise IntegrityError("source-item partition descriptor differs from its build receipt")
            partitions.append(_CatalogPartition(partition_id, member))
        if {value.member.blob_ref for value in partitions} != set(members_by_ref):
            raise IntegrityError("catalog receipt does not account for every source-items member")
        if receipt["itemCount"] != sum(value.member.record_count or 0 for value in partitions):
            raise IntegrityError("catalog build receipt item count differs from its source-item partitions")
        counts = receipt["dispositionCounts"]
        if sum(counts.values()) != receipt["itemCount"]:
            raise IntegrityError("catalog build receipt dispositions do not account for every row")
        _reconcile_reason_counts(receipt["reasonCounts"], counts)
        measurements = receipt["byteMeasurements"]
        payload_bytes = sum(value.member.byte_size for value in partitions)
        if (
            measurements["payloadBytesRead"] != payload_bytes
            or measurements["payloadBytesReused"] + measurements["payloadBytesWritten"]
            != payload_bytes
        ):
            raise IntegrityError("catalog build receipt payload byte measurements do not reconcile")
        publication_bytes = (
            policy_member.byte_size
            + receipt_member.byte_size
            + sum(value.byte_size for value in artifact.manifests)
            + len(_read_small(source, ROOT_OBJECT_KEY))
        )
        if measurements["publicationBytesWritten"] != publication_bytes:
            raise IntegrityError("catalog build receipt publication byte measurements do not reconcile")
        previous_join: str | None = None
        for coverage in receipt["joinCoverage"]:
            join_id = coverage["joinId"]
            if previous_join is not None and _utf16_key(join_id) <= _utf16_key(previous_join):
                raise IntegrityError("catalog join coverage must be ordered and distinct")
            previous_join = join_id
            if coverage["eligible"] != coverage["matched"] + coverage["unmatched"]:
                raise IntegrityError("catalog join coverage eligible count does not reconcile")
            if coverage["eligible"] + coverage["nullResult"] > receipt["itemCount"]:
                raise IntegrityError("catalog join coverage exceeds the catalog population")
        self.partitions = tuple(partitions)
        self.receipt = receipt
        self.summary = SourceCatalogSnapshotSummary(
            logical_id=artifact.pin.logical_id,
            artifact_digest=artifact.pin.artifact_digest,
            catalog_id=spec["catalogId"],
            catalog_state_digest=spec["catalogStateDigest"],
            requested_universe_set_digest=spec["requestedUniverseSetDigest"],
            selected_source_set_digest=spec["selectedSourceSetDigest"],
            item_count=receipt["itemCount"],
            disposition_counts=dict(counts),
            reason_counts=tuple(dict(value) for value in receipt["reasonCounts"]),
            partitions=tuple(value.partition_id for value in partitions),
            selection_policy={
                "policyId": spec["selectionPolicyId"],
                "policyVersion": spec["selectionPolicyVersion"],
                "policyDigest": spec["selectionPolicyDigest"],
            },
            partition_policy=dict(receipt["partitionPolicy"]),
            join_coverage=tuple(dict(value) for value in receipt["joinCoverage"]),
            diagnostic_digests={name: receipt[name] for name in _DIAGNOSTIC_DIGEST_FIELDS},
            source_native_inputs=tuple(dict(value) for value in receipt["sourceNativeInputs"]),
            byte_measurements=dict(receipt["byteMeasurements"]),
            succession=(
                None
                if "supersedes" not in root
                else _source_catalog_succession(root["supersedes"])
            ),
        )


class SourceCatalogBuildGateVerifier:
    """Add the producer-only full semantic pass to bounded receipt checks."""

    def __init__(self, producer: Producer, blob_source: SourceCatalogBlobSource) -> None:
        self._producer = producer
        self._blob_source = blob_source
        self.summary: SourceCatalogSnapshotSummary | None = None

    def __call__(self, artifact: VerifiedArtifact, source: MemberSource) -> None:
        receipt_verifier = SourceCatalogArtifactVerifier(self._producer, self._blob_source)
        receipt_verifier(artifact, source)
        summary = receipt_verifier.summary
        receipt = receipt_verifier.receipt
        if summary is None or receipt is None:
            raise RuntimeError("source catalog receipt verifier produced no summary")

        derived = _derive_catalog(
            self._blob_source,
            receipt_verifier.partitions,
            item_count=summary.item_count,
            selected_count=summary.disposition_counts[CatalogDisposition.SELECTED.value],
        )
        computed = {
            "catalogStateDigest": derived.catalog_state_digest,
            "requestedUniverseSetDigest": derived.requested_universe_set_digest,
            "selectedSourceSetDigest": derived.selected_source_set_digest,
        }
        expected = {
            "catalogStateDigest": summary.catalog_state_digest,
            "requestedUniverseSetDigest": summary.requested_universe_set_digest,
            "selectedSourceSetDigest": summary.selected_source_set_digest,
        }
        for name, digest in computed.items():
            if digest != expected[name]:
                raise IntegrityError(f"producer semantic gate recomputed a different {name}")
        if derived.disposition_counts != dict(summary.disposition_counts):
            raise IntegrityError("producer semantic gate recomputed different disposition counts")
        if derived.reason_counts != [dict(value) for value in summary.reason_counts]:
            raise IntegrityError("producer semantic gate recomputed different reason counts")
        for name, value in derived.diagnostics.items():
            if receipt[name] != value:
                raise IntegrityError(f"producer semantic gate recomputed a different {name}")
        self.summary = summary


class SourceCatalogArtifactReader(ImmutableSourceCatalogReader):
    """Open complete snapshots through an injected immutable member resolver."""

    def __init__(self, store: SourceCatalogStore, *, producer: Producer) -> None:
        self._store = store
        self._producer = producer
        self._verified: dict[tuple[str, str], SourceCatalogSnapshotSummary] = {}

    def open_snapshot(self, reference: SourceCatalogRef) -> SourceCatalogSnapshot:
        try:
            source = self._store.source_for(reference)
            blob_source = self._store.blob_source()
            verifier = SourceCatalogArtifactVerifier(self._producer, blob_source)
            admit_artifact(
                source,
                blob_source=blob_source,
                expected_pin=ArtifactPin(reference.catalog_id, reference.digest),
                semantic_verifier=verifier,
            )
        except ArtifactVerificationError as error:
            raise IntegrityError(f"source catalog artifact is invalid: {error}") from error
        if verifier.summary is None:
            raise RuntimeError("source catalog verifier produced no summary")
        # A reader that already ran the full gate on this exact digest
        # (verify_snapshot memoizes it) streams items without repeating the
        # per-row schema and canonicality proofs; structural checks -- row
        # ordering, partition placement, counts -- still run on every row.
        already_verified = (reference.catalog_id, reference.digest) in self._verified
        return SourceCatalogSnapshot(
            verifier.summary,
            _iter_located_catalog_rows(
                blob_source,
                verifier.partitions,
                verifier.summary.item_count,
                validate=not already_verified,
            ),
        )

    def verify_snapshot(self, reference: SourceCatalogRef) -> SourceCatalogSnapshotSummary:
        """Fully verify one snapshot, remembering the verdict for this reader.

        The consumer runs the same gate the producer ran: artifact admission
        hashes every member against the manifest, and the build-gate verifier
        re-derives every digest and diagnostic from the rows and refuses any
        mismatch with the sealed spec and receipt. (The item iteration this
        method used to run instead re-validated row shapes but never compared
        a single digest -- this is stronger and cheaper.) The verdict is
        memoized per (catalogId, digest) for the life of this reader: the
        store is content-addressed and immutable, so a repeated verify of the
        same digest would re-prove the same bytes.
        """

        key = (reference.catalog_id, reference.digest)
        cached = self._verified.get(key)
        if cached is not None:
            return cached
        try:
            source = self._store.source_for(reference)
            blob_source = self._store.blob_source()
            gate = SourceCatalogBuildGateVerifier(self._producer, blob_source)
            admit_artifact(
                source,
                blob_source=blob_source,
                expected_pin=ArtifactPin(reference.catalog_id, reference.digest),
                semantic_verifier=gate,
            )
        except ArtifactVerificationError as error:
            raise IntegrityError(f"source catalog artifact is invalid: {error}") from error
        if gate.summary is None:
            raise RuntimeError("source catalog gate verifier produced no summary")
        self._verified[key] = gate.summary
        return gate.summary


class SourceCatalogBuilder:
    """Create one complete snapshot from injected source, policy, and storage ports."""

    def __init__(
        self,
        *,
        store: SourceCatalogStore,
        policy: SourceCatalogPolicy,
        request: SourceCatalogBuildRequest,
        workspace_factory: Callable[
            [],
            AbstractContextManager[CatalogPolicyWorkspace],
        ],
        resume_batch_items: int = 10_000,
    ) -> None:
        self._store = store
        self._policy = policy
        self._request = request
        self._resume_batch_items = resume_batch_items
        self._workspace_factory = workspace_factory

    def build(self, sources: Sequence[SourceNativeRecordSource]) -> SourceCatalogBuildResult:
        if not sources:
            raise ValueError("a source catalog requires at least one source-native input")
        descriptions = tuple(source.describe() for source in sources)
        policy = {
            "format": CATALOG_POLICY_FORMAT,
            "formatVersion": CATALOG_FORMAT_VERSION,
            "policyId": self._policy.policy_id,
            "policyVersion": self._policy.policy_version,
            "configuration": dict(self._policy.configuration),
        }
        _schema_error(_POLICY_VALIDATOR, policy, "catalog policy")
        policy_bytes = canonical_json_bytes(policy)
        policy_digest = sha256_digest(policy_bytes)
        catalog_schema_digest = schema_bundle_digest(_SCHEMAS)

        with self._workspace_factory() as workspace, self._store.stage() as staging:
            ledger = _ResumeLedger(workspace)
            ledger.open(
                {
                    "catalogId": self._request.catalog_id,
                    "catalogSchemaDigest": catalog_schema_digest,
                    "policyDigest": policy_digest,
                    "producer": self._request.producer.as_dict(),
                    "inputs": [
                        {"logicalId": value.logical_id, "artifactDigest": value.artifact_digest}
                        for value in descriptions
                    ],
                }
            )
            row_partitioner = _CatalogRowPartitioner(
                _policy_rows(
                    sources,
                    descriptions,
                    self._policy,
                    policy_digest,
                    workspace,
                    ledger,
                ),
                ledger=ledger,
                batch_items=self._resume_batch_items,
            )
            if ledger.staged_state is not None:
                # Every row is staged and accounted; only publication remains.
                row_partitioner.restore(ledger.staged_state)
            else:
                if ledger.cursor_state is not None:
                    row_partitioner.restore(ledger.cursor_state)
                row_partitioner.stage(workspace)
                ledger.mark_staged(row_partitioner.state())
            partitions: list[_CatalogPartition] = []
            payload_bytes_reused = 0
            payload_bytes_written = 0
            for partition_id in sorted(row_partitioner.partition_counts, key=_utf16_key):
                blob_ref, byte_size = _measure_blob(
                    _CatalogRowPartitioner.chunks(workspace, partition_id)
                )
                write = staging.put_blob(
                    blob_ref,
                    byte_size,
                    _CatalogRowPartitioner.chunks(workspace, partition_id),
                )
                if write.reused:
                    payload_bytes_reused += write.byte_size
                else:
                    payload_bytes_written += write.byte_size
                partitions.append(
                    _CatalogPartition(
                        partition_id,
                        describe_member_from_receipt(
                            blob_ref=write.blob_ref,
                            role=CATALOG_ITEMS_ROLE,
                            media_type=CATALOG_ITEMS_MEDIA_TYPE,
                            byte_size=write.byte_size,
                            record_count=row_partitioner.partition_counts[partition_id],
                            schema_id=SOURCE_CATALOG_ITEM_SCHEMA_ID,
                        ),
                    )
                )
            selected_blob_source = staging.blob_source()
            # The builder reads back bytes it staged itself; the producer gate
            # independently re-validates every row before publication, so this
            # derivation skips the redundant schema pass.
            derived = _derive_catalog(
                selected_blob_source,
                partitions,
                item_count=row_partitioner.item_count,
                selected_count=row_partitioner.selected_count,
                validate_rows=False,
            )
            state_digest = derived.catalog_state_digest
            requested_digest = derived.requested_universe_set_digest
            selected_digest = derived.selected_source_set_digest
            diagnostics = derived.diagnostics
            spec = {
                "catalogId": self._request.catalog_id,
                "catalogSchemaDigest": catalog_schema_digest,
                "sourceSystemSetDigest": _source_system_set_digest(descriptions),
                "sourceNativeSchemaSetDigest": _source_schema_set_digest(descriptions),
                "selectionPolicyId": self._policy.policy_id,
                "selectionPolicyVersion": self._policy.policy_version,
                "selectionPolicyDigest": policy_digest,
                "requestedUniverseSetDigest": requested_digest,
                "selectedSourceSetDigest": selected_digest,
                "catalogStateDigest": state_digest,
            }
            inputs = tuple(
                ArtifactInput("source-native", value.logical_id, value.artifact_digest)
                for value in descriptions
            )
            ordered_inputs = tuple(
                sorted(
                    inputs,
                    key=lambda value: _utf16_key(value.logical_id.rsplit(":", 1)[-1]),
                )
            )
            payload_bytes_read = payload_bytes_reused + payload_bytes_written
            receipt: dict[str, Any] = {
                "format": CATALOG_RECEIPT_FORMAT,
                "formatVersion": CATALOG_FORMAT_VERSION,
                "catalogId": self._request.catalog_id,
                "catalogSchemaDigest": catalog_schema_digest,
                "sourceSystemSetDigest": spec["sourceSystemSetDigest"],
                "sourceNativeSchemaSetDigest": spec["sourceNativeSchemaSetDigest"],
                "selectionPolicyId": self._policy.policy_id,
                "selectionPolicyVersion": self._policy.policy_version,
                "selectionPolicyDigest": policy_digest,
                "sourceNativeInputs": [
                    {
                        "logicalId": value.logical_id,
                        "artifactDigest": value.artifact_digest,
                    }
                    for value in ordered_inputs
                ],
                "catalogStateDigest": state_digest,
                "requestedUniverseSetDigest": requested_digest,
                "selectedSourceSetDigest": selected_digest,
                "itemCount": row_partitioner.item_count,
                "dispositionCounts": row_partitioner.disposition_counts,
                "reasonCounts": row_partitioner.tally.reason_counts(),
                "partitionPolicy": _partition_policy(),
                "partitions": [value.to_receipt() for value in partitions],
                **diagnostics,
                "byteMeasurements": {
                    "payloadBytesRead": payload_bytes_read,
                    "payloadBytesReused": payload_bytes_reused,
                    "payloadBytesWritten": payload_bytes_written,
                    "publicationBytesWritten": 0,
                },
                "verifierId": self._request.producer.verifier_id,
                "verifierVersion": self._request.producer.verifier_version,
                "verifierImplementationId": self._request.producer.verifier_implementation_id,
                "semanticVerdict": "pass",
            }
            publication_bytes = -1
            for _ in range(8):
                receipt["byteMeasurements"]["publicationBytesWritten"] = publication_bytes
                receipt_bytes = canonical_json_bytes(receipt)
                local_members = (
                    describe_member_from_receipt(
                        object_key=CATALOG_POLICY_KEY,
                        sha256=sha256_digest(policy_bytes),
                        role=CATALOG_POLICY_ROLE,
                        media_type=CATALOG_JSON_MEDIA_TYPE,
                        byte_size=len(policy_bytes),
                        schema_id=SOURCE_CATALOG_POLICY_SCHEMA_ID,
                    ),
                    describe_member_from_receipt(
                        object_key=CATALOG_RECEIPT_KEY,
                        sha256=sha256_digest(receipt_bytes),
                        role=CATALOG_RECEIPT_ROLE,
                        media_type=CATALOG_JSON_MEDIA_TYPE,
                        byte_size=len(receipt_bytes),
                        schema_id=SOURCE_CATALOG_RECEIPT_SCHEMA_ID,
                    ),
                )
                members = (*local_members, *(value.member for value in partitions))
                manifest, manifest_bytes = MemberManifestReference.for_members(
                    scope_kind="global",
                    scope_id="catalog",
                    object_key=CATALOG_MANIFEST_KEY,
                    members=members,
                )
                root = build_artifact_root(
                    kind=CATALOG_KIND,
                    spec=spec,
                    producer=self._request.producer,
                    inputs=ordered_inputs,
                    manifests=(manifest,),
                    supersedes=self._request.supersedes,
                )
                root_bytes = canonical_json_bytes(root)
                measured_publication_bytes = (
                    len(policy_bytes) + len(receipt_bytes) + len(manifest_bytes) + len(root_bytes)
                )
                if measured_publication_bytes == publication_bytes:
                    break
                publication_bytes = measured_publication_bytes
            else:
                raise IntegrityError("catalog publication byte accounting did not stabilize")
            _schema_error(_RECEIPT_VALIDATOR, receipt, "catalog build receipt")
            staging.write(CATALOG_POLICY_KEY, (policy_bytes,))
            staging.write(CATALOG_RECEIPT_KEY, (receipt_bytes,))
            staging.write(CATALOG_MANIFEST_KEY, (manifest_bytes,))
            staging.write(ROOT_OBJECT_KEY, (root_bytes,))
            reference = SourceCatalogRef(
                root["logicalId"],
                f"{root['artifactDigest'].removeprefix('sha256:')}/{ROOT_OBJECT_KEY}",
                root["artifactDigest"],
            )
            verifier = SourceCatalogBuildGateVerifier(self._request.producer, selected_blob_source)
            try:
                admit_artifact(
                    staging,
                    blob_source=selected_blob_source,
                    expected_pin=ArtifactPin(reference.catalog_id, reference.digest),
                    semantic_verifier=verifier,
                )
            except ArtifactVerificationError as error:
                raise IntegrityError(f"built source catalog is structurally invalid: {error}") from error
            published = staging.commit(reference)
        if verifier.summary is None:
            raise RuntimeError("source catalog verifier produced no summary")
        return SourceCatalogBuildResult(
            published,
            verifier.summary,
            dict(receipt["byteMeasurements"]),
        )


__all__ = [
    "CATALOG_KIND",
    "SourceCatalogArtifactReader",
    "SourceCatalogArtifactVerifier",
    "SourceCatalogBuildGateVerifier",
    "SourceCatalogBuildRequest",
    "SourceCatalogBuildResult",
    "SourceCatalogBuilder",
    "requested_universe_set_digest",
    "selected_source_set_digest",
    "source_catalog_producer",
    "source_item_validator_implementation",
]
