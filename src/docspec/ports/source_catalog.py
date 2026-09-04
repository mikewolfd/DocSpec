"""Build and read immutable source catalogs through DocSpec-owned ports."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, BinaryIO, Protocol

from docspec.domain.identity import require_sha256, require_text
from docspec.domain.references import SourceCatalogRef
from docspec.domain.source_catalog import SOURCE_CATALOG_MAX_JOIN_IDS, SourceCatalogItem


@dataclass(frozen=True, slots=True)
class SourceNativeDescription:
    """Immutable identity and completeness facts for one source-native input."""

    logical_id: str
    artifact_digest: str
    source_system_id: str
    source_system_version: str
    source_state_scope: str
    source_state_digest: str
    source_native_schema_set_digest: str

    def __post_init__(self) -> None:
        require_text(self.logical_id, "source-native logical_id")
        require_sha256(self.artifact_digest, "source-native artifact_digest")
        require_text(self.source_system_id, "source_system_id")
        require_text(self.source_system_version, "source_system_version")
        if self.source_state_scope not in {"complete-snapshot", "observed-crawl"}:
            raise ValueError("source_state_scope must be complete-snapshot or observed-crawl")
        require_sha256(self.source_state_digest, "source_state_digest")
        require_sha256(self.source_native_schema_set_digest, "source_native_schema_set_digest")


class SourceNativeRecordSource(Protocol):
    """Stream one already-admitted source-native snapshot without producer types."""

    def describe(self) -> SourceNativeDescription: ...

    def iter_records(self) -> Iterator[Mapping[str, Any]]: ...

    def iter_renditions(self) -> Iterator[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class SourceInputSelector:
    """Plain policy data selecting one exact source-native row family."""

    source_system_id: str
    source_system_version: str
    scope_id: str
    schema_name: str
    schema_version: str

    def __post_init__(self) -> None:
        require_text(self.source_system_id, "source input source_system_id")
        require_text(self.source_system_version, "source input source_system_version")
        require_text(self.scope_id, "source input scope_id")
        require_text(self.schema_name, "source input schema_name")
        require_text(self.schema_version, "source input schema_version")

    def to_dict(self) -> dict[str, str]:
        return {
            "sourceSystemId": self.source_system_id,
            "sourceSystemVersion": self.source_system_version,
            "scopeId": self.scope_id,
            "schemaName": self.schema_name,
            "schemaVersion": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> SourceInputSelector:
        if not isinstance(value, Mapping) or set(value) != {
            "sourceSystemId",
            "sourceSystemVersion",
            "scopeId",
            "schemaName",
            "schemaVersion",
        }:
            raise ValueError("source input selector has an invalid closed shape")
        return cls(
            value["sourceSystemId"],
            value["sourceSystemVersion"],
            value["scopeId"],
            value["schemaName"],
            value["schemaVersion"],
        )


@dataclass(frozen=True, slots=True)
class SourceNativeRow:
    """One structurally admitted source record and its matching renditions."""

    description: SourceNativeDescription
    record: Mapping[str, Any]
    renditions: tuple[Mapping[str, Any], ...]
    #: Filings this run collapsed into ``record`` under DocSpec decision 0004,
    #: each ``{reasonCode, reason, record, renditions}``. Empty for every row
    #: that was never part of a collision, which is all but two of the
    #: 2,221,713 records in the 670 non-Federal-Register catalog-A releases.
    discarded_filings: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogResumePoint:
    """Where a resumed build picks up, as the policy needs to know it.

    ``indexed`` says the policy's own pre-pass (indexes, staged universe,
    sample draw) is already in the workspace and must not run again; ``after``
    is the last committed sourceItemId, and the builder's ordered universe
    reads already start past it; ``selected_count`` carries the count a
    selected-item budget had reached. A fresh build is ``FRESH_BUILD``.
    """

    indexed: bool
    after: str | None
    selected_count: int


FRESH_BUILD = CatalogResumePoint(indexed=False, after=None, selected_count=0)


class CatalogPolicyWorkspace(Protocol):
    """Bounded exact-key and ordered-row storage for one policy run.

    Ephemeral by default; a durable implementation may also ``commit`` so a
    killed build leaves what it had committed for a resumed one to read.
    """

    def put(
        self,
        namespace: str,
        key: tuple[str, ...],
        value: Mapping[str, Any],
    ) -> None: ...

    def get(self, namespace: str, key: tuple[str, ...]) -> Mapping[str, Any] | None: ...

    def replace(
        self,
        namespace: str,
        key: tuple[str, ...],
        value: Mapping[str, Any],
    ) -> None:
        """Overwrite one row this run already wrote, and refuse if none exists.

        ``put`` keeps refusing a repeated key, because a repeat is a defect
        almost everywhere and the refusal is how it is found. A caller that
        genuinely means to supersede a row it wrote itself says so here instead
        of making ``put`` lenient for everyone.
        """
        ...

    def iter_ordered(
        self, namespace: str, *, after: tuple[str, ...] | None = None
    ) -> Iterator[Mapping[str, Any]]: ...

    def commit(self) -> None:
        """Make everything written so far survive the process. Optional: a
        workspace that cannot resume may leave this unimplemented and the
        builder records no resume points."""
        ...


class CatalogPolicyInputs(Protocol):
    """Builder-owned access to admitted source-native inputs.

    **Source artifacts are read exactly once.** A second read could see
    different bytes than admission checked, because a file-state comparison
    authenticates what was present when it ran and not what is there later.
    That is the hazard the one-pass rule was written for and it still holds.

    **Staged rows may be read in order up to twice.** Once the loader has
    materialised them they are builder-owned SQLite this run wrote itself, and
    a second ordered read cannot see anything the first did not put there --
    the staleness hazard does not reach them. Two is the bound because a policy
    needs one pass to build the lookup indexes its joins require and one to
    emit items; a policy wanting a third is doing something this design did not
    anticipate and should be examined rather than accommodated.

    The distinction matters because the contract as previously written was
    stricter than its own reason, and that cost a byte-for-byte duplicate of
    every staged row.
    """

    @property
    def descriptions(self) -> tuple[SourceNativeDescription, ...]: ...

    @property
    def resume(self) -> CatalogResumePoint:
        """Where this run picks up. ``FRESH_BUILD`` unless the builder found a
        committed workspace staged under this build's own identity."""
        ...

    def iter_universe_rows(self) -> Iterator[SourceNativeRow]: ...

    def iter_lookup_rows(self, selector: SourceInputSelector) -> Iterator[SourceNativeRow]: ...


class SourceCatalogMemberSource(Protocol):
    """Read artifact members through the shared structural seam."""

    def keys(self) -> Iterable[str]: ...

    def open(self, object_key: str) -> AbstractContextManager[BinaryIO]: ...


class SourceCatalogBlobSource(Protocol):
    """Open one immutable catalog payload by its qualified content digest."""

    def open(self, blob_ref: str) -> AbstractContextManager[BinaryIO]: ...


@dataclass(frozen=True, slots=True)
class SourceCatalogBlobWrite:
    """Report whether one verified content-addressed payload required a write."""

    blob_ref: str
    byte_size: int
    reused: bool

    def __post_init__(self) -> None:
        require_sha256(self.blob_ref, "source catalog blob_ref")
        if (
            not isinstance(self.byte_size, int)
            or isinstance(self.byte_size, bool)
            or self.byte_size < 0
        ):
            raise ValueError("source catalog blob byte_size must be a non-negative integer")
        if not isinstance(self.reused, bool):
            raise ValueError("source catalog blob reused flag must be boolean")


class SourceCatalogStaging(SourceCatalogMemberSource, Protocol):
    """One unpublished write transaction that is also a readable member source."""

    def write(self, object_key: str, chunks: Iterable[bytes]) -> None: ...

    def put_blob(
        self,
        blob_ref: str,
        byte_size: int,
        chunks: Iterable[bytes],
    ) -> SourceCatalogBlobWrite: ...

    def blob_source(self) -> SourceCatalogBlobSource: ...

    def commit(self, reference: SourceCatalogRef) -> SourceCatalogRef: ...


class SourceCatalogStore(Protocol):
    """Create an isolated source-catalog transaction and resolve published roots."""

    def stage(self) -> AbstractContextManager[SourceCatalogStaging]: ...

    def source_for(self, reference: SourceCatalogRef) -> SourceCatalogMemberSource: ...

    def blob_source(self) -> SourceCatalogBlobSource: ...


@dataclass(frozen=True, slots=True)
class SourceRecordCollisionResolution:
    """Which filing owns a repeated sourceRecordId, and what is kept from the other.

    ``owner`` and ``discarded`` are both rows the loader built, so a policy
    chooses between them rather than constructing one. ``discarded`` is
    retained rather than dropped: see DocSpec decision 0004, where the two
    filings of one cross-filed document were measured to differ in 8 of 84 and
    6 of 90 leaf fields, including a docket association and a Federal Register
    volume citation carried by only one side.
    """

    owner: Mapping[str, Any]
    discarded: Mapping[str, Any]
    reason_code: str
    reason: str


class SourceCatalogPolicy(Protocol):
    """Apply one DocSpec-owned interpretation policy to neutral source rows."""

    @property
    def policy_id(self) -> str: ...

    @property
    def policy_version(self) -> str: ...

    @property
    def configuration(self) -> Mapping[str, Any]: ...

    @property
    def universe_inputs(self) -> tuple[SourceInputSelector, ...]: ...

    def iter_items(
        self,
        inputs: CatalogPolicyInputs,
        workspace: CatalogPolicyWorkspace,
    ) -> Iterator[SourceCatalogItem]: ...

    # Optional. A policy that omits it keeps the loader's refusal on a repeated
    # sourceRecordId, which is the right answer wherever a repeat is a defect.
    # Read structurally by the loader rather than declared here with a default,
    # so omission stays an explicit "refuse" instead of a silent opt-in.
    #
    # def resolve_source_record_collision(
    #     self,
    #     selector: SourceInputSelector,
    #     stored: Mapping[str, Any],
    #     incoming: Mapping[str, Any],
    # ) -> SourceRecordCollisionResolution | None: ...


@dataclass(frozen=True, slots=True)
class SourceCatalogSuccession:
    """Admitted predecessor evidence exposed without a storage dependency."""

    logical_id: str
    artifact_digest: str
    reason: str

    def __post_init__(self) -> None:
        require_text(self.logical_id, "source catalog predecessor logical_id")
        require_sha256(self.artifact_digest, "source catalog predecessor artifact_digest")
        require_text(self.reason, "source catalog succession reason")


@dataclass(frozen=True, slots=True)
class SourceCatalogSnapshotSummary:
    """Identity and completeness evidence exposed to downstream consumers."""

    logical_id: str
    artifact_digest: str
    catalog_id: str
    catalog_state_digest: str
    requested_universe_set_digest: str
    selected_source_set_digest: str
    item_count: int
    disposition_counts: Mapping[str, int]
    #: One row per (disposition, reasonCode) that occurred, as the receipt
    #: carries them; reconciled here so no consumer trusts a count the rows
    #: do not support. Selected rows carry no reason and never appear.
    reason_counts: tuple[Mapping[str, Any], ...]
    partitions: tuple[str, ...]
    selection_policy: Mapping[str, str]
    partition_policy: Mapping[str, Any]
    join_coverage: tuple[Mapping[str, Any], ...]
    diagnostic_digests: Mapping[str, str]
    source_native_inputs: tuple[Mapping[str, str], ...]
    byte_measurements: Mapping[str, int]
    succession: SourceCatalogSuccession | None = None

    def __post_init__(self) -> None:
        require_text(self.logical_id, "source catalog logical_id")
        require_sha256(self.artifact_digest, "source catalog artifact_digest")
        require_text(self.catalog_id, "catalog_id")
        require_sha256(self.catalog_state_digest, "catalog_state_digest")
        require_sha256(self.requested_universe_set_digest, "requested_universe_set_digest")
        require_sha256(self.selected_source_set_digest, "selected_source_set_digest")
        if self.item_count < 0 or any(value < 0 for value in self.disposition_counts.values()):
            raise ValueError("source catalog counts must be non-negative")
        if sum(self.disposition_counts.values()) != self.item_count:
            raise ValueError("source catalog disposition counts must account for every row")
        object.__setattr__(
            self,
            "disposition_counts",
            MappingProxyType(dict(self.disposition_counts)),
        )
        reason_rows = tuple(dict(value) for value in self.reason_counts)
        seen: set[tuple[str, str]] = set()
        reason_totals = {name: 0 for name in self.disposition_counts}
        for row in reason_rows:
            if set(row) != {"disposition", "reasonCode", "count"}:
                raise ValueError("source catalog reason counts must have a closed shape")
            disposition = require_text(row["disposition"], "source catalog reason count disposition")
            reason_code = require_text(row["reasonCode"], "source catalog reason count reasonCode")
            if disposition == "selected" or disposition not in reason_totals:
                raise ValueError("source catalog reason counts name a disposition that carries no reason")
            count = row["count"]
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise ValueError("source catalog reason counts must be positive integers")
            if (disposition, reason_code) in seen:
                raise ValueError("source catalog reason counts must be distinct")
            seen.add((disposition, reason_code))
            reason_totals[disposition] += count
        for name, total in reason_totals.items():
            if name != "selected" and total != self.disposition_counts[name]:
                raise ValueError("source catalog reason counts must account for every non-selected row")
        object.__setattr__(
            self,
            "reason_counts",
            tuple(MappingProxyType(row) for row in reason_rows),
        )
        selection_policy = dict(self.selection_policy)
        if set(selection_policy) != {"policyId", "policyVersion", "policyDigest"}:
            raise ValueError("source catalog selection policy must have a closed pin")
        require_text(selection_policy["policyId"], "source catalog selection policyId")
        require_text(selection_policy["policyVersion"], "source catalog selection policyVersion")
        require_sha256(
            selection_policy["policyDigest"],
            "source catalog selection policyDigest",
        )
        partition_policy = dict(self.partition_policy)
        if set(partition_policy) != {
            "policyId",
            "policyVersion",
            "policyDigest",
            "bucketCount",
        }:
            raise ValueError("source catalog partition policy must have a closed pin")
        require_text(partition_policy["policyId"], "source catalog partition policyId")
        require_text(partition_policy["policyVersion"], "source catalog partition policyVersion")
        require_sha256(
            partition_policy["policyDigest"],
            "source catalog partition policyDigest",
        )
        bucket_count = partition_policy["bucketCount"]
        if (
            not isinstance(bucket_count, int)
            or isinstance(bucket_count, bool)
            or not 1 <= bucket_count <= 65_536
        ):
            raise ValueError("source catalog partition bucketCount is invalid")
        coverage_rows: list[Mapping[str, Any]] = []
        join_ids: set[str] = set()
        if len(self.join_coverage) > SOURCE_CATALOG_MAX_JOIN_IDS:
            raise ValueError("source catalog join coverage exceeds its distinct-identity limit")
        for value in self.join_coverage:
            coverage = dict(value)
            if set(coverage) != {
                "joinId",
                "eligible",
                "matched",
                "unmatched",
                "nullResult",
            }:
                raise ValueError("source catalog join coverage must have a closed shape")
            join_id = coverage["joinId"]
            require_text(join_id, "source catalog joinId")
            if join_id in join_ids:
                raise ValueError("source catalog join coverage identities must be distinct")
            join_ids.add(join_id)
            for name in ("eligible", "matched", "unmatched", "nullResult"):
                count = coverage[name]
                if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                    raise ValueError("source catalog join coverage counts must be non-negative integers")
            if coverage["eligible"] != coverage["matched"] + coverage["unmatched"]:
                raise ValueError("source catalog join coverage eligible count does not reconcile")
            if coverage["eligible"] + coverage["nullResult"] > self.item_count:
                raise ValueError("source catalog join coverage exceeds the catalog population")
            coverage_rows.append(MappingProxyType(coverage))
        diagnostic_digests = dict(self.diagnostic_digests)
        if set(diagnostic_digests) != {
            "normalizedFieldsDigest",
            "joinedFieldsDigest",
            "dispositionsDigest",
            "reasonsDigest",
            "interpretationsDigest",
            "renditionChoicesDigest",
        }:
            raise ValueError("source catalog diagnostic digest set is incomplete")
        for name, digest in diagnostic_digests.items():
            require_sha256(digest, f"source catalog {name}")
        source_native_inputs: list[Mapping[str, str]] = []
        if not self.source_native_inputs:
            raise ValueError("source catalog must retain at least one source-native input pin")
        for value in self.source_native_inputs:
            source_input = dict(value)
            if set(source_input) != {"logicalId", "artifactDigest"}:
                raise ValueError("source catalog input pin must have a closed shape")
            require_text(source_input["logicalId"], "source catalog input logicalId")
            require_sha256(
                source_input["artifactDigest"],
                "source catalog input artifactDigest",
            )
            source_native_inputs.append(MappingProxyType(source_input))
        if len(
            {
                (value["logicalId"], value["artifactDigest"])
                for value in source_native_inputs
            }
        ) != len(source_native_inputs):
            raise ValueError("source catalog input pins must be distinct")
        byte_measurements = dict(self.byte_measurements)
        if set(byte_measurements) != {
            "payloadBytesRead",
            "payloadBytesReused",
            "payloadBytesWritten",
            "publicationBytesWritten",
        }:
            raise ValueError("source catalog byte measurements must have a closed shape")
        for value in byte_measurements.values():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError("source catalog byte measurements must be non-negative integers")
        if byte_measurements["payloadBytesRead"] != (
            byte_measurements["payloadBytesReused"]
            + byte_measurements["payloadBytesWritten"]
        ):
            raise ValueError("source catalog payload byte measurements do not reconcile")
        object.__setattr__(self, "selection_policy", MappingProxyType(selection_policy))
        object.__setattr__(self, "partition_policy", MappingProxyType(partition_policy))
        object.__setattr__(self, "join_coverage", tuple(coverage_rows))
        object.__setattr__(self, "diagnostic_digests", MappingProxyType(diagnostic_digests))
        object.__setattr__(self, "source_native_inputs", tuple(source_native_inputs))
        object.__setattr__(self, "byte_measurements", MappingProxyType(byte_measurements))
        if self.succession is not None and not isinstance(
            self.succession,
            SourceCatalogSuccession,
        ):
            raise TypeError("source catalog succession must use SourceCatalogSuccession")


@dataclass(frozen=True, slots=True)
class LocatedSourceCatalogItem:
    """One normative item and the exact immutable payload that supplied it."""

    item: SourceCatalogItem
    blob_ref: str

    def __post_init__(self) -> None:
        require_sha256(self.blob_ref, "source catalog item blob_ref")


@dataclass(slots=True)
class SourceCatalogSnapshot:
    """One verified root and its bounded full normative row stream."""

    summary: SourceCatalogSnapshotSummary
    located_items: Iterator[LocatedSourceCatalogItem]

    @property
    def items(self) -> Iterator[SourceCatalogItem]:
        """Expose existing consumers to the same single-pass located stream."""

        return (located.item for located in self.located_items)


class ImmutableSourceCatalogReader(Protocol):
    """Open one complete immutable DocSpec source-catalog snapshot."""

    def open_snapshot(self, reference: SourceCatalogRef) -> SourceCatalogSnapshot: ...

    def verify_snapshot(self, reference: SourceCatalogRef) -> SourceCatalogSnapshotSummary: ...


class SourceCatalogCurrentPointer(Protocol):
    """Conditionally advance one catalog series after complete admission."""

    def current(self, catalog_id: str) -> SourceCatalogRef | None: ...

    def advance(
        self,
        catalog_id: str,
        candidate: SourceCatalogRef,
        *,
        expected_current: SourceCatalogRef | None,
    ) -> SourceCatalogRef: ...
