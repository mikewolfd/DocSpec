"""Small shared constructors for DocSpec contract tests."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from docspec.errors import IntegrityError
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from rulespec_artifacts import Producer

from docspec.adapters.catalog_policy_workspace import SqliteCatalogPolicyWorkspace
from docspec.adapters.content_fetchers import LocalFileContentFetcher
from docspec.adapters.source_catalog_artifact import (
    SourceCatalogArtifactReader,
    SourceCatalogBuildRequest,
    SourceCatalogBuilder,
)
from docspec.adapters.source_catalog_store import LocalSourceCatalogStore
from docspec.domain.content import CandidateFile, SourceItem, SourceItemState
from docspec.domain.execution import (
    EXECUTE_AND_DELIVER_OPERATION_ID,
    ExecutionHandoff,
    ExecutionLimits,
    ExecutionProfile,
    StoreTaskResult,
    iter_store_tasks,
    summarize_store_tasks,
)
from docspec.domain.identity import (
    canonical_json_bytes,
    canonical_json_file_bytes,
    identity_digest,
    sha256_digest,
    stable_urn,
)
from docspec.domain.policies import DataUsePolicy, RetentionPolicy
from docspec.domain.processors import ProcessorPayload, ProcessorRecordRef, ProcessorRequest
from docspec.domain.profiles import ProfilePin, ProfileRole, ProfileSet
from docspec.domain.references import ArtifactRef, DocumentReleaseRef, LayerRef, SourceCatalogRef, StoreRef
from docspec.domain.storage import PartitionPolicy, RecordSchema
from docspec.domain.source_catalog import (
    CatalogDisposition,
    CatalogNormalizationField,
    CatalogRenditionFamily,
    CatalogSelectionDecision,
    SourceCatalogCandidate,
    SourceCatalogItem,
    SourceCatalogSelection,
)
from docspec.ports.content_fetcher import FetchStream
from docspec.ports.source_catalog import (
    CatalogPolicyInputs,
    CatalogPolicyWorkspace,
    SourceInputSelector,
    SourceNativeDescription,
)


EMPTY_DIGEST = sha256_digest(b"")
DATA_USE_POLICY = DataUsePolicy.local_content()
RETENTION_POLICY = RetentionPolicy.retain_all()
TASK_RESULT_SCHEMA = RecordSchema(
    "docspec-store-task-result-record/1.0",
    ("recordId", "sourceItemId", "result"),
    "recordId",
    "sourceItemId",
)
_FIXTURE_SOURCE_ORIGIN = "https://t.test/"
_FIXTURE_SCHEMA_DIGEST = "sha256:" + "f" * 64
_FIXTURE_SOURCE_SYSTEM = "urn:docspec:test:source-native"
_FIXTURE_SELECTOR = SourceInputSelector(
    _FIXTURE_SOURCE_SYSTEM,
    "1",
    "s",
    "i",
    "1.0",
)


def source_catalog_producer() -> Producer:
    implementation = "git+https://example.test/docspec@" + "1" * 40
    return Producer(
        "docspec",
        implementation,
        "urn:docspec:verifier:source-catalog",
        "1.0.0",
        implementation,
    )


def document_release_producer() -> Producer:
    implementation = "git+https://example.test/docspec@" + "1" * 40
    return Producer(
        "docspec",
        implementation,
        "urn:docspec:verifier:document-release",
        "1.0.0",
        implementation,
    )


@dataclass(frozen=True, slots=True)
class _FixtureCatalogPolicy:
    policy_id = "p"
    policy_version = "1"

    @property
    def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
        return (_FIXTURE_SELECTOR,)

    @property
    def configuration(self) -> Mapping[str, object]:
        return {
            "universeInputs": [
                selector.to_dict() for selector in self.universe_inputs
            ]
        }

    @property
    def policy_digest(self) -> str:
        return sha256_digest(
            canonical_json_bytes(
                {
                    "format": "docspec-catalog-policy",
                    "formatVersion": "1.0",
                    "policyId": self.policy_id,
                    "policyVersion": self.policy_version,
                    "configuration": dict(self.configuration),
                }
            )
        )

    def iter_items(
        self,
        inputs: CatalogPolicyInputs,
        workspace: CatalogPolicyWorkspace,
    ) -> Iterator[SourceCatalogItem]:
        del workspace
        for row in inputs.iter_universe_rows():
            payload = row.record["record"]
            if not isinstance(payload, Mapping) or set(payload) != {"catalogItem"}:
                raise ValueError("test source-native payload differs")
            yield SourceCatalogItem.from_dict(payload["catalogItem"])


@dataclass(slots=True)
class _FixtureSource:
    metadata: SourceNativeDescription
    records: tuple[Mapping[str, object], ...]

    def describe(self) -> SourceNativeDescription:
        return self.metadata

    def iter_records(self) -> Iterator[Mapping[str, object]]:
        yield from self.records

    def iter_renditions(self) -> Iterator[Mapping[str, object]]:
        return
        yield


def shared_source_record(item: SourceItem) -> dict[str, object]:
    """Map one processing fixture into the current normative catalog shape."""

    disposition = {
        SourceItemState.ACTIVE: "selected",
        SourceItemState.DELETED: "deleted",
        SourceItemState.EXCLUDED: "excluded",
    }[item.state]
    selected_disposition = CatalogDisposition(disposition)
    reason_code = None if disposition == "selected" else f"test.{disposition}"
    reason = None if disposition == "selected" else f"Test item is {disposition}."
    candidates: list[SourceCatalogCandidate] = []
    for candidate in item.candidates:
        locator = candidate.locator
        if not locator.startswith(("http://", "https://")):
            locator = _FIXTURE_SOURCE_ORIGIN + quote(locator, safe="/")
        candidates.append(
            SourceCatalogCandidate(
                candidate.candidate_id,
                candidate.media_type,
                "source-url",
                locator,
                candidate.expected_digest,
                candidate.expected_size,
            )
        )
    policy = _FixtureCatalogPolicy()
    pin = {
        "policyId": policy.policy_id,
        "policyVersion": policy.policy_version,
        "policyDigest": policy.policy_digest,
        "inputScopeIds": [_FIXTURE_SELECTOR.scope_id],
    }
    candidate_ids = [candidate.rendition_id for candidate in candidates]
    decision = CatalogSelectionDecision(
        "s",
        disposition == "selected",
        None if disposition == "selected" else selected_disposition,
        reason_code,
        reason,
    )
    normalized = {
        "title": "T",
        "agencies": [],
        "documentType": None,
        "publicationDate": None,
        "lastUpdatedDate": None,
        "docketIds": [],
        "regulationIdentifierNumbers": [],
        "commentCloseDate": None,
        "language": None,
        "sourceUrl": None,
    }
    return SourceCatalogItem(
        source_item_id=item.item_id,
        document_id=item.item_id,
        source_issued_version=item.version,
        source_native_facts=(
            {
                "scopeId": _FIXTURE_SELECTOR.scope_id,
                "schemaName": _FIXTURE_SELECTOR.schema_name,
                "schemaVersion": _FIXTURE_SELECTOR.schema_version,
                "schemaDigest": _FIXTURE_SCHEMA_DIGEST,
                "fields": {"metadata": item.metadata},
            },
        ),
        normalized_metadata=normalized,
        source_observed_topics=(),
        source_observations=(),
        interpretations=(
            {
                "interpretationKind": "exact-join",
                **pin,
                "result": {"joins": []},
            },
            {
                "interpretationKind": "normalization",
                **pin,
                "result": {
                    "fields": [
                        CatalogNormalizationField(
                            "title",
                            ("i",),
                            "source",
                            "normalized",
                            "T",
                        ).to_dict()
                    ]
                },
            },
            {
                "interpretationKind": "rendition-preference",
                **pin,
                "result": {
                    "orderedFamilyIds": ["f"],
                    "families": [CatalogRenditionFamily("f", tuple(candidate_ids)).to_dict()],
                    "selectedFamilyId": "f" if candidates else None,
                    "selectedRenditionIds": candidate_ids,
                },
            },
            {
                "interpretationKind": "sampling",
                **pin,
                "result": {
                    "frameAdmitted": True,
                    "partition": "p",
                    "stratum": ["s"],
                    "orderHash": None,
                    "rank": None,
                    "stratumSize": None,
                    "allocationMethod": "all",
                    "limit": None,
                    "drawn": True,
                },
            },
            {
                "interpretationKind": "selection",
                **pin,
                "result": {
                    "decisions": [decision.to_dict()],
                    "finalDisposition": disposition,
                    "reasonCode": reason_code,
                    "reason": reason,
                },
            },
            {
                "interpretationKind": "topic-recovery",
                **pin,
                "result": {
                    "sourceField": "t",
                    "outcome": "not-recovered",
                    "evidenceDigest": None,
                    "observedTopicIds": [],
                },
            },
        ),
        candidate_renditions=tuple(candidates),
        selection=SourceCatalogSelection(selected_disposition, reason_code, reason),
    ).to_dict()


def write_shared_source_catalog(
    root: Path,
    items: tuple[SourceItem, ...],
    *,
    name: str = "catalog",
) -> SourceCatalogRef:
    """Publish a small exact current-format source catalog for tests."""

    catalog_items = tuple(shared_source_record(item) for item in items)
    records = tuple(
        {
            "sourceRecordId": item.item_id,
            "scopeId": _FIXTURE_SELECTOR.scope_id,
            "schemaName": _FIXTURE_SELECTOR.schema_name,
            "schemaVersion": _FIXTURE_SELECTOR.schema_version,
            "schemaDigest": _FIXTURE_SCHEMA_DIGEST,
            "record": {"catalogItem": catalog_item},
            "fieldDiagnostics": [],
        }
        for item, catalog_item in zip(items, catalog_items, strict=True)
    )
    state_digest = sha256_digest(canonical_json_bytes(list(records)))
    source = _FixtureSource(
        SourceNativeDescription(
            logical_id="urn:docspec:test:source-native:" + state_digest.removeprefix("sha256:"),
            artifact_digest=state_digest,
            source_system_id=_FIXTURE_SELECTOR.source_system_id,
            source_system_version=_FIXTURE_SELECTOR.source_system_version,
            source_state_scope="complete-snapshot",
            source_state_digest=state_digest,
            source_native_schema_set_digest=_FIXTURE_SCHEMA_DIGEST,
        ),
        records,
    )
    result = SourceCatalogBuilder(
        store=LocalSourceCatalogStore(root),
        policy=_FixtureCatalogPolicy(),
        request=SourceCatalogBuildRequest(f"urn:docspec:test:catalog:{name}", source_catalog_producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((source,))
    return result.reference


def source_catalog_reader(root: Path) -> SourceCatalogArtifactReader:
    return SourceCatalogArtifactReader(
        LocalSourceCatalogStore(root, create=False),
        producer=source_catalog_producer(),
    )


class SharedFixtureContentFetcher:
    """Resolve the shared test HTTPS namespace through an injected local reader."""

    def __init__(self, root: Path) -> None:
        self._local = LocalFileContentFetcher(root)

    def fetch(self, candidate: CandidateFile, **kwargs):  # type: ignore[no-untyped-def]
        parsed = urlsplit(candidate.locator)
        if parsed.scheme != "https" or parsed.netloc != "t.test":
            raise ValueError("shared fixture candidate is outside the test source namespace")
        local = replace(candidate, locator=unquote(parsed.path.lstrip("/")))
        result = self._local.fetch(local, **kwargs)
        return FetchStream(
            replace(result.metadata, transport_version=candidate.transport_version),
            result.chunks,
            result.close_callback,
        )


def artifact(identifier: str, *, locator: str | None = None) -> ArtifactRef:
    payload = canonical_json_file_bytes({"id": identifier})
    return ArtifactRef(identifier, locator or f"memory://{identifier}", sha256_digest(payload), "application/json", len(payload))


def profile_set() -> ProfileSet:
    pins = tuple(
        sorted(
            (
                ProfilePin(
                    role=role,
                    profile_id=f"urn:docspec:test-profile:{role.value}",
                    version="1.0.0",
                    implementation_id=f"tests.{role.value}.v1",
                    configuration_digest=EMPTY_DIGEST,
                    description_digest=EMPTY_DIGEST,
                    capabilities=("test-fixture",),
                )
                for role in ProfileRole
            ),
            key=lambda item: item.role.value,
        )
    )
    return ProfileSet(pins)


def local_profile_set(*, result_profile_id: str = "urn:docspec:profile:result-delivery:durable-dataset:1") -> ProfileSet:
    identifiers = {
        ProfileRole.RELEASE_MANIFEST: (
            "urn:docspec:profile:release-manifest:canonical-json:1",
            "docspec.release-manifest.canonical-json.v1",
        ),
        ProfileRole.DOCUMENT_CATALOG: (
            "urn:docspec:profile:document-catalog:local-manifest:1",
            "docspec.document-catalog.local-manifest.v1",
        ),
        ProfileRole.RECORD_STORAGE: (
            "urn:docspec:profile:record-storage:local-jsonl:1",
            "docspec.record-storage.local-jsonl.v1",
        ),
        ProfileRole.BLOB_STORAGE: (
            "urn:docspec:profile:blob-storage:local-content-addressed:1",
            "docspec.blob-storage.local-content-addressed.v1",
        ),
        ProfileRole.DOCUMENT_STORE: (
            "urn:docspec:profile:document-store-persistence:local-json:1",
            "docspec.document-store-persistence.local-json.v1",
        ),
        ProfileRole.RESULT_DELIVERY: (result_profile_id, "docspec.result-delivery.durable-dataset.v1"),
    }
    pins = tuple(
        sorted(
            (
                ProfilePin(
                    role,
                    identifiers[role][0],
                    "1.0.0",
                    identifiers[role][1],
                    identity_digest({}),
                    identity_digest({"profile": identifiers[role][0]}),
                    ("local-reference",),
                )
                for role in ProfileRole
            ),
            key=lambda item: item.role.value,
        )
    )
    return ProfileSet(pins)


def persist_execution_evidence(
    *,
    controls,
    records,
    plan_ref: ArtifactRef,
    planned_store_ledger: LayerRef,
    planned_stores: Iterable[StoreRef],
    sealed_stores: Iterable[StoreRef],
    partition_policy: PartitionPolicy,
    base_release: DocumentReleaseRef | None = None,
) -> tuple[ArtifactRef, ArtifactRef, LayerRef]:
    """Build exact small execution evidence shared by catalog-focused tests."""

    worker = controls.put(
        kind="worker-compositions",
        artifact_id="urn:docspec:test:worker-composition",
        value={"implementationId": "tests.worker/v1"},
    )
    scheduler = controls.put(
        kind="scheduler-configurations",
        artifact_id="urn:docspec:test:scheduler-configuration",
        value={"adapterId": "docspec.local-threaded"},
    )
    profile = ExecutionProfile(
        "docspec.local-threaded",
        "1.0.0",
        worker,
        scheduler,
        ExecutionLimits(1, 1, 1, 1024**3, 1024**3, 100, 1, 1, 0, 0),
        2_000_000_000,
    )
    profile_ref = controls.put(
        kind="execution-profiles",
        artifact_id=profile.profile_id,
        value=profile.to_dict(),
    )
    tasks = tuple(
        iter_store_tasks(
            plan_ref.artifact_id,
            EXECUTE_AND_DELIVER_OPERATION_ID,
            planned_stores,
        )
    )
    task_count, task_digest = summarize_store_tasks(tasks)
    sink = controls.put(
        kind="sinks",
        artifact_id="urn:docspec:test:result-sink",
        value={"sinkId": "urn:docspec:test:result-sink"},
    )
    handoff = ExecutionHandoff(
        processing_plan=plan_ref,
        execution_profile=profile_ref,
        worker_composition=worker,
        planned_store_ledger=planned_store_ledger,
        operation_id=EXECUTE_AND_DELIVER_OPERATION_ID,
        expected_task_count=task_count,
        task_set_digest=task_digest,
        result_sink=sink,
        base_release=base_release,
    )
    handoff_ref = controls.put(
        kind="execution-handoffs",
        artifact_id=handoff.handoff_id,
        value=handoff.to_dict(),
    )
    outputs = {reference.store_id: reference for reference in sealed_stores}
    rows = []
    for task in tasks:
        output = outputs[task.input_store.store_id]
        result = StoreTaskResult.succeeded(
            handoff_id=handoff.handoff_id,
            task=task,
            output_store=output,
        )
        rows.append(
            {
                "recordId": task.input_store.store_id,
                "sourceItemId": task.input_store.store_id,
                "result": result.to_dict(),
            }
        )
    result_ledger = records.write_layer(
        rows,
        layer_kind="execution-task-results",
        schema=TASK_RESULT_SCHEMA,
        partition_policy=partition_policy,
    )
    return profile_ref, handoff_ref, result_ledger


def segment_processor_request(processor, segment, *, prerequisites=()) -> ProcessorRequest:
    """Construct one exact segment request for processor-focused tests."""

    description = processor.description
    input_reference = ProcessorRecordRef.for_segment(segment.segment)
    return ProcessorRequest(
        artifact("urn:docspec:test:processor-plan"),
        description.processor_id,
        identity_digest(description.to_dict()),
        segment.segment.source_item_id,
        (input_reference,),
        tuple(prerequisites),
        DATA_USE_POLICY.allowed_fields,
        description.item_limits,
        description.cache_policy.key_schema_id or "docspec-cache-disabled/1",
        stable_urn(
            "processor-invocation",
            {"processorId": description.processor_id, "segmentId": segment.segment.segment_id},
        ),
    )


def processor_payload(segment) -> ProcessorPayload:
    """Project one segment through the shared local-only test policy."""

    return ProcessorPayload.for_segment(
        segment.segment,
        segment.content,
        DATA_USE_POLICY.allowed_fields,
    )


class KillAfter:
    """Wrap a catalog policy so its item stream dies after ``yields`` items.

    Stands in for the harness kill that ended two catalog-A builds: the
    process stops mid-stream with no chance to finish, and whatever the
    workspace had committed is all a resume can see. Everything else
    delegates to the wrapped policy, so the builder's identity checks see
    the real policy.
    """

    def __init__(self, policy: object, yields: int) -> None:
        self._policy = policy
        self._yields = yields
        self.computed = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._policy, name)

    def iter_items(self, inputs: object, workspace: object) -> Iterator[object]:
        for item in self._policy.iter_items(inputs, workspace):  # type: ignore[attr-defined]
            if self.computed >= self._yields:
                raise IntegrityError("injected kill mid-stream")
            self.computed += 1
            yield item


class CountItems:
    """Delegate to a policy and count how many items it computed."""

    def __init__(self, policy: object) -> None:
        self._policy = policy
        self.computed = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._policy, name)

    def iter_items(self, inputs: object, workspace: object) -> Iterator[object]:
        for item in self._policy.iter_items(inputs, workspace):  # type: ignore[attr-defined]
            self.computed += 1
            yield item
