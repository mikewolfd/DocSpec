from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from rulespec_artifacts import Producer

from docspec.adapters.catalog_policy_workspace import SqliteCatalogPolicyWorkspace
from docspec.adapters.source_catalog_artifact import (
    SourceCatalogArtifactReader,
    SourceCatalogBuildRequest,
    SourceCatalogBuilder,
)
from docspec.adapters.source_catalog_store import LocalSourceCatalogStore
import docspec.adapters.spicyregs_source_native as spicyregs_adapter_module
from docspec.adapters.spicyregs_source_native import (
    SpicyRegsSourceNativeAdapter,
    spicyregs_source_profile,
)
from docspec.application.regulations_gov_catalog import (
    RegulationsGovCatalogPolicy,
    RegulationsGovSamplePolicy,
)
from docspec.domain.source_catalog import CatalogDisposition, SourceCatalogItem
from docspec.errors import IntegrityError
from docspec.ports.source_catalog import SourceInputSelector, SourceNativeDescription
from docspec.source_catalog_cli import build_parser as source_catalog_build_parser

_DOCUMENT_SYSTEM = "urn:test:regulations-gov:documents"
_DOCKET_SYSTEM = "urn:test:regulations-gov:dockets"
_COMMENT_SYSTEM = "urn:test:regulations-gov:comments"
_FEDERAL_REGISTER_SYSTEM = "https://www.federalregister.gov/api/v1"
_REGULATIONS_VERSION = "regulations.gov-v4-mirrulations-raw-data"
_SHA_A = "sha256:" + "a" * 64
_SHA_B = "sha256:" + "b" * 64
_SHA_C = "sha256:" + "c" * 64
_SHA_D = "sha256:" + "d" * 64
_SHA_E = "sha256:" + "e" * 64


def _producer() -> Producer:
    implementation = "git+https://example.test/docspec@" + "1" * 40
    return Producer(
        "docspec",
        implementation,
        "urn:docspec:verifier:source-catalog",
        "1.0.0",
        implementation,
    )


def _description(
    identity: str,
    source_system_id: str,
    source_system_version: str,
    *,
    state_scope: str = "complete-snapshot",
) -> SourceNativeDescription:
    artifact_digest = {
        "documents": _SHA_A,
        "dockets": _SHA_B,
        "federal-register": _SHA_C,
        "comments": _SHA_E,
    }[identity]
    return SourceNativeDescription(
        logical_id=f"urn:test:source-native:{artifact_digest.removeprefix('sha256:')}",
        artifact_digest=artifact_digest,
        source_system_id=source_system_id,
        source_system_version=source_system_version,
        source_state_scope=state_scope,
        source_state_digest=_SHA_D,
        source_native_schema_set_digest=_SHA_A,
    )


@dataclass
class _Source:
    description_value: SourceNativeDescription
    records: tuple[Mapping[str, Any], ...]
    renditions: tuple[Mapping[str, Any], ...] = ()

    def describe(self) -> SourceNativeDescription:
        return self.description_value

    def iter_records(self) -> Iterator[Mapping[str, Any]]:
        yield from self.records

    def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
        yield from self.renditions


def _source_record(
    identity: str,
    *,
    scope: str,
    schema: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "fieldDiagnostics": [],
        "record": dict(record),
        "schemaDigest": _SHA_A,
        "schemaName": schema,
        "schemaVersion": "1.0",
        "scopeId": scope,
        "sourceRecordId": identity,
    }


def _document(
    identity: str = "EPA-2026-0001-0001",
    **updates: object,
) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "additionalRins": ["2060-AV12", "not-a-rin"],
        "agencyId": "EPA",
        "commentEndDate": "2026-09-01T00:00:00Z",
        "docketId": "EPA-2026-0001",
        "documentType": "Notice",
        "frDocNum": "2026-10001",
        "modifyDate": "2026-08-25T01:02:03Z",
        "postedDate": "2026-08-24T04:00:00Z",
        "reasonWithdrawn": None,
        "title": "Exact source title",
        "topics": ["Air quality", {"id": "source-topic", "label": "Source topic"}],
        "withdrawn": False,
    }
    attributes.update(updates)
    return _source_record(
        identity,
        scope="regulations-gov-documents",
        schema="regulations-gov-document-raw",
        record={
            "data": {
                "id": identity,
                "type": "documents",
                "attributes": attributes,
                "links": {"self": f"https://api.regulations.gov/v4/documents/{identity}"},
            }
        },
    )


def _docket(
    identity: str = "EPA-2026-0001",
    *,
    include_link: bool = False,
    **updates: object,
) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "agencyId": "EPA",
        "dkAbstract": "Exact docket abstract",
        "docketType": "Rulemaking",
        "modifyDate": "2026-08-24T05:00:00Z",
        "rin": "2060-AZ99",
        "title": "Exact docket title",
    }
    attributes.update(updates)
    data: dict[str, Any] = {
        "id": identity,
        "type": "dockets",
        "attributes": attributes,
    }
    if include_link:
        data["links"] = {
            "self": f"https://api.regulations.gov/v4/dockets/{identity}"
        }
    return _source_record(
        identity,
        scope="regulations-gov-dockets",
        schema="regulations-gov-docket-raw",
        record={
            "data": data
        },
    )


def _comment(
    identity: str = "EPA-2026-0001-9001",
    *,
    modify_date: str | None = "2026-08-25T01:02:03Z",
    include_link: bool = True,
    include_body: bool = True,
    **updates: object,
) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "agencyId": "EPA",
        "commentOn": "source-object",
        "commentOnDocumentId": "EPA-2026-0001-0001",
        "docketId": "EPA-2026-0001",
        "documentType": "Public Submission",
        "modifyDate": modify_date,
        "postedDate": "2026-08-24T04:00:00Z",
        "reasonWithdrawn": None,
        "title": None,
        "withdrawn": False,
    }
    if include_body:
        attributes["comment"] = "Exact public comment body"
    attributes.update(updates)
    data: dict[str, Any] = {
        "id": identity,
        "type": "comments",
        "attributes": attributes,
    }
    if include_link:
        data["links"] = {
            "self": f"https://api.regulations.gov/v4/comments/{identity}"
        }
    return _source_record(
        identity,
        scope="regulations-gov-comments",
        schema="regulations-gov-comment-raw",
        record={
            "data": data,
            "included": [
                {
                    "id": f"{identity}-attachment",
                    "type": "attachments",
                    "attributes": {
                        "title": "Exact attachment",
                        "fileFormats": [
                            {
                                "fileUrl": f"https://downloads.regulations.gov/{identity}/attachment.pdf",
                                "format": "pdf",
                                "size": 123,
                            }
                        ],
                    },
                }
            ],
        },
    )


def _federal_register(identity: str = "2026-10001") -> dict[str, Any]:
    return _source_record(
        identity,
        scope="federal-register-documents",
        schema="federal-register-document",
        record={
            "document_number": identity,
            "docket_ids": ["EPA-2026-0001"],
            "html_url": f"https://www.federalregister.gov/d/{identity}",
            "regulation_id_numbers": ["2060-AX01"],
            "title": "Exact Federal Register title",
        },
    )


def _rendition(
    identity: str,
    rendition_id: str,
    locator: str,
    *,
    source_field: str,
    media_type: str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
) -> dict[str, Any]:
    return {
        "sourceRecordId": identity,
        "renditionId": rendition_id,
        "sourceField": source_field,
        "locator": locator,
        "mediaType": media_type,
        "expectedSha256": expected_sha256,
        "expectedByteSize": expected_byte_size,
    }


def _policy(*, include_comments: bool = False) -> RegulationsGovCatalogPolicy:
    return RegulationsGovCatalogPolicy(
        SourceInputSelector(
            _DOCUMENT_SYSTEM,
            _REGULATIONS_VERSION,
            "regulations-gov-documents",
            "regulations-gov-document-raw",
            "1.0",
        ),
        SourceInputSelector(
            _DOCKET_SYSTEM,
            _REGULATIONS_VERSION,
            "regulations-gov-dockets",
            "regulations-gov-docket-raw",
            "1.0",
        ),
        SourceInputSelector(
            _FEDERAL_REGISTER_SYSTEM,
            "v1",
            "federal-register-documents",
            "federal-register-document",
            "1.0",
        ),
        {"EPA": "Environmental Protection Agency"},
        comment_input=(
            SourceInputSelector(
                _COMMENT_SYSTEM,
                _REGULATIONS_VERSION,
                "regulations-gov-comments",
                "regulations-gov-comment-raw",
                "1.0",
            )
            if include_comments
            else None
        ),
    )


def _build_items(
    root: Path,
    documents: tuple[Mapping[str, Any], ...],
    *,
    policy: RegulationsGovCatalogPolicy | None = None,
    document_renditions: tuple[Mapping[str, Any], ...] = (),
    docket_records: tuple[Mapping[str, Any], ...] = (_docket(),),
    comment_records: tuple[Mapping[str, Any], ...] = (),
    comment_renditions: tuple[Mapping[str, Any], ...] = (),
    federal_register_records: tuple[Mapping[str, Any], ...] = (_federal_register(),),
    federal_register_renditions: tuple[Mapping[str, Any], ...] | None = None,
) -> tuple[SourceCatalogItem, ...]:
    if federal_register_renditions is None:
        federal_register_renditions = (
            _rendition(
                "2026-10001",
                "2026-10001/html",
                "https://www.federalregister.gov/d/2026-10001",
                source_field="html_url",
                media_type="text/html",
            ),
        )
    selected_policy = policy or _policy()
    sources: list[_Source] = [
        _Source(
            _description("documents", _DOCUMENT_SYSTEM, _REGULATIONS_VERSION),
            documents,
            document_renditions,
        ),
        _Source(
            _description("dockets", _DOCKET_SYSTEM, _REGULATIONS_VERSION),
            docket_records,
        ),
    ]
    if selected_policy.comment_input is not None:
        sources.append(
            _Source(
                _description("comments", _COMMENT_SYSTEM, _REGULATIONS_VERSION),
                comment_records,
                comment_renditions,
            )
        )
    sources.append(
        _Source(
            _description(
                "federal-register",
                _FEDERAL_REGISTER_SYSTEM,
                "v1",
                state_scope="observed-crawl",
            ),
            federal_register_records,
            federal_register_renditions,
        )
    )
    store = LocalSourceCatalogStore(root)
    result = SourceCatalogBuilder(
        store=store,
        policy=selected_policy,
        request=SourceCatalogBuildRequest("urn:test:catalog:regulations-gov", _producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build(tuple(sources))
    snapshot = SourceCatalogArtifactReader(store, producer=_producer()).open_snapshot(
        result.reference
    )
    return tuple(snapshot.items)


def _build(
    root: Path,
    document: Mapping[str, Any],
    *,
    policy: RegulationsGovCatalogPolicy | None = None,
    document_renditions: tuple[Mapping[str, Any], ...] = (),
    docket_records: tuple[Mapping[str, Any], ...] = (_docket(),),
    federal_register_records: tuple[Mapping[str, Any], ...] = (_federal_register(),),
    federal_register_renditions: tuple[Mapping[str, Any], ...] | None = None,
) -> SourceCatalogItem:
    items = _build_items(
        root,
        (document,),
        policy=policy,
        document_renditions=document_renditions,
        docket_records=docket_records,
        federal_register_records=federal_register_records,
        federal_register_renditions=federal_register_renditions,
    )
    identity = str(document["sourceRecordId"])
    return next(item for item in items if item.source_item_id == identity)


def _interpretation(item: SourceCatalogItem, kind: str) -> Mapping[str, Any]:
    return next(
        value["result"]
        for value in item.interpretations
        if value["interpretationKind"] == kind
    )


def test_exact_joins_preserve_all_three_source_facts_and_normalized_value(
    tmp_path: Path,
) -> None:
    document_id = "EPA-2026-0001-0001"
    item = _build(
        tmp_path,
        _document(document_id),
        document_renditions=(
            _rendition(
                document_id,
                "document-0000",
                f"https://downloads.regulations.gov/{document_id}/content.pdf",
                source_field="data.attributes.fileFormats[0]",
                media_type="application/pdf",
            ),
        ),
    )

    assert item.disposition is CatalogDisposition.SELECTED
    assert [fact["scopeId"] for fact in item.source_native_facts] == [
        "regulations-gov-documents",
        "regulations-gov-dockets",
        "federal-register-documents",
    ]
    assert item.normalized_metadata["regulationIdentifierNumbers"] == (
        "2060-AV12",
        "2060-AX01",
        "2060-AZ99",
    )
    artifact_root = next(
        path for path in tmp_path.iterdir() if path.is_dir() and not path.name.startswith(".")
    )
    receipt = json.loads((artifact_root / "catalog-build-receipt.json").read_text())
    assert receipt["joinCoverage"] == [
        {
            "joinId": "document-docket",
            "eligible": 1,
            "matched": 1,
            "unmatched": 0,
            "nullResult": 0,
        },
        {
            "joinId": "document-federal-register",
            "eligible": 1,
            "matched": 1,
            "unmatched": 0,
            "nullResult": 0,
        },
    ]
    assert item.normalized_metadata["agencies"] == (
        {
            "agencyId": "EPA",
            "agencyName": "Environmental Protection Agency",
        },
    )
    assert [value["outcome"] for value in _interpretation(item, "exact-join")["joins"]] == [
        "matched",
        "matched",
    ]
    assert [value.rendition_id for value in item.candidate_renditions] == [
        "regulations-gov/document-0000"
    ]
    assert _interpretation(item, "rendition-preference")["selectedFamilyId"] == (
        "regulations-gov-file"
    )
    assert {topic["observedTopicId"] for topic in item.source_observed_topics} == {
        "Air quality",
        "source-topic",
    }


def test_document_with_unmatched_docket_stays_selected_with_no_docket_fact(
    tmp_path: Path,
) -> None:
    """Gate B.2 evidence: a document whose docketId has no docket row (a real
    Mirrulations shape for whole agencies, e.g. SEC ships zero docket JSONs)
    must not be excluded from the catalog. It stays SELECTED, its
    document-docket join records outcome "no-match", it carries no docket
    source-native fact and no docket-sourced RIN, and the miss is visible as
    an "unmatched" build-receipt joinCoverage count rather than a per-item
    disposition.
    """

    document_id = "EPA-2026-0001-0001"
    item = _build(
        tmp_path,
        _document(document_id, docketId="EPA-DOES-NOT-EXIST"),
        document_renditions=(
            _rendition(
                document_id,
                "document-0000",
                f"https://downloads.regulations.gov/{document_id}/content.pdf",
                source_field="data.attributes.fileFormats[0]",
                media_type="application/pdf",
            ),
        ),
    )

    assert item.disposition is CatalogDisposition.SELECTED
    joins = _interpretation(item, "exact-join")["joins"]
    docket_join = next(value for value in joins if value["joinId"] == "document-docket")
    assert docket_join["outcome"] == "no-match"
    assert docket_join["sourceValue"] == "EPA-DOES-NOT-EXIST"
    assert docket_join["matchedSourceRecordId"] is None
    assert [fact["scopeId"] for fact in item.source_native_facts] == [
        "regulations-gov-documents",
        "federal-register-documents",
    ]
    assert item.normalized_metadata["docketIds"] == ("EPA-DOES-NOT-EXIST",)
    assert item.normalized_metadata["regulationIdentifierNumbers"] == (
        "2060-AV12",
        "2060-AX01",
    )

    artifact_root = next(
        path for path in tmp_path.iterdir() if path.is_dir() and not path.name.startswith(".")
    )
    receipt = json.loads((artifact_root / "catalog-build-receipt.json").read_text())
    document_docket_coverage = next(
        value for value in receipt["joinCoverage"] if value["joinId"] == "document-docket"
    )
    assert document_docket_coverage["unmatched"] >= 1
    assert document_docket_coverage["matched"] == 0


def test_join_uses_only_document_exact_keys_and_records_no_match(tmp_path: Path) -> None:
    item = _build(
        tmp_path,
        _document(docketId="EPA-DOES-NOT-EXIST", frDocNum="2026-DOES-NOT-EXIST"),
        federal_register_renditions=(),
    )

    joins = _interpretation(item, "exact-join")["joins"]
    assert [value["outcome"] for value in joins] == ["no-match", "no-match"]
    assert len(item.source_native_facts) == 1
    assert item.disposition is CatalogDisposition.UNAVAILABLE


def test_federal_register_rendition_is_only_a_fallback_for_an_exact_match(
    tmp_path: Path,
) -> None:
    item = _build(tmp_path, _document())

    assert item.disposition is CatalogDisposition.SELECTED
    assert [value.rendition_id for value in item.candidate_renditions] == [
        "federal-register/2026-10001/html"
    ]
    assert _interpretation(item, "rendition-preference")["selectedFamilyId"] == (
        "federal-register"
    )


def test_withdrawn_missing_and_unavailable_rows_have_distinct_dispositions(
    tmp_path: Path,
) -> None:
    withdrawn = _build(
        tmp_path / "withdrawn",
        _document(withdrawn=True, reasonWithdrawn="Issued in error"),
    )
    missing = _build(
        tmp_path / "missing",
        _document(title=None),
    )
    unavailable = _build(
        tmp_path / "unavailable",
        _document(frDocNum=None),
        federal_register_records=(),
        federal_register_renditions=(),
    )

    assert withdrawn.disposition is CatalogDisposition.DELETED
    assert withdrawn.selection.reason_code == "source.withdrawn-after-publication"
    assert withdrawn.candidate_renditions == ()
    assert missing.disposition is CatalogDisposition.FAILED
    assert missing.selection.reason_code == "source.normalized-field-missing"
    assert unavailable.disposition is CatalogDisposition.UNAVAILABLE
    assert unavailable.selection.reason_code == "source.no-candidate-rendition"


def test_document_with_no_modify_or_posted_date_gets_an_explicit_disposition(
    tmp_path: Path,
) -> None:
    """A document with neither modifyDate nor postedDate used to hard-abort
    the whole build (IntegrityError); it must instead disposition explicitly
    and let the build continue, the way the superseded passthrough minter
    excluded such rows gracefully.
    """

    item = _build(
        tmp_path,
        _document(modifyDate=None, postedDate=None),
    )

    assert item.disposition is CatalogDisposition.FAILED
    assert item.selection.reason_code == "source.normalized-field-missing"
    assert "publicationDate" in (item.selection.reason or "")
    assert item.source_issued_version == "unknown"


def test_docket_with_no_modify_date_gets_an_explicit_disposition(tmp_path: Path) -> None:
    """The same proof on the docket row: a missing modifyDate leaves required
    `lastUpdatedDate` absent, so the row is never SELECTED and the version
    placeholder is never served -- it must not abort the whole build.
    """

    items = _build_items(
        tmp_path,
        (),
        docket_records=(_docket(include_link=True, modifyDate=None),),
        federal_register_records=(),
        federal_register_renditions=(),
    )

    item = next(value for value in items if value.source_item_id == "EPA-2026-0001")
    assert item.disposition is CatalogDisposition.FAILED
    assert item.selection.reason_code == "source.normalized-field-missing"
    assert "lastUpdatedDate" in (item.selection.reason or "")
    assert item.source_issued_version == "unknown"


def test_dates_are_strict_and_policy_member_round_trips(tmp_path: Path) -> None:
    item = _build(
        tmp_path,
        _document(postedDate="2026-08-24T04:00:00+00:00"),
    )
    policy = _policy()

    assert item.disposition is CatalogDisposition.FAILED
    normalization = _interpretation(item, "normalization")["fields"]
    publication = next(
        value for value in normalization if value["normalizedField"] == "publicationDate"
    )
    assert publication["outcome"] == "unparseable"
    assert RegulationsGovCatalogPolicy.from_member(policy.to_member()).to_member() == (
        policy.to_member()
    )


def test_stratified_sample_is_deterministic_and_accounts_for_undrawn_rows(
    tmp_path: Path,
) -> None:
    identities = tuple(f"EPA-2026-0001-{value:04d}" for value in range(1, 5))
    documents = tuple(_document(identity) for identity in identities)
    renditions = tuple(
        _rendition(
            identity,
            "document-0000",
            f"https://downloads.regulations.gov/{identity}/content.pdf",
            source_field="data.attributes.fileFormats[0]",
            media_type="application/pdf",
        )
        for identity in identities
    )
    base = _policy()
    policy = RegulationsGovCatalogPolicy(
        base.document_input,
        base.docket_input,
        base.federal_register_input,
        base.agency_names,
        base.language,
        base.source_url_template,
        RegulationsGovSamplePolicy("stable-seed", 1),
    )

    first = _build_items(
        tmp_path / "first",
        documents,
        policy=policy,
        document_renditions=renditions,
    )
    repeated = _build_items(
        tmp_path / "repeated",
        documents,
        policy=policy,
        document_renditions=renditions,
    )

    first_documents = tuple(item for item in first if item.source_item_id in identities)
    repeated_documents = tuple(
        item for item in repeated if item.source_item_id in identities
    )
    assert [item.source_item_id for item in first_documents] == list(identities)
    assert [item.selection.to_dict() for item in repeated_documents] == [
        item.selection.to_dict() for item in first_documents
    ]
    selected = [
        item.source_item_id
        for item in first_documents
        if item.disposition is CatalogDisposition.SELECTED
    ]
    expected = min(
        identities,
        key=lambda identity: (
            hashlib.md5(
                f"{identity}:stable-seed".encode(),
                usedforsecurity=False,
            ).hexdigest(),
            identity,
        ),
    )
    assert selected == [expected]
    excluded = [
        item for item in first_documents if item.disposition is CatalogDisposition.EXCLUDED
    ]
    assert len(excluded) == 3
    assert {item.selection.reason_code for item in excluded} == {"policy.sample-not-drawn"}
    sampling = [_interpretation(item, "sampling") for item in first_documents]
    assert all(value["frameAdmitted"] is True for value in sampling)
    assert all(value["allocationMethod"] == "rank-over-sqrt-stratum-size" for value in sampling)
    assert all(value["limit"] == 1 for value in sampling)
    assert sorted(value["rank"] for value in sampling) == [1, 2, 3, 4]
    assert {value["stratumSize"] for value in sampling} == {4}
    assert sum(value["drawn"] is True for value in sampling) == 1
    assert all(
        _interpretation(item, "selection")["decisions"][-1]["decisionId"]
        == "sample-draw"
        for item in excluded
    )
    assert RegulationsGovCatalogPolicy.from_member(policy.to_member()).to_member() == (
        policy.to_member()
    )


def test_selected_item_budget_runs_after_source_and_rendition_checks(
    tmp_path: Path,
) -> None:
    identities = ("EPA-2026-0001-0001", "EPA-2026-0001-0002")
    base = _policy()
    policy = RegulationsGovCatalogPolicy(
        base.document_input,
        base.docket_input,
        base.federal_register_input,
        base.agency_names,
        base.language,
        base.source_url_template,
        None,
        1,
    )
    items = _build_items(
        tmp_path,
        tuple(_document(identity) for identity in identities),
        policy=policy,
    )

    document_items = [item for item in items if item.source_item_id in identities]
    assert [item.disposition for item in document_items] == [
        CatalogDisposition.SELECTED,
        CatalogDisposition.EXCLUDED,
    ]
    assert document_items[1].selection.reason_code == "policy.item-budget-exhausted"
    decisions = _interpretation(document_items[1], "selection")["decisions"]
    assert [value["decisionId"] for value in decisions] == [
        "source-withdrawal",
        "required-metadata",
        "candidate-rendition",
        "selected-item-budget",
    ]


def test_comments_and_dockets_are_first_class_ordered_universe_members(
    tmp_path: Path,
) -> None:
    comment_id = "EPA-2026-0001-9001"
    comment_renditions = (
        _rendition(
            comment_id,
            "attachment-0000-0000",
            f"https://downloads.regulations.gov/{comment_id}/attachment.pdf",
            source_field="included[0].attributes.fileFormats[0]",
            media_type="application/pdf",
            expected_byte_size=123,
        ),
        _rendition(
            comment_id,
            "comment-0000",
            f"https://downloads.regulations.gov/{comment_id}/comment.txt",
            source_field="data.attributes.fileFormats[0]",
            media_type="text/plain",
            expected_byte_size=42,
        ),
    )
    items = _build_items(
        tmp_path,
        (_document(),),
        policy=_policy(include_comments=True),
        docket_records=(_docket(include_link=True),),
        comment_records=(_comment(comment_id),),
        comment_renditions=comment_renditions,
    )

    assert [item.source_item_id for item in items] == sorted(
        ["EPA-2026-0001", "EPA-2026-0001-0001", comment_id]
    )
    docket = next(item for item in items if item.source_item_id == "EPA-2026-0001")
    comment = next(item for item in items if item.source_item_id == comment_id)
    assert docket.disposition is CatalogDisposition.SELECTED
    assert docket.document_id == docket.source_item_id
    assert docket.source_issued_version == "2026-08-24T05:00:00Z"
    assert docket.normalized_metadata["docketIds"] == ("EPA-2026-0001",)
    assert [value.rendition_id for value in docket.candidate_renditions] == [
        "regulations-gov/source-record"
    ]

    assert comment.disposition is CatalogDisposition.SELECTED
    assert comment.document_id == comment.source_item_id
    assert comment.source_issued_version == "2026-08-25T01:02:03Z"
    assert comment.normalized_metadata["title"] is None
    assert comment.normalized_metadata["docketIds"] == ("EPA-2026-0001",)
    assert comment.normalized_metadata["regulationIdentifierNumbers"] == (
        "2060-AZ99",
    )
    assert [fact["scopeId"] for fact in comment.source_native_facts] == [
        "regulations-gov-comments",
        "regulations-gov-dockets",
        "regulations-gov-documents",
    ]
    assert comment.source_native_facts[0]["fields"]["data"]["attributes"]["comment"] == (
        "Exact public comment body"
    )
    assert comment.source_native_facts[0]["fields"]["included"][0]["attributes"][
        "title"
    ] == "Exact attachment"
    assert [value.rendition_id for value in comment.candidate_renditions] == [
        "regulations-gov/attachment-0000-0000",
        "regulations-gov/comment-0000",
    ]
    assert [
        value["interpretationKind"] for value in comment.interpretations
    ] == [
        "exact-join",
        "normalization",
        "rendition-preference",
        "sampling",
        "selection",
        "topic-recovery",
    ]
    assert [
        value["outcome"] for value in _interpretation(comment, "exact-join")["joins"]
    ] == ["matched", "matched"]


def test_comment_null_modify_date_uses_explicit_exact_posted_date_policy(
    tmp_path: Path,
) -> None:
    comment = _comment(
        "EPA-2026-0001-9002",
        modify_date=None,
        include_body=False,
        title=None,
    )
    items = _build_items(
        tmp_path,
        (_document(),),
        policy=_policy(include_comments=True),
        comment_records=(comment,),
    )
    item = next(value for value in items if value.source_item_id == comment["sourceRecordId"])

    assert item.source_issued_version == "2026-08-24T04:00:00Z"
    own_fact = next(
        fact for fact in item.source_native_facts if fact["scopeId"] == "regulations-gov-comments"
    )
    attributes = own_fact["fields"]["data"]["attributes"]
    assert attributes["modifyDate"] is None
    assert "comment" not in attributes
    assert attributes["title"] is None
    observation = next(
        value
        for value in item.source_observations
        if value["observationKey"] == "comment/source-issued-version-policy"
    )["observationValue"]
    assert observation == {
        "exactSourceValue": "2026-08-24T04:00:00Z",
        "sourcePath": "data.attributes.postedDate",
        "reasonCode": "upstream-selected-comment-has-null-modify-date",
        "upstreamVersionPath": "data.attributes.modifyDate",
        "upstreamVersionValue": None,
    }


def test_docspec_refuses_to_recollapse_comment_observations(tmp_path: Path) -> None:
    identity = "EPA-2026-0001-9003"
    with pytest.raises(IntegrityError, match="strictly ordered by sourceRecordId"):
        _build_items(
            tmp_path,
            (),
            policy=_policy(include_comments=True),
            docket_records=(),
            comment_records=(
                _comment(identity, modify_date=None),
                _comment(identity, modify_date="2026-08-25T10:00:00Z"),
            ),
            federal_register_records=(),
            federal_register_renditions=(),
        )


def test_comment_and_docket_nonselected_dispositions_are_explicit(
    tmp_path: Path,
) -> None:
    comments = (
        _comment(
            "EPA-2026-0001-9101",
            withdrawn=True,
            reasonWithdrawn="Removed by the source",
        ),
        _comment("EPA-2026-0001-9102", agencyId="UNMAPPED"),
        _comment("EPA-2026-0001-9103", include_link=False),
    )
    items = _build_items(
        tmp_path / "comments",
        (),
        policy=_policy(include_comments=True),
        docket_records=(
            _docket("EPA-2026-1001", include_link=False),
            _docket("EPA-2026-1002", include_link=True, agencyId="UNMAPPED"),
        ),
        comment_records=comments,
        federal_register_records=(),
        federal_register_renditions=(),
    )
    outcomes = {
        item.source_item_id: (item.disposition, item.selection.reason_code)
        for item in items
    }
    assert outcomes == {
        "EPA-2026-0001-9101": (
            CatalogDisposition.DELETED,
            "source.withdrawn-after-publication",
        ),
        "EPA-2026-0001-9102": (
            CatalogDisposition.FAILED,
            "source.normalized-field-missing",
        ),
        "EPA-2026-0001-9103": (
            CatalogDisposition.UNAVAILABLE,
            "source.no-candidate-rendition",
        ),
        "EPA-2026-1001": (
            CatalogDisposition.UNAVAILABLE,
            "source.no-candidate-rendition",
        ),
        "EPA-2026-1002": (
            CatalogDisposition.FAILED,
            "source.normalized-field-missing",
        ),
    }

    base = _policy(include_comments=True)
    budget_policy = RegulationsGovCatalogPolicy(
        document_input=base.document_input,
        docket_input=base.docket_input,
        federal_register_input=base.federal_register_input,
        agency_names=base.agency_names,
        max_selected_items=1,
        comment_input=base.comment_input,
    )
    budget_comments = (
        _comment("EPA-2026-0001-9201"),
        _comment("EPA-2026-0001-9202"),
    )
    budget_items = _build_items(
        tmp_path / "budget",
        (),
        policy=budget_policy,
        docket_records=(),
        comment_records=budget_comments,
        federal_register_records=(),
        federal_register_renditions=(),
    )
    assert [item.disposition for item in budget_items] == [
        CatalogDisposition.SELECTED,
        CatalogDisposition.EXCLUDED,
    ]
    assert budget_items[1].selection.reason_code == "policy.item-budget-exhausted"


def test_content_addressed_candidate_requires_matching_digest_and_size(
    tmp_path: Path,
) -> None:
    comment_id = "EPA-2026-0001-9301"
    candidate = _rendition(
        comment_id,
        "attachment-immutable",
        _SHA_E,
        source_field="included[0].attributes.fileFormats[0]",
        media_type="application/pdf",
        expected_sha256=_SHA_E,
        expected_byte_size=123,
    )
    item = next(
        value
        for value in _build_items(
            tmp_path / "valid",
            (),
            policy=_policy(include_comments=True),
            docket_records=(),
            comment_records=(_comment(comment_id),),
            comment_renditions=(candidate,),
            federal_register_records=(),
            federal_register_renditions=(),
        )
        if value.source_item_id == comment_id
    )
    assert [value.to_dict() for value in item.candidate_renditions] == [
        {
            "renditionId": "regulations-gov/attachment-immutable",
            "mediaType": "application/pdf",
            "locatorKind": "immutable-object",
            "locator": _SHA_E,
            "expectedSha256": _SHA_E,
            "expectedByteSize": 123,
        }
    ]

    mismatch = {**candidate, "expectedSha256": _SHA_A}
    with pytest.raises(IntegrityError, match="differs from its supplied expected SHA-256"):
        _build_items(
            tmp_path / "mismatch",
            (),
            policy=_policy(include_comments=True),
            docket_records=(),
            comment_records=(_comment(comment_id),),
            comment_renditions=(mismatch,),
            federal_register_records=(),
            federal_register_renditions=(),
        )

    missing_size = {**candidate, "expectedByteSize": None}
    with pytest.raises(IntegrityError, match="requires a supplied non-negative byte size"):
        _build_items(
            tmp_path / "missing-size",
            (),
            policy=_policy(include_comments=True),
            docket_records=(),
            comment_records=(_comment(comment_id),),
            comment_renditions=(missing_size,),
            federal_register_records=(),
            federal_register_renditions=(),
        )


def test_installed_adapter_exposes_comment_profile_and_propagates_upstream_tie_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    comment_profile = object()
    profiles = SimpleNamespace(REGULATIONS_GOV_COMMENT_PROFILE=comment_profile)
    monkeypatch.setattr(spicyregs_adapter_module, "import_module", lambda _: profiles)
    assert spicyregs_source_profile("regulations-gov-comments") is comment_profile

    class RefusingReader:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise ValueError("upstream source-version tie")

    source_native = SimpleNamespace(
        SUPPORTED_PRODUCER_PRODUCTS=frozenset({"spicy-regs", "spicy-docs"}),
        SourceNativeReleaseReader=RefusingReader,
    )
    monkeypatch.setattr(spicyregs_adapter_module, "import_module", lambda _: source_native)
    with pytest.raises(ValueError, match="upstream source-version tie"):
        SpicyRegsSourceNativeAdapter(
            SimpleNamespace(),
            blob_source=SimpleNamespace(),
            profile=comment_profile,
            expected_pin=None,
            accepted_verifier_implementation_ids=frozenset(),
        )


def test_source_catalog_cli_accepts_the_comment_profile_choice() -> None:
    args = source_catalog_build_parser().parse_args(
        [
            "build",
            "--source-native",
            "comments",
            "--source-native-artifact-digest",
            _SHA_E,
            "--source-native-blob-store",
            "blobs",
            "--source-native-profile",
            "regulations-gov-comments",
            "--accepted-source-verifier-implementation-id",
            "urn:test:verifier",
            "--catalog-policy",
            "policy.json",
            "--implementation-id",
            "git+https://example.test/docspec@" + "1" * 40,
            "--verifier-implementation-id",
            "git+https://example.test/docspec@" + "1" * 40,
            "--destination",
            "catalog",
            "--receipt",
            "receipt.json",
        ]
    )
    assert args.source_native_profile == ["regulations-gov-comments"]
    assert args.source_native_blob_store == [Path("blobs")]
