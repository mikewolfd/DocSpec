"""DocSpec interpretation of exact Regulations.gov source-native facts."""

from __future__ import annotations

import bisect
import hashlib
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from docspec.application.catalog_policy import (
    http_url as _http_url,
    normalization_field as _field_outcome,
    normalized_rins as _normalized_rins,
    observed_topics,
    text_value as _text,
    utc_instant_date_value as _instant_date,
    utf16_key as _utf16_key,
)
from docspec.domain.identity import canonical_json_bytes, closed_mapping, sha256_digest
from docspec.domain.source_catalog import (
    CatalogDisposition,
    CatalogRenditionFamily,
    CatalogSelectionDecision,
    SourceCatalogCandidate,
    SourceCatalogItem,
    SourceCatalogSelection,
)
from docspec.errors import IntegrityError
from docspec.ports.source_catalog import (
    CatalogPolicyInputs,
    CatalogPolicyWorkspace,
    SourceInputSelector,
)

_DOCUMENT_SCOPE = "regulations-gov-documents"
_DOCUMENT_SCHEMA = "regulations-gov-document-raw"
_DOCKET_SCOPE = "regulations-gov-dockets"
_DOCKET_SCHEMA = "regulations-gov-docket-raw"
_COMMENT_SCOPE = "regulations-gov-comments"
_COMMENT_SCHEMA = "regulations-gov-comment-raw"
_FEDERAL_REGISTER_SCOPE = "federal-register-documents"
_FEDERAL_REGISTER_SCHEMA = "federal-register-document"
_SCHEMA_VERSION = "1.0"
_RENDITION_ORDER = ("regulations-gov-file", "federal-register")
_NORMALIZED_FIELDS = (
    "title",
    "agencies",
    "documentType",
    "publicationDate",
    "lastUpdatedDate",
    "docketIds",
    "regulationIdentifierNumbers",
    "commentCloseDate",
    "language",
    "sourceUrl",
)
_REQUIRED_NORMALIZED_FIELDS = (
    "title",
    "agencies",
    "documentType",
    "publicationDate",
    "sourceUrl",
)
_DOCKET_INDEX = "regulations-gov-catalog/dockets"
_DOCUMENT_INDEX = "regulations-gov-catalog/document-index"
_FEDERAL_REGISTER_INDEX = "regulations-gov-catalog/federal-register"
_UNIVERSE_ROWS = "regulations-gov-catalog/universe"
_DOCUMENT_ROWS = "regulations-gov-catalog/documents"
_SAMPLE_ORDER = "regulations-gov-catalog/sample-order"
_SAMPLE_COUNTS = "regulations-gov-catalog/sample-counts"
_SAMPLE_DRAWN = "regulations-gov-catalog/sample-drawn"
_SAMPLE_DETAILS = "regulations-gov-catalog/sample-details"
_UNKNOWN_STRATUM_PART = "unknown"


@dataclass(frozen=True, slots=True)
class RegulationsGovSamplePolicy:
    """Deterministic stratified draw retained from the proven source policy."""

    seed: str
    per_partition_limit: int

    def __post_init__(self) -> None:
        if not isinstance(self.seed, str) or not self.seed:
            raise ValueError("Regulations.gov sample seed must be nonempty")
        if (
            isinstance(self.per_partition_limit, bool)
            or not isinstance(self.per_partition_limit, int)
            or self.per_partition_limit < 1
        ):
            raise ValueError("Regulations.gov sample per-partition limit must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "allocation": "rank-over-sqrt-stratum-size",
            "orderHash": "md5-document-id-colon-seed",
            "partitionBy": "documentType",
            "perPartitionLimit": self.per_partition_limit,
            "seed": self.seed,
            "stratifyBy": ["agencyId", "publicationYear"],
        }

    @classmethod
    def from_dict(cls, value: object) -> RegulationsGovSamplePolicy:
        item = closed_mapping(
            value,
            {
                "allocation",
                "orderHash",
                "partitionBy",
                "perPartitionLimit",
                "seed",
                "stratifyBy",
            },
            "Regulations.gov sample policy",
            error=ValueError,
        )
        policy = cls(item["seed"], item["perPartitionLimit"])
        if item != policy.to_dict():
            raise ValueError("Regulations.gov sample policy differs from the installed policy")
        return policy


def _source_fact(record: Mapping[str, Any]) -> dict[str, Any]:
    native = record.get("record")
    if not isinstance(native, Mapping):
        raise IntegrityError("source-native record payload must be an object")
    return {
        "scopeId": record["scopeId"],
        "schemaName": record["schemaName"],
        "schemaVersion": record["schemaVersion"],
        "schemaDigest": record["schemaDigest"],
        "fields": dict(native),
    }


def _record_data(record: Mapping[str, Any], *, expected_type: str) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    native = record.get("record")
    if not isinstance(native, Mapping):
        raise IntegrityError(f"Regulations.gov {expected_type} payload must be an object")
    data = native.get("data")
    if not isinstance(data, Mapping) or data.get("type") != expected_type:
        raise IntegrityError(f"Regulations.gov {expected_type} payload has different data")
    attributes = data.get("attributes")
    if not isinstance(attributes, Mapping):
        raise IntegrityError(f"Regulations.gov {expected_type} attributes must be an object")
    return native, attributes


def _index_rows(
    inputs: CatalogPolicyInputs,
    workspace: CatalogPolicyWorkspace,
    selector: SourceInputSelector | None,
    namespace: str,
) -> None:
    if selector is None:
        return
    for row in inputs.iter_lookup_rows(selector):
        workspace.put(
            namespace,
            (str(row.record["sourceRecordId"]),),
            {
                "record": dict(row.record),
                "renditions": [dict(value) for value in row.renditions],
            },
        )


def _indexed_row(
    workspace: CatalogPolicyWorkspace,
    namespace: str,
    source_id: str | None,
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]] | None:
    if source_id is None:
        return None
    value = workspace.get(namespace, (source_id,))
    if value is None:
        return None
    return _stored_row(value)


def _stored_row(
    value: Mapping[str, Any],
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    if set(value) != {"record", "renditions"} or not isinstance(value["record"], Mapping):
        raise IntegrityError("catalog join index row has an invalid closed shape")
    raw_renditions = value["renditions"]
    if not isinstance(raw_renditions, Sequence) or isinstance(
        raw_renditions, (str, bytes, bytearray, memoryview)
    ):
        raise IntegrityError("catalog join index renditions must be an array")
    renditions = tuple(
        value
        for value in raw_renditions
        if isinstance(value, Mapping)
    )
    if len(renditions) != len(raw_renditions):
        raise IntegrityError("catalog join index contains a non-object rendition")
    return value["record"], renditions


def _source_identifier(value: object) -> tuple[str | None, tuple[Any, ...]]:
    return _text(value)


def _agency(
    value: object,
    agency_names: Mapping[str, str],
) -> tuple[list[dict[str, str]], tuple[Any, ...]]:
    agency_id, malformed = _text(value)
    if agency_id is None:
        return [], malformed
    agency_name = agency_names.get(agency_id)
    if not isinstance(agency_name, str) or not agency_name:
        return [], (agency_id,)
    return [{"agencyId": agency_id, "agencyName": agency_name}], ()


def _join_result(
    *,
    join_id: str,
    source_field: str,
    source_value: str | None,
    lookup_scope_id: str,
    matched: tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]] | None,
) -> dict[str, Any]:
    if source_value is None:
        outcome = "not-stated"
    elif matched is None:
        outcome = "no-match"
    else:
        outcome = "matched"
    return {
        "joinId": join_id,
        "sourceField": source_field,
        "sourceValue": source_value,
        "lookupScopeId": lookup_scope_id,
        "outcome": outcome,
        "matchedSourceRecordId": (
            matched[0]["sourceRecordId"] if matched is not None else None
        ),
    }


def _candidate_from_rendition(
    value: Mapping[str, Any],
    *,
    rendition_id: str,
) -> SourceCatalogCandidate | None:
    locator = value.get("locator")
    if locator is None:
        return None
    if not isinstance(locator, str) or not locator:
        raise IntegrityError("Regulations.gov rendition locator must be nonempty text or null")
    expected_sha256 = value.get("expectedSha256")
    expected_byte_size = value.get("expectedByteSize")
    source_url = _http_url(locator)
    if source_url is not None:
        locator_kind = "source-url"
    elif locator.startswith("sha256:"):
        if expected_sha256 != locator:
            raise IntegrityError(
                "immutable-object locator differs from its supplied expected SHA-256"
            )
        if (
            isinstance(expected_byte_size, bool)
            or not isinstance(expected_byte_size, int)
            or expected_byte_size < 0
        ):
            raise IntegrityError(
                "immutable-object candidate requires a supplied non-negative byte size"
            )
        locator_kind = "immutable-object"
    else:
        raise IntegrityError("Regulations.gov rendition locator kind is not supported")
    return SourceCatalogCandidate(
        rendition_id=rendition_id,
        media_type=value["mediaType"],
        locator_kind=locator_kind,
        locator=locator,
        expected_sha256=expected_sha256,
        expected_byte_size=expected_byte_size,
    )


def _selection_result(
    *,
    withdrawn: bool,
    withdrawal_reason: str | None,
    missing_fields: Sequence[str],
    candidates: tuple[SourceCatalogCandidate, ...],
    budget_available: bool,
) -> tuple[SourceCatalogSelection, tuple[CatalogSelectionDecision, ...]]:
    decisions: list[CatalogSelectionDecision] = []
    if withdrawn:
        reason = "The source marks this item withdrawn."
        if withdrawal_reason is not None:
            reason = f"The source marks this item withdrawn: {withdrawal_reason}"
        selection = SourceCatalogSelection(
            CatalogDisposition.DELETED,
            "source.withdrawn-after-publication",
            reason,
        )
        decisions.append(
            CatalogSelectionDecision(
                "source-withdrawal",
                False,
                selection.disposition,
                selection.reason_code,
                selection.reason,
            )
        )
        return selection, tuple(decisions)
    decisions.append(CatalogSelectionDecision("source-withdrawal", True))
    if missing_fields:
        reason = "Required normalized fields are absent or unparseable: " + ", ".join(
            missing_fields
        )
        selection = SourceCatalogSelection(
            CatalogDisposition.FAILED,
            "source.normalized-field-missing",
            reason,
        )
        decisions.append(
            CatalogSelectionDecision(
                "required-metadata",
                False,
                selection.disposition,
                selection.reason_code,
                selection.reason,
            )
        )
        return selection, tuple(decisions)
    decisions.append(CatalogSelectionDecision("required-metadata", True))
    if not candidates:
        selection = SourceCatalogSelection(
            CatalogDisposition.UNAVAILABLE,
            "source.no-candidate-rendition",
            "The source item offers no usable rendition.",
        )
        decisions.append(
            CatalogSelectionDecision(
                "candidate-rendition",
                False,
                selection.disposition,
                selection.reason_code,
                selection.reason,
            )
        )
        return selection, tuple(decisions)
    decisions.append(CatalogSelectionDecision("candidate-rendition", True))
    if not budget_available:
        selection = SourceCatalogSelection(
            CatalogDisposition.EXCLUDED,
            "policy.item-budget-exhausted",
            "The catalog selected-item budget is already exhausted.",
        )
        decisions.append(
            CatalogSelectionDecision(
                "selected-item-budget",
                False,
                selection.disposition,
                selection.reason_code,
                selection.reason,
            )
        )
        return selection, tuple(decisions)
    decisions.append(CatalogSelectionDecision("selected-item-budget", True))
    return SourceCatalogSelection(CatalogDisposition.SELECTED), tuple(decisions)


@dataclass(frozen=True, slots=True)
class RegulationsGovCatalogPolicy:
    """Join exact source keys and select capture candidates without producer imports."""

    document_input: SourceInputSelector
    docket_input: SourceInputSelector | None
    federal_register_input: SourceInputSelector | None
    agency_names: Mapping[str, str]
    language: str = "en"
    source_url_template: str = "https://www.regulations.gov/document/{documentId}"
    sample: RegulationsGovSamplePolicy | None = None
    max_selected_items: int | None = None
    comment_input: SourceInputSelector | None = None

    policy_id = "urn:docspec:catalog-policy:regulations-gov:1"
    policy_version = "1.1.0"

    def __post_init__(self) -> None:
        expected = (
            (self.document_input, _DOCUMENT_SCOPE, _DOCUMENT_SCHEMA),
            (self.docket_input, _DOCKET_SCOPE, _DOCKET_SCHEMA),
            (self.comment_input, _COMMENT_SCOPE, _COMMENT_SCHEMA),
            (
                self.federal_register_input,
                _FEDERAL_REGISTER_SCOPE,
                _FEDERAL_REGISTER_SCHEMA,
            ),
        )
        for selector, scope_id, schema_name in expected:
            if selector is not None and (
                selector.scope_id != scope_id
                or selector.schema_name != schema_name
                or selector.schema_version != _SCHEMA_VERSION
            ):
                raise ValueError("Regulations.gov catalog input selector differs from its source family")
        names = dict(self.agency_names)
        if any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, str)
            or not value
            for key, value in names.items()
        ):
            raise ValueError("Regulations.gov agency names must be nonempty text pairs")
        object.__setattr__(self, "agency_names", dict(sorted(names.items(), key=lambda pair: _utf16_key(pair[0]))))
        if not isinstance(self.language, str) or not self.language:
            raise ValueError("Regulations.gov catalog language must be nonempty")
        if self.source_url_template.count("{documentId}") != 1:
            raise ValueError("Regulations.gov source URL template must contain one {documentId}")
        if _http_url(self.source_url_template.replace("{documentId}", "probe")) is None:
            raise ValueError("Regulations.gov source URL template must be HTTP(S)")
        if self.max_selected_items is not None and (
            isinstance(self.max_selected_items, bool)
            or not isinstance(self.max_selected_items, int)
            or self.max_selected_items < 1
        ):
            raise ValueError("Regulations.gov selected-item budget must be positive")

    @property
    def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
        return tuple(
            selector
            for selector in (
                self.document_input,
                self.docket_input,
                self.comment_input,
            )
            if selector is not None
        )

    @property
    def configuration(self) -> Mapping[str, Any]:
        return {
            "sourceProfile": "regulations-gov",
            "universeInputs": [selector.to_dict() for selector in self.universe_inputs],
            "docketInput": self.docket_input.to_dict() if self.docket_input is not None else None,
            "commentInput": self.comment_input.to_dict() if self.comment_input is not None else None,
            "federalRegisterInput": (
                self.federal_register_input.to_dict()
                if self.federal_register_input is not None
                else None
            ),
            "agencyNames": dict(self.agency_names),
            "language": self.language,
            "sourceUrlTemplate": self.source_url_template,
            "sourceUrlTemplates": {
                "comments": "https://www.regulations.gov/comment/{sourceRecordId}",
                "dockets": "https://www.regulations.gov/docket/{sourceRecordId}",
                "documents": self.source_url_template.replace("{documentId}", "{sourceRecordId}"),
            },
            "sample": self.sample.to_dict() if self.sample is not None else None,
            "maxSelectedItems": self.max_selected_items,
            "normalizationFields": list(_NORMALIZED_FIELDS),
            "requiredNormalizedFields": list(_REQUIRED_NORMALIZED_FIELDS),
            "requiredNormalizedFieldsBySourceKind": {
                "comments": [
                    "agencies",
                    "documentType",
                    "publicationDate",
                    "sourceUrl",
                ],
                "dockets": ["title", "agencies", "lastUpdatedDate", "sourceUrl"],
                "documents": list(_REQUIRED_NORMALIZED_FIELDS),
            },
            "rinNormalization": "federal-register-rin-syntax/1",
            "renditionPreference": list(_RENDITION_ORDER),
            "sourceKindRenditionPreference": {
                "comments": ["regulations-gov-file", "regulations-gov-record"],
                "dockets": ["regulations-gov-record"],
                "documents": list(_RENDITION_ORDER),
            },
            "joins": [
                {
                    "joinId": "comment-docket",
                    "sourceField": "data.attributes.docketId",
                    "lookupScopeId": _DOCKET_SCOPE,
                },
                {
                    "joinId": "comment-document",
                    "sourceField": "data.attributes.commentOnDocumentId",
                    "lookupScopeId": _DOCUMENT_SCOPE,
                },
                {
                    "joinId": "document-docket",
                    "sourceField": "data.attributes.docketId",
                    "lookupScopeId": _DOCKET_SCOPE,
                },
                {
                    "joinId": "document-federal-register",
                    "sourceField": "data.attributes.frDocNum",
                    "lookupScopeId": _FEDERAL_REGISTER_SCOPE,
                },
            ],
            "samplingSourceKinds": ["documents"],
            "sourceIssuedVersionPolicy": {
                "comments": {
                    "primarySourcePath": "data.attributes.modifyDate",
                    "nullFallbackSourcePath": "data.attributes.postedDate",
                },
                "dockets": {"primarySourcePath": "data.attributes.modifyDate"},
                "documents": {
                    "primarySourcePath": "data.attributes.modifyDate",
                    "nullFallbackSourcePath": "data.attributes.postedDate",
                },
            },
            "topicRecovery": {
                "sourceField": "data.attributes.topics",
                "emptyOutcome": "not-recovered",
                "publisherDeclaredEmptyEvidenceDigest": None,
            },
            "selectionFailures": [
                {
                    "decisionId": "source-withdrawal",
                    "disposition": "deleted",
                    "reasonCode": "source.withdrawn-after-publication",
                },
                {
                    "decisionId": "sample-draw",
                    "disposition": "excluded",
                    "reasonCode": "policy.sample-not-drawn",
                },
                {
                    "decisionId": "required-metadata",
                    "disposition": "failed",
                    "reasonCode": "source.normalized-field-missing",
                },
                {
                    "decisionId": "candidate-rendition",
                    "disposition": "unavailable",
                    "reasonCode": "source.no-candidate-rendition",
                },
                {
                    "decisionId": "selected-item-budget",
                    "disposition": "excluded",
                    "reasonCode": "policy.item-budget-exhausted",
                },
            ],
        }

    def to_member(self) -> dict[str, Any]:
        return {
            "format": "docspec-catalog-policy",
            "formatVersion": "1.0",
            "policyId": self.policy_id,
            "policyVersion": self.policy_version,
            "configuration": dict(self.configuration),
        }

    @property
    def policy_digest(self) -> str:
        return sha256_digest(canonical_json_bytes(self.to_member()))

    @classmethod
    def from_member(cls, value: object) -> RegulationsGovCatalogPolicy:
        member = closed_mapping(
            value,
            {"format", "formatVersion", "policyId", "policyVersion", "configuration"},
            "Regulations.gov catalog policy",
            error=ValueError,
        )
        configuration = closed_mapping(
            member["configuration"],
            {
                "sourceProfile",
                "universeInputs",
                "docketInput",
                "commentInput",
                "federalRegisterInput",
                "agencyNames",
                "language",
                "sourceUrlTemplate",
                "sourceUrlTemplates",
                "sample",
                "maxSelectedItems",
                "normalizationFields",
                "requiredNormalizedFields",
                "requiredNormalizedFieldsBySourceKind",
                "rinNormalization",
                "renditionPreference",
                "sourceKindRenditionPreference",
                "joins",
                "samplingSourceKinds",
                "sourceIssuedVersionPolicy",
                "topicRecovery",
                "selectionFailures",
            },
            "Regulations.gov catalog policy configuration",
            error=ValueError,
        )

        def selector(raw: object) -> SourceInputSelector | None:
            return None if raw is None else SourceInputSelector.from_dict(raw)

        agency_names = configuration["agencyNames"]
        if not isinstance(agency_names, Mapping):
            raise ValueError("Regulations.gov catalog agencyNames must be an object")
        universe_inputs = configuration["universeInputs"]
        if not isinstance(universe_inputs, list) or not universe_inputs:
            raise ValueError(
                "Regulations.gov catalog policy requires universe inputs"
            )
        policy = cls(
            document_input=SourceInputSelector.from_dict(universe_inputs[0]),
            docket_input=selector(configuration["docketInput"]),
            federal_register_input=selector(configuration["federalRegisterInput"]),
            agency_names=dict(agency_names),
            language=configuration["language"],
            source_url_template=configuration["sourceUrlTemplate"],
            sample=(
                RegulationsGovSamplePolicy.from_dict(configuration["sample"])
                if configuration["sample"] is not None
                else None
            ),
            max_selected_items=configuration["maxSelectedItems"],
            comment_input=selector(configuration["commentInput"]),
        )
        if member != policy.to_member():
            raise ValueError("Regulations.gov catalog policy differs from the installed policy version")
        return policy

    def iter_items(
        self,
        inputs: CatalogPolicyInputs,
        workspace: CatalogPolicyWorkspace,
    ) -> Iterator[SourceCatalogItem]:
        _index_rows(inputs, workspace, self.federal_register_input, _FEDERAL_REGISTER_INDEX)
        self._stage_universe(inputs, workspace)
        selected_count = 0
        if self.sample is not None:
            self._draw_document_sample(workspace)
        for value in workspace.iter_ordered(_UNIVERSE_ROWS):
            record, renditions = _stored_row(value)
            source_item_id = str(record["sourceRecordId"])
            budget_available = (
                self.max_selected_items is None
                or selected_count < self.max_selected_items
            )
            if record["scopeId"] == _DOCUMENT_SCOPE:
                item = self._item_from_row(
                    record,
                    renditions,
                    workspace,
                    sample_drawn=(
                        workspace.get(_SAMPLE_DRAWN, (source_item_id,)) is not None
                        if self.sample is not None
                        else None
                    ),
                    budget_available=budget_available,
                )
            elif record["scopeId"] == _COMMENT_SCOPE:
                item = self._comment_item_from_row(
                    record,
                    renditions,
                    workspace,
                    budget_available=budget_available,
                )
            elif record["scopeId"] == _DOCKET_SCOPE:
                item = self._docket_item_from_row(
                    record,
                    renditions,
                    budget_available=budget_available,
                )
            else:
                raise IntegrityError("Regulations.gov policy received an undeclared universe scope")
            if item.disposition is CatalogDisposition.SELECTED:
                selected_count += 1
            yield item

    def _stage_universe(
        self,
        inputs: CatalogPolicyInputs,
        workspace: CatalogPolicyWorkspace,
    ) -> None:
        for row in inputs.iter_universe_rows():
            stored = {
                "record": dict(row.record),
                "renditions": [dict(value) for value in row.renditions],
            }
            source_item_id = str(row.record["sourceRecordId"])
            workspace.put(_UNIVERSE_ROWS, (source_item_id,), stored)
            if row.record["scopeId"] == _DOCUMENT_SCOPE:
                workspace.put(_DOCUMENT_ROWS, (source_item_id,), stored)
                workspace.put(_DOCUMENT_INDEX, (source_item_id,), stored)
            elif row.record["scopeId"] == _DOCKET_SCOPE:
                workspace.put(_DOCKET_INDEX, (source_item_id,), stored)
            elif row.record["scopeId"] != _COMMENT_SCOPE:
                raise IntegrityError("Regulations.gov policy received an undeclared universe scope")

    def _draw_document_sample(
        self,
        workspace: CatalogPolicyWorkspace,
    ) -> None:
        sample = self.sample
        if sample is None:
            raise AssertionError("sample staging requires a sample policy")
        for value in workspace.iter_ordered(_DOCUMENT_ROWS):
            record, _ = _stored_row(value)
            source_item_id = str(record["sourceRecordId"])
            _, attributes = _record_data(record, expected_type="documents")
            if attributes.get("withdrawn") is True:
                continue
            document_type, _ = _text(attributes.get("documentType"))
            agency_id, _ = _text(attributes.get("agencyId"))
            publication_date, _ = _instant_date(attributes.get("postedDate"))
            partition = document_type or _UNKNOWN_STRATUM_PART
            agency = agency_id or _UNKNOWN_STRATUM_PART
            year = (
                publication_date[:4]
                if publication_date is not None
                else _UNKNOWN_STRATUM_PART
            )
            order_hash = hashlib.md5(
                f"{source_item_id}:{sample.seed}".encode(),
                usedforsecurity=False,
            ).hexdigest()
            workspace.put(
                _SAMPLE_ORDER,
                (partition, agency, year, order_hash, source_item_id),
                {
                    "agency": agency,
                    "documentId": source_item_id,
                    "orderHash": order_hash,
                    "partition": partition,
                    "sourceItemId": source_item_id,
                    "year": year,
                },
            )

        previous_group: tuple[str, str, str] | None = None
        count = 0
        for value in workspace.iter_ordered(_SAMPLE_ORDER):
            group = (
                str(value["partition"]),
                str(value["agency"]),
                str(value["year"]),
            )
            if previous_group is not None and group != previous_group:
                workspace.put(
                    _SAMPLE_COUNTS,
                    previous_group,
                    {"count": count},
                )
                count = 0
            previous_group = group
            count += 1
        if previous_group is not None:
            workspace.put(_SAMPLE_COUNTS, previous_group, {"count": count})

        current_partition: str | None = None
        current_group: tuple[str, str, str] | None = None
        rank = 0
        selected: list[tuple[float, str, str, str]] = []

        def flush() -> None:
            for _, _, _, source_item_id in selected:
                workspace.put(
                    _SAMPLE_DRAWN,
                    (source_item_id,),
                    {"sourceItemId": source_item_id},
                )

        for value in workspace.iter_ordered(_SAMPLE_ORDER):
            partition = str(value["partition"])
            group = (partition, str(value["agency"]), str(value["year"]))
            if current_partition is not None and partition != current_partition:
                flush()
                selected = []
            if group != current_group:
                current_group = group
                rank = 0
            current_partition = partition
            rank += 1
            count_value = workspace.get(_SAMPLE_COUNTS, group)
            if (
                count_value is None
                or set(count_value) != {"count"}
                or isinstance(count_value["count"], bool)
                or not isinstance(count_value["count"], int)
                or count_value["count"] < rank
            ):
                raise IntegrityError("catalog sample stratum count is invalid")
            workspace.put(
                _SAMPLE_DETAILS,
                (str(value["sourceItemId"]),),
                {
                    "partition": partition,
                    "stratum": [str(value["agency"]), str(value["year"])],
                    "orderHash": str(value["orderHash"]),
                    "rank": rank,
                    "stratumSize": count_value["count"],
                },
            )
            candidate = (
                rank / math.sqrt(count_value["count"]),
                str(value["orderHash"]),
                str(value["documentId"]),
                str(value["sourceItemId"]),
            )
            bisect.insort(selected, candidate)
            if len(selected) > sample.per_partition_limit:
                selected.pop()
        if current_partition is not None:
            flush()

    def _sampling_result(
        self,
        source_item_id: str,
        *,
        withdrawn: bool,
        sample_drawn: bool | None,
        workspace: CatalogPolicyWorkspace,
    ) -> dict[str, Any]:
        if self.sample is None:
            return {
                "frameAdmitted": not withdrawn,
                "partition": None if withdrawn else "all",
                "stratum": [] if withdrawn else ["all"],
                "orderHash": None,
                "rank": None,
                "stratumSize": None,
                "allocationMethod": "all",
                "limit": None,
                "drawn": not withdrawn,
            }
        if withdrawn:
            return {
                "frameAdmitted": False,
                "partition": None,
                "stratum": [],
                "orderHash": None,
                "rank": None,
                "stratumSize": None,
                "allocationMethod": "rank-over-sqrt-stratum-size",
                "limit": self.sample.per_partition_limit,
                "drawn": False,
            }
        details = workspace.get(_SAMPLE_DETAILS, (source_item_id,))
        if details is None or set(details) != {
            "partition",
            "stratum",
            "orderHash",
            "rank",
            "stratumSize",
        }:
            raise IntegrityError("catalog sample details are missing or malformed")
        if sample_drawn is None:
            raise IntegrityError("catalog sample decision is missing")
        return {
            "frameAdmitted": True,
            **dict(details),
            "allocationMethod": "rank-over-sqrt-stratum-size",
            "limit": self.sample.per_partition_limit,
            "drawn": sample_drawn,
        }

    def _input_scope_ids(self) -> list[str]:
        scope_ids = [selector.scope_id for selector in self.universe_inputs]
        if self.federal_register_input is not None:
            scope_ids.append(self.federal_register_input.scope_id)
        return scope_ids

    def _interpretations(
        self,
        *,
        joins: Sequence[Mapping[str, Any]],
        normalization_fields: Sequence[Any],
        ordered_family_ids: Sequence[str],
        families: Sequence[CatalogRenditionFamily],
        selected_family_id: str | None,
        candidates: Sequence[SourceCatalogCandidate],
        sampling_result: Mapping[str, Any],
        selection: SourceCatalogSelection,
        decisions: Sequence[CatalogSelectionDecision],
        topic_source_field: str,
        topics: Sequence[Mapping[str, Any]] = (),
    ) -> tuple[dict[str, Any], ...]:
        pin = {
            "policyId": self.policy_id,
            "policyVersion": self.policy_version,
            "policyDigest": self.policy_digest,
            "inputScopeIds": self._input_scope_ids(),
        }
        return (
            {
                "interpretationKind": "exact-join",
                **pin,
                "result": {"joins": [dict(value) for value in joins]},
            },
            {
                "interpretationKind": "normalization",
                **pin,
                "result": {
                    "fields": [field.to_dict() for field in normalization_fields]
                },
            },
            {
                "interpretationKind": "rendition-preference",
                **pin,
                "result": {
                    "orderedFamilyIds": list(ordered_family_ids),
                    "families": [family.to_dict() for family in families],
                    "selectedFamilyId": selected_family_id,
                    "selectedRenditionIds": [value.rendition_id for value in candidates],
                },
            },
            {
                "interpretationKind": "sampling",
                **pin,
                "result": dict(sampling_result),
            },
            {
                "interpretationKind": "selection",
                **pin,
                "result": {
                    "decisions": [decision.to_dict() for decision in decisions],
                    "finalDisposition": selection.disposition.value,
                    "reasonCode": selection.reason_code,
                    "reason": selection.reason,
                },
            },
            {
                "interpretationKind": "topic-recovery",
                **pin,
                "result": {
                    "sourceField": topic_source_field,
                    "outcome": "observed" if topics else "not-recovered",
                    "evidenceDigest": None,
                    "observedTopicIds": [value["observedTopicId"] for value in topics],
                },
            },
        )

    @staticmethod
    def _source_observations(
        rows: Sequence[tuple[str, Mapping[str, Any] | None]],
    ) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        for prefix, source_record in rows:
            if source_record is None:
                continue
            diagnostics = source_record.get("fieldDiagnostics")
            if isinstance(diagnostics, list):
                observations.extend(
                    {
                        "observationKey": f"{prefix}/field-diagnostic/{index}",
                        "observationValue": value,
                    }
                    for index, value in enumerate(diagnostics)
                )
        return observations

    @staticmethod
    def _source_record_candidate(
        raw_source_url: object,
    ) -> SourceCatalogCandidate | None:
        locator = _http_url(raw_source_url)
        if locator is None:
            return None
        return SourceCatalogCandidate(
            rendition_id="regulations-gov/source-record",
            media_type="application/vnd.api+json",
            locator_kind="source-url",
            locator=locator,
        )

    @staticmethod
    def _source_kind_rendition_preference(
        renditions: tuple[Mapping[str, Any], ...],
        raw_source_url: object,
        *,
        include_files: bool,
    ) -> tuple[
        tuple[SourceCatalogCandidate, ...],
        tuple[CatalogRenditionFamily, ...],
        str | None,
        tuple[str, ...],
    ]:
        order = (
            ("regulations-gov-file", "regulations-gov-record")
            if include_files
            else ("regulations-gov-record",)
        )
        by_family: dict[str, list[SourceCatalogCandidate]] = {
            family: [] for family in order
        }
        claimed: set[str] = set()
        if include_files:
            for value in renditions:
                candidate = _candidate_from_rendition(
                    value,
                    rendition_id=f"regulations-gov/{value['renditionId']}",
                )
                if candidate is None or candidate.locator in claimed:
                    continue
                claimed.add(candidate.locator)
                by_family["regulations-gov-file"].append(candidate)
        record_candidate = RegulationsGovCatalogPolicy._source_record_candidate(
            raw_source_url
        )
        if record_candidate is not None and record_candidate.locator not in claimed:
            by_family["regulations-gov-record"].append(record_candidate)
        ordered = {
            family: tuple(
                sorted(by_family[family], key=lambda value: _utf16_key(value.rendition_id))
            )
            for family in order
        }
        families = tuple(
            CatalogRenditionFamily(
                family,
                tuple(value.rendition_id for value in ordered[family]),
            )
            for family in order
        )
        selected_family = next((family for family in order if ordered[family]), None)
        return (
            ordered[selected_family] if selected_family is not None else (),
            families,
            selected_family,
            order,
        )

    def _docket_item_from_row(
        self,
        record: Mapping[str, Any],
        renditions: tuple[Mapping[str, Any], ...],
        *,
        budget_available: bool,
    ) -> SourceCatalogItem:
        native, attributes = _record_data(record, expected_type="dockets")
        source_item_id = str(record["sourceRecordId"])
        data = native["data"]
        if not isinstance(data, Mapping) or data.get("id") != source_item_id:
            raise IntegrityError("Regulations.gov docket source identity differs")
        title, malformed_title = _text(attributes.get("title"))
        agencies, malformed_agencies = _agency(attributes.get("agencyId"), self.agency_names)
        document_type, malformed_document_type = _text(attributes.get("docketType"))
        modified_date, malformed_modified_date = _instant_date(attributes.get("modifyDate"))
        rin_value = attributes.get("rin")
        rins, malformed_rins = _normalized_rins([] if rin_value is None else [rin_value])
        source_link = data.get("links")
        raw_source_url = source_link.get("self") if isinstance(source_link, Mapping) else None
        source_url = _http_url(raw_source_url)
        malformed_source_url: tuple[Any, ...] = ()
        source_url_from_policy = False
        if raw_source_url is not None and source_url is None:
            malformed_source_url = (raw_source_url,)
        elif source_url is None:
            source_url = "https://www.regulations.gov/docket/" + quote(
                source_item_id, safe=""
            )
            source_url_from_policy = True
        normalized = {
            "title": title,
            "agencies": agencies,
            "documentType": document_type,
            "publicationDate": None,
            "lastUpdatedDate": modified_date,
            "docketIds": [source_item_id],
            "regulationIdentifierNumbers": rins,
            "commentCloseDate": None,
            "language": self.language,
            "sourceUrl": source_url,
        }
        normalization_fields = (
            _field_outcome("title", ("data.attributes.title",), title, unparseable_values=malformed_title),
            _field_outcome("agencies", ("data.attributes.agencyId",), agencies, unparseable_values=malformed_agencies),
            _field_outcome("documentType", ("data.attributes.docketType",), document_type, unparseable_values=malformed_document_type),
            _field_outcome("publicationDate", (), None),
            _field_outcome("lastUpdatedDate", ("data.attributes.modifyDate",), modified_date, unparseable_values=malformed_modified_date),
            _field_outcome("docketIds", ("data.id",), [source_item_id]),
            _field_outcome("regulationIdentifierNumbers", ("data.attributes.rin",), rins, unparseable_values=malformed_rins),
            _field_outcome("commentCloseDate", (), None),
            _field_outcome("language", ("policy.configuration.language",), self.language, value_source="policy"),
            _field_outcome(
                "sourceUrl",
                (
                    "policy.configuration.sourceUrlTemplates.dockets"
                    if source_url_from_policy
                    else "data.links.self",
                ),
                source_url,
                value_source="policy" if source_url_from_policy else "source",
                unparseable_values=malformed_source_url,
            ),
        )
        offers, families, selected_family, family_order = self._source_kind_rendition_preference(
            renditions,
            raw_source_url,
            include_files=False,
        )
        missing = [
            name
            for name in ("title", "agencies", "lastUpdatedDate", "sourceUrl")
            if not normalized[name]
        ]
        selection, decisions = _selection_result(
            withdrawn=False,
            withdrawal_reason=None,
            missing_fields=missing,
            candidates=offers,
            budget_available=budget_available,
        )
        # A missing modifyDate leaves required `lastUpdatedDate` absent, so
        # `selection` above is never SELECTED and the placeholder never reaches
        # a served item; the same fallback the document row applies.
        raw_issued_version = attributes.get("modifyDate")
        source_issued_version = (
            raw_issued_version
            if isinstance(raw_issued_version, str) and raw_issued_version
            else "unknown"
        )
        sampling_result = {
            "frameAdmitted": True,
            "partition": "all",
            "stratum": ["all"],
            "orderHash": None,
            "rank": None,
            "stratumSize": None,
            "allocationMethod": "all",
            "limit": None,
            "drawn": True,
        }
        return SourceCatalogItem(
            source_item_id=source_item_id,
            document_id=source_item_id,
            source_issued_version=source_issued_version,
            source_native_facts=(_source_fact(record),),
            normalized_metadata=normalized,
            source_observed_topics=(),
            source_observations=tuple(self._source_observations((("docket", record),))),
            interpretations=self._interpretations(
                joins=(),
                normalization_fields=normalization_fields,
                ordered_family_ids=family_order,
                families=families,
                selected_family_id=selected_family,
                candidates=offers,
                sampling_result=sampling_result,
                selection=selection,
                decisions=decisions,
                topic_source_field="data.attributes.topics",
            ),
            candidate_renditions=() if selection.disposition is CatalogDisposition.DELETED else offers,
            selection=selection,
        )

    def _comment_item_from_row(
        self,
        record: Mapping[str, Any],
        renditions: tuple[Mapping[str, Any], ...],
        workspace: CatalogPolicyWorkspace,
        *,
        budget_available: bool,
    ) -> SourceCatalogItem:
        native, attributes = _record_data(record, expected_type="comments")
        source_item_id = str(record["sourceRecordId"])
        data = native["data"]
        if not isinstance(data, Mapping) or data.get("id") != source_item_id:
            raise IntegrityError("Regulations.gov comment source identity differs")
        docket_id, malformed_docket_id = _source_identifier(attributes.get("docketId"))
        comment_on_document_id, malformed_comment_on_document_id = _source_identifier(
            attributes.get("commentOnDocumentId")
        )
        docket = _indexed_row(workspace, _DOCKET_INDEX, docket_id)
        document = _indexed_row(
            workspace,
            _DOCUMENT_INDEX,
            comment_on_document_id,
        )
        if docket is not None and docket[0]["sourceRecordId"] != docket_id:
            raise IntegrityError("Regulations.gov comment docket join returned a different key")
        if (
            document is not None
            and document[0]["sourceRecordId"] != comment_on_document_id
        ):
            raise IntegrityError("Regulations.gov comment document join returned a different key")

        title, malformed_title = _text(attributes.get("title"))
        agencies, malformed_agencies = _agency(attributes.get("agencyId"), self.agency_names)
        document_type, malformed_document_type = _text(attributes.get("documentType"))
        publication_date, malformed_publication_date = _instant_date(
            attributes.get("postedDate")
        )
        modified_date, malformed_modified_date = _instant_date(
            attributes.get("modifyDate")
        )
        raw_rins: list[Any] = []
        if docket is not None:
            _, docket_attributes = _record_data(docket[0], expected_type="dockets")
            if docket_attributes.get("rin") is not None:
                raw_rins.append(docket_attributes["rin"])
        rins, malformed_rins = _normalized_rins(raw_rins)
        docket_ids = [docket_id] if docket_id is not None else []
        source_link = data.get("links")
        raw_source_url = source_link.get("self") if isinstance(source_link, Mapping) else None
        source_url = _http_url(raw_source_url)
        malformed_source_url: tuple[Any, ...] = ()
        source_url_from_policy = False
        if raw_source_url is not None and source_url is None:
            malformed_source_url = (raw_source_url,)
        elif source_url is None:
            source_url = "https://www.regulations.gov/comment/" + quote(
                source_item_id, safe=""
            )
            source_url_from_policy = True
        normalized = {
            "title": title,
            "agencies": agencies,
            "documentType": document_type,
            "publicationDate": publication_date,
            "lastUpdatedDate": modified_date,
            "docketIds": docket_ids,
            "regulationIdentifierNumbers": rins,
            "commentCloseDate": None,
            "language": self.language,
            "sourceUrl": source_url,
        }
        normalization_fields = (
            _field_outcome("title", ("data.attributes.title",), title, unparseable_values=malformed_title),
            _field_outcome("agencies", ("data.attributes.agencyId",), agencies, unparseable_values=malformed_agencies),
            _field_outcome("documentType", ("data.attributes.documentType",), document_type, unparseable_values=malformed_document_type),
            _field_outcome("publicationDate", ("data.attributes.postedDate",), publication_date, unparseable_values=malformed_publication_date),
            _field_outcome("lastUpdatedDate", ("data.attributes.modifyDate",), modified_date, unparseable_values=malformed_modified_date),
            _field_outcome("docketIds", ("data.attributes.docketId",), docket_ids, unparseable_values=malformed_docket_id),
            _field_outcome(
                "regulationIdentifierNumbers",
                ("joinedDocket.data.attributes.rin",),
                rins,
                unparseable_values=malformed_rins,
            ),
            _field_outcome("commentCloseDate", (), None),
            _field_outcome("language", ("policy.configuration.language",), self.language, value_source="policy"),
            _field_outcome(
                "sourceUrl",
                (
                    "policy.configuration.sourceUrlTemplates.comments"
                    if source_url_from_policy
                    else "data.links.self",
                ),
                source_url,
                value_source="policy" if source_url_from_policy else "source",
                unparseable_values=malformed_source_url,
            ),
        )
        offers, families, selected_family, family_order = self._source_kind_rendition_preference(
            renditions,
            raw_source_url,
            include_files=True,
        )
        withdrawn = attributes.get("withdrawn") is True
        candidates = () if withdrawn else offers
        selected_family_id = None if withdrawn else selected_family
        withdrawal_reason, _ = _text(attributes.get("reasonWithdrawn"))
        missing = [
            name
            for name in ("agencies", "documentType", "publicationDate", "sourceUrl")
            if not normalized[name]
        ]
        selection, decisions = _selection_result(
            withdrawn=withdrawn,
            withdrawal_reason=withdrawal_reason,
            missing_fields=missing,
            candidates=candidates,
            budget_available=budget_available,
        )

        observations = self._source_observations(
            (
                ("comment", record),
                ("docket", docket[0] if docket is not None else None),
                ("document", document[0] if document is not None else None),
            )
        )
        modify_date = attributes.get("modifyDate")
        if isinstance(modify_date, str) and modify_date:
            source_issued_version = modify_date
            source_issued_version_field = "data.attributes.modifyDate"
            version_reason = "upstream-selected-newest-comment-version"
        elif modify_date is None:
            posted_date = attributes.get("postedDate")
            if not isinstance(posted_date, str) or not posted_date:
                raise IntegrityError(
                    "Regulations.gov comment with null modifyDate has no postedDate fallback"
                )
            source_issued_version = posted_date
            source_issued_version_field = "data.attributes.postedDate"
            version_reason = "upstream-selected-comment-has-null-modify-date"
        else:
            raise IntegrityError(
                "Regulations.gov comment modifyDate must be nonempty text or null"
            )
        observations.append(
            {
                "observationKey": "comment/source-issued-version-policy",
                "observationValue": {
                    "exactSourceValue": source_issued_version,
                    "sourcePath": source_issued_version_field,
                    "reasonCode": version_reason,
                    "upstreamVersionPath": "data.attributes.modifyDate",
                    "upstreamVersionValue": modify_date,
                },
            }
        )
        if malformed_comment_on_document_id:
            observations.append(
                {
                    "observationKey": "comment/unparseable-comment-on-document-id",
                    "observationValue": malformed_comment_on_document_id[0],
                }
            )
        observations.sort(key=lambda value: _utf16_key(value["observationKey"]))
        facts = [record]
        if docket is not None:
            facts.append(docket[0])
        if document is not None:
            facts.append(document[0])
        source_facts = tuple(
            _source_fact(value)
            for value in sorted(facts, key=lambda value: _utf16_key(str(value["scopeId"])))
        )
        joins = (
            _join_result(
                join_id="comment-docket",
                source_field="data.attributes.docketId",
                source_value=docket_id,
                lookup_scope_id=_DOCKET_SCOPE,
                matched=docket,
            ),
            _join_result(
                join_id="comment-document",
                source_field="data.attributes.commentOnDocumentId",
                source_value=comment_on_document_id,
                lookup_scope_id=_DOCUMENT_SCOPE,
                matched=document,
            ),
        )
        sampling_result = {
            "frameAdmitted": not withdrawn,
            "partition": None if withdrawn else "all",
            "stratum": [] if withdrawn else ["all"],
            "orderHash": None,
            "rank": None,
            "stratumSize": None,
            "allocationMethod": "all",
            "limit": None,
            "drawn": not withdrawn,
        }
        return SourceCatalogItem(
            source_item_id=source_item_id,
            document_id=source_item_id,
            source_issued_version=source_issued_version,
            source_native_facts=source_facts,
            normalized_metadata=normalized,
            source_observed_topics=(),
            source_observations=tuple(observations),
            interpretations=self._interpretations(
                joins=joins,
                normalization_fields=normalization_fields,
                ordered_family_ids=family_order,
                families=families,
                selected_family_id=selected_family_id,
                candidates=candidates,
                sampling_result=sampling_result,
                selection=selection,
                decisions=decisions,
                topic_source_field="data.attributes.topics",
            ),
            candidate_renditions=candidates,
            selection=selection,
        )

    def _item_from_row(
        self,
        record: Mapping[str, Any],
        renditions: tuple[Mapping[str, Any], ...],
        workspace: CatalogPolicyWorkspace,
        *,
        sample_drawn: bool | None,
        budget_available: bool,
    ) -> SourceCatalogItem:
        native, attributes = _record_data(record, expected_type="documents")
        source_item_id = str(record["sourceRecordId"])
        data = native["data"]
        if not isinstance(data, Mapping) or data.get("id") != source_item_id:
            raise IntegrityError("Regulations.gov document source identity differs")
        docket_id, malformed_docket_id = _source_identifier(attributes.get("docketId"))
        fr_doc_num, malformed_fr_doc_num = _source_identifier(attributes.get("frDocNum"))
        docket = _indexed_row(workspace, _DOCKET_INDEX, docket_id)
        federal_register = _indexed_row(
            workspace,
            _FEDERAL_REGISTER_INDEX,
            fr_doc_num,
        )
        if docket is not None and docket[0]["sourceRecordId"] != docket_id:
            raise IntegrityError("Regulations.gov docket join returned a different exact key")
        if federal_register is not None and federal_register[0]["sourceRecordId"] != fr_doc_num:
            raise IntegrityError("Federal Register join returned a different exact key")

        title, malformed_title = _text(attributes.get("title"))
        agencies, malformed_agencies = _agency(attributes.get("agencyId"), self.agency_names)
        document_type, malformed_document_type = _text(attributes.get("documentType"))
        publication_date, malformed_publication_date = _instant_date(attributes.get("postedDate"))
        modified_date, malformed_modified_date = _instant_date(attributes.get("modifyDate"))
        comment_close_date, malformed_comment_close_date = _instant_date(
            attributes.get("commentEndDate")
        )
        raw_rins: list[Any] = []
        additional_rins = attributes.get("additionalRins")
        if isinstance(additional_rins, list):
            raw_rins.extend(additional_rins)
        elif additional_rins is not None:
            raw_rins.append(additional_rins)
        if docket is not None:
            _, docket_attributes = _record_data(docket[0], expected_type="dockets")
            if docket_attributes.get("rin") is not None:
                raw_rins.append(docket_attributes["rin"])
        if federal_register is not None:
            fr_native = federal_register[0].get("record")
            if not isinstance(fr_native, Mapping):
                raise IntegrityError("Federal Register join payload must be an object")
            fr_rins = fr_native.get("regulation_id_numbers")
            if isinstance(fr_rins, list):
                raw_rins.extend(fr_rins)
            elif fr_rins is not None:
                raw_rins.append(fr_rins)
        rins, malformed_rins = _normalized_rins(raw_rins)
        docket_ids = [docket_id] if docket_id is not None else []
        source_link = data.get("links")
        raw_source_url = source_link.get("self") if isinstance(source_link, Mapping) else None
        source_url = _http_url(raw_source_url)
        malformed_source_url: tuple[Any, ...] = ()
        source_url_from_policy = False
        if raw_source_url is not None and source_url is None:
            malformed_source_url = (raw_source_url,)
        elif source_url is None:
            source_url = self.source_url_template.replace(
                "{documentId}", quote(source_item_id, safe="")
            )
            source_url_from_policy = True

        normalized = {
            "title": title,
            "agencies": agencies,
            "documentType": document_type,
            "publicationDate": publication_date,
            "lastUpdatedDate": modified_date,
            "docketIds": docket_ids,
            "regulationIdentifierNumbers": rins,
            "commentCloseDate": comment_close_date,
            "language": self.language,
            "sourceUrl": source_url,
        }
        normalization_fields = (
            _field_outcome("title", ("data.attributes.title",), title, unparseable_values=malformed_title),
            _field_outcome("agencies", ("data.attributes.agencyId",), agencies, unparseable_values=malformed_agencies),
            _field_outcome("documentType", ("data.attributes.documentType",), document_type, unparseable_values=malformed_document_type),
            _field_outcome("publicationDate", ("data.attributes.postedDate",), publication_date, unparseable_values=malformed_publication_date),
            _field_outcome("lastUpdatedDate", ("data.attributes.modifyDate",), modified_date, unparseable_values=malformed_modified_date),
            _field_outcome("docketIds", ("data.attributes.docketId",), docket_ids, unparseable_values=malformed_docket_id),
            _field_outcome(
                "regulationIdentifierNumbers",
                (
                    "data.attributes.additionalRins",
                    "joinedDocket.data.attributes.rin",
                    "joinedFederalRegister.regulation_id_numbers",
                ),
                rins,
                unparseable_values=malformed_rins,
            ),
            _field_outcome("commentCloseDate", ("data.attributes.commentEndDate",), comment_close_date, unparseable_values=malformed_comment_close_date),
            _field_outcome("language", ("policy.configuration.language",), self.language, value_source="policy"),
            _field_outcome(
                "sourceUrl",
                (
                    "policy.configuration.sourceUrlTemplate"
                    if source_url_from_policy
                    else "data.links.self",
                ),
                source_url,
                value_source="policy" if source_url_from_policy else "source",
                unparseable_values=malformed_source_url,
            ),
        )
        if tuple(value.normalized_field for value in normalization_fields) != _NORMALIZED_FIELDS:
            raise AssertionError("Regulations.gov normalization field order drifted")

        offers, families, selected_family = self._rendition_preference(
            renditions,
            federal_register[1] if federal_register is not None else (),
        )
        withdrawn = attributes.get("withdrawn") is True
        candidates = () if withdrawn else offers
        selected_family_id = None if withdrawn else selected_family
        decisions: list[CatalogSelectionDecision] = []
        if withdrawn:
            reason_withdrawn, _ = _text(attributes.get("reasonWithdrawn"))
            reason = "The source marks this document withdrawn."
            if reason_withdrawn is not None:
                reason = f"The source marks this document withdrawn: {reason_withdrawn}"
            selection = SourceCatalogSelection(
                CatalogDisposition.DELETED,
                "source.withdrawn-after-publication",
                reason,
            )
            decisions.append(
                CatalogSelectionDecision(
                    "source-withdrawal",
                    False,
                    CatalogDisposition.DELETED,
                    selection.reason_code,
                    selection.reason,
                )
            )
        else:
            decisions.append(CatalogSelectionDecision("source-withdrawal", True))
            if sample_drawn is False:
                limit = self.sample.per_partition_limit if self.sample is not None else 0
                reason = (
                    "The deterministic stratified sample takes at most "
                    f"{limit} items per document type; this item was not drawn."
                )
                selection = SourceCatalogSelection(
                    CatalogDisposition.EXCLUDED,
                    "policy.sample-not-drawn",
                    reason,
                )
                decisions.append(
                    CatalogSelectionDecision(
                        "sample-draw",
                        False,
                        CatalogDisposition.EXCLUDED,
                        selection.reason_code,
                        selection.reason,
                    )
                )
            else:
                if sample_drawn is True:
                    decisions.append(CatalogSelectionDecision("sample-draw", True))
                missing = [name for name in _REQUIRED_NORMALIZED_FIELDS if not normalized[name]]
                if missing:
                    reason = "Required normalized catalog values are unusable: " + ", ".join(missing)
                    selection = SourceCatalogSelection(
                        CatalogDisposition.FAILED,
                        "source.normalized-field-missing",
                        reason,
                    )
                    decisions.append(
                        CatalogSelectionDecision(
                            "required-metadata",
                            False,
                            CatalogDisposition.FAILED,
                            selection.reason_code,
                            selection.reason,
                        )
                    )
                else:
                    decisions.append(CatalogSelectionDecision("required-metadata", True))
                    if not candidates:
                        reason = "The source and its exact Federal Register match offer no usable rendition."
                        selection = SourceCatalogSelection(
                            CatalogDisposition.UNAVAILABLE,
                            "source.no-candidate-rendition",
                            reason,
                        )
                        decisions.append(
                            CatalogSelectionDecision(
                                "candidate-rendition",
                                False,
                                CatalogDisposition.UNAVAILABLE,
                                selection.reason_code,
                                selection.reason,
                            )
                        )
                    else:
                        decisions.append(CatalogSelectionDecision("candidate-rendition", True))
                        if not budget_available:
                            reason = (
                                "The catalog selected-item budget is already exhausted."
                            )
                            selection = SourceCatalogSelection(
                                CatalogDisposition.EXCLUDED,
                                "policy.item-budget-exhausted",
                                reason,
                            )
                            decisions.append(
                                CatalogSelectionDecision(
                                    "selected-item-budget",
                                    False,
                                    CatalogDisposition.EXCLUDED,
                                    selection.reason_code,
                                    selection.reason,
                                )
                            )
                        else:
                            if self.max_selected_items is not None:
                                decisions.append(
                                    CatalogSelectionDecision(
                                        "selected-item-budget",
                                        True,
                                    )
                                )
                            selection = SourceCatalogSelection(
                                CatalogDisposition.SELECTED
                            )

        topics = observed_topics(
            attributes.get("topics"),
            scheme="regulations.gov",
            identity_fields=("id", "slug"),
            label_fields=("label", "name"),
        )
        facts = [_source_fact(record)]
        if docket is not None:
            facts.append(_source_fact(docket[0]))
        if federal_register is not None:
            facts.append(_source_fact(federal_register[0]))
        observations: list[dict[str, Any]] = []
        for prefix, source_record in (
            ("document", record),
            ("docket", docket[0] if docket is not None else None),
            ("federal-register", federal_register[0] if federal_register is not None else None),
        ):
            if source_record is None:
                continue
            diagnostics = source_record.get("fieldDiagnostics")
            if isinstance(diagnostics, list):
                observations.extend(
                    {
                        "observationKey": f"{prefix}/field-diagnostic/{index}",
                        "observationValue": value,
                    }
                    for index, value in enumerate(diagnostics)
                )
        if malformed_fr_doc_num:
            observations.append(
                {
                    "observationKey": "unparseableFederalRegisterDocumentNumber",
                    "observationValue": malformed_fr_doc_num[0],
                }
            )
        input_scope_ids = self._input_scope_ids()
        pin = {
            "policyId": self.policy_id,
            "policyVersion": self.policy_version,
            "policyDigest": self.policy_digest,
            "inputScopeIds": input_scope_ids,
        }
        join_rows = (
            _join_result(
                join_id="document-docket",
                source_field="data.attributes.docketId",
                source_value=docket_id,
                lookup_scope_id=_DOCKET_SCOPE,
                matched=docket,
            ),
            _join_result(
                join_id="document-federal-register",
                source_field="data.attributes.frDocNum",
                source_value=fr_doc_num,
                lookup_scope_id=_FEDERAL_REGISTER_SCOPE,
                matched=federal_register,
            ),
        )
        sampling_result = self._sampling_result(
            source_item_id,
            withdrawn=withdrawn,
            sample_drawn=sample_drawn,
            workspace=workspace,
        )
        interpretations = (
            {
                "interpretationKind": "exact-join",
                **pin,
                "result": {"joins": list(join_rows)},
            },
            {
                "interpretationKind": "normalization",
                **pin,
                "result": {"fields": [field.to_dict() for field in normalization_fields]},
            },
            {
                "interpretationKind": "rendition-preference",
                **pin,
                "result": {
                    "orderedFamilyIds": list(_RENDITION_ORDER),
                    "families": [family.to_dict() for family in families],
                    "selectedFamilyId": selected_family_id,
                    "selectedRenditionIds": [value.rendition_id for value in candidates],
                },
            },
            {
                "interpretationKind": "sampling",
                **pin,
                "result": sampling_result,
            },
            {
                "interpretationKind": "selection",
                **pin,
                "result": {
                    "decisions": [decision.to_dict() for decision in decisions],
                    "finalDisposition": selection.disposition.value,
                    "reasonCode": selection.reason_code,
                    "reason": selection.reason,
                },
            },
            {
                "interpretationKind": "topic-recovery",
                **pin,
                "result": {
                    "sourceField": "data.attributes.topics",
                    "outcome": "observed" if topics else "not-recovered",
                    "evidenceDigest": None,
                    "observedTopicIds": [value["observedTopicId"] for value in topics],
                },
            },
        )
        # Neither date leaves required `publicationDate` absent, so `selection`
        # above is already DELETED, EXCLUDED, or FAILED: the placeholder never
        # reaches a SELECTED item, and one bad row cannot abort a long build.
        # `FederalRegisterCatalogPolicy` uses the same `"unknown"` fallback.
        raw_issued_version = attributes.get("modifyDate") or attributes.get("postedDate")
        source_issued_version = (
            raw_issued_version
            if isinstance(raw_issued_version, str) and raw_issued_version
            else "unknown"
        )
        return SourceCatalogItem(
            source_item_id=source_item_id,
            document_id=source_item_id,
            source_issued_version=source_issued_version,
            source_native_facts=tuple(facts),
            normalized_metadata=normalized,
            source_observed_topics=topics,
            source_observations=tuple(observations),
            interpretations=interpretations,
            candidate_renditions=candidates,
            selection=selection,
        )

    @staticmethod
    def _rendition_preference(
        regulations_renditions: tuple[Mapping[str, Any], ...],
        federal_register_renditions: tuple[Mapping[str, Any], ...],
    ) -> tuple[
        tuple[SourceCatalogCandidate, ...],
        tuple[CatalogRenditionFamily, ...],
        str | None,
    ]:
        by_family: dict[str, list[SourceCatalogCandidate]] = {
            family: [] for family in _RENDITION_ORDER
        }

        def add(
            family: str,
            values: tuple[Mapping[str, Any], ...],
            *,
            prefix: str,
        ) -> None:
            claimed: set[str] = set()
            for value in values:
                candidate = _candidate_from_rendition(
                    value,
                    rendition_id=f"{prefix}{value['renditionId']}",
                )
                if candidate is None or candidate.locator in claimed:
                    continue
                claimed.add(candidate.locator)
                by_family[family].append(candidate)

        add("regulations-gov-file", regulations_renditions, prefix="regulations-gov/")
        add("federal-register", federal_register_renditions, prefix="federal-register/")
        ordered = {
            family: tuple(
                sorted(by_family[family], key=lambda value: _utf16_key(value.rendition_id))
            )
            for family in _RENDITION_ORDER
        }
        families = tuple(
            CatalogRenditionFamily(
                family,
                tuple(value.rendition_id for value in ordered[family]),
            )
            for family in _RENDITION_ORDER
        )
        selected_family = next((family for family in _RENDITION_ORDER if ordered[family]), None)
        return (
            ordered[selected_family] if selected_family is not None else (),
            families,
            selected_family,
        )


__all__ = ["RegulationsGovCatalogPolicy", "RegulationsGovSamplePolicy"]
