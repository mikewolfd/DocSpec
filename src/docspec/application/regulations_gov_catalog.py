"""DocSpec interpretation of exact Regulations.gov source-native facts."""

from __future__ import annotations

import bisect
import hashlib
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
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
    FRESH_BUILD,
    SourceInputSelector,
    SourceRecordCollisionResolution,
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

#: Appended to every "no rendition" reason. The disposition is a statement about
#: the records this build acquired, and a reader reasonably hears it as a
#: statement about the document. Measured 2026-09-04: of 13 catalog-A items
#: randomly sampled from the 865,206 carrying this reason, 13 had downloadable
#: files at regulations.gov, in the `attachments` relationship that Mirrulations
#: -- the acquired source -- does not mirror. The disposition was accurate about
#: its input and read as a claim about the world.
_ACQUIRED_SOURCE_SCOPE = (
    " This states what the acquired source contains, not whether the publisher"
    " holds content for it."
)
#: The publisher's ``restrictReasonType`` values, each mapped to one reason
#: code, verbatim. Measured on catalog-A 2026-09-04: Copyrighted 53,580,
#: Other 19,965, Confidential Business Information 1,179, Personally
#: Identifiable Information 252 -- 74,976 of the 865,206 unavailable rows
#: carry one, and no sampled row carrying one had a file at the publisher
#: (decision 0005). A value outside this map is never inferred into a bucket:
#: the row fails with :data:`_RESTRICT_REASON_UNREAD` so the receipt shows it.
_PUBLISHER_WITHHOLDING_CODES: Mapping[str, str] = {
    "Copyrighted": "source.publisher-withheld.copyrighted",
    "Confidential Business Information": (
        "source.publisher-withheld.confidential-business-information"
    ),
    "Personally Identifiable Information": (
        "source.publisher-withheld.personally-identifiable-information"
    ),
    "Other": "source.publisher-withheld.other",
}
_RESTRICT_REASON_UNREAD = "source.restrict-reason-unread"
#: Regulations.gov publishes its own internal test fixtures through the same
#: public API as real filings, under one of these three source item id
#: prefixes -- anchored at the start, case-sensitive as the publisher writes
#: them. They 404 at both document and docket level on the public API, so
#: they carry no evidence a build could acquire. Measured independently twice
#: over catalog-A 2026-09-04: 41 items total (37 ``failed``, 4 ``deleted``).
#: Left alone they read as real acquisition failures and real withdrawals;
#: neither is true, so they are excluded under their own reason code instead.
_TEST_FIXTURE_ID_PREFIXES: tuple[str, ...] = ("TRAIN-", "ERULE-", "TEST-")
_TEST_FIXTURE_REASON_CODE = "source.publisher-test-fixture"
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
                **_carried_discards(row),
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


def _carried_discards(row: Any) -> dict[str, Any]:
    """Stage filings the loader collapsed into this row, and nothing when there are none.

    Written for both staging paths rather than the universe one alone, for
    structural symmetry rather than against a live hazard. The Federal Register
    index is the only lookup input today, and a Federal Register native record
    is flat -- no ``data`` key -- so ``_record_data(expected_type="documents")``
    refuses it, the resolver returns None and the loader's refusal stands. The
    lookup path therefore never carries a discarded filing today; staging it
    keeps the property true when a second lookup input appears.
    """

    if not row.discarded_filings:
        return {}
    return {"discardedFilings": [dict(value) for value in row.discarded_filings]}


def _stored_discards(value: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Read back what `_carried_discards` staged, refusing a shape it did not write."""

    raw = value.get("discardedFilings", [])
    if not isinstance(raw, list) or not all(isinstance(item, Mapping) for item in raw):
        raise IntegrityError("catalog join index discarded filings must be an array of objects")
    return tuple(raw)


def _stored_row(
    value: Mapping[str, Any],
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    # Subtracting the one optional key keeps the shape closed against every
    # other: a row may carry filings the loader collapsed into it, and nothing
    # else. `_carried_discards` writes it and `_stored_discards` reads it, and
    # this guard sits between them -- widened together with them rather than
    # left refusing what its own module had started writing.
    if set(value) - {"discardedFilings"} != {"record", "renditions"} or not isinstance(
        value["record"], Mapping
    ):
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


def _no_rendition_selection(
    attributes: Mapping[str, Any], acquired_reason: str
) -> SourceCatalogSelection:
    """Disposition a record with no usable rendition by what the publisher declared.

    ``restrictReasonType`` is the publisher's own statement that it withholds
    the content, so a row carrying one is unavailable for that declared reason.
    A row without one is unavailable for ``acquired_reason``, which says only
    what the acquired source contains. Nothing is inferred in either direction.
    """

    declared = attributes.get("restrictReasonType")
    if declared is None:
        return SourceCatalogSelection(
            CatalogDisposition.UNAVAILABLE, "source.no-candidate-rendition", acquired_reason
        )
    code = _PUBLISHER_WITHHOLDING_CODES.get(declared) if isinstance(declared, str) else None
    if code is None:
        return SourceCatalogSelection(
            CatalogDisposition.FAILED,
            _RESTRICT_REASON_UNREAD,
            f"The publisher declares a restrictReasonType this policy does not read: {declared!r}.",
        )
    reason = f"The publisher withholds this record's content; restrictReasonType is {declared!r}"
    subtype = attributes.get("subtype")
    if isinstance(subtype, str) and subtype:
        reason += f" and subtype is {subtype!r}"
    return SourceCatalogSelection(CatalogDisposition.UNAVAILABLE, code, reason + ".")


def _test_fixture_selection(source_item_id: str) -> SourceCatalogSelection | None:
    """Exclude a Regulations.gov internal test fixture, or defer with ``None``.

    Matches only at the start of the source item id, case-sensitive, against
    :data:`_TEST_FIXTURE_ID_PREFIXES`. A real filing merely containing one of
    these letter groups (``EPA-TRAIN-2020-0001``, ``PRETEST-1``) is untouched.
    """

    if not source_item_id.startswith(_TEST_FIXTURE_ID_PREFIXES):
        return None
    return SourceCatalogSelection(
        CatalogDisposition.EXCLUDED,
        _TEST_FIXTURE_REASON_CODE,
        f"The source item id {source_item_id!r} carries a Regulations.gov"
        " internal test-fixture prefix (TRAIN-, ERULE- or TEST-).",
    )


def _selection_result(
    *,
    source_item_id: str,
    attributes: Mapping[str, Any],
    withdrawn: bool,
    withdrawal_reason: str | None,
    missing_fields: Sequence[str],
    candidates: tuple[SourceCatalogCandidate, ...],
    budget_available: bool,
) -> tuple[SourceCatalogSelection, tuple[CatalogSelectionDecision, ...]]:
    decisions: list[CatalogSelectionDecision] = []
    fixture_selection = _test_fixture_selection(source_item_id)
    if fixture_selection is not None:
        decisions.append(
            CatalogSelectionDecision(
                "publisher-test-fixture",
                False,
                fixture_selection.disposition,
                fixture_selection.reason_code,
                fixture_selection.reason,
            )
        )
        return fixture_selection, tuple(decisions)
    decisions.append(CatalogSelectionDecision("publisher-test-fixture", True))
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
        selection = _no_rendition_selection(
            attributes,
            "The acquired source record offers no usable rendition." + _ACQUIRED_SOURCE_SCOPE,
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
    #: Filled on first use by :attr:`policy_digest`; never an input or an
    #: identity, so it stays out of ``__init__``, ``__eq__`` and ``__repr__``.
    _policy_digest: str | None = field(default=None, init=False, repr=False, compare=False)

    policy_id = "urn:docspec:catalog-policy:regulations-gov:1"
    policy_version = "1.2.0"

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
            "publisherWithholding": {
                "sourceField": "data.attributes.restrictReasonType",
                "reasonCodes": dict(_PUBLISHER_WITHHOLDING_CODES),
                "unreadReasonCode": _RESTRICT_REASON_UNREAD,
            },
            "testFixtures": {
                "sourceField": "sourceItemId",
                "idPrefixes": list(_TEST_FIXTURE_ID_PREFIXES),
                "reasonCode": _TEST_FIXTURE_REASON_CODE,
            },
            "selectionFailures": [
                {
                    "decisionId": "publisher-test-fixture",
                    "disposition": "excluded",
                    "reasonCode": _TEST_FIXTURE_REASON_CODE,
                },
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
                *(
                    {
                        "decisionId": "candidate-rendition",
                        "disposition": "unavailable",
                        "reasonCode": code,
                    }
                    for code in _PUBLISHER_WITHHOLDING_CODES.values()
                ),
                {
                    "decisionId": "candidate-rendition",
                    "disposition": "failed",
                    "reasonCode": _RESTRICT_REASON_UNREAD,
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
        """Return this policy's digest, canonicalizing the member at most once.

        The digest is a pure function of the fields, but every interpretation of
        every catalog row stamps it, so the uncached property canonicalized the
        whole policy member once per item. That member is 17,880 bytes here --
        ``configuration`` embeds a 314-entry agency map -- and measured 166.5 us
        per call, so a 49,884-item build spent 8.3 s of its 82.9 s wall clock,
        and a profiled run 26.9 s of 218 s, rebuilding one constant. Worse, the
        cost is O(items x policy size): every agency added to ``agencyNames``
        slowed down every row.

        Filling the cache lazily rather than in ``__post_init__`` keeps
        construction -- and the errors a malformed configuration raises -- exactly
        where they were.
        """

        cached = self._policy_digest
        if cached is not None:
            return cached
        digest = sha256_digest(canonical_json_bytes(self.to_member()))
        object.__setattr__(self, "_policy_digest", digest)
        return digest

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
                "publisherWithholding",
                "testFixtures",
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

    def resolve_source_record_collision(
        self,
        selector: SourceInputSelector,
        stored: Mapping[str, Any],
        incoming: Mapping[str, Any],
    ) -> SourceRecordCollisionResolution | None:
        """Pick the owning filing when one document is mirrored under two agencies.

        Regulations.gov publishes a Federal Register document under each agency
        that filed it, so the same ``documentId`` can arrive from two releases.
        DocSpec decision 0004 rules that this is one item with the non-owning
        filing recorded rather than dropped.

        The owner is decided by measurement, not preference. Two tests over all
        1,797,201 document records carrying both a documentId and a docketId --
        the population the rule can be evaluated over -- produce four exceptions
        between them:

        * ``documentId`` starts with ``docketId + "-"`` -- three exceptions.
        * ``docketId`` starts with ``agencyId`` -- one exception.

        A filing that fails either test is the cross-file; the one that passes
        both owns the document. Both blocking records are resolved this way and
        neither is caught by both tests, so the rules are not redundant.

        Prefix *containment* is deliberate, not "docket plus one trailing
        segment": 40,485 of those records carry two-segment sequences such as
        ``DOT-OST-1995-125-0050-0001`` in docket ``DOT-OST-1995-125``, and the
        narrower reading reports every one of them as a violation.

        Returns ``None`` when neither filing can be distinguished, which keeps
        the loader's refusal rather than guessing. This rule answers "which
        mirror owns one document id"; it does not answer "which of two
        differing document ids is canonical", and 0004 measures that it
        resolves none of the 16,652 groups posing that second question.
        """

        candidates = [stored, incoming]
        owners = [row for row in candidates if self._owns_its_filing(row)]
        if len(owners) != 1:
            return None
        owner = owners[0]
        discarded = incoming if owner is stored else stored
        return SourceRecordCollisionResolution(
            owner=owner,
            discarded=discarded,
            reason_code="source.cross-filed-under-another-agency",
            reason=(
                "the same document is mirrored under another agency, whose filing "
                "does not reconstruct its own document id"
            ),
        )

    @staticmethod
    def _owns_its_filing(row: Mapping[str, Any]) -> bool:
        """Both measured tests, which a filing must pass to own its document.

        Reuses ``_record_data`` rather than reaching through the payload here,
        so a shape this policy cannot read is refused in one place. A row whose
        payload is not a readable document simply does not own it, which leaves
        the loader's refusal in charge rather than resolving on a guess.
        """

        try:
            _, attributes = _record_data(row["record"], expected_type="documents")
        except (IntegrityError, KeyError, TypeError):
            return False
        document_id = row["record"].get("sourceRecordId") or ""
        docket_id = attributes.get("docketId") or ""
        agency_id = attributes.get("agencyId") or ""
        if not document_id or not docket_id or not agency_id:
            return False
        return document_id.startswith(f"{docket_id}-") and docket_id.startswith(agency_id)

    def iter_items(
        self,
        inputs: CatalogPolicyInputs,
        workspace: CatalogPolicyWorkspace,
    ) -> Iterator[SourceCatalogItem]:
        resume = getattr(inputs, "resume", FRESH_BUILD)
        if not resume.indexed:
            _index_rows(inputs, workspace, self.federal_register_input, _FEDERAL_REGISTER_INDEX)
            self._stage_universe(inputs, workspace)
            if self.sample is not None:
                self._draw_document_sample(workspace)
        # A resumed run starts past its last committed item with the budget
        # it had reached; forgetting either would publish a different catalog.
        selected_count = resume.selected_count
        for row in inputs.iter_universe_rows():
            record = row.record
            renditions = row.renditions
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
                    discarded_filings=row.discarded_filings,
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
        """Stage the ordered universe plus the indexes this policy will read.

        Every staged copy costs one canonical serialization and one SQLite row
        of the full record and its renditions, so this loop decides most of the
        workspace's size. It used to write each document row three times:
        ``_UNIVERSE_ROWS`` for the ordered scan, plus ``_DOCUMENT_ROWS`` and
        ``_DOCUMENT_INDEX``, which received byte-identical values under
        identical keys and differed only in namespace.

        Those two are now one namespace, read two ways -- ordered by
        ``_draw_document_sample`` and by key by ``_comment_item_from_row`` --
        and it is staged only when one of those readers is configured. With
        ``sample`` and ``comment_input`` both null, which is the production
        Regulations.gov configuration, neither reader exists and two of the
        three writes were pure cost: measured at 49.6 us and 4.7 MB per
        thousand rows each, and 48.7% of all full-payload workspace bytes.

        ``_DOCKET_INDEX`` stays unconditional because ``_item_from_row`` joins
        every document to its docket. When ``docket_input`` is null no docket
        row reaches this loop, so the namespace is simply empty.
        """

        index_documents = self.sample is not None or self.comment_input is not None
        for row in inputs.iter_universe_rows():
            stored = {
                "record": dict(row.record),
                "renditions": [dict(value) for value in row.renditions],
                **_carried_discards(row),
            }
            source_item_id = str(row.record["sourceRecordId"])
            if row.record["scopeId"] == _DOCUMENT_SCOPE:
                if index_documents:
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
        for value in workspace.iter_ordered(_DOCUMENT_INDEX):
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
            source_item_id=source_item_id,
            attributes=attributes,
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
            source_item_id=source_item_id,
            attributes=attributes,
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
        discarded_filings: tuple[Mapping[str, Any], ...] = (),
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
        fixture_selection = _test_fixture_selection(source_item_id)
        if fixture_selection is not None:
            selection = fixture_selection
            decisions.append(
                CatalogSelectionDecision(
                    "publisher-test-fixture",
                    False,
                    selection.disposition,
                    selection.reason_code,
                    selection.reason,
                )
            )
        else:
            decisions.append(CatalogSelectionDecision("publisher-test-fixture", True))
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
                            reason = (
                                "Neither the acquired source record nor its exact "
                                "Federal Register match offers a usable rendition."
                                + _ACQUIRED_SOURCE_SCOPE
                            )
                            selection = _no_rendition_selection(attributes, reason)
                            decisions.append(
                                CatalogSelectionDecision(
                                    "candidate-rendition",
                                    False,
                                    selection.disposition,
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
        # A filing this document was cross-filed under, collapsed by the loader
        # and kept here rather than dropped. Decision 0004: the two filings of a
        # real cross-filed document were measured to differ in 8 of 84 and 6 of
        # 90 leaf fields, so the discarded side carries evidence -- a docket
        # association and a Federal Register volume citation that exist on one
        # side only. sourceObservations already takes a free-form key and an
        # unconstrained value, so this needs no schema version.
        observations.extend(
            {
                "observationKey": f"cross-file-discard/{index}",
                "observationValue": dict(filing),
            }
            for index, filing in enumerate(discarded_filings)
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
