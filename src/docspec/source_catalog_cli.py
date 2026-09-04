"""Dependency-light CLI for DocSpec-owned immutable source catalogs."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from docspec.adapters.catalog_policy_workspace import SqliteCatalogPolicyWorkspace
from docspec.adapters.source_catalog_artifact import (
    SourceCatalogArtifactReader,
    SourceCatalogBuildRequest,
    SourceCatalogBuilder,
    source_catalog_producer,
    source_item_validator_implementation,
)
from docspec.adapters.source_catalog_store import (
    LocalSourceCatalogPublication,
    LocalSourceCatalogStore,
)
from docspec.application.federal_register_catalog import FederalRegisterCatalogPolicy
from docspec.application.regulations_gov_catalog import RegulationsGovCatalogPolicy
from docspec.domain.identity import (
    canonical_json_file_bytes,
    parse_canonical_json,
    parse_closed_json,
    require_sha256,
    stable_urn,
    thaw_json,
)
from docspec.domain.references import SourceCatalogRef
from docspec.domain.source_catalog import CatalogDisposition
from docspec.domain.security import redact, redact_text, require_secret_free
from docspec.errors import DocSpecError

_MAX_JSON_BYTES = 16 * 1024 * 1024
_BUILD_RECEIPT_NAME = "source-catalog-build-command-receipt.json"
_SOURCE_NATIVE_PROFILES = (
    "federal-register",
    "regulations-gov-documents",
    "regulations-gov-dockets",
    "regulations-gov-comments",
)


class SourceCatalogCliError(DocSpecError):
    """A source-catalog operator action failed preflight or verification."""


def _read_bytes(path: Path, *, label: str) -> bytes:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise SourceCatalogCliError(f"{label} must be a regular, non-symlink file: {path}")
    with path.open("rb") as stream:
        payload = stream.read(_MAX_JSON_BYTES + 1)
    if len(payload) > _MAX_JSON_BYTES:
        raise SourceCatalogCliError(f"{label} exceeds the {_MAX_JSON_BYTES}-byte limit")
    return payload


def _read_object(path: Path, *, label: str, canonical: bool) -> dict[str, Any]:
    payload = _read_bytes(path, label=label)
    parser = parse_canonical_json if canonical else parse_closed_json
    value = thaw_json(parser(payload, label=label))
    if not isinstance(value, dict):
        raise SourceCatalogCliError(f"{label} must be a JSON object")
    return value


def _existing_root(path: Path, *, label: str) -> Path:
    path = Path(path)
    if path.is_symlink() or not path.is_dir():
        raise SourceCatalogCliError(f"{label} must be an existing, non-symlink directory: {path}")
    return path.resolve(strict=True)


def _emit(value: object, *, error: bool = False) -> None:
    if error:
        value = redact(value)
    else:
        require_secret_free(value, label="CLI output")
    stream = sys.stderr.buffer if error else sys.stdout.buffer
    stream.write(canonical_json_file_bytes(value))
    stream.flush()


def _paths_overlap(first: Path, second: Path) -> bool:
    resolved_first = Path(first).resolve(strict=False)
    resolved_second = Path(second).resolve(strict=False)
    return (
        resolved_first == resolved_second
        or resolved_first in resolved_second.parents
        or resolved_second in resolved_first.parents
    )


def _blob_store_evidence(
    args: argparse.Namespace,
    measurements: Mapping[str, int],
) -> dict[str, object] | None:
    value = getattr(args, "blob_store", None)
    if value is None:
        return None
    return {
        "path": Path(value).resolve(strict=False).as_posix(),
        "retention": "verified-content-addressed-blobs-retained-for-reuse",
        "accountingStatus": "complete",
        "payloadBytesWritten": measurements["payloadBytesWritten"],
        "payloadBytesReused": measurements["payloadBytesReused"],
    }


def _require_new_outputs(
    destination: Path,
    receipt: Path,
    blob_store: Path | None,
    source_native_roots: tuple[Path, ...],
) -> None:
    expected_receipt = destination / _BUILD_RECEIPT_NAME
    if receipt.resolve(strict=False) != expected_receipt.resolve(strict=False):
        raise SourceCatalogCliError(
            f"operation receipt must be the atomic artifact member: {expected_receipt}"
        )
    if blob_store is not None and _paths_overlap(destination, blob_store):
        raise SourceCatalogCliError("artifact and blob store paths must not contain one another")
    if any(_paths_overlap(destination, root) for root in source_native_roots):
        raise SourceCatalogCliError(
            "artifact and source-native input paths must not contain one another"
        )
    if destination.exists() or destination.is_symlink():
        raise SourceCatalogCliError(f"refusing to replace existing artifact: {destination}")


def _producer(args: argparse.Namespace):
    return source_catalog_producer(
        implementation_id=args.implementation_id,
        verifier_id="urn:docspec:verifier:source-catalog",
        verifier_version="1.0.0",
        verifier_implementation_id=args.verifier_implementation_id,
    )


def _load_build_command_receipt(
    root: Path,
) -> tuple[dict[str, Any], SourceCatalogRef]:
    """Parse and validate the one closed command receipt at a catalog root."""

    label = "source catalog build command receipt"
    receipt = _read_object(root / _BUILD_RECEIPT_NAME, label=label, canonical=True)

    def closed(
        value: object,
        fields: set[str],
        *,
        nested_label: str,
    ) -> dict[str, Any]:
        if not isinstance(value, dict) or set(value) != fields:
            raise SourceCatalogCliError(f"{nested_label} has an invalid closed shape")
        return value

    def text(value: object, *, nested_label: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise SourceCatalogCliError(f"{nested_label} must be a non-empty string")
        return value

    def count(value: object, *, nested_label: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise SourceCatalogCliError(f"{nested_label} must be a non-negative integer")
        return value

    def absolute_path(value: object, *, nested_label: str) -> str:
        selected = text(value, nested_label=nested_label)
        if not Path(selected).is_absolute():
            raise SourceCatalogCliError(f"{nested_label} must be an absolute path")
        return selected

    receipt = closed(
        receipt,
        {
            "acceptedSourceVerifierImplementationIds",
            "blobStore",
            "byteMeasurements",
            "catalog",
            "catalogPolicy",
            "catalogStateDigest",
            "destination",
            "diagnosticDigests",
            "dispositionCounts",
            "format",
            "formatVersion",
            "itemCount",
            "joinCoverage",
            "operation",
            "partitionPolicy",
            "producer",
            "reasonCounts",
            "receiptId",
            "requestedUniverseSetDigest",
            "selectedSourceSetDigest",
            "sourceNativeInputs",
            "verdict",
        },
        nested_label=label,
    )
    if (
        receipt["format"] != "docspec-source-catalog-build-command-receipt"
        or receipt["formatVersion"] != "1.0"
        or receipt["operation"] != "source-catalog.build"
        or receipt["verdict"] != "pass"
    ):
        raise SourceCatalogCliError(f"{label} has an unsupported identity or verdict")

    content = {
        key: value
        for key, value in receipt.items()
        if key not in {"format", "formatVersion", "receiptId"}
    }
    expected_receipt_id = stable_urn("source-catalog-build-command-receipt", content)
    if receipt["receiptId"] != expected_receipt_id:
        raise SourceCatalogCliError(f"{label} receiptId does not match its content")

    raw_inputs = receipt["sourceNativeInputs"]
    if not isinstance(raw_inputs, list) or not raw_inputs:
        raise SourceCatalogCliError(f"{label} sourceNativeInputs must be a non-empty array")
    input_pins: list[tuple[str, str]] = []
    for index, raw_input in enumerate(raw_inputs):
        source_input = closed(
            raw_input,
            {"artifactDigest", "blobStore", "locator", "logicalId", "profile"},
            nested_label=f"{label} sourceNativeInputs[{index}]",
        )
        absolute_path(source_input["locator"], nested_label=f"{label} source-native locator")
        absolute_path(
            source_input["blobStore"],
            nested_label=f"{label} source-native blob store",
        )
        text(source_input["logicalId"], nested_label=f"{label} source-native logicalId")
        try:
            require_sha256(
                source_input["artifactDigest"],
                f"{label} source-native artifactDigest",
            )
        except ValueError as error:
            raise SourceCatalogCliError(str(error)) from error
        if source_input["profile"] not in _SOURCE_NATIVE_PROFILES:
            raise SourceCatalogCliError(f"{label} contains an unsupported source-native profile")
        input_pins.append((source_input["logicalId"], source_input["artifactDigest"]))
    if len(set(input_pins)) != len(input_pins):
        raise SourceCatalogCliError(f"{label} source-native input pins must be distinct")

    accepted_verifiers = receipt["acceptedSourceVerifierImplementationIds"]
    if not isinstance(accepted_verifiers, list) or not accepted_verifiers:
        raise SourceCatalogCliError(
            f"{label} acceptedSourceVerifierImplementationIds must be a non-empty array"
        )
    for value in accepted_verifiers:
        text(value, nested_label=f"{label} accepted source verifier implementation ID")
    if accepted_verifiers != sorted(set(accepted_verifiers)):
        raise SourceCatalogCliError(
            f"{label} accepted source verifier implementation IDs must be sorted and distinct"
        )

    catalog_policy = closed(
        receipt["catalogPolicy"],
        {"policyDigest", "policyId", "policyVersion"},
        nested_label=f"{label} catalogPolicy",
    )
    text(catalog_policy["policyId"], nested_label=f"{label} catalog policyId")
    text(catalog_policy["policyVersion"], nested_label=f"{label} catalog policyVersion")
    try:
        require_sha256(catalog_policy["policyDigest"], f"{label} catalog policyDigest")
    except ValueError as error:
        raise SourceCatalogCliError(str(error)) from error

    producer = closed(
        receipt["producer"],
        {
            "implementationId",
            "product",
            "verifierId",
            "verifierImplementationId",
            "verifierVersion",
        },
        nested_label=f"{label} producer",
    )
    for name, value in producer.items():
        text(value, nested_label=f"{label} producer {name}")

    catalog = closed(
        receipt["catalog"],
        {"catalogId", "digest", "locator"},
        nested_label=f"{label} catalog",
    )
    try:
        catalog_reference = SourceCatalogRef.from_dict(catalog)
    except (TypeError, ValueError) as error:
        raise SourceCatalogCliError(f"{label} catalog reference is invalid: {error}") from error

    count(receipt["itemCount"], nested_label=f"{label} itemCount")
    for name in (
        "catalogStateDigest",
        "requestedUniverseSetDigest",
        "selectedSourceSetDigest",
    ):
        try:
            require_sha256(receipt[name], f"{label} {name}")
        except ValueError as error:
            raise SourceCatalogCliError(str(error)) from error
    dispositions = closed(
        receipt["dispositionCounts"],
        {value.value for value in CatalogDisposition},
        nested_label=f"{label} dispositionCounts",
    )
    for name, value in dispositions.items():
        count(value, nested_label=f"{label} dispositionCounts.{name}")
    reason_counts = receipt["reasonCounts"]
    if not isinstance(reason_counts, list):
        raise SourceCatalogCliError(f"{label} reasonCounts must be an array")
    for index, raw_row in enumerate(reason_counts):
        row = closed(
            raw_row,
            {"count", "disposition", "reasonCode"},
            nested_label=f"{label} reasonCounts[{index}]",
        )
        text(row["disposition"], nested_label=f"{label} reasonCounts[{index}].disposition")
        text(row["reasonCode"], nested_label=f"{label} reasonCounts[{index}].reasonCode")
        count(row["count"], nested_label=f"{label} reasonCounts[{index}].count")
    partition_policy = closed(
        receipt["partitionPolicy"],
        {"bucketCount", "policyDigest", "policyId", "policyVersion"},
        nested_label=f"{label} partitionPolicy",
    )
    text(partition_policy["policyId"], nested_label=f"{label} partition policyId")
    text(
        partition_policy["policyVersion"],
        nested_label=f"{label} partition policyVersion",
    )
    try:
        require_sha256(
            partition_policy["policyDigest"],
            f"{label} partition policyDigest",
        )
    except ValueError as error:
        raise SourceCatalogCliError(str(error)) from error
    bucket_count = count(
        partition_policy["bucketCount"],
        nested_label=f"{label} partition bucketCount",
    )
    if not 1 <= bucket_count <= 65_536:
        raise SourceCatalogCliError(f"{label} partition bucketCount is invalid")
    join_coverage = receipt["joinCoverage"]
    if not isinstance(join_coverage, list):
        raise SourceCatalogCliError(f"{label} joinCoverage must be an array")
    for index, value in enumerate(join_coverage):
        coverage = closed(
            value,
            {"eligible", "joinId", "matched", "nullResult", "unmatched"},
            nested_label=f"{label} joinCoverage[{index}]",
        )
        text(coverage["joinId"], nested_label=f"{label} joinCoverage[{index}].joinId")
        for name in ("eligible", "matched", "nullResult", "unmatched"):
            count(
                coverage[name],
                nested_label=f"{label} joinCoverage[{index}].{name}",
            )
    diagnostic_digests = closed(
        receipt["diagnosticDigests"],
        {
            "dispositionsDigest",
            "interpretationsDigest",
            "joinedFieldsDigest",
            "normalizedFieldsDigest",
            "reasonsDigest",
            "renditionChoicesDigest",
        },
        nested_label=f"{label} diagnosticDigests",
    )
    for name, digest in diagnostic_digests.items():
        try:
            require_sha256(digest, f"{label} {name}")
        except ValueError as error:
            raise SourceCatalogCliError(str(error)) from error

    measurements = closed(
        receipt["byteMeasurements"],
        {
            "payloadBytesRead",
            "payloadBytesReused",
            "payloadBytesWritten",
            "publicationBytesWritten",
        },
        nested_label=f"{label} byteMeasurements",
    )
    for name, value in measurements.items():
        count(value, nested_label=f"{label} byteMeasurements.{name}")
    if measurements["payloadBytesRead"] != (
        measurements["payloadBytesReused"] + measurements["payloadBytesWritten"]
    ):
        raise SourceCatalogCliError(f"{label} payload byte measurements do not reconcile")

    blob_store = receipt["blobStore"]
    if blob_store is not None:
        blob_store = closed(
            blob_store,
            {
                "accountingStatus",
                "path",
                "payloadBytesReused",
                "payloadBytesWritten",
                "retention",
            },
            nested_label=f"{label} blobStore",
        )
        absolute_path(blob_store["path"], nested_label=f"{label} blob-store path")
        count(
            blob_store["payloadBytesReused"],
            nested_label=f"{label} blobStore.payloadBytesReused",
        )
        count(
            blob_store["payloadBytesWritten"],
            nested_label=f"{label} blobStore.payloadBytesWritten",
        )
        if (
            blob_store["retention"]
            != "verified-content-addressed-blobs-retained-for-reuse"
            or blob_store["accountingStatus"] != "complete"
            or blob_store["payloadBytesReused"] != measurements["payloadBytesReused"]
            or blob_store["payloadBytesWritten"] != measurements["payloadBytesWritten"]
        ):
            raise SourceCatalogCliError(f"{label} blob-store evidence is invalid")

    absolute_path(receipt["destination"], nested_label=f"{label} destination")
    return receipt, catalog_reference


def _verify(args: argparse.Namespace) -> int:
    root = _existing_root(args.root, label="source catalog root")
    receipt, receipt_reference = _load_build_command_receipt(root)
    if receipt["receiptId"] != args.expected_command_receipt_id:
        raise SourceCatalogCliError(
            "source catalog build command receipt differs from the expected receipt identity"
        )
    supplied_reference = SourceCatalogRef.from_dict(
        _read_object(args.reference, label="source catalog reference", canonical=False)
    )
    if supplied_reference != receipt_reference:
        raise SourceCatalogCliError(
            "source catalog reference differs from the published build command receipt"
        )
    producer = _producer(args)
    if receipt["producer"] != producer.as_dict():
        raise SourceCatalogCliError(
            "source catalog build command receipt producer differs from the installed implementation"
        )
    if receipt["destination"] != root.as_posix():
        raise SourceCatalogCliError(
            "source catalog build command receipt destination differs from the explicit store root"
        )
    summary = SourceCatalogArtifactReader(
        LocalSourceCatalogStore(
            root,
            create=False,
        ),
        producer=producer,
    ).verify_snapshot(receipt_reference)
    comparisons = {
        "artifactDigest": (summary.artifact_digest, receipt_reference.digest),
        "byteMeasurements": (
            dict(summary.byte_measurements),
            receipt["byteMeasurements"],
        ),
        "catalogStateDigest": (summary.catalog_state_digest, receipt["catalogStateDigest"]),
        "diagnosticDigests": (dict(summary.diagnostic_digests), receipt["diagnosticDigests"]),
        "dispositionCounts": (dict(summary.disposition_counts), receipt["dispositionCounts"]),
        "reasonCounts": ([dict(value) for value in summary.reason_counts], receipt["reasonCounts"]),
        "itemCount": (summary.item_count, receipt["itemCount"]),
        "joinCoverage": (
            [dict(value) for value in summary.join_coverage],
            receipt["joinCoverage"],
        ),
        "logicalId": (summary.logical_id, receipt_reference.catalog_id),
        "partitionPolicy": (dict(summary.partition_policy), receipt["partitionPolicy"]),
        "requestedUniverseSetDigest": (
            summary.requested_universe_set_digest,
            receipt["requestedUniverseSetDigest"],
        ),
        "selectedSourceSetDigest": (
            summary.selected_source_set_digest,
            receipt["selectedSourceSetDigest"],
        ),
        "selectionPolicy": (dict(summary.selection_policy), receipt["catalogPolicy"]),
        "sourceNativeInputs": (
            {
                (value["logicalId"], value["artifactDigest"])
                for value in summary.source_native_inputs
            },
            {
                (value["logicalId"], value["artifactDigest"])
                for value in receipt["sourceNativeInputs"]
            },
        ),
    }
    for name, (actual, expected) in comparisons.items():
        if actual != expected:
            raise SourceCatalogCliError(
                f"source catalog build command receipt {name} differs from the admitted catalog"
            )
    _emit(
        {
            "format": "docspec-source-catalog-verification",
            "formatVersion": "1.0",
            "commandReceiptId": receipt["receiptId"],
            "logicalId": summary.logical_id,
            "artifactDigest": summary.artifact_digest,
            "catalogId": summary.catalog_id,
            "catalogStateDigest": summary.catalog_state_digest,
            "requestedUniverseSetDigest": summary.requested_universe_set_digest,
            "selectedSourceSetDigest": summary.selected_source_set_digest,
            "itemCount": summary.item_count,
            "partitions": list(summary.partitions),
            "dispositionCounts": dict(summary.disposition_counts),
            "reasonCounts": [dict(value) for value in summary.reason_counts],
            "selectionPolicy": dict(summary.selection_policy),
            "partitionPolicy": dict(summary.partition_policy),
            "joinCoverage": [dict(value) for value in summary.join_coverage],
            "diagnosticDigests": dict(summary.diagnostic_digests),
            "sourceNativeInputs": [dict(value) for value in summary.source_native_inputs],
            "byteMeasurements": dict(summary.byte_measurements),
            "verdict": "pass",
        }
    )
    return 0


def _build(args: argparse.Namespace) -> int:
    # A successful build publishes its receipt inside the immutable destination.
    # A failed build has no destination to contain a trustworthy success receipt.
    args._suppress_failure_receipt = True
    lengths = {
        len(args.source_native),
        len(args.source_native_artifact_digest),
        len(args.source_native_blob_store),
        len(args.source_native_profile),
    }
    if len(lengths) != 1:
        raise SourceCatalogCliError(
            "each --source-native requires one --source-native-artifact-digest, "
            "--source-native-blob-store, and --source-native-profile"
        )
    policy_member = _read_object(args.catalog_policy, label="catalog policy", canonical=True)
    policy_id = policy_member.get("policyId")
    if policy_id == FederalRegisterCatalogPolicy.policy_id:
        policy = FederalRegisterCatalogPolicy.from_member(policy_member)
    elif policy_id == RegulationsGovCatalogPolicy.policy_id:
        policy = RegulationsGovCatalogPolicy.from_member(policy_member)
    else:
        raise SourceCatalogCliError("catalog policy is not implemented by this DocSpec version")
    accepted_verifiers = frozenset(args.accepted_source_verifier_implementation_id)

    # Import the producer adapter only after the operator selects it. Help and
    # verification do not require the producer package.
    from docspec.adapters.spicyregs_source_native import (
        SpicyRegsSourceNativeAdapter,
        spicyregs_source_profile,
    )

    source_inputs = tuple(
        (
            _existing_root(locator, label="source-native artifact"),
            digest,
            _existing_root(blob_root, label="source-native blob store"),
            profile_name,
        )
        for locator, digest, blob_root, profile_name in zip(
            args.source_native,
            args.source_native_artifact_digest,
            args.source_native_blob_store,
            args.source_native_profile,
            strict=True,
        )
    )
    sources = tuple(
        SpicyRegsSourceNativeAdapter.from_local(
            locator,
            blob_root=blob_root,
            artifact_digest=digest,
            profile=spicyregs_source_profile(profile_name),
            accepted_verifier_implementation_ids=accepted_verifiers,
        )
        for locator, digest, blob_root, profile_name in source_inputs
    )
    descriptions = tuple(source.describe() for source in sources)
    catalog_id = stable_urn(
        "source-catalog-series",
        {
            "policyId": policy.policy_id,
            "sourceSystemIds": sorted({value.source_system_id for value in descriptions}),
        },
    )
    producer = _producer(args)
    destination = Path(args.destination)
    receipt_path = Path(args.receipt)
    blob_store = Path(args.blob_store) if args.blob_store is not None else None
    _require_new_outputs(
        destination,
        receipt_path,
        blob_store,
        tuple(source_input[0] for source_input in source_inputs),
    )
    # Record which schema engine will decide every row. jsonschema-rs is a
    # declared dependency, so this should always name it; if an install ever
    # loses it, a ~116x slowdown announces itself here instead of being read as
    # "the build is just slow". stderr, so the receipt on stdout is unchanged.
    _emit(
        {
            "format": "docspec-source-catalog-build-diagnostic",
            "formatVersion": "1.0",
            "sourceItemValidator": source_item_validator_implementation(),
        },
        error=True,
    )
    with LocalSourceCatalogPublication(destination) as publication:
        destination = publication.destination
        catalog_store = publication.store(shared_blob_root=blob_store)
        if blob_store is not None:
            blob_store = blob_store.resolve(strict=True)
            args.blob_store = blob_store
        result = SourceCatalogBuilder(
            store=catalog_store,
            policy=policy,
            request=SourceCatalogBuildRequest(catalog_id, producer),
            workspace_factory=lambda: (
                SqliteCatalogPolicyWorkspace(path=args.resume_workspace)
                if args.resume_workspace is not None
                else SqliteCatalogPolicyWorkspace(directory=publication.root)
            ),
        ).build(sources)
        if args.resume_workspace is not None:
            # Published, so the workspace is now tens of gigabytes of nothing.
            for suffix in ("", "-journal"):
                Path(f"{args.resume_workspace}{suffix}").unlink(missing_ok=True)
        publication.remove_empty_directory(".staging")
        content = {
            "operation": "source-catalog.build",
            "acceptedSourceVerifierImplementationIds": sorted(accepted_verifiers),
            "sourceNativeInputs": [
                {
                    "locator": Path(locator).resolve(strict=True).as_posix(),
                    "blobStore": Path(blob_root).resolve(strict=True).as_posix(),
                    "profile": profile_name,
                    "logicalId": description.logical_id,
                    "artifactDigest": description.artifact_digest,
                }
                for (locator, _, blob_root, profile_name), description in zip(
                    source_inputs,
                    descriptions,
                    strict=True,
                )
            ],
            "catalogPolicy": {
                "policyId": policy.policy_id,
                "policyVersion": policy.policy_version,
                "policyDigest": policy.policy_digest,
            },
            "producer": producer.as_dict(),
            "destination": destination.resolve(strict=False).as_posix(),
            "catalog": result.reference.to_dict(),
            "catalogStateDigest": result.summary.catalog_state_digest,
            "requestedUniverseSetDigest": result.summary.requested_universe_set_digest,
            "selectedSourceSetDigest": result.summary.selected_source_set_digest,
            "itemCount": result.summary.item_count,
            "dispositionCounts": dict(result.summary.disposition_counts),
            "reasonCounts": [dict(value) for value in result.summary.reason_counts],
            "partitionPolicy": dict(result.summary.partition_policy),
            "joinCoverage": [dict(value) for value in result.summary.join_coverage],
            "diagnosticDigests": dict(result.summary.diagnostic_digests),
            "byteMeasurements": dict(result.byte_measurements),
            "blobStore": _blob_store_evidence(args, result.byte_measurements),
            "verdict": "pass",
        }
        receipt = {
            "format": "docspec-source-catalog-build-command-receipt",
            "formatVersion": "1.0",
            "receiptId": stable_urn("source-catalog-build-command-receipt", content),
            **content,
        }
        publication.write_file(
            _BUILD_RECEIPT_NAME,
            canonical_json_file_bytes(receipt),
        )
        publication.publish()
    _emit(receipt)
    return 0


def _add_subcommands(source_catalog: argparse.ArgumentParser) -> None:
    source_commands = source_catalog.add_subparsers(dest="source_catalog_command", required=True)
    source_build = source_commands.add_parser("build", help="Build one complete immutable source-catalog snapshot")
    source_build.add_argument("--source-native", action="append", type=Path, required=True)
    source_build.add_argument("--source-native-artifact-digest", action="append", required=True)
    source_build.add_argument(
        "--source-native-blob-store",
        action="append",
        type=Path,
        required=True,
        help="Read-only SpicyRegs content-addressed blob store paired with one source-native input",
    )
    source_build.add_argument(
        "--source-native-profile",
        action="append",
        required=True,
        choices=_SOURCE_NATIVE_PROFILES,
    )
    source_build.add_argument("--accepted-source-verifier-implementation-id", action="append", required=True)
    source_build.add_argument("--catalog-policy", type=Path, required=True)
    source_build.add_argument("--implementation-id", required=True)
    source_build.add_argument("--verifier-implementation-id", required=True)
    source_build.add_argument("--destination", type=Path, required=True)
    source_build.add_argument(
        "--resume-workspace",
        type=Path,
        default=None,
        help=(
            "Keep the build workspace at this path and commit it as the build"
            " progresses. A build that dies leaves it behind; running the same"
            " command again resumes from the last commit and publishes the"
            " identical artifact. Removed after a successful publish."
        ),
    )
    source_build.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help=(
            "Required atomic receipt member; must be "
            "<destination>/source-catalog-build-command-receipt.json"
        ),
    )
    source_build.add_argument(
        "--blob-store",
        type=Path,
        help=(
            "Explicit persistent content-addressed blob store used for verified reuse; "
            "must share the destination filesystem"
        ),
    )
    source_build.set_defaults(func=_build, operation="source-catalog.build")

    source_verify = source_commands.add_parser("verify", help="Verify a complete local source-catalog distribution")
    source_verify.add_argument("--root", type=Path, required=True)
    source_verify.add_argument("--reference", type=Path, required=True, help="JSON SourceCatalogRef")
    source_verify.add_argument("--expected-command-receipt-id", required=True)
    source_verify.add_argument("--implementation-id", required=True)
    source_verify.add_argument("--verifier-implementation-id", required=True)
    source_verify.set_defaults(func=_verify)


def add_source_catalog_command(commands: argparse._SubParsersAction) -> None:
    source_catalog = commands.add_parser(
        "source-catalog",
        help="Build and verify immutable source-catalog inputs",
    )
    _add_subcommands(source_catalog)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="docspec source-catalog",
        description="Build and verify DocSpec-owned immutable source catalogs.",
    )
    _add_subcommands(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (DocSpecError, OSError, TypeError, ValueError) as error:
        _emit(
            {
                "format": "docspec-cli-error",
                "formatVersion": "1.0",
                "errorType": type(error).__name__,
                "message": redact_text(str(error)),
                "verdict": "fail",
            },
            error=True,
        )
        return 2


__all__ = ["add_source_catalog_command", "build_parser", "main"]
