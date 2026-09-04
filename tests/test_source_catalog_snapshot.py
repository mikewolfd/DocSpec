from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from rulespec_artifacts import (
    ArtifactPin,
    ArtifactVerificationError,
    LocalBlobSource,
    MemberSourceError,
    Producer,
)

import docspec.adapters.source_catalog_artifact as source_catalog_artifact
import docspec.adapters.source_catalog_store as source_catalog_store
from docspec.adapters.catalog_policy_workspace import SqliteCatalogPolicyWorkspace
from docspec.application.federal_register_catalog import FederalRegisterCatalogPolicy
from docspec.adapters.source_catalog_artifact import (
    SourceCatalogArtifactReader,
    SourceCatalogBuildRequest,
    SourceCatalogBuilder,
    requested_universe_set_digest,
    selected_source_set_digest,
)
from docspec.adapters.source_catalog_store import (
    LocalSourceCatalogPublication,
    LocalSourceCatalogStore,
)
from docspec.adapters.spicyregs_source_native import SpicyRegsSourceNativeAdapter
from docspec.domain.source_catalog import (
    CatalogDisposition,
    SOURCE_CATALOG_MAX_JOIN_IDS,
    SourceCatalogItem,
)
from docspec.domain.identity import (
    canonical_json_bytes,
    canonical_json_file_bytes,
    sha256_digest,
    stable_urn,
)
from docspec.domain.references import SourceCatalogRef
from docspec.errors import IntegrityError, LimitExceededError
from docspec.ports.source_catalog import (
    CatalogPolicyInputs,
    CatalogPolicyWorkspace,
    SourceInputSelector,
    SourceNativeDescription,
)
from docspec.entrypoint import main
from tests.helpers import CountItems, KillAfter

_SHA_A = "sha256:" + "a" * 64
_SHA_B = "sha256:" + "b" * 64
_SHA_C = "sha256:" + "c" * 64
_FEDERAL_REGISTER_SOURCE = "https://www.federalregister.gov/api/v1"
_ACCEPTED_SOURCE_VERIFIERS = (
    "urn:test:source-verifier:sha256:" + "8" * 64,
    "urn:test:source-verifier:sha256:" + "9" * 64,
)


def producer() -> Producer:
    implementation = "git+https://example.test/docspec@" + "1" * 40
    return Producer(
        "docspec",
        implementation,
        "urn:docspec:verifier:source-catalog",
        "1.0.0",
        implementation,
    )


def test_local_source_catalog_publication_pins_the_destination_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "outputs"
    parent.mkdir()
    retained = tmp_path / "outputs-retained"
    replacement = tmp_path / "replacement"
    replacement.mkdir()

    with LocalSourceCatalogPublication(parent / "catalog") as publication:
        publication.write_file("receipt.json", b"complete\n")
        parent.rename(retained)
        parent.symlink_to(replacement, target_is_directory=True)

        with pytest.raises(IntegrityError, match="destination parent.*non-symlink"):
            publication.publish()

    assert not tuple(replacement.iterdir())
    assert not tuple(retained.iterdir())


def test_local_source_catalog_publication_store_refuses_a_replaced_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "outputs"
    parent.mkdir()
    retained = tmp_path / "outputs-retained"
    replacement = tmp_path / "replacement"
    replacement.mkdir()

    with LocalSourceCatalogPublication(parent / "catalog") as publication:
        stage_name = publication.root.name
        parent.rename(retained)
        parent.symlink_to(replacement, target_is_directory=True)
        (replacement / stage_name).mkdir()

        with pytest.raises(ValueError, match="source-catalog root changed since admission"):
            publication.store()

    assert not tuple(replacement.joinpath(stage_name).iterdir())
    assert not (retained / stage_name).exists()


def test_local_source_catalog_publication_survives_process_exit_after_rename(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "catalog"
    process_id = os.fork()
    if process_id == 0:
        try:
            publication = LocalSourceCatalogPublication(destination)
            publication.write_file("artifact.json", b"artifact\n")
            publication.write_file(
                "source-catalog-build-command-receipt.json",
                b"receipt\n",
            )
            publication.publish()
        except BaseException:
            os._exit(99)
        os._exit(23)

    _, status = os.waitpid(process_id, 0)
    assert os.waitstatus_to_exitcode(status) == 23
    assert (destination / "artifact.json").read_bytes() == b"artifact\n"
    assert (destination / "source-catalog-build-command-receipt.json").read_bytes() == b"receipt\n"


def test_local_source_catalog_publication_process_exit_before_rename_leaves_no_root_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "catalog"
    process_id = os.fork()
    if process_id == 0:
        publication = LocalSourceCatalogPublication(destination)
        publication.write_file("artifact.json", b"unpublished\n")
        os._exit(31)

    _, status = os.waitpid(process_id, 0)
    assert os.waitstatus_to_exitcode(status) == 31
    assert not destination.exists()
    unpublished = tuple(tmp_path.glob(".catalog.*"))
    assert len(unpublished) == 1

    with LocalSourceCatalogPublication(destination) as publication:
        publication.write_file("artifact.json", b"published\n")
        publication.write_file(
            "source-catalog-build-command-receipt.json",
            b"receipt\n",
        )
        publication.publish()

    assert (destination / "artifact.json").read_bytes() == b"published\n"
    assert unpublished[0].is_dir()


def description(*, scope: str = "complete-snapshot") -> SourceNativeDescription:
    return SourceNativeDescription(
        logical_id="urn:spicy:artifact:spicyregs-source-native-release:" + "a" * 64,
        artifact_digest=_SHA_A,
        source_system_id=_FEDERAL_REGISTER_SOURCE,
        source_system_version="v1",
        source_state_scope=scope,
        source_state_digest=_SHA_B,
        source_native_schema_set_digest=_SHA_C,
    )


def record(identity: str, *, malformed_rin: bool = False, agencies: bool = True) -> dict[str, Any]:
    return {
        "sourceRecordId": identity,
        "scopeId": "federal-register-documents",
        "schemaName": "federal-register-document",
        "schemaVersion": "1.0",
        "schemaDigest": _SHA_C,
        "record": {
            "document_number": identity,
            "title": f"Federal Register {identity}",
            "type": "Rule",
            "publication_date": "2026-08-24",
            "agencies": (
                [{"slug": "environmental-protection-agency", "name": "Environmental Protection Agency"}]
                if agencies
                else []
            ),
            "html_url": f"https://www.federalregister.gov/d/{identity}",
            "pdf_url": f"https://example.test/{identity}.pdf",
            "docket_ids": ["EPA-HQ-2026-0001"],
            "regulation_id_numbers": ["not a rin" if malformed_rin else "2060-AV12"],
            "topics": [{"slug": "air-quality", "name": "Air quality"}],
        },
        "fieldDiagnostics": [],
    }


def renditions(identity: str) -> tuple[dict[str, Any], ...]:
    return (
        {
            "sourceRecordId": identity,
            "renditionId": f"{identity}/html",
            "sourceField": "html_url",
            "locator": f"https://www.federalregister.gov/d/{identity}",
            "mediaType": "text/html",
            "expectedSha256": None,
            "expectedByteSize": None,
        },
        {
            "sourceRecordId": identity,
            "renditionId": f"{identity}/pdf",
            "sourceField": "pdf_url",
            "locator": f"https://example.test/{identity}.pdf",
            "mediaType": "application/pdf",
            "expectedSha256": None,
            "expectedByteSize": None,
        },
    )


@dataclass
class FakeSource:
    metadata: SourceNativeDescription
    records: tuple[Mapping[str, Any], ...]
    renditions: tuple[Mapping[str, Any], ...]

    def describe(self) -> SourceNativeDescription:
        return self.metadata

    def iter_records(self) -> Iterator[Mapping[str, Any]]:
        yield from self.records

    def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
        yield from self.renditions


def build(root: Path, source: FakeSource):
    store = LocalSourceCatalogStore(root)
    result = SourceCatalogBuilder(
        store=store,
        policy=FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((source,))
    return store, result


def interpretation_result(item: SourceCatalogItem, kind: str) -> Mapping[str, Any]:
    interpretation = next(value for value in item.interpretations if value["interpretationKind"] == kind)
    result = interpretation["result"]
    assert isinstance(result, Mapping)
    return result


def normalization_fields(item: SourceCatalogItem) -> dict[str, Mapping[str, Any]]:
    fields = interpretation_result(item, "normalization")["fields"]
    assert isinstance(fields, tuple)
    return {field["normalizedField"]: field for field in fields}


def assert_no_published_catalog(root: Path) -> None:
    assert not [path for path in root.iterdir() if path.name != ".staging"]
    staging = root / ".staging"
    if staging.exists():
        assert not tuple(staging.iterdir())


def assert_outside_sentinel_unchanged(root: Path) -> None:
    assert [path.name for path in root.iterdir()] == ["sentinel.txt"]
    assert (root / "sentinel.txt").read_bytes() == b"outside must stay unchanged"


def build_with_store(store: LocalSourceCatalogStore) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    SourceCatalogBuilder(
        store=store,
        policy=FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((source,))


def test_local_source_catalog_store_rejects_symlink_root_without_mutation(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    root = tmp_path / "catalog-store"
    root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="non-symlink directory"):
        LocalSourceCatalogStore(root)

    assert_outside_sentinel_unchanged(outside)


def test_local_source_catalog_store_rejects_non_directory_root(tmp_path: Path) -> None:
    root = tmp_path / "catalog-store"
    root.write_bytes(b"not a directory")

    with pytest.raises(ValueError, match="non-symlink directory"):
        LocalSourceCatalogStore(root)

    assert root.read_bytes() == b"not a directory"


def test_local_source_catalog_readers_reject_a_replaced_store_root(tmp_path: Path) -> None:
    root = tmp_path / "catalog-store"
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    store, result = build(root, source)
    retained = tmp_path / "catalog-store-retained"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    root.rename(retained)
    root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ArtifactVerificationError, match="parent directory must be a real directory"):
        store.source_for(result.reference)
    with pytest.raises(ArtifactVerificationError, match="parent directory must be a real directory"):
        store.blob_source()

    assert_outside_sentinel_unchanged(outside)


def test_local_source_catalog_store_rejects_symlink_staging_root_without_mutation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog-store"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    (root / ".staging").symlink_to(outside, target_is_directory=True)
    store = LocalSourceCatalogStore(root, create=False)

    with pytest.raises(IntegrityError, match="staging root.*non-symlink directory"):
        with store.stage():
            raise AssertionError("unsafe staging root must fail before yielding")

    assert_outside_sentinel_unchanged(outside)


def test_local_source_catalog_store_uses_pinned_staging_root_during_cleanup(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog-store"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    store = LocalSourceCatalogStore(root)

    with pytest.raises(IntegrityError, match="staging root changed during use"):
        with store.stage():
            staging_root = root / ".staging"
            staging_root.rename(root / ".staging-retained")
            staging_root.symlink_to(outside, target_is_directory=True)

    assert_outside_sentinel_unchanged(outside)
    assert not tuple((root / ".staging-retained").iterdir())


def test_local_source_catalog_store_creates_session_under_pinned_staging_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "catalog-store"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    store = LocalSourceCatalogStore(root)
    actual_mkdir = source_catalog_store.os.mkdir
    swapped = False

    def swap_staging_before_session_mkdir(
        path: str | bytes,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal swapped
        if not swapped and isinstance(path, str) and path.startswith("catalog-"):
            staging_root = root / ".staging"
            staging_root.rename(root / ".staging-retained")
            staging_root.symlink_to(outside, target_is_directory=True)
            swapped = True
        actual_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(source_catalog_store.os, "mkdir", swap_staging_before_session_mkdir)

    with pytest.raises(IntegrityError, match="staging root changed during use"):
        with store.stage():
            pass

    assert swapped
    assert_outside_sentinel_unchanged(outside)
    assert not tuple((root / ".staging-retained").iterdir())


def test_local_source_catalog_store_refuses_same_name_session_replacement_cleanup(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog-store"
    store = LocalSourceCatalogStore(root)
    replacement: Path | None = None
    admitted: Path | None = None

    with pytest.raises(IntegrityError, match="staging session changed before cleanup"):
        with store.stage():
            staging_root = root / ".staging"
            replacement = next(staging_root.glob("catalog-*"))
            admitted = staging_root / f"{replacement.name}-admitted"
            replacement.rename(admitted)
            replacement.mkdir()
            (replacement / "sentinel.txt").write_bytes(b"replacement must stay unchanged")

    assert replacement is not None and admitted is not None
    assert (replacement / "sentinel.txt").read_bytes() == b"replacement must stay unchanged"
    assert admitted.is_dir()


def test_local_source_catalog_store_refuses_same_name_tombstone_replacement_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "catalog-store"
    store = LocalSourceCatalogStore(root)
    actual_clear = source_catalog_store._clear_directory_contents_at
    replacement: Path | None = None
    retained: Path | None = None
    swapped = False

    def swap_tombstone_before_descriptor_relative_clear(
        directory: source_catalog_store._PinnedDirectory,
    ) -> None:
        nonlocal replacement, retained, swapped
        if not swapped:
            staging_root = root / ".staging"
            replacement = next(staging_root.glob(".cleanup-*"))
            retained = staging_root / f"{replacement.name}-admitted"
            replacement.rename(retained)
            replacement.mkdir()
            (replacement / "sentinel.txt").write_bytes(b"replacement must stay unchanged")
            swapped = True
        actual_clear(directory)

    monkeypatch.setattr(
        source_catalog_store,
        "_clear_directory_contents_at",
        swap_tombstone_before_descriptor_relative_clear,
    )

    with pytest.raises(IntegrityError, match="cleanup tombstone changed during use"):
        with store.stage():
            pass

    assert swapped and replacement is not None and retained is not None
    assert (replacement / "sentinel.txt").read_bytes() == b"replacement must stay unchanged"
    assert retained.is_dir()
    assert not tuple(retained.iterdir())


@pytest.mark.parametrize("relative", (Path(".blobs"), Path(".blobs/sha256")))
def test_local_source_catalog_store_rejects_symlink_blob_roots_without_mutation(
    tmp_path: Path,
    relative: Path,
) -> None:
    root = tmp_path / "catalog-store"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    attacked = root / relative
    attacked.parent.mkdir(parents=True, exist_ok=True)
    attacked.symlink_to(outside, target_is_directory=True)
    store = LocalSourceCatalogStore(root, create=False)

    with pytest.raises(IntegrityError, match="blob root.*non-symlink directory|SHA-256 root.*non-symlink directory"):
        build_with_store(store)

    assert_outside_sentinel_unchanged(outside)


def test_local_source_catalog_store_rejects_shared_sha_root_symlink_without_mutation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "catalog-store"
    shared = tmp_path / "shared-blobs"
    outside = tmp_path / "outside"
    root.mkdir()
    shared.mkdir()
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    (shared / "sha256").symlink_to(outside, target_is_directory=True)
    store = LocalSourceCatalogStore(root, create=False, shared_blob_root=shared)

    with pytest.raises(IntegrityError, match="shared source-catalog SHA-256 root.*non-symlink directory"):
        build_with_store(store)

    assert_outside_sentinel_unchanged(outside)


def test_local_source_catalog_store_creates_pending_blob_under_pinned_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "catalog-store"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    store = LocalSourceCatalogStore(root)
    actual_open = source_catalog_store.os.open
    swapped = False

    def swap_pending_before_file_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if (
            not swapped
            and isinstance(path, str)
            and path.startswith("blob-")
            and flags & source_catalog_store.os.O_CREAT
        ):
            pending = next((root / ".staging").glob("catalog-*/blobs/.pending"))
            pending.rename(pending.with_name(".pending-retained"))
            pending.symlink_to(outside, target_is_directory=True)
            swapped = True
        return actual_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(source_catalog_store.os, "open", swap_pending_before_file_open)

    with pytest.raises(IntegrityError, match="pending blob root changed during use"):
        build_with_store(store)

    assert swapped
    assert_outside_sentinel_unchanged(outside)


@pytest.mark.parametrize("destination_kind", ("local", "shared"))
def test_local_source_catalog_store_links_published_blobs_under_pinned_sha_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_kind: str,
) -> None:
    root = tmp_path / "catalog-store"
    shared = tmp_path / "shared-blobs"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    store = LocalSourceCatalogStore(
        root,
        shared_blob_root=shared if destination_kind == "shared" else None,
    )
    target = (root / ".blobs" if destination_kind == "local" else shared) / "sha256"
    retained = target.with_name("sha256-retained")
    actual_link = source_catalog_store.os.link
    swapped = False

    def swap_sha_before_link(
        source: str | bytes | Path,
        destination: str | bytes | Path,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        nonlocal swapped
        if not swapped and dst_dir_fd is not None and target.is_dir():
            named = target.lstat()
            opened = source_catalog_store.os.fstat(dst_dir_fd)
            if (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino):
                target.rename(retained)
                target.symlink_to(outside, target_is_directory=True)
                swapped = True
        actual_link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(source_catalog_store.os, "link", swap_sha_before_link)

    with pytest.raises(IntegrityError, match="published SHA-256 root changed during use"):
        build_with_store(store)

    assert swapped
    assert_outside_sentinel_unchanged(outside)
    assert not [path for path in root.iterdir() if not path.name.startswith(".")]


def test_local_source_catalog_store_links_shared_reuse_under_pinned_staging_sha_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "catalog-store"
    shared = tmp_path / "shared-blobs"
    outside = tmp_path / "outside"
    payload = b"shared source-catalog payload"
    blob_ref = sha256_digest(payload)
    shared_sha = shared / "sha256"
    shared_sha.mkdir(parents=True)
    (shared_sha / blob_ref.removeprefix("sha256:")).write_bytes(payload)
    outside.mkdir()
    (outside / "sentinel.txt").write_bytes(b"outside must stay unchanged")
    store = LocalSourceCatalogStore(root, shared_blob_root=shared)
    actual_link = source_catalog_store.os.link
    swapped = False

    def swap_staged_sha_before_link(
        source: str | bytes | Path,
        destination: str | bytes | Path,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        nonlocal swapped
        candidates = tuple((root / ".staging").glob("catalog-*/blobs/sha256"))
        if not swapped and dst_dir_fd is not None and candidates:
            target = candidates[0]
            named = target.lstat()
            opened = source_catalog_store.os.fstat(dst_dir_fd)
            if (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino):
                target.rename(target.with_name("sha256-retained"))
                target.symlink_to(outside, target_is_directory=True)
                swapped = True
        actual_link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(source_catalog_store.os, "link", swap_staged_sha_before_link)

    with pytest.raises(IntegrityError, match="staged SHA-256 root changed during use"):
        with store.stage() as staging:
            staging.put_blob(blob_ref, len(payload), (payload,))

    assert swapped
    assert_outside_sentinel_unchanged(outside)


def test_builds_and_streams_one_complete_normative_snapshot(tmp_path: Path) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    store, result = build(tmp_path, source)

    snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)

    assert snapshot.summary == result.summary
    assert snapshot.summary.logical_id == result.reference.catalog_id
    assert snapshot.summary.artifact_digest == result.reference.digest
    assert snapshot.summary.item_count == 1
    assert snapshot.summary.disposition_counts == {
        "selected": 1,
        "excluded": 0,
        "deleted": 0,
        "unavailable": 0,
        "failed": 0,
    }
    assert snapshot.summary.requested_universe_set_digest == requested_universe_set_digest(
        1,
        iter(("2026-00001",)),
    )
    assert snapshot.summary.selected_source_set_digest == selected_source_set_digest(
        1,
        iter((("2026-00001", "2026-00001"),)),
    )
    assert snapshot.summary.selection_policy["policyId"] == ("urn:docspec:catalog-policy:federal-register:1")
    assert snapshot.summary.partition_policy["bucketCount"] == 64
    assert snapshot.summary.join_coverage == ()
    assert set(snapshot.summary.diagnostic_digests) == {
        "normalizedFieldsDigest",
        "joinedFieldsDigest",
        "dispositionsDigest",
        "reasonsDigest",
        "interpretationsDigest",
        "renditionChoicesDigest",
    }
    item = next(snapshot.items)
    assert item.source_item_id == "2026-00001"
    assert item.disposition is CatalogDisposition.SELECTED
    assert item.normalized_metadata["regulationIdentifierNumbers"] == ("2060-AV12",)
    assert item.source_native_facts[0]["fields"]["document_number"] == "2026-00001"
    assert [value.media_type for value in item.candidate_renditions] == ["text/html"]
    expected_policy_digest = FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).policy_digest
    assert all(value["policyDigest"] == expected_policy_digest for value in item.interpretations)
    assert [value["interpretationKind"] for value in item.interpretations] == [
        "exact-join",
        "normalization",
        "rendition-preference",
        "sampling",
        "selection",
        "topic-recovery",
    ]
    assert interpretation_result(item, "exact-join") == {"joins": ()}
    assert interpretation_result(item, "sampling") == {
        "frameAdmitted": True,
        "partition": "all",
        "stratum": ("all",),
        "orderHash": None,
        "rank": None,
        "stratumSize": None,
        "allocationMethod": "all",
        "limit": None,
        "drawn": True,
    }
    fields = normalization_fields(item)
    assert list(fields) == [
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
    ]
    assert fields["lastUpdatedDate"]["outcome"] == "absent"
    assert fields["language"]["valueSource"] == "policy"
    assert interpretation_result(item, "selection")["decisions"] == (
        {
            "decisionId": "required-metadata",
            "outcome": "pass",
            "disposition": None,
            "reasonCode": None,
            "reason": None,
        },
        {
            "decisionId": "candidate-rendition",
            "outcome": "pass",
            "disposition": None,
            "reasonCode": None,
            "reason": None,
        },
    )
    processing = item.to_processing_item()
    assert processing.item_id == item.source_item_id
    assert processing.metadata["sourceCatalogRow"] == item.to_dict()
    with pytest.raises(StopIteration):
        next(snapshot.items)

    processing_snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)
    assert processing_snapshot.summary.disposition_counts == {
        "selected": 1,
        "excluded": 0,
        "deleted": 0,
        "unavailable": 0,
        "failed": 0,
    }
    assert [value.to_processing_item().item_id for value in processing_snapshot.items] == ["2026-00001"]


def test_identity_is_deterministic_and_one_row_change_moves_it(tmp_path: Path) -> None:
    initial = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    _, first = build(tmp_path / "first", initial)
    _, repeated = build(tmp_path / "repeated", initial)
    changed_record = record("2026-00001")
    changed_record["record"]["title"] = "Changed title"
    _, changed = build(
        tmp_path / "changed",
        FakeSource(
            replace(description(), source_state_digest="sha256:" + "d" * 64),
            (changed_record,),
            renditions("2026-00001"),
        ),
    )

    assert repeated.reference == first.reference
    assert changed.reference.catalog_id != first.reference.catalog_id
    assert changed.reference.digest != first.reference.digest


def test_multipart_successor_reuses_unchanged_blob_refs_and_writes_only_changed_partition(
    tmp_path: Path,
) -> None:
    identities_by_partition: dict[str, str] = {}
    for index in range(1, 100):
        identity = f"2026-{index:05d}"
        identities_by_partition.setdefault(source_catalog_artifact._partition_id(identity), identity)
        if len(identities_by_partition) == 3:
            break
    identities = tuple(sorted(identities_by_partition.values()))
    assert len(identities) == 3

    initial_source = FakeSource(
        description(),
        tuple(record(identity) for identity in identities),
        tuple(value for identity in identities for value in renditions(identity)),
    )
    store, initial = build(tmp_path, initial_source)
    initial_root = tmp_path / initial.reference.digest.removeprefix("sha256:")
    initial_receipt = json.loads((initial_root / "catalog-build-receipt.json").read_text())
    initial_partitions = {value["partitionId"]: value for value in initial_receipt["partitions"]}

    assert len(initial_partitions) == 3
    assert initial.summary.partitions == tuple(initial_partitions)
    assert initial_receipt["byteMeasurements"] == {
        "payloadBytesRead": sum(value["byteSize"] for value in initial_partitions.values()),
        "payloadBytesReused": 0,
        "payloadBytesWritten": sum(value["byteSize"] for value in initial_partitions.values()),
        "publicationBytesWritten": sum(path.stat().st_size for path in initial_root.rglob("*") if path.is_file()),
    }
    assert initial_receipt["joinCoverage"] == []
    for name in (
        "normalizedFieldsDigest",
        "joinedFieldsDigest",
        "dispositionsDigest",
        "reasonsDigest",
        "interpretationsDigest",
        "renditionChoicesDigest",
    ):
        assert initial_receipt[name].startswith("sha256:")

    changed_identity = identities[0]
    changed_record = record(changed_identity)
    changed_record["record"]["title"] = "Changed title"
    changed_records = tuple(
        changed_record if identity == changed_identity else record(identity) for identity in identities
    )
    changed_description = replace(
        description(),
        logical_id="urn:spicy:artifact:spicyregs-source-native-release:" + "d" * 64,
        artifact_digest="sha256:" + "d" * 64,
        source_state_digest="sha256:" + "e" * 64,
    )
    _, successor = build(
        tmp_path,
        FakeSource(
            changed_description,
            changed_records,
            tuple(value for identity in identities for value in renditions(identity)),
        ),
    )
    successor_root = tmp_path / successor.reference.digest.removeprefix("sha256:")
    successor_receipt = json.loads((successor_root / "catalog-build-receipt.json").read_text())
    successor_partitions = {value["partitionId"]: value for value in successor_receipt["partitions"]}
    changed_partition = source_catalog_artifact._partition_id(changed_identity)

    assert successor.reference.catalog_id != initial.reference.catalog_id
    assert successor_partitions[changed_partition]["blobRef"] != initial_partitions[changed_partition]["blobRef"]
    unchanged_partitions = set(initial_partitions) - {changed_partition}
    assert {partition_id: successor_partitions[partition_id]["blobRef"] for partition_id in unchanged_partitions} == {
        partition_id: initial_partitions[partition_id]["blobRef"] for partition_id in unchanged_partitions
    }
    assert successor_receipt["byteMeasurements"]["payloadBytesReused"] == sum(
        initial_partitions[partition_id]["byteSize"] for partition_id in unchanged_partitions
    )
    assert (
        successor_receipt["byteMeasurements"]["payloadBytesWritten"]
        == successor_partitions[changed_partition]["byteSize"]
    )
    located = tuple(
        SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(successor.reference).located_items
    )
    assert [value.item.source_item_id for value in located] == list(identities)
    assert {value.item.source_item_id: value.blob_ref for value in located} == {
        identity: successor_partitions[source_catalog_artifact._partition_id(identity)]["blobRef"]
        for identity in identities
    }
    assert [
        item.source_item_id
        for item in SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(successor.reference).items
    ] == list(identities)


def test_normalized_diagnostic_values_use_stable_repeated_value_indices() -> None:
    assert list(source_catalog_artifact._indexed_values(["EPA", "DOE"])) == [
        (0, "EPA"),
        (1, "DOE"),
    ]
    assert list(source_catalog_artifact._indexed_values([])) == [(0, [])]
    assert list(source_catalog_artifact._indexed_values("EPA")) == [(0, "EPA")]


def test_generic_builder_accepts_a_second_injected_policy_configuration_shape(tmp_path: Path) -> None:
    @dataclass(frozen=True)
    class AlternatePolicy:
        policy_id = "urn:docspec:test:catalog-policy:alternate"
        policy_version = "2.0.0"

        @property
        def configuration(self) -> Mapping[str, Any]:
            return {"mode": "alternate", "settings": {"preserveSourceOrder": True}}

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

        @property
        def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
            return FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).universe_inputs

        def iter_items(
            self,
            inputs: CatalogPolicyInputs,
            workspace: CatalogPolicyWorkspace,
        ) -> Iterator[SourceCatalogItem]:
            for item in FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).iter_items(inputs, workspace):
                value = item.to_dict()
                for interpretation in value["interpretations"]:
                    interpretation["policyId"] = self.policy_id
                    interpretation["policyVersion"] = self.policy_version
                    interpretation["policyDigest"] = self.policy_digest
                yield SourceCatalogItem.from_dict(value)

    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    store = LocalSourceCatalogStore(tmp_path)
    result = SourceCatalogBuilder(
        store=store,
        policy=AlternatePolicy(),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:alternate", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((source,))

    snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)
    assert snapshot.summary.item_count == 1
    assert next(snapshot.items).source_item_id == "2026-00001"


def test_join_coverage_refuses_unbounded_row_authored_identities(tmp_path: Path) -> None:
    @dataclass(frozen=True)
    class ExcessiveJoinPolicy:
        policy_id = "urn:docspec:test:catalog-policy:excessive-joins"
        policy_version = "1.0.0"

        @property
        def configuration(self) -> Mapping[str, Any]:
            return {"mode": "excessive-joins"}

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

        @property
        def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
            return FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).universe_inputs

        def iter_items(
            self,
            inputs: CatalogPolicyInputs,
            workspace: CatalogPolicyWorkspace,
        ) -> Iterator[SourceCatalogItem]:
            for item in FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).iter_items(inputs, workspace):
                value = item.to_dict()
                for interpretation in value["interpretations"]:
                    interpretation["policyId"] = self.policy_id
                    interpretation["policyVersion"] = self.policy_version
                    interpretation["policyDigest"] = self.policy_digest
                exact_join = next(
                    interpretation
                    for interpretation in value["interpretations"]
                    if interpretation["interpretationKind"] == "exact-join"
                )
                exact_join["result"]["joins"] = [
                    {
                        "joinId": f"join-{index:04d}",
                        "sourceField": "document_number",
                        "sourceValue": item.source_item_id,
                        "lookupScopeId": "federal-register-documents",
                        "outcome": "no-match",
                        "matchedSourceRecordId": None,
                    }
                    for index in range(SOURCE_CATALOG_MAX_JOIN_IDS + 1)
                ]
                yield SourceCatalogItem.from_dict(value)

    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    with pytest.raises(LimitExceededError, match="distinct-identity limit"):
        SourceCatalogBuilder(
            store=LocalSourceCatalogStore(tmp_path),
            policy=ExcessiveJoinPolicy(),
            request=SourceCatalogBuildRequest("urn:docspec:catalog:excessive-joins", producer()),
            workspace_factory=SqliteCatalogPolicyWorkspace,
        ).build((source,))

    assert_no_published_catalog(tmp_path)


def test_physical_rebuild_preserves_logical_identity_and_moves_artifact_evidence(
    tmp_path: Path,
) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    store, initial = build(tmp_path, source)
    _, rebuilt = build(tmp_path, source)

    assert rebuilt.reference.catalog_id == initial.reference.catalog_id
    assert rebuilt.reference.digest != initial.reference.digest

    initial_snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(initial.reference)
    rebuilt_snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(rebuilt.reference)
    assert initial_snapshot.summary.logical_id == rebuilt_snapshot.summary.logical_id
    assert initial_snapshot.summary.artifact_digest != rebuilt_snapshot.summary.artifact_digest
    assert tuple(initial_snapshot.items) == tuple(rebuilt_snapshot.items)

    with store.source_for(initial.reference).open("catalog-build-receipt.json") as stream:
        initial_receipt = json.load(stream)
    with store.source_for(rebuilt.reference).open("catalog-build-receipt.json") as stream:
        rebuilt_receipt = json.load(stream)

    assert initial_receipt["byteMeasurements"]["payloadBytesWritten"] > 0
    assert initial_receipt["byteMeasurements"]["payloadBytesReused"] == 0
    assert rebuilt_receipt["partitions"] == initial_receipt["partitions"]
    assert rebuilt_receipt["byteMeasurements"]["payloadBytesRead"] > 0
    assert rebuilt_receipt["byteMeasurements"]["payloadBytesWritten"] == 0
    assert rebuilt_receipt["byteMeasurements"]["payloadBytesReused"] > 0

    def chunks_must_not_be_consumed() -> Iterator[bytes]:
        raise AssertionError("verified CAS reuse must not rewrite payload bytes")
        yield b""  # pragma: no cover

    existing_partition = initial_receipt["partitions"][0]
    with store.stage() as staging:
        reused = staging.put_blob(
            existing_partition["blobRef"],
            existing_partition["byteSize"],
            chunks_must_not_be_consumed(),
        )
    assert reused.reused is True


def test_builder_verifies_existing_blob_before_reuse(tmp_path: Path) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    _, initial = build(tmp_path, source)
    initial_root = tmp_path / initial.reference.digest.removeprefix("sha256:")
    receipt = json.loads((initial_root / "catalog-build-receipt.json").read_text())
    blob_path = tmp_path / ".blobs" / "sha256" / receipt["partitions"][0]["blobRef"].removeprefix("sha256:")
    blob_path.write_bytes(blob_path.read_bytes() + b"tamper")

    with pytest.raises(IntegrityError, match="differs from its content identity"):
        build(tmp_path, source)

    assert [path.name for path in tmp_path.iterdir() if not path.name.startswith(".")] == [
        initial.reference.digest.removeprefix("sha256:")
    ]


def test_root_publish_failure_exposes_no_artifact_and_recovers_by_blob_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_publish = source_catalog_store._publish_directory_no_replace_at
    attempts = 0

    def fail_root_publication(*args: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise IntegrityError("injected root publication failure")
        actual_publish(*args)

    monkeypatch.setattr(
        source_catalog_store,
        "_publish_directory_no_replace_at",
        fail_root_publication,
    )
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))

    with pytest.raises(IntegrityError, match="injected root publication failure"):
        build(tmp_path, source)

    assert not [path for path in tmp_path.iterdir() if not path.name.startswith(".")]
    blob_paths = tuple((tmp_path / ".blobs" / "sha256").iterdir())
    assert len(blob_paths) == 1

    store, recovered = build(tmp_path, source)

    assert recovered.byte_measurements["payloadBytesWritten"] == 0
    assert recovered.byte_measurements["payloadBytesReused"] == blob_paths[0].stat().st_size
    assert (
        SourceCatalogArtifactReader(store, producer=producer()).verify_snapshot(recovered.reference)
        == recovered.summary
    )


def test_multi_source_rows_are_streamed_once_and_globally_merged(tmp_path: Path) -> None:
    class OnePassSource(FakeSource):
        records_opened = 0
        renditions_opened = 0

        def iter_records(self) -> Iterator[Mapping[str, Any]]:
            self.records_opened += 1
            assert self.records_opened == 1
            yield from self.records

        def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
            self.renditions_opened += 1
            assert self.renditions_opened == 1
            yield from self.renditions

    first = OnePassSource(
        description(),
        (record("2026-00001"), record("2026-00003")),
        (*renditions("2026-00001"), *renditions("2026-00003")),
    )
    second = OnePassSource(
        replace(
            description(),
            logical_id="urn:spicy:artifact:spicyregs-source-native-release:" + "d" * 64,
            artifact_digest="sha256:" + "d" * 64,
            source_state_digest="sha256:" + "e" * 64,
        ),
        (record("2026-00002"), record("2026-00004")),
        (*renditions("2026-00002"), *renditions("2026-00004")),
    )
    store = LocalSourceCatalogStore(tmp_path)
    result = SourceCatalogBuilder(
        store=store,
        policy=FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((first, second))

    items = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items
    assert [item.source_item_id for item in items] == [
        "2026-00001",
        "2026-00002",
        "2026-00003",
        "2026-00004",
    ]
    assert (first.records_opened, first.renditions_opened) == (1, 1)
    assert (second.records_opened, second.renditions_opened) == (1, 1)


def test_one_pass_facade_selects_separate_row_families_from_the_same_source_system(
    tmp_path: Path,
) -> None:
    class OnePassSource(FakeSource):
        records_opened = 0
        renditions_opened = 0

        def iter_records(self) -> Iterator[Mapping[str, Any]]:
            self.records_opened += 1
            assert self.records_opened == 1
            yield from self.records

        def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
            self.renditions_opened += 1
            assert self.renditions_opened == 1
            yield from self.renditions

    lookup_selector = SourceInputSelector(
        _FEDERAL_REGISTER_SOURCE,
        "v1",
        "federal-register-agencies",
        "federal-register-agency",
        "1.0",
    )
    lookup_record = {
        "sourceRecordId": "environmental-protection-agency",
        "scopeId": lookup_selector.scope_id,
        "schemaName": lookup_selector.schema_name,
        "schemaVersion": lookup_selector.schema_version,
        "schemaDigest": _SHA_C,
        "record": {"slug": "environmental-protection-agency"},
        "fieldDiagnostics": [],
    }
    universe_source = OnePassSource(
        description(),
        (record("2026-00001"),),
        renditions("2026-00001"),
    )
    lookup_source = OnePassSource(
        replace(
            description(),
            logical_id="urn:spicy:artifact:spicyregs-source-native-release:" + "d" * 64,
            artifact_digest="sha256:" + "d" * 64,
            source_state_digest="sha256:" + "e" * 64,
        ),
        (lookup_record,),
        (),
    )

    @dataclass(frozen=True)
    class LookupPolicy:
        delegate: FederalRegisterCatalogPolicy

        @property
        def policy_id(self) -> str:
            return self.delegate.policy_id

        @property
        def policy_version(self) -> str:
            return self.delegate.policy_version

        @property
        def configuration(self) -> Mapping[str, Any]:
            return self.delegate.configuration

        @property
        def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
            return self.delegate.universe_inputs

        def iter_items(
            self,
            inputs: CatalogPolicyInputs,
            workspace: CatalogPolicyWorkspace,
        ) -> Iterator[SourceCatalogItem]:
            assert [row.record["sourceRecordId"] for row in inputs.iter_lookup_rows(lookup_selector)] == [
                "environmental-protection-agency"
            ]
            yield from self.delegate.iter_items(inputs, workspace)

    store = LocalSourceCatalogStore(tmp_path)
    result = SourceCatalogBuilder(
        store=store,
        policy=LookupPolicy(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE)),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    ).build((universe_source, lookup_source))

    assert result.summary.item_count == 1
    assert (universe_source.records_opened, universe_source.renditions_opened) == (1, 1)
    assert (lookup_source.records_opened, lookup_source.renditions_opened) == (1, 1)


def test_duplicate_source_item_across_inputs_cannot_publish(tmp_path: Path) -> None:
    first = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    second = FakeSource(
        replace(
            description(),
            logical_id="urn:spicy:artifact:spicyregs-source-native-release:" + "d" * 64,
            artifact_digest="sha256:" + "d" * 64,
        ),
        (record("2026-00001"),),
        renditions("2026-00001"),
    )
    store = LocalSourceCatalogStore(tmp_path)
    builder = SourceCatalogBuilder(
        store=store,
        policy=FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    )

    with pytest.raises(IntegrityError, match="repeat a sourceRecordId"):
        builder.build((first, second))

    assert_no_published_catalog(tmp_path)


def test_policy_must_account_for_every_universe_row_before_publication(tmp_path: Path) -> None:
    @dataclass(frozen=True)
    class DroppingPolicy:
        delegate: FederalRegisterCatalogPolicy

        @property
        def policy_id(self) -> str:
            return self.delegate.policy_id

        @property
        def policy_version(self) -> str:
            return self.delegate.policy_version

        @property
        def configuration(self) -> Mapping[str, Any]:
            return self.delegate.configuration

        @property
        def universe_inputs(self) -> tuple[SourceInputSelector, ...]:
            return self.delegate.universe_inputs

        def iter_items(
            self,
            inputs: CatalogPolicyInputs,
            workspace: CatalogPolicyWorkspace,
        ) -> Iterator[SourceCatalogItem]:
            for index, item in enumerate(self.delegate.iter_items(inputs, workspace)):
                if index != 1:
                    yield item

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    builder = SourceCatalogBuilder(
        store=LocalSourceCatalogStore(tmp_path),
        policy=DroppingPolicy(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE)),
        request=SourceCatalogBuildRequest("urn:docspec:catalog:federal-register", producer()),
        workspace_factory=SqliteCatalogPolicyWorkspace,
    )

    with pytest.raises(IntegrityError, match="complete universe"):
        builder.build((source,))

    assert_no_published_catalog(tmp_path)


def test_source_stream_failure_cannot_publish_a_partial_catalog(tmp_path: Path) -> None:
    class FailingSource(FakeSource):
        def iter_records(self) -> Iterator[Mapping[str, Any]]:
            yield self.records[0]
            raise RuntimeError("source stream failed")

    source = FailingSource(
        description(),
        (record("2026-00001"), record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )

    with pytest.raises(RuntimeError, match="source stream failed"):
        build(tmp_path, source)

    assert_no_published_catalog(tmp_path)


def test_catalog_row_limit_fails_before_publication(tmp_path: Path) -> None:
    oversized = record("2026-00001")
    oversized["record"]["title"] = "x" * source_catalog_artifact.MAX_CATALOG_ROW_BYTES
    source = FakeSource(description(), (oversized,), renditions("2026-00001"))

    with pytest.raises(LimitExceededError, match="row exceeds"):
        build(tmp_path, source)

    assert_no_published_catalog(tmp_path)


def test_source_rendition_count_limit_fails_before_eager_record_allocation(
    tmp_path: Path,
) -> None:
    class ExcessiveRenditionSource(FakeSource):
        def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
            for index in range(source_catalog_artifact.MAX_SOURCE_RENDITIONS_PER_RECORD + 1):
                yield {
                    "sourceRecordId": "2026-00001",
                    "renditionId": f"2026-00001/{index:05d}",
                    "sourceField": "html_url",
                    "locator": f"https://example.test/{index}",
                    "mediaType": "text/html",
                    "expectedSha256": None,
                    "expectedByteSize": None,
                }

    source = ExcessiveRenditionSource(
        description(),
        (record("2026-00001"),),
        (),
    )
    with pytest.raises(LimitExceededError, match="rendition count"):
        build(tmp_path, source)

    assert_no_published_catalog(tmp_path)


def test_source_rendition_aggregate_byte_limit_fails_before_publication(
    tmp_path: Path,
) -> None:
    oversized = dict(renditions("2026-00001")[0])
    oversized["locator"] = "https://example.test/" + "x" * source_catalog_artifact.MAX_SOURCE_RENDITION_BYTES_PER_RECORD
    source = FakeSource(description(), (record("2026-00001"),), (oversized,))

    with pytest.raises(LimitExceededError, match="rendition bytes"):
        build(tmp_path, source)

    assert_no_published_catalog(tmp_path)


def test_concurrent_builders_publish_only_valid_immutable_physical_outcomes(
    tmp_path: Path,
) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(build, tmp_path, source) for _ in range(2)]
    results = []
    errors = []
    for future in futures:
        try:
            results.append(future.result())
        except IntegrityError as error:
            errors.append(error)

    assert len(results) in {1, 2}
    assert len(results) + len(errors) == 2
    assert len({result.reference.catalog_id for _, result in results}) == 1
    assert len({result.reference.digest for _, result in results}) == len(results)
    for store, result in results:
        summary = SourceCatalogArtifactReader(store, producer=producer()).verify_snapshot(result.reference)
        assert summary == result.summary
    if len(results) == 2:
        measurements = [result.byte_measurements for _, result in results]
        assert sorted(value["payloadBytesWritten"] for value in measurements)[0] == 0
        assert sorted(value["payloadBytesReused"] for value in measurements)[1] > 0


def test_producer_gate_recomputes_state_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_derivation = source_catalog_artifact._derive_catalog
    calls = 0

    def wrong_initial_state(*args: Any, **kwargs: Any) -> source_catalog_artifact._DerivedCatalog:
        nonlocal calls
        calls += 1
        derived = actual_derivation(*args, **kwargs)
        if calls == 1:
            derived = source_catalog_artifact._DerivedCatalog(
                "sha256:" + "f" * 64,
                derived.requested_universe_set_digest,
                derived.selected_source_set_digest,
                derived.disposition_counts,
                derived.reason_counts,
                derived.diagnostics,
            )
        return derived

    monkeypatch.setattr(source_catalog_artifact, "_derive_catalog", wrong_initial_state)
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))

    with pytest.raises(IntegrityError, match="catalogStateDigest"):
        build(tmp_path, source)

    assert calls == 2
    assert_no_published_catalog(tmp_path)


def test_malformed_rin_is_retained_but_not_normalized_and_does_not_abort_neighbor(tmp_path: Path) -> None:
    source = FakeSource(
        description(),
        (record("2026-00001", malformed_rin=True), record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    items = tuple(SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items)

    assert items[0].normalized_metadata["regulationIdentifierNumbers"] == ()
    assert items[0].source_native_facts[0]["fields"]["regulation_id_numbers"] == ("not a rin",)
    malformed = normalization_fields(items[0])["regulationIdentifierNumbers"]
    assert malformed["outcome"] == "unparseable"
    assert malformed["value"] == ()
    assert malformed["unparseableValues"] == ("not a rin",)
    assert items[1].normalized_metadata["regulationIdentifierNumbers"] == ("2060-AV12",)


def test_mixed_valid_and_malformed_metadata_values_are_reported_without_aborting(
    tmp_path: Path,
) -> None:
    mixed = record("2026-00001")
    mixed["record"]["docket_ids"] = ["EPA-HQ-2026-0001", 7, 7]
    source = FakeSource(description(), (mixed,), renditions("2026-00001"))
    store, result = build(tmp_path, source)
    item = next(SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items)

    field = normalization_fields(item)["docketIds"]
    assert item.disposition is CatalogDisposition.SELECTED
    assert item.normalized_metadata["docketIds"] == ("EPA-HQ-2026-0001",)
    assert field["outcome"] == "unparseable"
    assert field["value"] == ("EPA-HQ-2026-0001",)
    assert field["unparseableValues"] == (7,)


def test_missing_required_metadata_is_an_explicit_row_disposition(tmp_path: Path) -> None:
    source = FakeSource(description(), (record("2026-00001", agencies=False),), renditions("2026-00001"))
    store, result = build(tmp_path, source)
    snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)
    item = next(snapshot.items)

    assert snapshot.summary.item_count == 1
    assert snapshot.summary.disposition_counts["failed"] == 1
    assert item.disposition is CatalogDisposition.FAILED
    assert item.selection.reason_code == "source.normalized-field-missing"
    decisions = interpretation_result(item, "selection")["decisions"]
    assert len(decisions) == 1
    assert decisions[0]["decisionId"] == "required-metadata"
    assert decisions[0]["outcome"] == "fail"
    assert decisions[0]["disposition"] == "failed"


def test_missing_rendition_is_unavailable_without_affecting_a_neighbor(tmp_path: Path) -> None:
    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002")),
        renditions("2026-00002"),
    )
    store, result = build(tmp_path, source)
    snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)
    unavailable, selected = tuple(snapshot.items)

    assert snapshot.summary.disposition_counts["unavailable"] == 1
    assert snapshot.summary.disposition_counts["selected"] == 1
    assert unavailable.disposition is CatalogDisposition.UNAVAILABLE
    assert unavailable.selection.reason_code == "source.no-candidate-rendition"
    assert unavailable.candidate_renditions == ()
    decisions = interpretation_result(unavailable, "selection")["decisions"]
    assert [decision["decisionId"] for decision in decisions] == [
        "required-metadata",
        "candidate-rendition",
    ]
    assert [decision["outcome"] for decision in decisions] == ["pass", "fail"]
    assert decisions[-1]["disposition"] == "unavailable"
    assert selected.disposition is CatalogDisposition.SELECTED


def test_rendition_preference_records_every_offer_and_selects_the_first_family(
    tmp_path: Path,
) -> None:
    identity = "2026-00001"
    body = {
        "sourceRecordId": identity,
        "renditionId": f"{identity}/body-html",
        "sourceField": "body_html_url",
        "locator": f"https://www.federalregister.gov/d/{identity}/body",
        "mediaType": "text/html",
        "expectedSha256": None,
        "expectedByteSize": None,
    }
    source_record = record(identity)
    source_record["record"]["body_html_url"] = body["locator"]
    source = FakeSource(description(), (source_record,), (body, *renditions(identity)))
    store, result = build(tmp_path, source)
    item = next(SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items)

    assert [candidate.rendition_id for candidate in item.candidate_renditions] == [f"{identity}/body-html"]
    preference = interpretation_result(item, "rendition-preference")
    assert preference["orderedFamilyIds"] == (
        "body_html_url",
        "html_url",
        "pdf_url",
    )
    assert preference["selectedFamilyId"] == "body_html_url"
    assert [family["offeredRenditionIds"] for family in preference["families"]] == [
        (f"{identity}/body-html",),
        (f"{identity}/html",),
        (f"{identity}/pdf",),
    ]


def test_empty_topics_are_not_recovered_without_evidence_and_do_not_affect_a_neighbor(
    tmp_path: Path,
) -> None:
    empty = record("2026-00001")
    empty["record"]["topics"] = []
    source = FakeSource(
        description(),
        (empty, record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    empty_item, observed_item = tuple(
        SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items
    )

    assert empty_item.disposition is CatalogDisposition.SELECTED
    assert empty_item.source_observed_topics == ()
    assert interpretation_result(empty_item, "topic-recovery") == {
        "sourceField": "record.topics",
        "outcome": "not-recovered",
        "evidenceDigest": None,
        "observedTopicIds": (),
    }
    assert observed_item.disposition is CatalogDisposition.SELECTED
    assert interpretation_result(observed_item, "topic-recovery")["outcome"] == "observed"
    assert interpretation_result(observed_item, "topic-recovery")["observedTopicIds"] == ("air-quality",)


def test_accounts_for_an_observed_crawl_without_claiming_source_completeness(
    tmp_path: Path,
) -> None:
    observed = FakeSource(
        description(scope="observed-crawl"),
        (record("2026-00001"),),
        renditions("2026-00001"),
    )
    store, result = build(tmp_path / "observed", observed)
    snapshot = SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)

    assert snapshot.summary.item_count == 1
    assert [item.source_item_id for item in snapshot.items] == ["2026-00001"]


def test_refuses_unknown_boundary_fields(tmp_path: Path) -> None:
    unknown = record("2026-00001")
    unknown["surprise"] = True
    with pytest.raises(IntegrityError, match="invalid closed shape"):
        build(
            tmp_path / "unknown",
            FakeSource(description(), (unknown,), renditions("2026-00001")),
        )


def test_tampering_fails_before_a_snapshot_row_is_returned(tmp_path: Path) -> None:
    source = FakeSource(description(), (record("2026-00001"),), renditions("2026-00001"))
    store, result = build(tmp_path, source)
    artifact_root = tmp_path / result.reference.digest.removeprefix("sha256:")
    receipt = json.loads((artifact_root / "catalog-build-receipt.json").read_text())
    blob_ref = receipt["partitions"][0]["blobRef"]
    item_path = tmp_path / ".blobs" / "sha256" / blob_ref.removeprefix("sha256:")
    item_path.write_bytes(item_path.read_bytes() + b"{}\n")

    with pytest.raises(IntegrityError, match="source catalog artifact is invalid"):
        SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference)


def install_fake_source_native(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeReader:
        def __init__(
            self,
            source,
            *,
            blob_source: object,
            profile: object,
            accepted_verifier_implementation_ids: frozenset[str],
            expected_pin: ArtifactPin | None = None,
        ) -> None:
            assert source is not None
            assert isinstance(blob_source, LocalBlobSource)
            assert profile is fake_profile
            assert expected_pin is None
            assert accepted_verifier_implementation_ids == frozenset(
                _ACCEPTED_SOURCE_VERIFIERS
            )
            self.pin = ArtifactPin(description().logical_id, description().artifact_digest)
            self.source_state_scope = description().source_state_scope
            self.source_system_id = description().source_system_id
            self.source_system_version = description().source_system_version
            self.source_state_digest = description().source_state_digest
            self.source_native_schema_set_digest = description().source_native_schema_set_digest

        def iter_records(self):
            yield record("2026-00001")

        def iter_renditions(self):
            yield from renditions("2026-00001")

    fake_profile = object()

    # Shadow the producer package the adapter resolves first, so these tests
    # exercise the fake reader whether or not a real one is installed.
    package_name = "spicy_docs"
    module_name = package_name + ".source_native"
    profiles_module_name = package_name + ".source_native_profiles"
    package = ModuleType(package_name)
    package.__path__ = []  # type: ignore[attr-defined]
    module = ModuleType(module_name)
    profiles_module = ModuleType(profiles_module_name)
    module.SUPPORTED_PRODUCER_PRODUCTS = frozenset({"spicy-regs", "spicy-docs"})  # type: ignore[attr-defined]
    module.SourceNativeReleaseReader = FakeReader  # type: ignore[attr-defined]
    profiles_module.FEDERAL_REGISTER_PROFILE = fake_profile  # type: ignore[attr-defined]
    profiles_module.REGULATIONS_GOV_DOCUMENT_PROFILE = object()  # type: ignore[attr-defined]
    profiles_module.REGULATIONS_GOV_DOCKET_PROFILE = object()  # type: ignore[attr-defined]
    profiles_module.REGULATIONS_GOV_COMMENT_PROFILE = object()  # type: ignore[attr-defined]
    package.source_native = module  # type: ignore[attr-defined]
    package.source_native_profiles = profiles_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, package_name, package)
    monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setitem(sys.modules, profiles_module_name, profiles_module)


def test_spicyregs_adapter_pins_the_source_blob_root_across_streams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"original source bytes\n"
    blob_ref = sha256_digest(payload)
    source_root = tmp_path / "source"
    blob_root = tmp_path / "blobs"
    source_root.mkdir()
    blob = blob_root / "sha256" / blob_ref.removeprefix("sha256:")
    blob.parent.mkdir(parents=True)
    blob.write_bytes(payload)
    observed: list[bytes] = []

    class FakeReader:
        def __init__(
            self,
            source: object,
            *,
            blob_source: object,
            profile: object,
            accepted_verifier_implementation_ids: frozenset[str],
            expected_pin: ArtifactPin | None = None,
        ) -> None:
            del source, profile, accepted_verifier_implementation_ids
            assert expected_pin is None
            assert isinstance(blob_source, LocalBlobSource)
            self._blob_source = blob_source
            self.pin = ArtifactPin(description().logical_id, description().artifact_digest)
            self.source_state_scope = description().source_state_scope
            self.source_system_id = description().source_system_id
            self.source_system_version = description().source_system_version
            self.source_state_digest = description().source_state_digest
            self.source_native_schema_set_digest = description().source_native_schema_set_digest

        def iter_records(self):
            with self._blob_source.open(blob_ref) as stream:
                observed.append(stream.read())
            return iter(())

        def iter_renditions(self):
            return iter(())

    module_name = "spicy_docs.source_native"
    source_native = ModuleType(module_name)
    source_native.SUPPORTED_PRODUCER_PRODUCTS = frozenset({"spicy-regs", "spicy-docs"})  # type: ignore[attr-defined]
    source_native.SourceNativeReleaseReader = FakeReader  # type: ignore[attr-defined]
    monkeypatch.setattr(
        "docspec.adapters.spicyregs_source_native.import_module",
        lambda name: source_native
        if name == module_name
        else (_ for _ in ()).throw(ModuleNotFoundError(name, name=name)),
    )
    adapter = SpicyRegsSourceNativeAdapter.from_local(
        source_root,
        blob_root=blob_root,
        artifact_digest=description().artifact_digest,
        profile=object(),
        accepted_verifier_implementation_ids=frozenset({"urn:test:verifier"}),
    )

    retained = tmp_path / "retained-blobs"
    blob_root.rename(retained)
    replacement = blob_root / "sha256" / blob_ref.removeprefix("sha256:")
    replacement.parent.mkdir(parents=True)
    replacement.write_bytes(b"changed source bytes!\n")

    with pytest.raises(MemberSourceError, match="artifact root changed"):
        tuple(adapter.iter_records())
    assert observed == []
    assert replacement.read_bytes() == b"changed source bytes!\n"


def source_catalog_build_arguments(
    tmp_path: Path,
    *,
    destination: Path,
    receipt_path: Path,
    blob_store: Path | None = None,
) -> list[str]:
    source_root = tmp_path / "source-native"
    source_root.mkdir(exist_ok=True)
    source_blob_store = tmp_path / "source-native-blobs"
    source_blob_store.mkdir(exist_ok=True)
    policy_path = tmp_path / "catalog-policy.json"
    policy_path.write_bytes(
        canonical_json_file_bytes(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE).to_member())
    )
    implementation_id = "git+https://example.test/docspec@" + "1" * 40
    arguments = [
        "source-catalog",
        "build",
        "--source-native",
        str(source_root),
        "--source-native-artifact-digest",
        _SHA_A,
        "--source-native-blob-store",
        str(source_blob_store),
        "--source-native-profile",
        "federal-register",
        "--accepted-source-verifier-implementation-id",
        _ACCEPTED_SOURCE_VERIFIERS[1],
        "--accepted-source-verifier-implementation-id",
        _ACCEPTED_SOURCE_VERIFIERS[0],
        "--catalog-policy",
        str(policy_path),
        "--implementation-id",
        implementation_id,
        "--verifier-implementation-id",
        implementation_id,
        "--destination",
        str(destination),
        "--receipt",
        str(receipt_path),
    ]
    if blob_store is not None:
        arguments.extend(("--blob-store", str(blob_store)))
    return arguments


def test_cli_composes_the_optional_source_adapter_and_emits_a_verifiable_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    arguments = source_catalog_build_arguments(
        tmp_path,
        destination=destination,
        receipt_path=receipt_path,
    )
    implementation_id = "git+https://example.test/docspec@" + "1" * 40

    assert main(arguments) == 0
    output = json.loads(capfd.readouterr().out)
    assert output["verdict"] == "pass"
    assert output["itemCount"] == 1
    assert output["catalog"] == json.loads(receipt_path.read_text())["catalog"]
    assert output["byteMeasurements"]["payloadBytesRead"] > 0
    assert output["byteMeasurements"]["payloadBytesReused"] == 0
    assert output["byteMeasurements"]["payloadBytesWritten"] > 0
    assert output["byteMeasurements"]["publicationBytesWritten"] > 0
    assert output["blobStore"] is None
    assert output["acceptedSourceVerifierImplementationIds"] == list(
        _ACCEPTED_SOURCE_VERIFIERS
    )
    assert [value["profile"] for value in output["sourceNativeInputs"]] == [
        "federal-register"
    ]

    reference_path = tmp_path / "source-catalog-ref.json"
    reference_path.write_bytes(canonical_json_file_bytes(output["catalog"]))
    assert (
        main(
            [
                "source-catalog",
                "verify",
                "--root",
                str(destination),
                "--reference",
                str(reference_path),
                "--expected-command-receipt-id",
                output["receiptId"],
                "--implementation-id",
                implementation_id,
                "--verifier-implementation-id",
                implementation_id,
            ]
        )
        == 0
    )
    verification = json.loads(capfd.readouterr().out)
    assert verification["commandReceiptId"] == output["receiptId"]
    assert verification["logicalId"] == output["catalog"]["catalogId"]
    assert "itemMemberPath" not in verification
    assert verification["partitions"]
    assert verification["selectionPolicy"] == output["catalogPolicy"]
    assert verification["partitionPolicy"] == output["partitionPolicy"]
    assert verification["joinCoverage"] == output["joinCoverage"]
    assert verification["diagnosticDigests"] == output["diagnosticDigests"]


def _verify_source_catalog_arguments(
    destination: Path,
    reference_path: Path,
    expected_command_receipt_id: str,
) -> list[str]:
    implementation_id = "git+https://example.test/docspec@" + "1" * 40
    return [
        "source-catalog",
        "verify",
        "--root",
        str(destination),
        "--reference",
        str(reference_path),
        "--expected-command-receipt-id",
        expected_command_receipt_id,
        "--implementation-id",
        implementation_id,
        "--verifier-implementation-id",
        implementation_id,
    ]


def _rewrite_command_receipt(
    receipt_path: Path,
    receipt: dict[str, Any],
    *,
    recompute_id: bool,
) -> None:
    if recompute_id:
        content = {
            key: value
            for key, value in receipt.items()
            if key not in {"format", "formatVersion", "receiptId"}
        }
        receipt["receiptId"] = stable_urn(
            "source-catalog-build-command-receipt",
            content,
        )
    receipt_path.write_bytes(canonical_json_file_bytes(receipt))


@pytest.mark.parametrize("change", ["missing", "unknown-field"])
def test_cli_verify_requires_one_closed_build_command_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    change: str,
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    command_receipt = json.loads(capfd.readouterr().out)
    reference_path = tmp_path / "source-catalog-ref.json"
    reference_path.write_bytes(canonical_json_file_bytes(command_receipt["catalog"]))

    if change == "missing":
        receipt_path.unlink()
    else:
        command_receipt["unknown"] = True
        _rewrite_command_receipt(receipt_path, command_receipt, recompute_id=True)

    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                command_receipt["receiptId"],
            )
        )
        == 2
    )
    error = capfd.readouterr().err
    if change == "missing":
        assert "must be a regular, non-symlink file" in error
    else:
        assert "invalid closed shape" in error


@pytest.mark.parametrize(
    ("changed_fact", "expected_label"),
    [
        ("catalog-state", "catalogStateDigest"),
        ("source-logical-id", "sourceNativeInputs"),
        ("source-artifact-digest", "sourceNativeInputs"),
        ("byte-measurements", "byteMeasurements"),
    ],
)
def test_cli_verify_rejects_a_self_consistent_command_summary_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    changed_fact: str,
    expected_label: str,
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    command_receipt = json.loads(capfd.readouterr().out)
    reference_path = tmp_path / "source-catalog-ref.json"
    reference_path.write_bytes(canonical_json_file_bytes(command_receipt["catalog"]))
    if changed_fact == "catalog-state":
        command_receipt["catalogStateDigest"] = "sha256:" + "f" * 64
    elif changed_fact == "source-logical-id":
        command_receipt["sourceNativeInputs"][0]["logicalId"] = "urn:test:different-source"
    elif changed_fact == "source-artifact-digest":
        command_receipt["sourceNativeInputs"][0]["artifactDigest"] = "sha256:" + "f" * 64
    else:
        command_receipt["byteMeasurements"]["payloadBytesRead"] += 1
        command_receipt["byteMeasurements"]["payloadBytesWritten"] += 1
    _rewrite_command_receipt(receipt_path, command_receipt, recompute_id=True)

    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                command_receipt["receiptId"],
            )
        )
        == 2
    )
    assert f"{expected_label} differs from the admitted catalog" in capfd.readouterr().err


def test_cli_verify_requires_the_expected_command_receipt_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    command_receipt = json.loads(capfd.readouterr().out)
    expected_receipt_id = command_receipt["receiptId"]
    reference_path = tmp_path / "source-catalog-ref.json"
    reference_path.write_bytes(canonical_json_file_bytes(command_receipt["catalog"]))
    command_receipt["acceptedSourceVerifierImplementationIds"] = [
        "urn:test:different-source-verifier"
    ]
    _rewrite_command_receipt(receipt_path, command_receipt, recompute_id=True)

    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                expected_receipt_id,
            )
        )
        == 2
    )
    assert "differs from the expected receipt identity" in capfd.readouterr().err


def test_cli_verify_binds_the_selected_source_profile_to_the_receipt_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    command_receipt = json.loads(capfd.readouterr().out)
    reference_path = tmp_path / "source-catalog-ref.json"
    reference_path.write_bytes(canonical_json_file_bytes(command_receipt["catalog"]))
    command_receipt["sourceNativeInputs"][0]["profile"] = "regulations-gov-documents"
    _rewrite_command_receipt(receipt_path, command_receipt, recompute_id=False)

    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                command_receipt["receiptId"],
            )
        )
        == 2
    )
    assert "receiptId does not match its content" in capfd.readouterr().err

    command_receipt["sourceNativeInputs"][0]["profile"] = "unregistered"
    _rewrite_command_receipt(receipt_path, command_receipt, recompute_id=True)
    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                command_receipt["receiptId"],
            )
        )
        == 2
    )
    assert "unsupported source-native profile" in capfd.readouterr().err


@pytest.mark.parametrize("changed_pin", ["destination", "reference"])
def test_cli_verify_binds_the_command_receipt_to_the_explicit_admission_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    changed_pin: str,
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    command_receipt = json.loads(capfd.readouterr().out)
    reference = dict(command_receipt["catalog"])
    reference_path = tmp_path / "source-catalog-ref.json"
    if changed_pin == "destination":
        moved_destination = tmp_path / "moved-catalog-store"
        destination.rename(moved_destination)
        destination = moved_destination
    else:
        reference["catalogId"] = "urn:test:different-catalog"
    reference_path.write_bytes(canonical_json_file_bytes(reference))

    assert (
        main(
            _verify_source_catalog_arguments(
                destination,
                reference_path,
                command_receipt["receiptId"],
            )
        )
        == 2
    )
    error = capfd.readouterr().err
    if changed_pin == "destination":
        assert "destination differs from the explicit store root" in error
    else:
        assert "reference differs from the published build command receipt" in error


def test_cli_new_destinations_reuse_verified_shared_content_blobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destinations = (tmp_path / "catalog-store-a", tmp_path / "catalog-store-b")
    receipts = tuple(destination / "source-catalog-build-command-receipt.json" for destination in destinations)
    blob_store = tmp_path / "catalog-blob-store"
    outputs: list[Mapping[str, Any]] = []

    for destination, receipt in zip(destinations, receipts, strict=True):
        assert (
            main(
                source_catalog_build_arguments(
                    tmp_path,
                    destination=destination,
                    receipt_path=receipt,
                    blob_store=blob_store,
                )
            )
            == 0
        )
        outputs.append(json.loads(capfd.readouterr().out))

    initial, rebuilt = outputs
    assert rebuilt["catalog"]["catalogId"] == initial["catalog"]["catalogId"]
    assert rebuilt["catalog"]["digest"] != initial["catalog"]["digest"]
    assert initial["byteMeasurements"]["payloadBytesWritten"] > 0
    assert initial["byteMeasurements"]["payloadBytesReused"] == 0
    assert rebuilt["byteMeasurements"]["payloadBytesWritten"] == 0
    assert rebuilt["byteMeasurements"]["payloadBytesReused"] > 0
    assert rebuilt["blobStore"] == {
        "path": blob_store.resolve().as_posix(),
        "retention": "verified-content-addressed-blobs-retained-for-reuse",
        "accountingStatus": "complete",
        "payloadBytesWritten": 0,
        "payloadBytesReused": rebuilt["byteMeasurements"]["payloadBytesReused"],
    }

    build_receipts = [
        json.loads(
            (
                destination / output["catalog"]["digest"].removeprefix("sha256:") / "catalog-build-receipt.json"
            ).read_text()
        )
        for destination, output in zip(destinations, outputs, strict=True)
    ]
    assert build_receipts[1]["partitions"] == build_receipts[0]["partitions"]


def test_cli_ignores_a_crash_stale_legacy_publish_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    legacy_lock = tmp_path / ".catalog-store.publish.lock"
    legacy_lock.write_text("abandoned-owner", encoding="utf-8")

    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )

    output = json.loads(capfd.readouterr().out)
    assert output == json.loads(receipt_path.read_text(encoding="utf-8"))
    assert legacy_lock.read_text(encoding="utf-8") == "abandoned-owner"


def test_cli_receipt_write_failure_leaves_no_published_artifact_or_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    import docspec.source_catalog_cli as source_catalog_cli

    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    blob_store = tmp_path / "catalog-blob-store"
    arguments = source_catalog_build_arguments(
        tmp_path,
        destination=destination,
        receipt_path=receipt_path,
        blob_store=blob_store,
    )

    actual_write_file = source_catalog_cli.LocalSourceCatalogPublication.write_file

    def fail_success_receipt_write(
        publication: object,
        name: str,
        payload: bytes,
    ) -> None:
        if name == "source-catalog-build-command-receipt.json":
            assert not destination.exists()
            raise OSError("injected receipt write failure")
        actual_write_file(publication, name, payload)  # type: ignore[arg-type]

    monkeypatch.setattr(
        source_catalog_cli.LocalSourceCatalogPublication,
        "write_file",
        fail_success_receipt_write,
    )

    assert main(arguments) == 2
    assert "injected receipt write failure" in capfd.readouterr().err
    assert not destination.exists()
    assert not receipt_path.exists()
    assert blob_store.is_dir()
    assert not tuple(tmp_path.glob(".catalog-store.*"))


@pytest.mark.parametrize("receipt_location", ["outside", "wrong-member"])
def test_cli_requires_the_atomic_receipt_member_without_poisoning_either_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    receipt_location: str,
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog"
    receipt_path = tmp_path / "receipt.json" if receipt_location == "outside" else destination / "wrong-name.json"

    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 2
    )

    assert "must be the atomic artifact member" in capfd.readouterr().err
    assert not destination.exists()
    assert not receipt_path.exists()


def test_cli_rejects_source_native_containment_and_accepts_a_separate_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    source_root = tmp_path / "source-native"

    for destination in (source_root / "catalog-store", tmp_path):
        receipt_path = destination / "source-catalog-build-command-receipt.json"
        assert (
            main(
                source_catalog_build_arguments(
                    tmp_path,
                    destination=destination,
                    receipt_path=receipt_path,
                )
            )
            == 2
        )
        assert (
            "artifact and source-native input paths must not contain one another"
            in capfd.readouterr().err
        )
        assert not receipt_path.exists()

    destination = tmp_path / "catalog-store"
    receipt_path = destination / "source-catalog-build-command-receipt.json"
    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
            )
        )
        == 0
    )
    output = json.loads(capfd.readouterr().out)
    assert output["destination"] == destination.resolve().as_posix()
    assert receipt_path.is_file()


def test_cli_rejects_blob_store_containment_and_receipts_explicit_retention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    install_fake_source_native(monkeypatch)
    destination = tmp_path / "catalog-store"
    blob_store = destination / "blobs"
    receipt_path = destination / "source-catalog-build-command-receipt.json"

    assert (
        main(
            source_catalog_build_arguments(
                tmp_path,
                destination=destination,
                receipt_path=receipt_path,
                blob_store=blob_store,
            )
        )
        == 2
    )

    assert "must not contain one another" in capfd.readouterr().err
    assert not destination.exists()
    assert not receipt_path.exists()


def test_cli_concurrent_publishers_leave_one_artifact_and_one_success_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import docspec.source_catalog_cli as source_catalog_cli

    install_fake_source_native(monkeypatch)
    monkeypatch.setattr(source_catalog_cli, "_emit", lambda *_args, **_kwargs: None)
    destinations = (tmp_path / "catalog-store", tmp_path / "catalog-store")
    receipt_paths = tuple(destination / "source-catalog-build-command-receipt.json" for destination in destinations)
    argument_sets = tuple(
        source_catalog_build_arguments(
            tmp_path,
            destination=destination,
            receipt_path=receipt_path,
        )
        for destination, receipt_path in zip(destinations, receipt_paths, strict=True)
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(main, argument_sets))

    assert sorted(results) == [0, 2]
    existing_destinations = {path for path in destinations if path.exists()}
    assert len(existing_destinations) == 1
    receipts = [json.loads(path.read_text()) for path in set(receipt_paths) if path.exists()]
    successful_receipts = [value for value in receipts if value["verdict"] == "pass"]
    assert len(successful_receipts) == 1
    receipt = successful_receipts[0]
    assert receipt["verdict"] == "pass"
    published_destination = next(iter(existing_destinations))
    assert receipt["destination"] == published_destination.resolve().as_posix()
    reference = SourceCatalogRef.from_dict(receipt["catalog"])
    summary = SourceCatalogArtifactReader(
        LocalSourceCatalogStore(published_destination, create=False),
        producer=producer(),
    ).verify_snapshot(reference)
    assert summary.item_count == 1
    assert not tuple(tmp_path.glob(".catalog-store.*"))


def test_the_incremental_framer_equals_rulespec_batch_framing_byte_for_byte() -> None:
    """The one-pass derivation only holds if the incremental hasher IS the protocol.

    ``_FramedSectionHasher`` re-states ``framed_section_digest``'s byte layout so
    ten digests can share one pass over the rows. This pins the two functions to
    each other across shapes: empty sections, one record, many records, nested
    values, non-ASCII text, and empty payload objects.
    """

    from rulespec_artifacts import FramedSection, framed_section_digest

    cases: list[tuple[str, str, list[dict[str, object]]]] = [
        ("docspec-test-domain/1", "records", []),
        ("docspec-test-domain/1", "records", [{"a": 1}]),
        ("docspec-test-domain/2", "members", [{"k": v, "n": [v, {"d": v}]} for v in range(50)]),
        ("docspec-test-domain/3", "rows", [{"text": "naïve — ünïcode ✓"}, {}]),
    ]
    for domain, name, records in cases:
        expected = framed_section_digest(domain, (FramedSection(name, len(records), iter(records)),))
        hasher = source_catalog_artifact._FramedSectionHasher(domain, name, len(records))
        for record in records:
            hasher.add(record)
        assert hasher.digest() == expected

    over = source_catalog_artifact._FramedSectionHasher("docspec-test-domain/1", "records", 1)
    over.add({"a": 1})
    with pytest.raises(IntegrityError, match="exceeds its declared count"):
        over.add({"a": 2})
    under = source_catalog_artifact._FramedSectionHasher("docspec-test-domain/1", "records", 2)
    under.add({"a": 1})
    with pytest.raises(IntegrityError, match="declared 2 records but yielded 1"):
        under.digest()


def test_a_stored_catalog_row_is_byte_identical_to_its_reserialized_item(tmp_path: Path) -> None:
    """The state digest frames raw row bytes; this is the identity that permits it.

    Every staged row must satisfy raw == canonical(to_dict(from_dict(parse(raw)))),
    or framing raw bytes would diverge from framing re-serialized items. Proven
    here on a real built catalog rather than assumed.
    """

    from rulespec_artifacts import canonical_json_bytes, parse_canonical_json

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002"), record("2026-00003")),
        (*renditions("2026-00001"), *renditions("2026-00002"), *renditions("2026-00003")),
    )
    store, result = build(tmp_path, source)
    verifier_reader = SourceCatalogArtifactReader(store, producer=producer())
    summary = verifier_reader.verify_snapshot(result.reference)
    checked = 0
    snapshot = verifier_reader.open_snapshot(result.reference)
    for item in snapshot.items:
        raw = canonical_json_bytes(item.to_dict())
        parsed = parse_canonical_json(raw, path="roundtrip")
        assert canonical_json_bytes(SourceCatalogItem.from_dict(parsed).to_dict()) == raw
        checked += 1
    assert checked == summary.item_count == 3


def test_the_compiled_validator_and_the_authority_agree_on_real_and_mutated_rows(
    tmp_path: Path,
) -> None:
    """The fast validator may only short-circuit acceptance, never decide refusal.

    Pins jsonschema-rs to python-jsonschema on this schema: every row of a real
    built catalog, plus systematic mutations of one (each required key dropped,
    each top-level field type-flipped, an unknown key added), must get the same
    accept/reject verdict from both engines -- and the gate's own error() must
    raise exactly when the authority rejects, with the authority's message.
    """

    gate = source_catalog_artifact._ITEM_VALIDATOR
    assert gate._fast is not None, "compiled validator failed to build for the item schema"
    authority = gate._authority

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002", malformed_rin=True)),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    reader.verify_snapshot(result.reference)
    rows = [item.to_dict() for item in reader.open_snapshot(result.reference).items]
    assert rows

    def verdicts(value: object) -> tuple[bool, bool, bool]:
        fast_ok = gate._fast.is_valid(value)
        authority_ok = not list(authority.iter_errors(value))
        try:
            gate.error(value, "differential row")
            gate_ok = True
        except IntegrityError:
            gate_ok = False
        return fast_ok, authority_ok, gate_ok

    mutants: list[object] = [dict(rows[0])]
    for key in list(rows[0]):
        dropped = dict(rows[0])
        del dropped[key]
        mutants.append(dropped)
        flipped = dict(rows[0])
        flipped[key] = 12345 if not isinstance(flipped[key], int) else "not-an-integer"
        mutants.append(flipped)
    unknown = dict(rows[0])
    unknown["unknownExtraKey"] = "x"
    mutants.append(unknown)

    for value in [*rows, *mutants]:
        fast_ok, authority_ok, gate_ok = verdicts(value)
        assert gate_ok == authority_ok, f"gate diverged from authority: {value!r:.120}"
        assert fast_ok == authority_ok, f"engines disagree (authority decides, but pin it): {value!r:.120}"


def test_the_parallel_derivation_is_byte_identical_to_the_serial_one(tmp_path: Path) -> None:
    """Workers may change wall time, never a digest.

    Builds a real multi-partition catalog, then derives serially and with two
    forced workers: every digest, count, and diagnostic must be identical --
    the parallel path spills the same payload bytes the serial helpers build
    and merges them in the same global order.
    """

    from rulespec_artifacts import LocalMemberSource, admit_artifact

    identities = [f"2026-0000{i}" for i in range(1, 8)]
    source = FakeSource(
        description(),
        tuple(record(identity) for identity in identities),
        tuple(r for identity in identities for r in renditions(identity)),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    summary = reader.verify_snapshot(result.reference)

    blob_source = store.blob_source()
    artifact_root = Path(store.root) / result.reference.digest.removeprefix("sha256:")
    verifier = source_catalog_artifact.SourceCatalogArtifactVerifier(producer(), blob_source)
    admit_artifact(
        LocalMemberSource(artifact_root),
        blob_source=blob_source,
        expected_pin=None,
        scratch_directory=tmp_path / "admit-scratch",
        semantic_verifier=verifier,
    )
    assert len(verifier.partitions) > 1, "test needs a multi-partition catalog"

    selected_count = summary.disposition_counts[
        source_catalog_artifact.CatalogDisposition.SELECTED.value
    ]
    serial = source_catalog_artifact._derive_catalog(
        blob_source,
        verifier.partitions,
        item_count=summary.item_count,
        selected_count=selected_count,
        workers=1,
    )
    parallel = source_catalog_artifact._derive_catalog(
        blob_source,
        verifier.partitions,
        item_count=summary.item_count,
        selected_count=selected_count,
        workers=2,
    )
    assert parallel == serial
    assert serial.catalog_state_digest == summary.catalog_state_digest


def test_the_automatic_worker_count_resolves_on_this_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The auto path must not depend on APIs newer than the pinned Python.

    Explicit worker counts pass straight through; below the threshold the
    derivation stays serial; at or above it the count resolves from the real
    interpreter's CPU API (this call is the regression: it once used a 3.13-only
    function that every explicit-workers test skipped past).
    """

    resolve = source_catalog_artifact._derive_worker_count
    assert resolve(10, 3) == 3
    assert resolve(10, None) == 1
    monkeypatch.setattr(source_catalog_artifact, "_PARALLEL_ROW_THRESHOLD", 5)
    automatic = resolve(10, None)
    assert 1 <= automatic <= source_catalog_artifact._MAX_DERIVE_WORKERS


def test_the_fast_canonical_writer_equals_the_rulespec_writer_on_its_guarded_domain(
    tmp_path: Path,
) -> None:
    """The guard is the proof: ASCII keys + no floats => identical bytes.

    Every projection record a real catalog produces must serialize identically
    through the fast writer and the Rulespec writer, and the guard must route
    out-of-domain values (non-ASCII keys, floats, non-BMP keys) to the Rulespec
    writer rather than risk divergence. Unicode VALUES stay in-domain and must
    still agree byte for byte.
    """

    from rulespec_artifacts import canonical_json_bytes

    fast = source_catalog_artifact._canonical_record_payload
    from docspec.adapters.framing import is_fast_canonical_safe as safe

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002", malformed_rin=True)),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    reader.verify_snapshot(result.reference)
    checked = 0
    for item in reader.open_snapshot(result.reference).items:
        item_dict = item.to_dict()
        records: list[Mapping[str, Any]] = [
            {"sourceItemId": item.source_item_id},
            {"sourceItemId": item.source_item_id, "disposition": item.disposition.value},
            {"sourceItemId": item.source_item_id, "reason": item.selection.reason},
            source_catalog_artifact._rendition_choice_record(item_dict),
            *source_catalog_artifact._normalized_field_records_for(item_dict),
            *source_catalog_artifact._joined_field_records_for(item_dict),
            *source_catalog_artifact._interpretation_records_for(item_dict),
        ]
        for value in records:
            assert fast(value) == canonical_json_bytes(value)
            checked += 1
    assert checked > 20

    unicode_value = {"text": "naïve — ünïcode ✓ \U0001f600", "nested": ["🚀", {"k": "é"}]}
    assert safe(unicode_value)
    assert fast(unicode_value) == canonical_json_bytes(unicode_value)

    assert not safe({"naïve-key": 1})
    assert not safe({"ok": [1, {"\U0001f600": 2}]})
    assert not safe({"ok": 1.5})
    non_bmp_keys = {"\U0001f600": 1, "\U0001f601": 2}
    assert fast({"wrap": non_bmp_keys}) == canonical_json_bytes({"wrap": non_bmp_keys})


def test_verify_snapshot_re_derives_digests_and_memoizes_per_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The consumer's verify must be the producer's gate, run again, once.

    A fresh reader's verify_snapshot performs the full independent derivation
    (spied), refuses a spec whose digest its derivation contradicts, and a
    second verify of the same digest returns the memoized verdict with no new
    derivation.
    """

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)

    calls = {"derive": 0}
    actual = source_catalog_artifact._derive_catalog

    def spy(*args: Any, **kwargs: Any) -> source_catalog_artifact._DerivedCatalog:
        calls["derive"] += 1
        return actual(*args, **kwargs)

    monkeypatch.setattr(source_catalog_artifact, "_derive_catalog", spy)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    summary = reader.verify_snapshot(result.reference)
    assert summary == result.summary
    assert calls["derive"] == 1
    assert reader.verify_snapshot(result.reference) == summary
    assert calls["derive"] == 1

    def lying(*args: Any, **kwargs: Any) -> source_catalog_artifact._DerivedCatalog:
        derived = actual(*args, **kwargs)
        return source_catalog_artifact._DerivedCatalog(
            "sha256:" + "e" * 64,
            derived.requested_universe_set_digest,
            derived.selected_source_set_digest,
            derived.disposition_counts,
            derived.reason_counts,
            derived.diagnostics,
        )

    monkeypatch.setattr(source_catalog_artifact, "_derive_catalog", lying)
    fresh = SourceCatalogArtifactReader(store, producer=producer())
    with pytest.raises(IntegrityError, match="catalogStateDigest"):
        fresh.verify_snapshot(result.reference)


def test_a_verified_reader_streams_items_without_repeating_the_row_proofs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After verify_snapshot memoizes a digest, open_snapshot's items stream skips
    the per-row schema and canonicality proofs it already ran; a reader that
    never verified still validates every row."""

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002")),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    seen: list[bool] = []
    actual = source_catalog_artifact._iter_located_catalog_rows

    def spy(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs.get("validate", True))
        return actual(*args, **kwargs)

    monkeypatch.setattr(source_catalog_artifact, "_iter_located_catalog_rows", spy)

    fresh = SourceCatalogArtifactReader(store, producer=producer())
    assert len(list(fresh.open_snapshot(result.reference).items)) == 2
    verified = SourceCatalogArtifactReader(store, producer=producer())
    verified.verify_snapshot(result.reference)
    assert len(list(verified.open_snapshot(result.reference).items)) == 2
    assert seen[0] is True, "an unverified reader must validate every row"
    assert seen[-1] is False, "a verified reader must not repeat the proofs"


def test_trusted_construction_equals_validated_construction_on_real_rows(tmp_path: Path) -> None:
    """Wrapping alone must yield the same items as full validation on admitted rows.

    A verified reader re-streams its catalog under trusted_json_input; every
    item it constructs must equal the item full validation constructs from the
    same bytes, and trusted construction must never leak past its context.
    """

    from docspec.domain.identity import trusted_json_input

    source = FakeSource(
        description(),
        (record("2026-00001"), record("2026-00002", malformed_rin=True)),
        (*renditions("2026-00001"), *renditions("2026-00002")),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    reader.verify_snapshot(result.reference)
    validated = list(SourceCatalogArtifactReader(store, producer=producer()).open_snapshot(result.reference).items)
    trusted = list(reader.open_snapshot(result.reference).items)
    assert trusted == validated
    assert [i.to_dict() for i in trusted] == [i.to_dict() for i in validated]
    with pytest.raises(ValueError):
        SourceCatalogItem.from_dict({"sourceItemId": 1.5})  # outside any trusted context
    with trusted_json_input():
        pass
    with pytest.raises((ValueError, TypeError)):
        SourceCatalogItem.from_dict({"sourceItemId": 1.5})



def test_derive_workers_receive_a_stream_not_the_partition_bytes(tmp_path: Path) -> None:
    """What crosses the process boundary must not grow with the partition.

    The parallel derivation once read each partition whole and pickled the
    bytes to a worker. Because the bucket count is fixed, a partition is always
    1/64th of the catalog, so that made peak memory a fixed fraction of the
    corpus -- measured at 2.78 GB for a real 7.61 GB catalog -- rather than a
    bound on it.

    The equivalence test above cannot see this: it passes byte-identically
    whether the payload is streamed or copied. So assert the property directly.
    Every task argument, with the descriptor handle scrubbed out, must be
    smaller than the smallest partition it stands for; that can only hold while
    the payload is absent from the arguments. The partition sizes and count are
    asserted too, so the test cannot pass by deriving nothing.
    """

    import pickle

    from rulespec_artifacts import LocalMemberSource, admit_artifact

    identities = [f"2026-0000{i}" for i in range(1, 8)]
    source = FakeSource(
        description(),
        tuple(record(identity) for identity in identities),
        tuple(r for identity in identities for r in renditions(identity)),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    summary = reader.verify_snapshot(result.reference)
    blob_source = store.blob_source()
    artifact_root = Path(store.root) / result.reference.digest.removeprefix("sha256:")
    verifier = source_catalog_artifact.SourceCatalogArtifactVerifier(producer(), blob_source)
    admit_artifact(
        LocalMemberSource(artifact_root),
        blob_source=blob_source,
        expected_pin=None,
        scratch_directory=tmp_path / "admit-scratch",
        semantic_verifier=verifier,
    )
    partitions = verifier.partitions
    assert len(partitions) > 1, "test needs a multi-partition catalog"
    partition_sizes = [value.member.byte_size or 0 for value in partitions]
    assert min(partition_sizes) > 512, "test needs partitions with real bytes in them"

    recorded: list[int] = []

    def scrubbed_size(arguments: tuple[Any, ...]) -> int:
        # Measure the data an argument carries, standing in for anything that
        # is not plain data. Pickling the live descriptor handle would register
        # a second descriptor with the resource sharer that nothing collects,
        # and its size says nothing about the payload either way. A payload
        # smuggled back in as bytes or text is still measured.
        return len(
            pickle.dumps(
                tuple(
                    value
                    if isinstance(value, (str, int, bool, bytes, bytearray))
                    else "<handle>"
                    for value in arguments
                )
            )
        )

    class RecordingPool:
        def __init__(self, pool: Any) -> None:
            self._pool = pool

        def __enter__(self) -> RecordingPool:
            self._pool.__enter__()
            return self

        def __exit__(self, *details: object) -> Any:
            return self._pool.__exit__(*details)

        def apply(self, function: Any, *args: Any, **kwargs: Any) -> Any:
            return self._pool.apply(function, *args, **kwargs)

        def apply_async(self, function: Any, args: tuple[Any, ...] = (), **kwargs: Any) -> Any:
            if args:  # the worker probe carries none; only real tasks are measured
                recorded.append(scrubbed_size(args[0]))
            return self._pool.apply_async(function, args, **kwargs)

    inner = source_catalog_artifact._derive_pool_context()

    class RecordingContext:
        def Pool(self, *args: Any, **kwargs: Any) -> RecordingPool:
            return RecordingPool(inner.Pool(*args, **kwargs))

    selected_count = summary.disposition_counts[
        source_catalog_artifact.CatalogDisposition.SELECTED.value
    ]
    serial = source_catalog_artifact._derive_catalog(
        blob_source,
        partitions,
        item_count=summary.item_count,
        selected_count=selected_count,
        workers=1,
    )
    original = source_catalog_artifact._derive_pool_context
    source_catalog_artifact._derive_pool_context = RecordingContext  # type: ignore[assignment]
    try:
        parallel = source_catalog_artifact._derive_catalog(
            blob_source,
            partitions,
            item_count=summary.item_count,
            selected_count=selected_count,
            workers=2,
        )
    finally:
        source_catalog_artifact._derive_pool_context = original  # type: ignore[assignment]

    assert parallel == serial, "streaming must not change a single derived value"
    assert len(recorded) == len(partitions), "every partition must be submitted once"
    assert max(recorded) < min(partition_sizes), (
        f"task arguments carry the partition payload: largest argument "
        f"{max(recorded)} B against smallest partition {min(partition_sizes)} B"
    )


def test_a_worker_pool_that_never_starts_falls_back_instead_of_hanging(tmp_path: Path) -> None:
    """The probe must time out, because a dead worker never raises.

    _derive_catalog_parallel guards itself with a no-op probe so that an
    interpreter whose __main__ cannot be re-imported -- frozen, embedded, or a
    script fed on stdin -- falls back to the serial derivation. The blocking
    pool.apply() could not do that: the child fails inside its own bootstrap,
    and Pool responds to a dead worker by starting another, forever. Measured,
    a derivation driven from a stdin script spawned workers until it was
    killed, so the guard never fired for the exact case it names.

    Assert the shape that makes the guard work: the probe goes through the
    timed asynchronous form, and a probe that does not answer falls back to a
    serial derivation whose result is identical.
    """

    import multiprocessing

    from rulespec_artifacts import LocalMemberSource, admit_artifact

    identities = [f"2026-0000{i}" for i in range(1, 8)]
    source = FakeSource(
        description(),
        tuple(record(identity) for identity in identities),
        tuple(r for identity in identities for r in renditions(identity)),
    )
    store, result = build(tmp_path, source)
    reader = SourceCatalogArtifactReader(store, producer=producer())
    summary = reader.verify_snapshot(result.reference)
    blob_source = store.blob_source()
    artifact_root = Path(store.root) / result.reference.digest.removeprefix("sha256:")
    verifier = source_catalog_artifact.SourceCatalogArtifactVerifier(producer(), blob_source)
    admit_artifact(
        LocalMemberSource(artifact_root),
        blob_source=blob_source,
        expected_pin=None,
        scratch_directory=tmp_path / "admit-scratch",
        semantic_verifier=verifier,
    )
    assert len(verifier.partitions) > 1, "test needs a multi-partition catalog"

    class NeverAnswers:
        def get(self, timeout: float | None = None) -> Any:
            assert timeout is not None, (
                "the probe must pass a timeout: a worker that dies during "
                "bootstrap makes Pool respawn it forever, so an untimed wait "
                "never returns and never raises"
            )
            raise multiprocessing.TimeoutError

    class DeadPool:
        def __enter__(self) -> DeadPool:
            return self

        def __exit__(self, *details: object) -> bool:
            return False

        def apply(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError(
                "the probe must not use the blocking apply(); see NeverAnswers"
            )

        def apply_async(self, *args: Any, **kwargs: Any) -> NeverAnswers:
            return NeverAnswers()

    class DeadContext:
        def Pool(self, *args: Any, **kwargs: Any) -> DeadPool:
            return DeadPool()

    selected_count = summary.disposition_counts[
        source_catalog_artifact.CatalogDisposition.SELECTED.value
    ]
    serial = source_catalog_artifact._derive_catalog(
        blob_source,
        verifier.partitions,
        item_count=summary.item_count,
        selected_count=selected_count,
        workers=1,
    )
    original = source_catalog_artifact._derive_pool_context
    source_catalog_artifact._derive_pool_context = DeadContext  # type: ignore[assignment]
    try:
        fell_back = source_catalog_artifact._derive_catalog(
            blob_source,
            verifier.partitions,
            item_count=summary.item_count,
            selected_count=selected_count,
            workers=2,
        )
    finally:
        source_catalog_artifact._derive_pool_context = original  # type: ignore[assignment]

    assert fell_back == serial
    assert fell_back.catalog_state_digest == summary.catalog_state_digest


def test_receipt_reason_counts_must_be_ordered_distinct_and_reconciled() -> None:
    """The schema closes each row; this is the cross-section arithmetic it cannot
    express: sealed order, no repeats, and every non-selected bucket accounted for.
    """

    reconcile = source_catalog_artifact._reconcile_reason_counts
    counts = {"selected": 3, "excluded": 0, "deleted": 1, "unavailable": 2, "failed": 0}
    good = [
        {"disposition": "deleted", "reasonCode": "source.withdrawn-after-publication", "count": 1},
        {"disposition": "unavailable", "reasonCode": "source.no-candidate-rendition", "count": 1},
        {"disposition": "unavailable", "reasonCode": "source.publisher-withheld.other", "count": 1},
    ]

    reconcile(good, counts)
    with pytest.raises(IntegrityError, match="ordered and distinct"):
        reconcile(list(reversed(good)), counts)
    with pytest.raises(IntegrityError, match="ordered and distinct"):
        reconcile([good[0], good[0]], {**counts, "deleted": 2, "unavailable": 0})
    with pytest.raises(IntegrityError, match="do not account for every non-selected row"):
        reconcile(good[:2], counts)
    with pytest.raises(IntegrityError, match="do not account for every non-selected row"):
        reconcile(good, {**counts, "excluded": 1})


def test_a_build_resumed_from_a_killed_workspace_publishes_the_identical_artifact(
    tmp_path: Path,
) -> None:
    """The acceptance rule for resume, written before the feature existed.

    A build that dies mid-stream and is resumed from its committed workspace
    publishes byte-for-byte the artifact a fresh build publishes, computes only
    the items after its last commit, and a workspace staged under another build
    identity is refused rather than reused. Two catalog-A builds died this way
    at 23 and 27.7 minutes with nothing readable left behind.
    """

    identities = tuple(f"2026-{index:05d}" for index in range(1, 8))

    def source() -> FakeSource:
        return FakeSource(
            description(),
            tuple(record(identity) for identity in identities),
            tuple(value for identity in identities for value in renditions(identity)),
        )

    def builder(
        root: Path,
        policy: object,
        workspace_factory: Any,
        *,
        build_producer: Producer | None = None,
    ) -> SourceCatalogBuilder:
        return SourceCatalogBuilder(
            store=LocalSourceCatalogStore(root),
            policy=policy,  # type: ignore[arg-type]
            request=SourceCatalogBuildRequest(
                "urn:docspec:catalog:federal-register", build_producer or producer()
            ),
            workspace_factory=workspace_factory,
            resume_batch_items=2,
        )

    def receipt_bytes(root: Path, reference: SourceCatalogRef) -> bytes:
        return (root / reference.digest.removeprefix("sha256:") / "catalog-build-receipt.json").read_bytes()

    fresh_root = tmp_path / "fresh"
    fresh = builder(
        fresh_root, FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE), SqliteCatalogPolicyWorkspace
    ).build((source(),))

    workspace_path = tmp_path / "resume" / "workspace.sqlite3"

    def durable() -> SqliteCatalogPolicyWorkspace:
        return SqliteCatalogPolicyWorkspace(path=workspace_path)

    resumed_root = tmp_path / "resumed"
    killed = KillAfter(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE), yields=5)
    with pytest.raises(IntegrityError, match="injected kill mid-stream"):
        builder(resumed_root, killed, durable).build((source(),))
    assert killed.computed == 5
    assert workspace_path.exists()

    counted = CountItems(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE))
    resumed = builder(resumed_root, counted, durable).build((source(),))

    assert resumed.reference == fresh.reference
    # Items 1-4 were committed in two batches of two; item 5 was staged in a
    # batch the kill rolled back, so the resumed run recomputes 5, 6 and 7.
    assert counted.computed == 3
    assert receipt_bytes(resumed_root, resumed.reference) == receipt_bytes(fresh_root, fresh.reference)

    other_path = tmp_path / "other" / "workspace.sqlite3"
    other_root = tmp_path / "other-store"
    with pytest.raises(IntegrityError, match="injected kill mid-stream"):
        builder(
            other_root,
            KillAfter(FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE), yields=5),
            lambda: SqliteCatalogPolicyWorkspace(path=other_path),
        ).build((source(),))
    other_implementation = "git+https://example.test/docspec@" + "2" * 40
    other_producer = Producer(
        "docspec",
        other_implementation,
        "urn:docspec:verifier:source-catalog",
        "1.0.0",
        other_implementation,
    )
    with pytest.raises(IntegrityError, match="staged by a different build"):
        builder(
            other_root,
            FederalRegisterCatalogPolicy(_FEDERAL_REGISTER_SOURCE),
            lambda: SqliteCatalogPolicyWorkspace(path=other_path),
            build_producer=other_producer,
        ).build((source(),))
