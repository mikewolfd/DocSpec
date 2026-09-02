from __future__ import annotations

import ast
import configparser
import re
import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

from docspec import __version__
from docspec.domain.identity import canonical_json_file_bytes
from docspec.domain.source_catalog import source_catalog_schemas


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = ROOT / "src" / "docspec"
SOURCE_CATALOG_SCHEMA_ROOT = PRODUCTION_ROOT / "schemas" / "source_catalog" / "1.0"
PACKAGED_SCALE_SCHEMAS = {
    "docspec/schemas/scale_profile/2.0/scale-profile.schema.json": (
        ROOT / "conformance" / "scale-profile.schema.json"
    ),
    "docspec/schemas/scale_result/1.0/scale-result.schema.json": (
        ROOT / "conformance" / "scale-result.schema.json"
    ),
}
REPOSITORY_CODE_ROOTS = ("src", "tests", "tools")

# A path naming the sibling checkout: the name flanked by separators, or ending
# the string after one. `spicy_regs` on its own is a package name, not a path --
# ADAPTER_ONLY_SIBLING_PACKAGES above is exactly that -- and a remote URL such as
# `git@github.com:civictechdc/spicy-regs.git` names a repository to clone, not a
# directory on this machine, so neither form matches.
SIBLING_CHECKOUT_PATH = re.compile(r"(?:^|/)spicy[-_]regs(?:/|\Z)")
SIBLING_MODULE_PATH = re.compile(r"\bspicy_regs\.")
SIBLING_PACKAGE_ROOTS = frozenset({"spicy_regs", "spicyregs"})
OPTIONAL_SOURCE_ADAPTER = "src/docspec/adapters/spicyregs_source_native.py"
# The adapter's own test names the fallback reader module as test data: it
# injects a fake under that name to prove resolution order and refusal.
OPTIONAL_SOURCE_ADAPTER_TEST = "tests/test_spicyregs_source_native.py"
PINNED_INSTALLED_SOURCE_PROBE = "tests/test_source_catalog_installed_wheel.py"
OPTIONAL_SOURCE_MODULES = frozenset(
    {
        "spicy_" + "regs.source_native",
        "spicy_" + "regs.source_native_profiles",
    }
)
OPTIONAL_SOURCE_COMPOSITION_ROOTS = frozenset(
    {"src/docspec/cli.py", "src/docspec/source_catalog_cli.py"}
)
# This module names a URN namespace reserved for a registry DocSpec does not
# own. It is data the catalog refuses to mint, not an import of that product.
RESERVED_NAMESPACE_ROOTS = frozenset({"src/docspec/application/catalog_policy.py"})
# An absolute path whose first segment is a home-directory root belongs to one
# developer's machine, so it can only reach code this repository does not own.
HOME_DIRECTORY_ROOTS = frozenset({"Users", "home"})

# Every expression this repository passes as a subprocess working directory.
# `ROOT` and `REPO_ROOT` are this checkout; `root` is a parameter its callers
# bind to this checkout or to a temporary copy of it;
# `tmp_path` is the pytest temporary directory. A crossing returns as a new
# name here, which fails until someone adds it deliberately.
REPOSITORY_ROOTED_WORKING_DIRECTORIES = frozenset({"REPO_ROOT", "ROOT", "root", "tmp_path"})

ADAPTER_ONLY_SIBLING_PACKAGES = frozenset(
    {
        "refspec",
        "rulespec",
        "spicy_regs",
        "spicyregs",
        "spicysearch",
    }
)
ARCHIVED_PRODUCT_AREAS = frozenset(
    {
        "candidate_release",
        "corpora",
        "docpipeline",
        "document_file_pipeline",
        "document_release",
        "document_release_v3",
        "document_release_v3_cli",
        "document_release_v3_compact",
        "document_release_v3_diff",
        "document_release_v3_verify",
        "document_release_v3_writer",
        "enrichment",
        "evaluate_tag_quality",
        "evaluation_boundary",
        "ontology",
        "pipelines",
        "published",
        "retrieval",
        "rulespec_testbed",
        "source_profile_artifacts",
        "source_profile_artifacts_cli",
        "source_profiles",
        "sources",
        "transforms",
    }
)


def _production_files() -> list[Path]:
    return sorted(PRODUCTION_ROOT.rglob("*.py"))


def _absolute_imports(path: Path) -> set[str]:
    imports: set[str] = set()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.add(node.module)
    return imports


def _repository_code_files() -> list[Path]:
    return sorted(path for name in REPOSITORY_CODE_ROOTS for path in (ROOT / name).rglob("*.py"))


def _working_directory_expression(node: ast.expr) -> str:
    while isinstance(node, (ast.Attribute, ast.Subscript)):
        node = node.value
    while isinstance(node, ast.BinOp):
        node = node.left
    if isinstance(node, ast.Call):
        node = node.func
        while isinstance(node, ast.Attribute):
            node = node.value
    return node.id if isinstance(node, ast.Name) else ast.unparse(node)


def test_no_repository_code_names_a_sibling_checkout_or_an_outside_working_directory() -> None:
    """Repository code consumes pinned inputs rather than sibling worktrees."""

    files = _repository_code_files()
    assert files, "the repository must contain code under src/, tests/, and tools/"

    violations: list[str] = []
    working_directories: set[str] = set()
    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value
                segments = value.split("/")
                if len(segments) > 1 and not segments[0] and segments[1] in HOME_DIRECTORY_ROOTS:
                    violations.append(f"{relative}:{node.lineno} names a home directory: {value!r}")
                if "/" in value and SIBLING_CHECKOUT_PATH.search(value):
                    violations.append(f"{relative}:{node.lineno} names a SpicyRegs path: {value!r}")
                if SIBLING_MODULE_PATH.search(value) and not (
                    (
                        relative in (OPTIONAL_SOURCE_ADAPTER, OPTIONAL_SOURCE_ADAPTER_TEST)
                        and value in OPTIONAL_SOURCE_MODULES
                    )
                    or relative == PINNED_INSTALLED_SOURCE_PROBE
                ):
                    violations.append(f"{relative}:{node.lineno} names a spicy_regs module: {value!r}")
            elif isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg == "cwd":
                        expression = _working_directory_expression(keyword.value)
                        working_directories.add(expression)
                        if expression not in REPOSITORY_ROOTED_WORKING_DIRECTORIES:
                            violations.append(
                                f"{relative}:{node.lineno} runs a subprocess in {expression}"
                            )

    for imported in (name for path in files for name in _absolute_imports(path)):
        if imported.partition(".")[0] in SIBLING_PACKAGE_ROOTS:
            violations.append(f"a repository module imports {imported}")

    assert violations == []
    # Any allowance that stops being used is removed rather than left standing.
    assert working_directories == REPOSITORY_ROOTED_WORKING_DIRECTORIES


def test_project_declares_a_stdlib_core_and_one_command() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["version"] == __version__
    assert project["project"]["dependencies"] == [
        "jsonschema>=4.23,<5",
        "rulespec-artifacts==1.0.9",
    ]
    assert project["tool"]["uv"]["sources"]["rulespec-artifacts"] == {
        "path": "vendor/rulespec_artifacts-1.0.9-py3-none-any.whl"
    }
    assert set(project["tool"]["uv"]["sources"]) == {"rulespec-artifacts"}
    assert project["project"]["scripts"] == {"docspec": "docspec.entrypoint:main"}
    assert set(project["project"]["optional-dependencies"]) == {
        "dagster",
        "fast",
        "http",
        "pdf",
        "s3",
        "tokens",
    }

    extras = project["project"]["optional-dependencies"]
    assert any(requirement.startswith("httpx") for requirement in extras["http"])
    assert any(requirement.startswith("pymupdf") for requirement in extras["pdf"])
    assert any(requirement.startswith("pypdf") for requirement in extras["pdf"])
    assert any(requirement.startswith("boto3") for requirement in extras["s3"])
    assert any(requirement.startswith("dagster") for requirement in extras["dagster"])
    assert any(requirement.startswith("tiktoken") for requirement in extras["tokens"])
    assert "archive" not in project["tool"]["ruff"]["exclude"]
    assert project["tool"]["pytest"]["ini_options"]["testpaths"] == ["tests"]


def test_checked_in_source_catalog_schemas_equal_domain_generation() -> None:
    generated = source_catalog_schemas()

    assert {path.name for path in SOURCE_CATALOG_SCHEMA_ROOT.iterdir()} == set(generated)
    for name, schema in generated.items():
        assert (SOURCE_CATALOG_SCHEMA_ROOT / name).read_bytes() == canonical_json_file_bytes(schema)


def test_superseded_source_formats_are_absent_from_repository_code() -> None:
    import docspec.adapters as adapters
    import docspec.ports as ports

    assert not (PRODUCTION_ROOT / "adapters/source_catalog.py").exists()
    assert not (PRODUCTION_ROOT / "adapters/wire_source_release.py").exists()
    assert not (PRODUCTION_ROOT / "ports/source_release.py").exists()
    assert not (ROOT / "tests/legacy_wire_source_release.py").exists()
    assert not (ROOT / "tests/test_wire_source_release.py").exists()
    assert not any((ROOT / "fixtures/wire/source-catalog-release-v1").rglob("*"))
    assert not any((ROOT / "fixtures/conformance/core-v1").rglob("*"))
    assert not any((ROOT / "fixtures/qualification/fr-mirrulations-10k-v1").rglob("*"))
    for relative in (
        "tests/legacy_" + "source_catalog.py",
        "tests/legacy_" + "source_release.py",
        "tests/test_" + "source_catalog.py",
        "tests/test_fr_" + "mirrulations_qualification.py",
        "tools/fr_" + "mirrulations_support.py",
        "tools/fr_" + "mirrulations_qualification.py",
        "tools/export_" + "selection_ledger.py",
    ):
        assert not (ROOT / relative).exists()
    for name in (
        "JsonSchemaWireSourceReleaseGate",
        "LocalJsonlSourceCatalog",
        "LocalSourceReleaseReader",
        "LocalWireSourceReleaseReader",
        "SourceReleaseCatalogView",
    ):
        assert not hasattr(adapters, name)
    for name in ("SourceReleasePin", "SourceReleaseReader", "SourceReleaseSchemaGate"):
        assert not hasattr(ports, name)

    cli_source = (PRODUCTION_ROOT / "cli.py").read_text(encoding="utf-8")
    assert "LocalJsonlSourceCatalog" not in cli_source
    assert "wire_source_release" not in cli_source

    forbidden_tokens = (
        "tests.legacy_source_" + "catalog",
        "tests.legacy_source_" + "release",
        "LocalJsonl" + "SourceCatalog",
        "LocalSource" + "ReleaseReader",
        "SourceRelease" + "CatalogView",
        "SourceRelease" + "Pin",
    )
    violations: list[str] = []
    for root_name in REPOSITORY_CODE_ROOTS:
        for path in sorted((ROOT / root_name).rglob("*")):
            if not path.is_file() or path == Path(__file__):
                continue
            try:
                source = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for token in forbidden_tokens:
                if token in source:
                    violations.append(f"{path.relative_to(ROOT).as_posix()} contains {token!r}")
    assert violations == []


def test_production_imports_stay_inside_the_standalone_boundary() -> None:
    files = _production_files()
    assert files, "the installed DocSpec package must contain production modules"

    violations: list[str] = []
    for path in files:
        relative_parts = path.relative_to(PRODUCTION_ROOT).parts
        is_adapter = relative_parts[0] == "adapters"
        is_command_surface = relative_parts[0] in {"cli.py", "entrypoint.py", "source_catalog_cli.py"}
        for imported in _absolute_imports(path):
            root_name = imported.partition(".")[0]
            if not is_adapter and not is_command_surface and root_name in ADAPTER_ONLY_SIBLING_PACKAGES:
                violations.append(f"{path.relative_to(ROOT)} imports {imported}")
            if (
                not is_adapter
                and not is_command_surface
                and root_name != "docspec"
                and root_name not in sys.stdlib_module_names
            ):
                violations.append(f"{path.relative_to(ROOT)} imports non-stdlib core dependency {imported}")
            if imported.startswith("docspec."):
                area = imported.split(".", 2)[1]
                if area in ARCHIVED_PRODUCT_AREAS:
                    violations.append(f"{path.relative_to(ROOT)} imports archived area {imported}")

    assert violations == []


def test_non_docspec_product_areas_are_absent_from_production() -> None:
    top_level_names = {path.stem if path.is_file() else path.name for path in PRODUCTION_ROOT.iterdir()}
    assert top_level_names.isdisjoint(ARCHIVED_PRODUCT_AREAS)

    violations: list[str] = []
    for path in _production_files():
        relative = path.relative_to(ROOT).as_posix()
        if path.relative_to(PRODUCTION_ROOT).parts[0] == "adapters":
            continue
        source = path.read_text(encoding="utf-8").casefold()
        for word in ADAPTER_ONLY_SIBLING_PACKAGES:
            if relative in OPTIONAL_SOURCE_COMPOSITION_ROOTS and word == "spicyregs":
                continue
            if relative in RESERVED_NAMESPACE_ROOTS and word == "refspec":
                continue
            if re.search(rf"\b{re.escape(word.casefold())}\b", source):
                violations.append(f"{path.relative_to(ROOT)} names {word}")
    assert violations == []


def test_git_is_the_only_predecessor_code_record() -> None:
    assert not (ROOT / "archive/legacy-2026-08-05").exists()
    assert not (ROOT / "conformance/predecessor-code-fingerprints-v1.json").exists()
    assert not (ROOT / "ownership/modules.json").exists()
    assert not (ROOT / "tools/generate_archive_manifest.py").exists()
    assert not (ROOT / "tools/generate_ownership_manifest.py").exists()
    assert not (ROOT / "tools/predecessor_code_fingerprints.py").exists()


def test_core_import_and_cli_help_need_no_optional_dependency() -> None:
    import_result = subprocess.run(
        [sys.executable, "-I", "-c", "import docspec"],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert import_result.returncode == 0, import_result.stderr

    help_result = subprocess.run(
        [sys.executable, "-I", "-m", "docspec.entrypoint", "--help"],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert "DocSpec" in help_result.stdout or "docspec" in help_result.stdout


def test_dagster_adapter_has_one_lazy_runtime_resource_and_no_deployment_schema() -> None:
    import docspec.adapters as adapters

    assert adapters.DagsterRuntime.__name__ == "DagsterRuntime"
    for removed_name in ("DagsterAdapterProfile", "DagsterDeploymentConfig"):
        assert not hasattr(adapters, removed_name)
    assert not any((ROOT / "profiles/schedulers").glob("*.json"))
    isolated = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys; from docspec.adapters import DagsterRuntime; assert 'dagster' not in sys.modules",
        ],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert isolated.returncode == 0, isolated.stderr


def test_docspec_metadata_wheel_has_no_legacy_document_dependency(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv is not None, "the package release test requires uv"

    result = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    wheel = next(tmp_path.glob("docspec-*.whl"))

    with zipfile.ZipFile(wheel) as archive:
        members = set(archive.namelist())
        assert members
        assert all(name.startswith(("docspec/", "docspec-")) for name in members)
        assert not any("archive" in Path(name).parts for name in members)
        assert not any(Path(name).suffix in {".pyc", ".pyo"} for name in members)

        packaged_areas = {
            parts[1]
            for name in members
            if name.startswith("docspec/")
            for parts in [Path(name).parts]
            if len(parts) > 2
        }
        assert packaged_areas.isdisjoint(ARCHIVED_PRODUCT_AREAS)

        for name, schema in source_catalog_schemas().items():
            member = f"docspec/schemas/source_catalog/1.0/{name}"
            assert member in members
            assert archive.read(member) == canonical_json_file_bytes(schema)
        for member, checked_in_schema in PACKAGED_SCALE_SCHEMAS.items():
            assert member in members
            assert archive.read(member) == checked_in_schema.read_bytes()

        entry_points_name = next(name for name in members if name.endswith(".dist-info/entry_points.txt"))
        parser = configparser.ConfigParser()
        parser.read_string(archive.read(entry_points_name).decode("utf-8"))
        assert dict(parser["console_scripts"]) == {"docspec": "docspec.entrypoint:main"}

    environment = tmp_path / "wheel-environment"
    create_environment = subprocess.run(
        [uv, "venv", "--python", sys.executable, str(environment)],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert create_environment.returncode == 0, create_environment.stderr
    environment_python = environment / "bin" / "python"
    # Until package publication is separately authorized, these exact wheels
    # are the metadata consumer installation bundle. No sibling checkout,
    # legacy document extra, or editable path participates in this test.
    install = subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(environment_python),
            str(ROOT / "vendor" / "rulespec_artifacts-1.0.9-py3-none-any.whl"),
            str(wheel),
        ],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert install.returncode == 0, install.stderr
    import_result = subprocess.run(
        [
            environment_python,
            "-I",
            "-c",
            (
                "import docspec; "
                "import docspec.entrypoint; "
                "from docspec.adapters import DagsterRuntime; "
                "assert DagsterRuntime.__name__ == 'DagsterRuntime'; "
                "from docspec.source_catalog import requested_universe_set_digest; "
                "assert requested_universe_set_digest(0, ()).startswith('sha256:'); "
                "import importlib.util, sys; "
                    "import docspec.cli; "
                    "import docspec.adapters.platform_artifact; "
                "assert importlib.util.find_spec('rulespec_conformance') is None; "
                "assert importlib.util.find_spec('refspec') is None; "
                "assert importlib.util.find_spec('rdflib') is None; "
                "assert importlib.util.find_spec('dagster') is None"
            ),
        ],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )
    assert import_result.returncode == 0, import_result.stderr
    help_result = subprocess.run(
        [environment / "bin" / "docspec", "--help"],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert "DocSpec" in help_result.stdout or "docspec" in help_result.stdout
    source_catalog_help = subprocess.run(
        [environment / "bin" / "docspec", "source-catalog", "--help"],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )
    assert source_catalog_help.returncode == 0, source_catalog_help.stderr
    assert "source-catalog" in source_catalog_help.stdout
