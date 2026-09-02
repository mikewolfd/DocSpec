"""Producer resolution and refusal for the source-native adapter (D7).

Every test here injects fake producer modules. DocSpec declares neither
producer package as a dependency, so no test may depend on one being installed
in the developer's environment. A `None` entry in `sys.modules` is how the
import machinery itself reports an absent package.
"""

from __future__ import annotations

import sys
import types

import pytest

from docspec.adapters import spicyregs_source_native as adapter_module

_PREFERRED_READER_MODULE_NAME = "spicy_docs.source_native"
_FALLBACK_READER_MODULE_NAME = "spicy_regs.source_native"


def _reader(name: str, *, products: frozenset[str] | None) -> types.ModuleType:
    module = types.ModuleType(name)
    if products is not None:
        module.SUPPORTED_PRODUCER_PRODUCTS = products  # type: ignore[attr-defined]
    return module


def test_resolves_spicy_docs_before_spicy_regs_when_both_are_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both packages ship the module under one name; spicy-docs wins."""

    preferred = _reader(
        _PREFERRED_READER_MODULE_NAME, products=adapter_module.ACCEPTED_PRODUCER_PRODUCTS
    )
    monkeypatch.setitem(sys.modules, _PREFERRED_READER_MODULE_NAME, preferred)
    monkeypatch.setitem(
        sys.modules,
        _FALLBACK_READER_MODULE_NAME,
        _reader(_FALLBACK_READER_MODULE_NAME, products=None),
    )

    resolved = adapter_module._resolve_producer_module("source_native")

    assert resolved is preferred
    assert adapter_module._require_accepted_reader(resolved) is resolved


def test_resolves_the_profiles_module_and_one_named_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """The profile seam resolves through the same order without a producer-set check."""

    profiles = types.ModuleType("spicy_docs.source_native_profiles")
    profiles.FEDERAL_REGISTER_PROFILE = object()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "spicy_docs.source_native_profiles", profiles)

    assert adapter_module.spicyregs_source_profile("federal-register") is profiles.FEDERAL_REGISTER_PROFILE


def test_refuses_a_reader_whose_producer_set_is_a_strict_subset() -> None:
    """A reader declaring only {'spicy-regs'} is refused even though it resolved cleanly."""

    reader = _reader(_FALLBACK_READER_MODULE_NAME, products=frozenset({"spicy-regs"}))

    with pytest.raises(RuntimeError, match=_FALLBACK_READER_MODULE_NAME):
        adapter_module._require_accepted_reader(reader)


def test_refuses_a_reader_missing_the_producer_set_attribute() -> None:
    """A reader with no SUPPORTED_PRODUCER_PRODUCTS attribute is refused, not silently trusted."""

    reader = _reader(_FALLBACK_READER_MODULE_NAME, products=None)

    with pytest.raises(RuntimeError, match=_FALLBACK_READER_MODULE_NAME):
        adapter_module._require_accepted_reader(reader)


def test_falls_back_to_spicy_regs_when_spicy_docs_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent spicy-docs falls through to spicy-regs, in that order."""

    monkeypatch.delitem(sys.modules, _PREFERRED_READER_MODULE_NAME, raising=False)
    monkeypatch.setitem(sys.modules, "spicy_docs", None)
    fallback = _reader(
        _FALLBACK_READER_MODULE_NAME, products=adapter_module.ACCEPTED_PRODUCER_PRODUCTS
    )
    monkeypatch.setitem(sys.modules, _FALLBACK_READER_MODULE_NAME, fallback)

    assert adapter_module._resolve_producer_module("source_native") is fallback


def test_reports_no_producer_package_when_neither_is_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    for package in ("spicy_docs", "spicy_regs"):
        monkeypatch.delitem(sys.modules, f"{package}.source_native", raising=False)
        monkeypatch.setitem(sys.modules, package, None)

    with pytest.raises(RuntimeError, match="requires an installed spicy-docs or spicy-regs package"):
        adapter_module._resolve_producer_module("source_native")


def test_a_broken_spicy_docs_install_raises_instead_of_serving_the_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing transitive import inside spicy-docs must not look like an absent package."""

    fallback = _reader(
        _FALLBACK_READER_MODULE_NAME, products=adapter_module.ACCEPTED_PRODUCER_PRODUCTS
    )

    def import_module(name: str) -> types.ModuleType:
        if name == _PREFERRED_READER_MODULE_NAME:
            raise ModuleNotFoundError("No module named 'polars'", name="polars")
        if name == _FALLBACK_READER_MODULE_NAME:
            return fallback
        raise ModuleNotFoundError(f"No module named {name!r}", name=name)

    monkeypatch.setattr(adapter_module, "import_module", import_module)

    with pytest.raises(ModuleNotFoundError, match="polars"):
        adapter_module._resolve_producer_module("source_native")
