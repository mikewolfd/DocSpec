"""Shared catalog-policy helpers (`docspec.application.catalog_policy`)."""

from __future__ import annotations

import pytest

from docspec.application.catalog_policy import observed_topics
from docspec.errors import IntegrityError


def test_observed_topics_refuses_a_refspec_namespaced_identity() -> None:
    """A publisher-declared topic id may not impersonate a RefSpec concept (D6)."""

    with pytest.raises(IntegrityError, match="reserved concept namespace"):
        observed_topics(
            [{"id": "urn:ref:concept:1234", "label": "Air quality"}],
            scheme="regulations.gov",
            identity_fields=("id", "slug"),
            label_fields=("label", "name"),
        )
