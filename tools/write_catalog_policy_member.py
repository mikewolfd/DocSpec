"""Generate a sealed catalog-policy member from its free fields (A0.5).

`from_member` demands `member == policy.to_member()` byte-for-byte, so no
policy member can be hand-written. This builds the policy object from an input
JSON of its free fields, proves the canonical bytes round-trip back through
`from_member` unchanged -- the check `source-catalog build` applies at read
time -- and only then creates the output, never overwriting one.

The input's keys are the policy's own field names; its nested selector and
sample objects use the closed camelCase shapes `SourceInputSelector.from_dict`
and `RegulationsGovSamplePolicy.from_dict` already parse. `agency_names` is
that mapping inline, or a path to it relative to the input file. An omitted
optional key keeps the policy dataclass's own default.

    uv run python -m tools.write_catalog_policy_member \\
        --policy regulations-gov --input fields.json --output policy-member.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from docspec.application.federal_register_catalog import FederalRegisterCatalogPolicy
from docspec.application.regulations_gov_catalog import (
    RegulationsGovCatalogPolicy,
    RegulationsGovSamplePolicy,
)
from docspec.domain.identity import canonical_json_file_bytes
from docspec.ports.source_catalog import SourceInputSelector
from docspec.source_catalog_cli import _MAX_JSON_BYTES, _read_object

_POLICY_CHOICES = ("regulations-gov", "federal-register")


def _optional_selector(value: object) -> SourceInputSelector | None:
    return None if value is None else SourceInputSelector.from_dict(value)


def _agency_names(value: object, *, input_path: Path) -> dict[str, str]:
    if isinstance(value, str):
        mapping_path = Path(value)
        if not mapping_path.is_absolute():
            mapping_path = input_path.parent / mapping_path
        value = json.loads(mapping_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("agency_names must be a JSON object or a path to one")
    return dict(value)


def build_policy(policy_name: str, fields: dict[str, Any], *, input_path: Path) -> object:
    if policy_name == "regulations-gov":
        if fields.get("document_input") is None:
            raise ValueError("regulations-gov document_input is required and may not be null")
        sample = fields.get("sample")
        return RegulationsGovCatalogPolicy(
            document_input=SourceInputSelector.from_dict(fields["document_input"]),
            docket_input=_optional_selector(fields.get("docket_input")),
            federal_register_input=_optional_selector(fields.get("federal_register_input")),
            agency_names=_agency_names(fields["agency_names"], input_path=input_path),
            sample=None if sample is None else RegulationsGovSamplePolicy.from_dict(sample),
            max_selected_items=fields.get("max_selected_items"),
            comment_input=_optional_selector(fields.get("comment_input")),
            **{
                name: fields[name]
                for name in ("language", "source_url_template")
                if name in fields
            },
        )
    if policy_name == "federal-register":
        return FederalRegisterCatalogPolicy(fields["expected_source_system_id"])
    raise ValueError(f"unsupported --policy: {policy_name}")


def write_member(policy_name: str, input_path: Path, output_path: Path) -> bytes:
    fields = _read_object(input_path, label="catalog policy fields", canonical=False)
    policy = build_policy(policy_name, fields, input_path=input_path)
    member_bytes = canonical_json_file_bytes(policy.to_member())
    if len(member_bytes) > _MAX_JSON_BYTES:
        raise ValueError(f"the policy member exceeds the CLI's {_MAX_JSON_BYTES}-byte limit")

    round_tripped = type(policy).from_member(json.loads(member_bytes))
    if canonical_json_file_bytes(round_tripped.to_member()) != member_bytes:
        raise AssertionError("round-trip through from_member produced different bytes")

    # Every proof above is complete before anything is written. "x" is
    # O_CREAT|O_EXCL, which refuses an existing file and a symlink alike, so a
    # sealed policy member is never overwritten in place.
    with output_path.open("xb") as stream:
        stream.write(member_bytes)
    return member_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--policy", choices=_POLICY_CHOICES, required=True)
    parser.add_argument("--input", type=Path, required=True, help="JSON file of the policy's free fields")
    parser.add_argument("--output", type=Path, required=True, help="destination for the canonical policy member")
    args = parser.parse_args()

    member_bytes = write_member(args.policy, args.input, args.output)
    print(f"wrote {args.output} ({len(member_bytes)} bytes), round-trip verified")


if __name__ == "__main__":
    main()
