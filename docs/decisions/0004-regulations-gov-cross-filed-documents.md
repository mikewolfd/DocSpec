# Decision 0004: a cross-filed Regulations.gov document is one item with a recorded discard

- Date: 2026-09-03
- Status: **accepted.** Ruled by the product owner 2026-09-03. Not yet implemented.
- Raised-by: the full 671-input catalog-A build, which refuses and cannot complete
- Beside, not inside, `0003-federal-register-record-identity.md`: doc1 ruled that a
  different profile, a different mechanism and a different question deserve their
  own record rather than being folded into one titled for the Federal Register.

## The failure

`docspec source-catalog build` over the 671 catalog-A inputs aborts in about
ninety seconds with

> source-native inputs repeat a sourceRecordId for one policy selector

raised at `adapters/source_catalog_artifact.py:726`. Two records, out of
2,221,713 scanned across the 670 non-Federal-Register releases:

| sourceRecordId | appears in |
| --- | --- |
| `DHS_FRDOC_0001-2737` | `regs-documents-CISA`, `regs-documents-DHS` |
| `DHS_FRDOC_0001-2740` | `regs-documents-DHS`, `regs-documents-USCIS` |

Measured independently three times — by spicy9, by a blind auditor scanning all
2,221,713 records, and by a full scan of the 335 document releases here, which
found these are the **only** two documentIds appearing in more than one release
corpus-wide.

## This is not 0003's problem, and it is nearly its inverse

0003 is one identifier naming **two genuinely different documents**: the
publisher reused a Federal Register number, and the builder collapses to the
newest publication date, so the older document never becomes a record at all.

This is one document appearing as **two source records**, because the mirror
files it under two agency prefixes. `frDocNum` is identical within each pair —
`2025-23504` for -2737, `2025-23853` for -2740 — which establishes one document
rather than assuming it.

Opposite failure costs, in refb's framing: 0003 silently merges two things, this
silently splits one. Only 0003 is an identity question.

The loud refusal that surfaces both is **not** 0003's doing, and no ruling on
0003 could weaken it. It comes from the workspace's primary key over namespace
and ordered key, plus the explicit re-raise above.

## The owner is decided by measurement, not by judgement

RefSpec supplied a rule; it was re-run here over all 1,797,201 document records
in the 335 releases. Two tests, **four exceptions corpus-wide**, and both
blocking records are among them — each caught by exactly one test and neither by
both:

| test | exceptions | catches |
| --- | --- | --- |
| `documentId` starts with `docketId + "-"` | 3 | `DHS_FRDOC_0001-2740`, whose docket is `USCIS-2025-0040` → **USCIS is the cross-file** |
| `docketId` starts with `agencyId` | 1 | `DHS_FRDOC_0001-2737`, docket `DHS_FRDOC_0001`, agencyId `CISA` → **CISA is the cross-file** |

Both resolve to **DHS**. The docket-prefix tiebreak is itself a measured rule
rather than a fallback: it has exactly one exception in 1,797,201 records, and
that exception is the record it is being used to decide.

The rule must be stated as **prefix containment**, not "docket plus one trailing
segment". 1,756,713 sequences are one segment and **40,485 are two** —
`DOT-OST-1995-125-0050-0001` in docket `DOT-OST-1995-125`. The narrow phrasing
survives a 917-record sample and reports 40,488 false violations at full scale.

The other two exceptions to the first test — `EPA_FRDOC_0001-3113` in docket
`EPA-HQ-OAR-2002-0064` and `OSHA_FRDOC_0001-0003` in docket `OSHA-2007-0034` —
are **not** cross-files. Each appears in exactly one release. They are left
deliberately unclaimed: two specimens cannot settle whether a publisher may file
a Federal Register document into a subject docket, and RefSpec would rather it
sit visibly unruled than take a thin ruling.

## The copies are not redundant, which decides the remedy

The two filings were compared field by field — the only pairs in the corpus that
have been.

**`DHS_FRDOC_0001-2737`**, 8 of 84 leaf fields differ. Same docket, same title,
same `frVolNum` `90 FR 59851`. Differs on `postedDate` by **17 days** (DHS
2025-12-22, CISA 2026-01-08), on `commentStartDate` (CISA only), and on
`displayProperties` (a page count, DHS only).

**`DHS_FRDOC_0001-2740`**, 6 of 90 differ. Differs on `docketId`
(`DHS_FRDOC_0001` against **`USCIS-2025-0040`**, a real distinct docket), on
`frVolNum` (`90 FR 60864`, USCIS only), on `commentStartDate`, and on title
typography only — "To File" against "to File", and a hyphen-minus against an
en-dash in H-1B.

So a plain discard destroys real filing metadata. The proportionality argument —
two records in 2.2 million — survives, but it cannot rest on the copies being
redundant, because they are not.

**A caveat the owner ruled with.** The corpus-wide figure of 13,328 clean
duplicate groups was measured over four fields (title, posted date, Federal
Register number, page count) out of roughly eighty-five. Both pairs above agree
on all four and still differ elsewhere. That number means "agree on four
fields", not "are identical".

## What was ruled

**1. Collapse to the measured owner, and retain the non-owner filing as a
recorded observation.** Not a discard. This is 0003's own remedy applied to an
easier case: 0003 must justify discarding a distinct document, while this
discards a redundant *filing* of the same one. It needs no exception from 0003
and never did.

**2. Where two filings differ only by dash typography, the ASCII hyphen wins.**
An en-dash and a hyphen-minus are different search tokens, so the surviving
title decides whether an exact-match query for `H-1B` finds the rule, and
normalization cannot undo it afterwards.

### Selection is not substitution

Ruling 2 selects **which filing's title the item carries**. It does not rewrite
either filing's bytes. The retained observation keeps its own spelling verbatim.

Implemented as a normalization that mutates source text it would break the
exact-evidence rule, and a served match would stop resolving to its pinned
source. The general principle is recorded in 0003; this is its first instance.

The test that pins it is **not** that the item's title is the ASCII spelling —
a normalization implemented by mistake passes that. It is that **the retained
observation's title bytes are unchanged**.

## Cost: one schema version, and what else rides on it

Both schemas involved are `additionalProperties: false`:

- `schemas/source_catalog/1.0/source-item.schema.json` — the `selection` block
  is closed over `disposition`, `reason`, `reasonCode`. The retained observation
  needs a field.
- `schemas/source_catalog/1.0/catalog-build-receipt.schema.json` — closed, and
  `dispositionCounts` is itself closed over five integer buckets.

`catalogSchemaDigest` moves, so every existing catalog stops opening under the
new version. That cost is paid once, which is why a second change rides with it:

**`dispositionCounts` gains its reason-code labelling in the same move.** Today
the receipt reports `failed: 5678` with no reason anywhere, and a `reasonsDigest`
that pins content which is **not a member of the distribution** — catalog-B's
manifest declares 66 members and none of them is a reasons file. An auditor
holding the handover sees a bare count and a digest they cannot dereference,
while the word "failed" reads as breakage. The per-item reason is recoverable —
all 5,678 of catalog-B's carry `source.normalized-field-missing`, "Required
normalized catalog values are unusable: agencies" — but only by rescanning 7.6 GB.

spicy9 flagged that this cannot ship without moving `catalogSchemaDigest`.
Landing it beside the collapse costs nothing extra and is cheaper now than later.

## What this does not do

- It does not make the mirror agency identity-bearing. RefSpec's position is that
  the agency is already inside the identifier — `DHS_FRDOC_0001-2737` decomposes
  as docket `DHS_FRDOC_0001` plus sequence, and every agency holds exactly one
  `AGENCY_FRDOC_0001` docket — so keying on mirror-agency plus document would put
  the agency in the key twice, and the second copy would be the crawl's agency
  rather than the document's.
- It does not re-key the item. `SourceInputSelector` carries five fields and
  agency is not among them, so agency-scoped keys would add a dimension the
  selector does not have: a format break, not a tightening.
- It does not rule on `EPA_FRDOC_0001-3113` or `OSHA_FRDOC_0001-0003`.
- It does not touch the two Federal Register questions 0003 owns.
