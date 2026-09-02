# DocSpec Bulk Content Processing Platform

## Implementation and conformance specification

Editor's Draft — 25 August 2026

## Status

This specification defines the work required to make DocSpec a standalone
platform for acquiring and processing large collections of files.

This Markdown file records intent. It does not prove implementation or
conformance. Current code and machine-generated evidence establish what exists.
Each requirement becomes complete only when its named executable check passes
against the exact code, configuration, inputs, and outputs under review.

## Abstract

DocSpec builds and owns one complete immutable `SourceCatalog` snapshot from
pinned source-native inputs. A successor snapshot contains complete current
logical state and reuses unchanged partition bytes through exact Rulespec
`blobRef` values. `DocumentCatalog` opens an optional prior
`DocumentRelease`, which is one immutable snapshot of DocSpec's corpus state. A
planner divides the requested changes into bounded
`DocumentStore` jobs and emits small, serializable task references. A local
runner or an established scheduler such as Dagster executes those tasks. Workers
acquire files, preserve exact bytes, extract representations, create segments,
run injected processors, and deliver bulk results directly to an injected sink.
The execution tool streams terminal job references back to DocSpec.
`DocumentCatalog` reconciles the complete verified job set and commits the next
`DocumentRelease`.

DocSpec supports two workloads:

1. a bounded initial backfill that may contain millions of files, images, pages,
   or segments; and
2. incremental updates that acquire and process only additions, changes,
   deletions, repairs, or outputs invalidated by a changed processor.

The initial backfill is a one-time operation. Routine updates MUST NOT rebuild,
redownload, or republish the complete collection.

```text
immutable source-native inputs -> SourceCatalog
                                      |
                         DocumentRelease N
                                      |
                                      v
        DocumentCatalog.open(N) -> Planner -> saved DocumentStore jobs
                                                |
                                      small task/reference stream
                                                |
                                                v
                          local runner / Dagster / Ray / queue
                                                |
                       terminal StoreRefs + direct sink delivery
                                                |
                                                v
                               verify + reconcile planned job set
                                                |
                                                v
                              DocumentCatalog.commit(N, run receipt)
                                                |
                                                v
                                      DocumentRelease N+1
```

DocSpec is the document-state and evidence kernel in this flow. It does not
reimplement a scheduler, queue, cache service, object store, table format, or
analytics engine. It defines the small messages, immutable identities, evidence,
idempotency rules, receipts, and publication checks that let maintained packages
perform those jobs safely.

## 1. Outcome

A conforming DocSpec deployment answers four questions.

### 1.1 What goes in?

- Immutable, versioned, digest-pinned source-native records.
- A sealed, complete DocSpec `SourceCatalog` snapshot.
- Stable source-item identifiers and file locators.
- An optional prior `DocumentRelease`.
- Extraction and segmentation policies.
- Zero or more injected processor definitions.
- A selected set of release-manifest, catalog, record, blob, job-persistence,
  and delivery profiles.
- Resource, retention, delivery, and publication policies.
- A sealed execution profile that selects a local runner or external execution
  tool and records its operational limits without changing document semantics.

### 1.2 What happens?

`DocumentCatalog` opens prior state. DocSpec plans and saves bounded
`DocumentStore` jobs. The selected execution tool consumes their small
references, while workers acquire exact bytes, derive representations, create
segments, execute processors, checkpoint verified stages, and deliver bulk
results directly to the selected sink. DocSpec accepts terminal references in
any order, rejects missing or conflicting results, reconciles the run, and
commits the next immutable catalog snapshot.

### 1.3 What comes out?

- Sealed `DocumentStore` job receipts.
- Content-addressed source and derived files when retention is requested.
- Partitioned record datasets for file, representation, segment, and processor
  records when the selected sink saves a dataset.
- A bounded result stream when the selected sink returns data.
- Complete dispositions for every selected source item.
- A `DocumentRelease` for every stateful run.
- Receipts that identify exact inputs, implementations, policies, counts,
  failures, resource use, and output digests.
- A bounded task and terminal-reference stream suitable for a scheduler, queue,
  or local caller.

### 1.4 How is it checked?

Independent verifiers check artifact membership, digests, schemas, evidence
coordinates, dispositions, incremental equivalence, recovery, and scale. A
successful command, clean working tree, or prose status statement does not
establish conformance.

## 2. Scope

### 2.1 Required capabilities

DocSpec MUST:

- define, build, verify, publish, and stream the normative `SourceCatalog` and
  `SourceItem` model from immutable source-native inputs;
- open corpus state through `DocumentCatalog` at an explicit `DocumentRelease`;
- divide catalog entries into bounded `DocumentStore` jobs;
- acquire whole files, including images, PDFs, XML, HTML, JSON, and text;
- preserve exact source bytes before interpretation;
- deduplicate identical bytes without losing logical source membership;
- derive one or more representations through replaceable extractors;
- create deterministic, source-grounded segments;
- execute optional injected processors against files, representations, segments,
  or earlier derived outputs;
- isolate processor meaning and dependencies from the DocSpec core;
- save large outputs as partitioned datasets, stream bounded results back to a
  caller, or do both;
- hand saved jobs to a replaceable execution tool as small references and accept
  terminal results in any order;
- select physical release-manifest, catalog, record, blob, job-persistence, and
  delivery formats through replaceable profiles;
- resume work without repeating verified tasks;
- publish configured durable outputs atomically and acknowledge returned data;
- commit each stateful run as a new immutable `DocumentRelease`;
- perform incremental updates without recurring full dumps; and
- process millions of image or page units within a sealed scale profile.

### 2.2 Excluded capabilities

DocSpec does not define or reimplement:

- source-specific acquisition or the authoritative meaning of source-native
  facts;
- the semantic meaning of tags, classifications, or other processor outputs;
- a mandatory parser, model, record format, table format, storage vendor,
  scheduler, or cloud;
- scheduler placement, worker pools, queues, triggers, or retry timing;
- a distributed cache, object-storage service, table engine, or analytical
  transformation engine;
- a query, ranking, search, or public-serving interface;
- legal or policy conclusions; or
- approval of a processor's output for a downstream use.

DocSpec executes declared processing and preserves evidence. The processor and
its consumer remain responsible for the output's meaning. A deployment SHOULD
use maintained packages for general infrastructure. Dagster, Ray, or a queue may
schedule work; Parquet or Arrow libraries may encode records; Iceberg, Delta, or
a database may manage tables; dbt may run an injected dataset transformation;
and Redis may provide a disposable acceleration cache. None becomes DocSpec's
source of truth.

## 3. Core concepts

All DocSpec semantic arrays described as sorted use the Rulespec platform
artifact comparator: unsigned UTF-16 code-unit order for strings, numeric order
for integers, and lexicographic tuple order. When this specification names a
primary field, compare it first and break a tie by the complete canonical JSON
bytes as unsigned bytes. When it names no primary field, compare the complete
canonical JSON bytes. Exact duplicates are invalid. Semantic-order arrays such
as processor dependencies retain their declared order unless their generated
schema explicitly marks them sorted.

**SourceCatalog**
: A complete DocSpec-owned sealed snapshot that accounts for a requested source
  universe, applies one identity-bound catalog policy, and identifies source
  items and candidate files. A successor may reuse unchanged partition bytes by
  content reference but requires no prior catalog to reconstruct current state.

**SourceItem**
: One logical catalog entry with stable source and document identities,
  source-native facts, explicit DocSpec interpretations, candidate files, and a
  first-class catalog disposition. A source item may name several candidate
  files or renditions.

**Captured file**
: Exact acquired bytes, identified by a SHA-256 digest and a capture receipt.

**Representation**
: Content derived from a captured file by a named extractor and configuration.
  Examples include embedded text, optical character recognition text, page
  images, normalized HTML, or decoded JSON records.

**Segment**
: A deterministic unit derived from a file or representation. Examples include
  a page, image region, paragraph, section, table, frame, or record.

**Processor**
: An injected implementation that consumes declared DocSpec records and emits a
  named derived record set. A processor may tag, classify, redact, summarize,
  transform, embed, measure, or perform another bounded operation.

**Document entry**
: One source item within a job. It records file information, requested work,
  content and output references, stage outcomes, dispositions, and receipts.

**DocumentStore**
: A bounded job manifest and its evolving execution ledger. A planned store
  contains a fixed set of document entries and requested stages. A sealed store
  is the immutable receipt for that job. It references large bytes through
  locators; it is not the physical blob store.

**Store task**
: A small, serializable request that names a stable task identity, operation,
  saved `DocumentStore` reference, worker-composition profile, and optional sink
  reference. It never contains a source file or complete result set.

**Store task result**
: A small, serializable terminal message that names the task and its processed or
  sealed `DocumentStore` reference, or a persisted terminal failure reference.
  Bulk bytes and records travel through blob and result-sink adapters, not the
  execution tool's control messages.

**Execution profile**
: A sealed description of how one run is handed to a local runner or external
  tool. It identifies the adapter and configuration digest, worker-composition
  profile, concurrency and in-flight bounds, queue or pool, scratch limits,
  provider limits, cache state, and deadline. It governs operations, not logical
  document meaning.

**Result sink**
: An injected destination that accepts verified result records and returns a
  delivery receipt. A sink may persist a dataset, send a bounded stream to a
  caller, or combine both behaviors.

**DocumentCatalog**
: The DocSpec-owned interface for opening, querying, comparing, and advancing
  corpus state. Its state is always identified by a `DocumentRelease`; it is not
  a second corpus artifact.

**Storage profile**
: A versioned, machine-readable description of one physical implementation for
  catalog state, record datasets, blobs, saved jobs, result delivery, or release
  manifests. Profiles implement DocSpec interfaces; they do not change DocSpec's
  logical records.

**Layer release**
: One immutable dataset for files, representations, segments, dispositions, or
  a processor's output.

**DocumentRelease**
: One immutable `DocumentCatalog` snapshot. It pins the prior release, source
  catalog, active logical layers, selected storage profiles and their state
  references, blob roots, schemas, policies, sealed store receipts, counts,
  coverage, and integrity digests.

**Receipt**
: A closed machine record of one plan, `DocumentStore`, attempt, delivery, layer
  build, verification, or publication.

## 4. Architecture boundary

### 4.1 Dependency direction

DocSpec MUST use this dependency direction:

```text
commands -> application services -> DocSpec ports and domain records
                                      ^
                                      |
 source-native/source-catalog/document-catalog/blob-store/extractor/segmenter/
 processor/execution-tool/result-sink adapters
```

Application services and domain records MUST NOT import adapters or vendor
software. One composition root MUST select and inject concrete adapters.

### 4.2 Required ports

The core MUST define project-owned ports for:

- `SourceNativeRecordSource`;
- `SourceCatalogStore`;
- `SourceCatalogReader`;
- `DocumentCatalog`;
- `DocumentStoreRepository`;
- `BlobStore`;
- `RecordStorage`;
- `Extractor`;
- `Segmenter`;
- `Processor`;
- `ExecutionBackend`;
- `ResultSink`.

Ports MUST exchange DocSpec records. `SourceNativeRecordSource` returns one
DocSpec-owned neutral boundary record plus the immutable input pin; it MUST NOT
expose a producer class. No port exposes storage-format or cloud-provider
response types, a model provider's types, or a batch engine's task objects.

### 4.3 Adapter rules

Adapters MAY depend on external packages. The core package MUST install and run
with local adapters and in-memory fakes only.

A source-native adapter MAY import its source-format client. A source-catalog,
document-catalog, or storage adapter MAY import its storage client. A processor
adapter MAY import its model or transformation library. Those imports MUST
remain inside the registered adapter package.

Section 12 defines the one first-slice composition root and optional SpicyRegs
outer adapter. DocSpec domain, ports, and application modules never import
SpicyRegs or that adapter; the base package, help, and unrelated commands work
without the extra.

The default implementation SHOULD adapt maintained packages instead of
duplicating them. Local JSON, SQLite, file, and threaded adapters MAY provide a
small portable reference and test fixture. A production adapter MAY delegate to
Dagster, Ray, a queue, S3 or R2, Parquet or Arrow, Iceberg or Delta, dbt, Redis,
or another maintained package. DocSpec MUST NOT copy those systems' scheduling,
locking, caching, table, or storage engines into its core.

Redis or another cache MAY accelerate leases, throttling, duplicate suppression,
or verified-result lookup. Cache loss, eviction, or stale data MUST NOT change a
logical output or publication decision. Durable object digests, stage receipts,
partition roots, and releases remain authoritative.

Tests MUST prove that each adapter can be replaced without changing application
services, `DocumentStore` schemas, or `DocumentRelease` schemas. A durable
dataset sink MAY compose `LayerWriter` components behind `ResultSink`;
`DocumentCatalog` alone commits the release.

### 4.4 Repository boundary

The installed package MUST contain only DocSpec capabilities. Vocabulary
management, search engines, ranking, query serving, and source-specific
acquisition MUST remain outside the core package.

DocSpec MUST contain no copied production implementation from the pinned
SpicyRegs predecessor. `BOUNDARY-CODE` MUST use Git-native history, move/copy
detection, package-boundary checks, and focused behavior fixtures; it MUST NOT
maintain a project-specific source archive, per-file hash manifest, syntax
fingerprint corpus, or hand-maintained production-module inventory. Registered
adapters may target current external products; no production adapter imports a
superseded format or historical implementation. A one-time migration executes
the exact predecessor from its pinned Git revision outside the installed DocSpec
package.
Package metadata, import checks, and the executable application path establish
the production boundary.

## 5. Source catalog, document catalog, and planning

### 5.1 SourceCatalog and SourceCatalogReader

DocSpec owns one normative `SourceCatalog`/`SourceItem` specification, packaged
schema, builder, reader, semantic verifier, and conformance corpus. A catalog
MUST be a complete immutable snapshot. No prior catalog or event sequence is
required to reconstruct current state.

The catalog application service receives `SourceNativeRecordSource`,
`SourceCatalogStore`, and the closed catalog policy through explicit
constructor or function parameters. It MUST NOT import SpicyRegs, choose a
source adapter, open a mutable producer database, or construct a concrete store.
The composition root selects those adapters. The bounded builder and semantic
verifier operate on the same DocSpec records and policy; there is no second
publisher-specific catalog model or change-set representation.

Every `SourceItem` MUST contain:

- a stable `sourceItemId`, stable `documentId`, and `sourceIssuedVersion`;
- source-native facts preserved without rewriting their meaning;
- explicit DocSpec interpretations for normalization, joins, sampling,
  rendition preference, and selection;
- the normalized title, agency, document type, publication and update dates,
  docket identifiers, Regulation Identifier Numbers, comment closing date,
  language, source URL, and source-observed topics when those values exist;
- zero or more candidate renditions, each with a stable candidate identity,
  media type, locator kind, exact source-stated or immutable locator, and
  expected digest or byte size only when the source supplies that value before
  capture; and
- exactly one first-class catalog disposition: `selected`, `excluded`,
  `deleted`, `unavailable`, or `failed`, plus a reason for every non-selected
  disposition.

Catalog disposition is distinct from DocSpec processing state. The five-way
meaning MUST NOT be collapsed into a smaller state enum or retained only in
arbitrary metadata. `sourceIssuedVersion` is a source observation and MUST
remain distinct from the later `captureDigest` over acquired bytes.

The catalog root MUST pin every immutable source-native input and one catalog
policy identity and digest. That policy covers discovery, the requested
universe, publication window, normalization declarations, crosswalks, exact
joins, sampling, rendition preference, and selection. Changing any of these or
any source input MUST change catalog identity.

The first catalog-policy profile preserves this exact evaluation order. A
source-declared non-available outcome is retained first. Otherwise the builder
requires usable metadata, applies scope and publication-window admission, and
draws the sample over only available, normalized, scope-admitted items. It then
excludes an undrawn frame member, checks required normalized fields and agency
names, requires a candidate rendition, and finally applies the selected-item
budget. Sampling MUST precede required-field and rendition checks, so later
availability cannot change the draw.

That profile partitions by document type and stratifies by the sorted agency-ID
tuple plus publication year, substituting the literal `unknown` part for an
absent value. Within a stratum it orders by lowercase hexadecimal
`MD5(UTF-8(documentId + ":" + seed))` and then `documentId`, assigns one-based
rank, orders the complete partition by
`(rank / sqrt(stratumSize), hash, documentId, sourceItemId)`, and takes the
first `perPartitionLimit`. MD5 is a deterministic ordering function, not a
security primitive. The first profile evaluates `sqrt` and division as finite
IEEE 754 binary64 with round-to-nearest, ties-to-even, and compares the resulting
numbers ascending. Changing that comparator requires a new policy version and
an accepted differential. Rendition preference examines ordered family IDs and
returns every candidate from the first non-empty family only. It MUST NOT merge
candidates from lower-preference families.

`SourceCatalog` uses the Rulespec container with product kind
`docspec-source-catalog`. Its opaque Rulespec `spec` object has these exact
DocSpec-owned fields: `catalogId`, `catalogSchemaDigest`, `sourceSystemSetDigest`,
`sourceNativeSchemaSetDigest`, `selectionPolicyId`, `selectionPolicyVersion`,
`selectionPolicyDigest`, `requestedUniverseSetDigest`,
`selectedSourceSetDigest`, and `catalogStateDigest`. IDs are absolute; digests
are qualified lowercase SHA-256. `catalogId`
identifies the source/policy series; it is not the Rulespec root logical ID.
Every root `inputs` array contains one or more pins with role
`source-native`, one for every source-native distribution and no other roles.
Their `(role, logicalDigest)` pairs are duplicate-free under Rulespec. The exact
catalog policy is not another artifact family or
external input: one canonical
JSON product member with role `catalog-policy` carries its complete closed
configuration, and `selectionPolicyId`, `selectionPolicyVersion`, and
`selectionPolicyDigest` MUST identify and digest those bytes. The installed
DocSpec implementation supplies executable policy behavior, while this member
supplies its data. That member has exactly `format`, `formatVersion`, `policyId`,
`policyVersion`, and `configuration`; the first two values are
`docspec-catalog-policy` and `1.0`, and `configuration` is closed by its generated
schema. The digest is over the complete canonical member bytes, so it does not
contain a self-digest. The selection policy covers all catalog interpretations
named above, not only the final selected/not-selected decision. The admitted
`source-native` inputs are immutable verified releases, not an implied
cross-release event stream. The builder derives one `SourceItem` for every
policy-universe row carried by those exact releases, the complete requested
universe `U` for that input, selected set `S`, and `catalogStateDigest` under the
pinned policy on every publication. This is a catalog-completeness statement,
not a claim that an upstream source exposed every possible record: a
`complete-snapshot` input can support that stronger source claim, while an
`observed-crawl` cannot. Build evidence contains no representation selector,
storage mode, or previous-catalog field.
Rulespec validates none of these product fields. The installed DocSpec semantic
verifier validates them after Rulespec admits the one shared container.
`sourceSystemSetDigest` calls the installed Rulespec
`framedSectionDigest` with domain `docspec-source-system-set/1` and one
`sources` section. Its closed records contain exactly `sourceSystemId`,
`sourceSystemVersion`, `logicalDigest`, `sourceStateScope`,
`sourceStateDigest`, and `sourceNativeSchemaSetDigest` for each admitted
source-native input. Records sort by
`(sourceSystemId, sourceSystemVersion, logicalDigest)` and reject a duplicate
tuple. The projection excludes the exact artifact digest and locator, which
remain bound by the root input pin. This supports a catalog policy that joins
several source systems without pretending they form one source or making
physical repacks logical changes.
For the first publisher, every source-native input pin MUST resolve to a
verified `spicyregs-source-native-release`; an adapter for another source may
accept another product-owned source-native kind only when its own installed
semantic verifier and closed source schema qualify that input first.
Every admitted input MUST declare and prove either `complete-snapshot` or
`observed-crawl`. In both cases, `U` is exactly the policy-selected row family
in the pinned release and every member receives one catalog disposition. An
`observed-crawl` MUST remain identified as such in `sourceSystemSetDigest`; it
does not establish source-wide omissions, deletions, or completeness, and no
catalog or consumer may broaden it into that claim.

A mutable current-catalog pointer may move only to a verified candidate whose
generic Rulespec `supersedes` record exactly names the pointer's current logical
ID and artifact digest and gives a nonempty reason. The DocSpec verifier also
proves both roots have the same `catalogId`; a different series receives a new
pointer. This succession evidence is excluded from `catalogStateDigest` and
logical identity but remains bound by the exact artifact digest.

`catalogStateDigest` calls the installed Rulespec `framedSectionDigest`; DocSpec
does not implement or restate its byte framing. It declares domain
`docspec-source-catalog-state/1` and one section, `sourceItems`, containing the
complete closed logical `SourceItem` rows in strict `sourceItemId` order. A
duplicate or out-of-order `sourceItemId` fails. The projection includes every
nested source-native fact, normalized value, observation, interpretation,
candidate rendition, disposition, and reason exactly as specified below. It
therefore includes each candidate rendition's source locator: changing the
source object offered for later capture changes catalog meaning. It excludes
only container and member storage locators outside the `SourceItem`, plus member
partitioning, compression, manifests, build receipts, timestamps, resource use,
and other publication evidence. The producer's semantic build gate MUST
recompute it before publication. A consumer open checks the sealed build receipt
and exact digests but does not repeat this corpus-wide semantic pass before
yielding a row. Because Rulespec includes the complete product `spec` in logical
identity, any logical row change moves the catalog logical ID; an evidence-only
or physical-layout change preserves that logical ID and moves only
`artifactDigest`.

`requestedUniverseSetDigest` and `selectedSourceSetDigest` call the installed
Rulespec `framedSectionDigest`; DocSpec does not implement or restate its byte
framing. The requested digest declares domain
`docspec-requested-universe-set/1`, section `members`, and closed items containing
exactly `sourceItemId`, sorted by that value. The selected digest uses domain
`docspec-selected-source-set/1`, section `members`, and closed items containing
exactly `sourceItemId` and `documentId`, sorted by that tuple. An empty set uses
the same framing with item count zero and no item bytes. Duplicate projected
items, missing requested members, or a selected member absent from the requested
set fail.

The installed package MUST generate and ship one closed schema family under
`docspec/schemas/source_catalog/1.0/`, from the DocSpec domain records rather
than hand-maintaining a second schema. The checked-in generated schemas, domain
records, serializer, parser, and schema-generation test MUST agree byte for
byte. The family contains `source-item.schema.json`,
`catalog-build-receipt.schema.json`, and `catalog-policy.schema.json`.
`catalogSchemaDigest` calls the installed `rulespec-artifacts`
`schemaBundleDigest` over the complete product-relative path-to-schema map. The
exact installed family bytes
and declared IDs remain member evidence. Unknown fields, schema bytes, versions,
or enum values fail; an unfamiliar declared namespace may warn but cannot fail when
the digests and validation behavior match.

A snapshot row has exactly `sourceItemId`, `documentId`,
`sourceIssuedVersion`, `sourceNativeFacts`, `normalizedMetadata`,
`sourceObservedTopics`, `sourceObservations`, `interpretations`,
`candidateRenditions`, and `selection`. Arrays use stable order and reject
duplicates. The nested forms are:

- `sourceNativeFacts`: a sorted array of closed objects containing `scopeId`,
  `schemaName`, `schemaVersion`, `schemaDigest`, and `fields`. Each adapter
  declares every source
  column in its pinned closed schema; a new or unclassified input column fails
  the build instead of disappearing. `fields` preserves the source value and
  null exactly. A regulations.gov document item's `sourceNativeFacts` include
  its matched docket's exact record under schema name
  `regulations-gov-docket-raw` — the source SpicySearch's
  `fields/data/attributes/dkAbstract` pointer resolves against — only when
  that docket exists in the catalog's docket universe. A docket miss does not
  by itself change the document's disposition; the item records the
  `document-docket` join outcome `no-match`, and the build receipt counts it in
  `joinCoverage`.
- `normalizedMetadata`: one closed object containing `title`, `agencies`,
  `documentType`, `publicationDate`, `lastUpdatedDate`, `docketIds`,
  `regulationIdentifierNumbers`, `commentCloseDate`, `language`, and
  `sourceUrl`. Every field is present; an allowed absent value is explicit
  `null` or an empty array as defined by the generated schema. The first Federal
  Register profile uses `federal-register-rin-syntax/1`: trim Unicode
  whitespace, apply NFKC, uppercase ASCII letters, and admit the value only when
  it matches `^[0-9]{4}-[A-Z][A-Z0-9]{3}$`. A nonmatching publisher value stays
  only in `sourceNativeFacts` and an `unparseable` normalization interpretation;
  it MUST NOT enter `normalizedMetadata.regulationIdentifierNumbers`.
- `sourceObservedTopics`: sorted closed objects containing
  `observedTopicId`, `observedTopicScheme`, and `label`.
- `sourceObservations`: sorted closed objects containing `observationKey` and
  `observationValue`.
- `interpretations`: sorted closed objects containing `interpretationKind`,
  `policyId`, `policyVersion`, `policyDigest`, `inputScopeIds`, and `result`.
  The schema closes `result` separately for `normalization`, `exact-join`,
  `sampling`, `rendition-preference`, `topic-recovery`, and `selection`.
  `normalization` records every source path read, declared substitute or
  crosswalk entry, normalized field/value, and a closed per-field outcome of
  `normalized`, `absent`, or `unparseable`. An unparseable normalized value
  retains its exact source-native fact and cannot abort or remove unrelated
  rows. `exact-join` records `joinId`, ordered source keys, the exact
  matched row identity or explicit `no-match`, and whether the result is null.
  `sampling` records frame admission, partition, stratum, hash, one-based rank,
  stratum size, allocation method, limit, and drawn decision. It records the
  integer inputs to the allocation rather than a floating-point text rendering.
  `rendition-preference`
  records the ordered family IDs, each family's ordered offered rendition IDs,
  the selected family or explicit absence, and the selected rendition IDs.
  `topic-recovery` records the source field, one result of `observed`,
  `publisher-declared-empty`, or `not-recovered`, and the exact evidence digest
  that permits the second result. Without that evidence, an empty source array
  is `not-recovered` and cannot assert publisher-declared absence.
  `selection` records each applicable decision in evaluation order and the final
  disposition, reason code, and reason. Omitting an intermediate decision that
  affected the result is invalid.
- `candidateRenditions`: sorted closed objects containing `renditionId`,
  `mediaType`, `locatorKind`, `locator`, `expectedSha256`, and
  `expectedByteSize`. `locatorKind` is `source-url` or `immutable-object`. A
  source URL is the exact source-stated acquisition address and is not claimed
  immutable; its catalog meaning is pinned with `sourceIssuedVersion`. An
  immutable-object locator is content stable under its owning store. The last
  two fields are explicit `null` when the source did not supply them before
  capture. DocSpec capture later dereferences the chosen candidate, computes the
  actual digest and size, and seals those bytes before document processing.
- `selection`: one closed object containing `disposition`, `reasonCode`, and
  `reason`. The reason fields are `null` only for `selected` and required for
  every other disposition.

The first Federal Register adapter applies those generic rules to the pinned
predecessor baseline. It maps the native title to `normalizedMetadata.title`,
preserves every agency name or slug and explicit absence, retains malformed raw
RIN text with an `unparseable` normalization outcome, and exposes every stated
HTML, PDF, and body-HTML locator as a candidate before rendition preference.
Its raw `topics_json` remains in `sourceNativeFacts`. The predecessor measured
890,013 empty arrays whose meaning was unresolved; the adapter records them as
`not-recovered` until a pinned 30-row live-source receipt supports a different
interpretation. Missing agency, malformed RIN, missing rendition, or empty
topics may affect that item's explicit interpretations and disposition but MUST
NOT abort or silently remove another source item.
Federal Register fixtures prove malformed RIN text remains visible as source
evidence and a diagnostic but produces no normalized RIN, exact lookup, filter,
facet, join, or relation value. Valid neighboring rows and their RIN behavior
remain unchanged.

Catalog payload members use only role `source-items`. They are canonical JSON
Lines, globally ordered by `sourceItemId`, and divided by one stable logical
partition policy. Every source-item partition descriptor uses Rulespec
`blobRef`. The builder deterministically emits each partition, calculates its
content identity, verifies an existing blob before reuse, and writes bytes only
when that exact blob is absent. Every snapshot still declares every current
partition and row; a reader never replays a prior catalog.

Exactly one canonical JSON member with role `catalog-build-receipt` records
input, policy, schema, and verifier pins; partition policy; counts; disposition
totals; join coverage; `U`, `S`, and state digests; canonical domain-result
digests for normalized fields, joined fields, dispositions, reasons,
interpretations, and rendition choices; and a closed byte-measurement object containing
`payloadBytesRead`, `payloadBytesReused`, `payloadBytesWritten`, and
`publicationBytesWritten`. The measurements reconcile with the declared
members and actual store writes.

`catalogStateDigest` is the sole digest of complete ordered `SourceItem` rows;
the receipt and migration fixtures do not add an `orderedRowsDigest`. Every
diagnostic result digest calls the installed Rulespec `framedSectionDigest`
with one `records` section. DocSpec declares these domains, closed projections,
and total-order keys:

- `normalizedFieldsDigest` uses domain `docspec-catalog-normalized-fields/1`
  over `{sourceItemId, fieldPath, valueIndex, value, diagnostics}`, ordered by
  `(sourceItemId, fieldPath, valueIndex)`;
- `joinedFieldsDigest` uses domain `docspec-catalog-joined-fields/1` over
  `{sourceItemId, joinId, outputPath, valueIndex, value, outcome, evidence}`,
  ordered by `(sourceItemId, joinId, outputPath, valueIndex)`;
- `dispositionsDigest` uses domain `docspec-catalog-dispositions/1` over
  `{sourceItemId, disposition}`, ordered by `sourceItemId`;
- `reasonsDigest` uses domain `docspec-catalog-reasons/1` over
  `{sourceItemId, reason}`, ordered by `sourceItemId` and preserving null;
- `interpretationsDigest` uses domain `docspec-catalog-interpretations/1` over
  `{sourceItemId, interpretationKind, interpretationId, value, diagnostics}`,
  ordered by `(sourceItemId, interpretationKind, interpretationId)`; and
- `renditionChoicesDigest` uses domain `docspec-catalog-rendition-choices/1`
  over `{sourceItemId, selectedFamilyId, candidateIds}`, ordered by
  `sourceItemId`; `candidateIds` preserves the catalog's stable candidate order.

The generated catalog-result schemas close every object named above. Duplicate
or out-of-order keys and a declared/observed count mismatch fail before the
shared helper runs. These diagnostics attribute a state change; they do not
replace `catalogStateDigest` or define another framing algorithm.

The closed DocSpec product-role set is exactly `catalog-policy`, `source-items`,
and `catalog-build-receipt`. `storageMode`, `baseCatalog`, `changeKind`, and role
`source-item-changes` are unknown fields or roles and fail closed.

A changed logical row, policy, schema, `U`, `S`, or `catalogStateDigest`
produces a new catalog logical ID and artifact digest. An unchanged partition
retains its exact `blobRef`. A physical-only rebuild of identical logical state
may preserve logical ID while moving only `artifactDigest`.

The producer's independent semantic verifier creates the build receipt only
after recomputing the complete state and both set proofs. The receipt also names
its verifier ID, version, released implementation pin, verdict, and the exact product
schema and policy digests. Its verifier tuple MUST equal the Rulespec root
producer record and the installed accepted verifier descriptor. Consumer
admission compares the receipt's state, set, schema, policy, sequence, and
verifier pins with the structurally verified root and refuses any mismatch or
non-pass verdict. It does not rerun the corpus-wide semantic derivation;
immutable member digests make a changed post-publication corpus fail
structurally.

The landing migration baseline is ordinary executable test data tracked in
DocSpec, not another Rulespec artifact family. One directory contains
`baseline.json`, bounded `differential-cases.jsonl`, and
`differential-expected.jsonl`. The exact DocSpec commit pins those files, their
schemas, the independent verifier, and the acceptance tests. `baseline.json`
contains the predecessor archive commit, exact source-native input artifact
pins, catalog policy, expected `U` and `S`, `catalogStateDigest`, counts,
disposition totals, join coverage, and the diagnostic result digests declared
above. These are domain results, not redundant hashes of Git-tracked source
files.

Each differential case contains exactly `caseId`, `baseInputPins`,
`mutationKind`, and `mutation`. `mutation` is closed per kind and covers scope
and window boundaries, source outcome, missing metadata, sample seed/key/order,
partition and stratum, small and large strata, required fields, agency
crosswalks, exact join match/no-match/null behavior, rendition-family presence
and order, selected budget, and duplicate identity. Each expected row contains
exactly `caseId`, `requestedUniverseSetDigest`,
`selectedSourceSetDigest`, `catalogStateDigest`, `normalizedFieldsDigest`,
`joinedFieldsDigest`, `dispositionsDigest`, `reasonsDigest`,
`interpretationsDigest`, `renditionChoicesDigest`, and `expectedFailureCode`.
The last field is null for a successful case and a
closed failure code otherwise. Cases and expected rows sort by `caseId`, match
one-to-one, and reject unknown mutation kinds or fields. `expectedCounts`
contains exactly `requested` and `selected`; `expectedDispositionTotals`
contains exactly the five catalog dispositions; and `expectedJoinCoverage` is
sorted by `joinId` and contains closed objects with `joinId`, `eligible`,
`matched`, `unmatched`, and `nullResult`. All count values are non-negative
integers and every named domain-result digest is qualified SHA-256.

The closure preamble in `../spicy-regs-landing/PLAN.md` is the sole archive
runbook. Before this migration runs, its separately authorized procedure MUST
produce the verified child commit and non-moving archive reference that preserve
the complete Git-visible predecessor tree, including tracked ignored evidence,
file modes, dependency locks, and the broken `RefSpec` link without
dereferencing it. DocSpec depends only on that commit identity; it neither owns
nor restates the archive mechanics. DocSpec MUST NOT add a source-tree artifact,
source manifest, file-hash inventory, tree digest, patch digest, fingerprint
corpus, or custom extractor around Git.

One read-only migration command checks out that exact archive commit in a
temporary Git worktree. The archived lock remains historical evidence because
its broken local RefSpec path is not portable. The command installs separately
pinned, hash-verified wheel sets for predecessor and replacement, including a
RefSpec wheel built from its pinned commit where the predecessor requires it; it
never dereferences the archived `RefSpec` link or reads a sibling checkout. It
runs both implementations against the same exact input artifact pins and emits
one closed migration result. The result contains the predecessor commit, DocSpec
commit and package version, input pins, catalog policy pin, candidate catalog
identity and artifact digest, case counts, classified differences, acceptance
authority and decision, verifier identity, and `pass` or `fail`. It is one-time
transition evidence, not a runtime input or permanent conformance artifact
family. The independent verifier owns the expected fixtures and result; the replacement
DocSpec builder MUST NOT write or approve them.

The snapshot MUST account for every member of requested universe `U` exactly
once and define selected set `S` by the exact framed membership digests above.
For the first
publisher, selected `sourceItemId` and `documentId` values are independently
unique and map one-to-one; grouping several source items into one document is
outside that profile. Every selected item MUST have at least one acquisition
candidate with a valid locator kind and syntax; a source URL need not have a
pre-capture digest or immutable-content guarantee. Counts, including selected
count `N`, are diagnostics and
MUST NOT substitute for exact set membership or its digest.

Publication is atomic. Capture and document-processing results MUST NOT feed
facts back into the same published catalog. No published item may disappear,
change disposition, or change meaning in place; every logical change creates a
new complete snapshot identity. Reused immutable blobs reduce bytes written but
do not make a prior snapshot part of the new snapshot's meaning.

`SourceCatalogReader` MUST:

- open one complete snapshot by expected identity and digest;
- verify the complete distribution and compare the sealed semantic-build
  receipt's declared `U`, `S`, state, schema, policy, and verifier pins with the
  root before yielding, without recomputing the full membership or state
  projections at consumer open;
- reject missing, extra, unknown, duplicate, escaped, digest-mismatched, or
  semantically invalid content;
- stream records in stable order without loading the corpus;
- expose every required identity, fact, interpretation, candidate, disposition,
  count, partition, coverage record, and policy pin; and
- operate without importing a source producer or reading its mutable state.

The full semantic verifier is a producer build-gate API. The consumer reader is
a separate bounded API: after structural and receipt verification it validates
each row's closed schema and order as that row is streamed, without a second
pre-yield catalog scan. A consumer may explicitly request an offline semantic
audit, but ordinary admission and open MUST NOT run it.

`SOURCE-CATALOG-CONTRACT` MUST cover the sole complete-snapshot form; every field
and disposition; one-to-one selected identity; candidate usability;
policy-driven identity movement; complete `U` and `S`; immutability; stable
bounded multi-part streaming; generated-schema equality; exact member roles and
media types; source-column classification refusal; closed interpretation
results; deterministic partitions; unchanged `blobRef` reuse; changed-partition
writes; reconciled byte-write measurements; and rejection of partial successors
and legacy change-set fields or roles. It MUST also prove stable cross-partition
ordering, exact requested/selected digest framing including empty sets and
duplicate refusal, acceptance of syntactically valid `source-url` candidates
without pre-capture digests, and producer-gate recomputation of
`catalogStateDigest`. Consumer tests MUST prove that root/member/receipt
mismatches fail before the first row, while a valid open yields after bounded
structural and receipt checks without a second corpus-wide semantic pass or
prior-catalog replay. It MUST run the pinned predecessor and replacement against
the complete differential corpus and compare exact digests, dispositions,
reasons, interpretations, and rendition choices. An intentional difference is
allowed only when the closed catalog policy names it and acceptance identifies
the approving decision; every other difference fails. Changing one logical row
MUST move catalog logical identity; changing only a build receipt, compression,
or physical partitioning MUST preserve logical identity and move only the
artifact digest.

### 5.2 DocumentCatalog

`DocumentCatalog` MUST:

- open corpus state by exact `DocumentRelease` identity and digest;
- verify the release before exposing records;
- look up a document, version, file, representation, segment, derived record,
  store receipt, or disposition;
- scan active records and changes in stable order without loading the corpus;
- compare two releases by logical identity;
- stage verified outputs without making them current;
- commit sealed `DocumentStore` receipts against one expected base release;
- reject a stale base or conflicting commit;
- publish and return the new `DocumentRelease`; and
- reconstruct the same logical state from the committed release.

`DocumentCatalog` MUST NOT maintain corpus state that its `DocumentRelease`
cannot identify and reproduce. A mutable `current` pointer MAY help operators,
but consumers and jobs MUST use an explicit release identity. Moving that
pointer requires the candidate Rulespec root's `supersedes` record to match the
current root exactly with a nonempty reason, and the DocSpec verifier must prove
series continuity. The release-local `previousRelease` field still records the
logical corpus chain; the generic field protects the physical pointer update.

In this model, `DocumentCatalog` corresponds to the versioned corpus and
`DocumentRelease` corresponds to one commit. The ordered release lineage is the
catalog's history.

### 5.3 ProcessingPlan

Every run MUST begin from a sealed `ProcessingPlan`. The plan owns choices that
can change logical document state or the interpretation of a result. It MUST
identify:

- the source-catalog input and, as publication evidence only, an optional exact
  prior `DocumentRelease` reference used for incremental comparison;
- selected source partitions or an exact selection rule;
- `DocumentStore` size and cost limits;
- document-catalog, record-storage, blob-storage, document-store persistence,
  result-delivery, and release-manifest profiles;
- extractor and segmenter policies;
- the ordered processor graph;
- retention and data-use policies;
- per-file, per-entry, per-stage, and per-job byte, memory, item, attempt, and
  duration safety limits that affect acceptance or output;
- failure classification, finite stage-attempt, and accepted-failure policies;
- logical partitioning policy; and
- result sink and delivery policy.

Every `ProcessingPlan` is one Rulespec container with product kind
`docspec-processing-plan`. Its closed product `spec` contains exactly
`semanticProjectionDigest`, and its closed product-role set contains exactly one
canonical `processing-plan` member. Generic producer evidence remains outside
that set. The verifier derives, but does not store, `semanticProjection` from the
complete plan member. That projection is a closed object with exactly
`sourceCatalog`, `selection`, `extractorPolicy`, `segmenterPolicy`,
`processorGraph`, `retentionPolicy`, `dataUsePolicy`, `acceptanceLimits`,
`acceptedFailurePolicy`, and `partitioningPolicy`. `sourceCatalog` contains
exactly its logical digest suffix and `catalogStateDigest`; the remaining fields
reuse their closed plan-domain records. `semanticProjectionDigest` is qualified
SHA-256 over the canonical `semanticProjection` bytes. The plan's Rulespec root
inputs are exactly one `source-catalog` input and no other role. Its Rulespec
`logicalId` is the semantic plan identity.

The complete plan member may carry a closed `previousRelease` reference with
the prior release logical ID, exact artifact digest, and locator. The verifier
opens that exact release before incremental comparison, and the plan artifact
digest binds the reference. The reference is excluded from the semantic
projection and Rulespec logical inputs because it describes the route used to
reach current state, not the requested logical result. A clean rebuild and an
incremental run with the same source catalog and behavior-bearing choices must
share the same plan logical identity even though their evidence differs.

The semantic projection excludes the six storage and delivery profile pins,
locators, media types, scheduler choices, result-delivery mechanics, batch
sizing, resource-only limits, and other fields that cannot alter canonical
logical records. Changing a semantic field MUST move both identities; changing
only an excluded physical or execution field MUST preserve the plan logical ID
and move its `artifactDigest`. The plan verifier recomputes the projection
digest, Rulespec logical ID, and exact artifact digest and rejects any mismatch.

The plan MUST NOT encode scheduler worker placement, queue implementation,
cluster topology, or a cache product's native configuration. Those operational
choices belong to a separate sealed `ExecutionProfile`. The execution profile
MUST identify:

- the execution adapter and its version or deployment identity;
- a digest-pinned worker-composition profile that can reconstruct all adapters;
- worker, concurrency, and in-flight bounds;
- queue, pool, partition, or task-mapping configuration by identity and digest;
- scratch-disk, network, request-rate, and provider limits;
- retry timing owned by the execution tool;
- cache implementation and initial cache state, if any; and
- an absolute deadline.

The run receipt MUST pin the execution profile and the execution tool's returned
run or event-log reference. A `ScaleProfile` MUST pin it when operational
resources are part of a performance claim. Changing only an execution profile
does not invalidate document content. If an operational change also changes a
logical input, policy, accepted failure, or deterministic output, the
`ProcessingPlan` MUST change as well.

DocSpec MAY provide a local default execution profile. A deployment SHOULD let
Dagster, Ray, a queue, or another maintained tool own worker placement,
concurrency, triggers, and retry timing rather than reproducing those features in
DocSpec.

### 5.4 Change planning

For an initial backfill, the planner MUST enumerate the selected catalog once
and create bounded `DocumentStore` jobs. Each job identity MUST derive from the
plan, logical partition, ordered document-entry identities, and requested
stages.

For an update, the planner MUST compare the new source-catalog state with the
prior `DocumentRelease` opened through `DocumentCatalog`. It MUST classify each
item as:

- `added`;
- `changed`;
- `unchanged`;
- `deleted`;
- `repair`; or
- `excluded`.

The planner MUST schedule only work whose inputs or governing policy changed.
An unchanged item MUST reuse verified content and layer records by digest. The
planner MUST NOT place the full corpus into one `DocumentStore`.

## 6. Acquisition and exact files

### 6.1 Capture requirements

For each selected candidate, DocSpec MUST record:

- source-item and candidate identities;
- source locator;
- acquisition start and completion time;
- transport version metadata;
- downloader identity and configuration;
- declared and actual byte size;
- media type;
- SHA-256 digest;
- task and attempt identities;
- retry history; and
- final disposition.

The system MUST stream downloads through configured byte limits. It MUST NOT
load an unbounded file into memory.

A candidate's `expectedSha256` and `expectedByteSize` are source-stated
expectations and MAY be null. When present, acquisition MUST match them. When
absent, acquisition still computes the exact `captureDigest` and actual size,
and no document derivation may consume that candidate until the captured bytes
and receipt seal. This keeps digest discovery in DocSpec's acquisition stage
rather than forcing the source-native publisher to download document bodies.

### 6.2 Exact-byte preservation

DocSpec MUST preserve source bytes exactly throughout acquisition and
processing. Extraction, normalization, OCR, and redaction MUST create new
representations or derived objects; they MUST NOT replace the captured object.

The retention policy MAY discard source bytes after the `DocumentStore` seals.
In that case, the receipt MUST preserve the source locator, digest, size,
transport version, and `notRetained` disposition. A durable sink that claims
source-byte availability MUST retain the verified object.

Identical bytes MAY share one physical object. Every logical source item MUST
still appear in the document-entry ledger and, when used, the durable file
layer.

### 6.3 BlobStore

`BlobStore` MUST support:

- stat by immutable locator;
- bounded streaming reads;
- contained local materialization;
- put-if-absent while computing or checking SHA-256;
- range reads when supported;
- verification by size and digest; and
- immutable object identity.

The default object key SHOULD be:

```text
objects/sha256/<first-two-hex>/<64-lowercase-hex>
```

The reference implementation MUST provide local, Amazon S3, and S3-compatible
adapters. All adapters MUST pass the same behavioral suite.

### 6.4 Dispositions

Each selected item MUST end in exactly one acquisition disposition:

- `captured`;
- `unchanged`;
- `deleted`;
- `excluded`;
- `accepted-failure`; or
- `rejected-run`.

A missing disposition MUST stop the store from sealing. A deletion MUST remain
visible in the sealed store and any new durable release. It MUST NOT erase prior
immutable history.

## 7. Representation and segmentation

### 7.1 Extractors

An extractor MUST accept exact file bytes or an immutable object locator. It
MUST return representations, evidence mappings, warnings, and a receipt.

Each representation MUST identify:

- the source file digest;
- extractor name and version;
- configuration digest;
- media type and encoding;
- representation digest and byte size;
- page, frame, record, or region boundaries when available;
- warnings and fallback outcomes; and
- resource use.

A representation identity MUST change when its file, extractor, configuration,
preprocessing, model, or material fallback changes.

### 7.2 Supported content

The reference profile MUST process:

- standalone raster images;
- multi-page PDFs;
- XML;
- HTML;
- JSON; and
- plain text.

Additional formats MAY use registered extractor adapters.

For images, representations MAY include image metadata, optical character
recognition text, thumbnails, tiles, or detected regions. Each derivative MUST
retain its exact relationship to the source image.

For PDFs, DocSpec MUST retain the PDF and identify page boundaries. Optical
character recognition MUST create a named representation; it MUST NOT overwrite
embedded or publisher text.

### 7.3 Segmenters

A segmenter MUST accept one immutable file or representation and emit ordered,
bounded segments.

Every segment MUST identify:

- source-item, file, and representation identities;
- segmenter name, version, and policy digest;
- stable segment identity and ordinal;
- structural kind;
- segment content or object digest;
- half-open byte coordinates for text when available;
- page, frame, record, or image-region coordinates when available; and
- ordered derivation steps.

The same semantic inputs and policy MUST produce the same segment identities.

### 7.4 Evidence

Every segment MUST resolve to its exact representation and captured file.
Text coordinates MUST reproduce exact text. Image coordinates MUST identify the
source image or page and a closed coordinate system. Lossy derivations MUST name
the loss and preserve the preceding evidence link.

`EVIDENCE-ROUNDTRIP` MUST verify every fixture segment. Evidence failure MUST
stop the affected store from sealing or delivering the invalid segment.

### 7.5 Complete searchable-corpus profile

The `complete-searchable-corpus/1` profile is the DocSpec publication surface
used by a search consumer. For every member of catalog universe `U`, its
`DocumentRelease` MUST carry the catalog disposition. For every member of `S`,
it MUST carry:

- source-item metadata and provenance;
- a capture record linked to catalog identity, `sourceItemId`, `documentId`,
  `sourceIssuedVersion`, candidate identity, and exact captured-byte digest;
- immutable captured rendition bytes;
- exactly one selected human-readable Unicode representation;
- source-derived structural nodes for headings, paragraphs, pages, or records;
- ordered, bounded search segments with `structuralParentId`, ordinal, heading
  path, representation range, and reversible captured-evidence coordinates;
- explicit excluded representation ranges and reasons for visible text not
  searchable under the profile; and
- complete processing dispositions, failures, coverage, policies, receipts,
  and canonical selected-source, document/version, segment, and
  source-to-document mapping digests.

The selected-source digest is the exact catalog `selectedSourceSetDigest` and
is not recomputed under another name. The other ordered sets call the installed
Rulespec `framedSectionDigest`, each with one `members` section:

- `documentSetDigest` uses domain `docspec-document-set/1` and
  `{documentId}`, ordered by `documentId`;
- `documentVersionSetDigest` uses domain `docspec-document-version-set/1` and
  `{documentVersionId}`, ordered by `documentVersionId`;
- `representationSetDigest` uses domain `docspec-representation-set/1` and
  `{representationId}`, ordered by `representationId`;
- `segmentSetDigest` uses domain `docspec-segment-set/1` and `{segmentId}`,
  ordered by `segmentId`; and
- `sourceToDocumentDigest` uses domain `docspec-source-to-document/1` and
  `{sourceItemId, documentId, documentVersionId}`, ordered by `sourceItemId`.

Every key is unique. The generated schema closes each projection, and the
declared count must equal the streamed count. Profiles do not define another
set-digest algorithm.

HTML and XML markup is not search text. Extraction MUST produce visible text
before segmentation. Every byte range of the selected visible-text
representation MUST appear in at least one search segment or in the explicit
exclusion ledger. Oversized structures MUST be split under a versioned bounded
policy while preserving source coordinates. An expected candidate digest MUST
match captured bytes or the build fails.

This profile allows no accepted processing failure for a selected source item.
Every selected item MUST map one-to-one to one document version and produce at
least one search segment. A failure MUST NOT become a downstream exclusion.
`COMPLETE-SEARCH-CORPUS` MUST prove the set and mapping digests, gapless visible
text accounting, exact evidence round trips, expected-digest refusal, zero
accepted failure, and verification from a clean installed package without a
source-producer checkout.

## 8. Injected processors

### 8.1 Processor description

Each processor MUST publish a closed `ProcessorDescription` that identifies:

- processor name and version;
- accepted input record kinds and schemas;
- output schema and media types;
- configuration digest;
- external resources and model identities;
- determinism and cache policy;
- data-use policy;
- item-level limits;
- retry policy; and
- processor dependencies.

Processor dependencies MUST form an acyclic graph. The plan MUST pin the exact
graph before execution.

### 8.2 Execution boundary

A processor MAY consume captured files, representations, segments, or earlier
derived records. It MUST emit a named derived record set in the
`DocumentStore`. A durable sink MUST save each processor's records in a separate
derived layer.

DocSpec MUST invoke a processor with a closed, reference-based
`ProcessorRequest`. The request MUST name the pinned `ProcessorDescription`,
exact input record identities and digests, prerequisite processor outputs,
allowed fields from the data-use policy, item limits, and invocation identity.
The processor MUST return a closed `ProcessorResult` containing its disposition,
derived records, resource use, warnings, and provider or implementation receipt.
Application services MUST depend on these DocSpec-owned records and ports, not a
concrete processing package's result classes.

A processor MUST NOT:

- mutate a captured file, representation, segment, or prior derived layer;
- change base-layer eligibility or dispositions;
- fetch undeclared external resources;
- write directly to the published artifact namespace; or
- expose provider-specific types to DocSpec application services.

Running a processor in the DocSpec worker pool is an execution choice. It does
not make the processor's semantics part of DocSpec.

A processor adapter MAY delegate to a model SDK, OCR package, dbt invocation,
container, or another maintained transformation tool. DocSpec owns the input and
result records and verifies their declared limits; it does not reimplement that
tool's execution engine.

### 8.3 Processor output

Each output record MUST identify:

- the processor description;
- exact input records and digests;
- output schema;
- output value or object digest;
- provider or implementation receipt;
- warnings;
- review or acceptance state when the processor defines one; and
- final processor disposition.

Each scheduled item MUST end as `produced`, `abstained`, `excluded`,
`accepted-failure`, or `rejected-run`.

### 8.4 Reuse

A deterministic processor result MAY be reused only when all input identities,
processor versions, configurations, external resources, policies, and output
schemas match.

Exact-result reuse MUST be mediated by a `ProcessorResultCache` or stage-result
repository port. The durable cached value is an immutable verified result and
receipt. Redis or another cache MAY index or accelerate that lookup, but a cache
miss or outage MUST fall back to normal execution and MUST NOT change the
logical result or publication decision.

A processor that declares non-deterministic behavior MUST create a distinct run
identity. Its output MUST NOT replace an earlier derived layer in place.

Changing one processor MUST invalidate only that processor and processors that
depend on it. It MUST NOT reacquire unchanged files or rerun unaffected
extractors, segmenters, or processors.

## 9. DocumentStore and result delivery

### 9.1 DocumentStore role

One `DocumentStore` represents one bounded job. It MUST contain:

- store identity, schema version, revision, and state;
- plan identity and logical partition;
- a fixed ordered set of document entries;
- requested extractor, segmenter, and processor stages;
- stage inputs, outputs, dispositions, and warnings by entry;
- content and output locators rather than unbounded inline bytes;
- byte, entry, segment, memory, and time limits;
- attempt and delivery receipts; and
- reconciled counts and final verdict.

The allowed states are `planned`, `running`, and `sealed`. A state transition
MUST create a new store revision. A sealed store is immutable, carries a
`completed`, `accepted-failure`, or `rejected` verdict, and is the authoritative
receipt for that job. An interrupted store remains `running` and resumable; a
failure is a sealed outcome, not mutable runtime state.

A `DocumentStore` MAY include small inline metadata or results within its sealed
byte limit. It MUST reference source files, large representations, images, and
large processor outputs through immutable locators.

### 9.2 Ephemeral and saved stores

A small synchronous job MAY keep its active `DocumentStore` in memory. It MUST
still emit a sealed store receipt before it reports success.

A resumable or distributed job MUST save the planned store before execution and
save verified checkpoints through `DocumentStoreRepository`. A worker restart
MUST reconstruct the job from the saved store and referenced objects without
repeating verified stages.

Checkpoints MUST be granular enough to preserve a verified capture, extraction,
segmentation result, and processor invocation independently. Each checkpoint
MUST have a stable work identity derived from exact inputs and governing policy.
Concurrent or repeated attempts MAY settle the same work identity only when the
verified output is identical.

A saved store MUST use the selected `DocumentStorePersistenceProfile`. The
profile MUST support immutable revisions, bounded reads and writes, checkpoints,
sealed receipts, and independent verification. A store whose entry ledger
exceeds the configured inline limit MUST store the ledger separately through a
bounded record format. A `DocumentStore` MUST NOT become one unbounded object.

A portable profile MAY use a small UTF-8 JSON root with partitioned record
members. SQL, scheduler metadata, or another format MAY implement the same
interface when it preserves the same logical revisions and receipts.

The run-level receipt MUST list every planned store and its terminal store
revision. Missing, duplicate, or unsealed stores MUST fail the run.

### 9.3 ResultSink

`ResultSink` MUST accept a bounded stream of verified records and return a closed
`DeliveryReceipt`. The receipt MUST identify:

- result-sink implementation and configuration;
- source `DocumentStore` and entry population;
- accepted, rejected, retried, and undelivered record counts;
- saved object or stream acknowledgement identities;
- bytes delivered; and
- final verdict.

The receipt counts and entry population MUST be identity-bearing. A successful
receipt MUST account for every offered record exactly once. A parse-only or
structural inspection MUST identify its limited verification scope and MUST NOT
emit a complete `pass` verdict.

Every delivered record MUST carry an idempotency key derived from its source
store, entry, stage, and output identity. A retryable returned-result sink MUST
require the receiver to acknowledge that key. A receiver without idempotent
acknowledgement MAY be used only by an explicitly non-resumable ephemeral job.

DocSpec MUST provide three sink behaviors:

1. a durable dataset sink that publishes immutable objects and partitioned
   record datasets;
2. a returned-result sink that streams records to a caller with backpressure and
   acknowledgement; and
3. a hybrid sink that persists configured objects and returns configured
   records.

The returned-result sink MUST NOT aggregate a multi-million-record result in
memory. A lost or incomplete delivery MUST prevent the `DocumentStore` from
sealing as successful.

### 9.4 Durable dataset sink

The durable sink MUST stage separate immutable layers for:

1. file records and capture dispositions;
2. representations;
3. segments and evidence;
4. each injected processor's derived records;
5. failures and coverage; and
6. document-store, build, delivery, and verification receipts.

A `DocumentRelease` MUST identify the exact active state reference for each
saved layer under its selected profile. A processor-only update MUST be able to
commit a new derived layer and `DocumentRelease` without rebuilding base layers.

The sink stages data; `DocumentCatalog` verifies the staged outputs, reconciles
the sealed stores, and commits the release. A sink MUST NOT advance catalog
state directly.

### 9.5 Storage profile system

DocSpec's logical records and application services MUST remain independent of
physical formats. A deployment MUST select a compatible profile for each role:

- `ReleaseManifestProfile` as a thin adapter that resolves and conditionally
  publishes the shared Rulespec root without defining another format;
- `DocumentCatalogProfile` for catalog lookup, comparison, staging, and commit;
- `RecordStorageProfile` for large logical record layers;
- `BlobStorageProfile` for source bytes and large derivatives;
- `DocumentStorePersistenceProfile` for planned jobs, revisions, checkpoints,
  and sealed job receipts; and
- `ResultDeliveryProfile` for saved, returned, or hybrid delivery.

A profile MUST have a closed machine description containing its role, profile
identifier, version, implementation identity, configuration digest, supported
logical schemas, physical media types, capabilities, limits, compatibility
rules, and verifier identity. The composition root MUST inject the selected
adapters. Core modules MUST NOT import their client libraries or expose their
native objects.

Each stateful `ProcessingPlan` MUST pin the complete selected profile set. Its
`DocumentRelease` MUST pin the same profile identifiers and versions, plus the
opaque state reference and digest for each active logical layer. Changing a
profile or identity-bearing configuration creates a new plan artifact and new
release artifact; it MUST NOT reinterpret an existing release in place. When
the change is physical only and the canonical logical records are identical,
the release `documentStateDigest` and logical ID remain stable while the exact
artifact digest moves.

Every conforming profile set MUST provide:

- bounded streaming reads and writes;
- stable logical identity and deterministic comparison;
- lookup and scan without loading the corpus;
- immutable snapshot or release identity;
- staged writes and one explicit activation point;
- incremental reuse and stable logical partitioning;
- pruning or indexed access suitable for the declared scale;
- closed schema and evolution rules;
- complete membership and integrity verification; and
- export to the canonical logical records used by conformance tests.

The reference implementation MUST provide at least one portable profile set and
at least one profile set that passes the scale class. One profile set MAY satisfy
both requirements. A profile SHOULD be a thin adapter over a maintained package
when one already implements the physical work. Manifest files, Apache Iceberg,
Delta Lake, or a database MAY implement catalog state. Parquet, Arrow IPC, JSON
Lines, or another bounded record format MAY implement record layers. Local
files, S3, R2, or another immutable object service MAY implement blob storage.

JSON Lines and local files are sufficient portable defaults. Parquet on
immutable object storage is the preferred first scale profile when columnar
scans and compression matter. Iceberg or Delta SHOULD be added only when a
deployment needs table transactions, multi-writer commits, long snapshot
history, or row-level table maintenance. DocSpec MUST NOT implement a new table
engine to satisfy this specification.

### 9.6 Canonical DocumentRelease semantic member

Every published `DocumentRelease` is one Rulespec container with product-owned
kind `docspec-document-release`. Its closed product `spec` contains exactly
`releaseSchemaDigest` and `documentStateDigest`.
`DocumentRelease` does not embed Rulespec's optional `DerivationRelation`:
DocSpec's sealed `ProcessingPlan`, exact root inputs, and recomputed logical
state already define this derivation, and a second relation description would
repeat them.
Its root inputs are exactly one `source-catalog` and one `processing-plan`; no
other role is valid. Their logical IDs and exact artifact digests MUST equal the
corresponding verified `release.json` references. The optional prior release is
recorded only as exact publication lineage in `release.json.previousRelease`.
It is integrity-bound by the release artifact digest, but it is not a Rulespec
logical input. No store, profile, receipt, execution artifact, or lineage pin
may add a logical input role. For an incremental publication, that reference
MUST equal the verified ProcessingPlan's evidence-only `previousRelease`
reference. Both are null for an initial build; a mismatch fails publication.
The
Rulespec `artifact.json` is the sole interchange root and carries the shared
logical ID, artifact digest, inputs, `spec`, manifests, membership, and aggregate
member counts. Product completeness and coverage live in `release.json`; the
generic root carries neither. DocSpec MUST NOT publish a second artifact root,
second membership format, or second structural verifier.

All `DocumentCatalogProfile` implementations MUST read and publish one
canonical UTF-8 JSON semantic member named `release.json`, with role
`release-state` and media type
`application/vnd.docspec.document-release+json`:

```text
format: docspec-document-release
formatVersion: 2.0
releaseId: urn:spicy:artifact:docspec-document-release:<logical-digest>
documentStateDigest: sha256:<logical-state-digest>
```

The closed DocSpec product-role vocabulary for this kind is exactly
`release-state`, `record-layer`, `retained-blob`, `document-store-receipt`,
`run-receipt`, and `catalog-commit-receipt`. `release-state` appears exactly
once. The other roles appear only when the selected physical profiles carry
those bytes inside the release container; otherwise `release.json` references
their independently verified immutable state. The DocSpec semantic verifier
derives the actual role set from the manifests and checks it against this
vocabulary. An unknown role cannot authorize itself through artifact-authored
metadata.

The installed package MUST generate and ship
`docspec/schemas/document_release/2.0/document-release.schema.json` from the
same typed domain records used by the serializer and parser. A checked-in
generator test compares the generated schema bytes, installed schema bytes,
record fields, and parser fields exactly. `releaseSchemaDigest` calls the
installed `rulespec-artifacts` `schemaBundleDigest` over the complete product-relative
path-to-schema map. Declared schema IDs and exact installed-file digests remain
artifact evidence.

The top-level `release.json` object has exactly `format`, `formatVersion`,
`releaseId`, `documentStateDigest`, `previousRelease`, `sourceCatalog`,
`processingPlan`, `profiles`, `activeLayers`, `blobRoots`,
`retentionDispositions`, `storeReceiptSetDigest`, `runReceipt`,
`catalogCommitReceipt`, `counts`, `failures`, `coverage`, `partitionPolicy`, and
`physicalShardPolicy`. Its generated nested schemas are:

- `previousRelease`: `null` or a closed object containing `releaseId`,
  `locator`, and `artifactDigest`;
- `sourceCatalog`: a closed object containing `catalogLogicalId`, `locator`,
  `catalogArtifactDigest`, `catalogStateDigest`, `catalogSchemaId`,
  `catalogSchemaDigest`, `requestedUniverseSetDigest`, and
  `selectedSourceSetDigest`;
- `processingPlan`: a closed artifact reference containing `artifactId`,
  `locator`, `digest`, `mediaType`, and `byteSize`; `artifactId` is the verified
  ProcessingPlan Rulespec logical ID;
- `profiles`: exactly one sorted pin for each of the six registered profile
  roles, with each pin's role, profile ID, version, implementation ID,
  configuration digest, description digest, and capabilities;
- `activeLayers`: sorted closed objects containing `layerId`, `layerKind`,
  `schemaId`, `schemaDigest`, `profileId`, `stateRef`, `stateDigest`,
  `logicalRecordsDigest`, and `recordCount`; `stateDigest` is the exact digest of
  the bounded immutable physical state root returned and verified by the
  selected profile;
- `blobRoots`, `runReceipt`, and `catalogCommitReceipt`: closed artifact
  references; `blobRoots` is sorted and duplicate-free;
- `retentionDispositions`: the closed versioned `RetentionPolicy` record;
- `storeReceiptSetDigest`: the installed Rulespec `framedSectionDigest` with
  domain `docspec-store-receipt-set/1`, one `receipts` section, and complete
  closed `StoreRef` records containing exactly `storeId`, `revision`, `locator`,
  and `digest`, sorted by unique `storeId`;
- `counts`: a closed object with non-negative `sourceItems`, `documents`,
  `documentVersions`, `files`, `representations`, `segments`,
  `excludedRanges`, and `derivedRecords`;
- `failures`: a sorted array of closed objects containing `failureCode`,
  `sourceItemId`, `stageId`, `disposition`, and `evidenceDigest`;
- `coverage`: a closed object containing `complete`, the selected-source,
  document-version, representation, and segment set digests, and visible-text,
  exclusion, processing-failure, and evidence-resolution counts; and
- `partitionPolicy`: a closed logical object containing `policyId`,
  `policyVersion`, `policyDigest`, and `bucketCount`; `policyDigest` covers only
  stable logical bucket assignment; and
- `physicalShardPolicy`: a closed evidence object containing
  `targetMemberBytes` and `hardMaxMemberBytes`.

Every registered layer schema declares one immutable digest domain, complete
logical-record projection, and total-order key. `logicalRecordsDigest` calls the
installed Rulespec `framedSectionDigest` with that domain and one `records`
section. The selected physical profile supplies the bounded iterable and
declared count but cannot change the projection, ordering, or framing. Duplicate
or out-of-order logical keys fail before the helper runs.

Unknown, missing, extra, mistyped, unsorted, or duplicate content fails before
publication or read. Required evidence fields never become optional merely to
preserve a logical ID; the state-digest preimage below selects semantic fields,
while `artifactDigest` binds the entire required member.

This member describes DocSpec logical state inside the shared artifact; it is
not independently addressed or pinned. Its `releaseId` MUST equal the Rulespec
root `logicalId`. Its `documentStateDigest` MUST equal the product `spec` value.
The installed `rulespec-artifacts` reader first verifies the complete physical distribution.
The installed DocSpec verifier then validates this member, the product `spec`,
required roles, and DocSpec logical
invariants through the same bounded `MemberSource`. All catalog state, record
layers, blobs, saved jobs, and delivery mechanisms remain profile-selected. The
declared DocSpec payload-role set is complete; there are no generic producer-code
member roles.

DocSpec computes `documentStateDigest` before it computes the Rulespec logical
ID. It calls the installed Rulespec `framedSectionDigest` with domain
`docspec-document-state/2` and these ordered sections:

1. `sourceCatalog`: one record containing the exact `SourceCatalog` logical
   digest, `catalogStateDigest`, schema digest, requested-universe and
   selected-set digests, and catalog-policy digest.
2. `processingPlan`: one record containing the semantic ProcessingPlan logical
   digest derived from `processingPlan.artifactId` and every behavior-bearing
   policy.
3. `activeLayers`: one record per active logical layer containing its `layerId`,
   `layerKind`, semantic `schemaDigest`, `recordCount`, and
   `logicalRecordsDigest`, sorted by `(layerKind, layerId)` with duplicates
   refused.
4. `retentionDispositions`: one complete logical retention record.
5. `counts`: one complete semantic-count record.
6. `failures`: the complete accepted product-failure records sorted by
   `(sourceItemId, stageId, failureCode, disposition, evidenceDigest)` with
   duplicates refused.
7. `coverage`: one complete logical coverage record.
8. `partitionPolicy`: one record containing exactly `policyId`,
   `policyVersion`, `policyDigest`, and `bucketCount`.

Each section preserves explicit nulls. Singleton sections have declared count
one. The digest contains no `releaseId`, execution receipt, or physical storage
selection.

The digest uses the final 64-hex logical digest suffixes of
`sourceCatalog.catalogLogicalId` and `processingPlan.artifactId`, plus the listed
catalog state, schema, and policy digests. It excludes every profile
  pin, every active-layer `profileId`, `stateRef`, and `stateDigest`, and every
  locator or exact physical pin in `previousRelease`, `sourceCatalog`,
  `processingPlan`, and `blobRoots`, including `artifactDigest`,
  `catalogArtifactDigest`, and `digest`.
It excludes declared `catalogSchemaId` and active-layer `schemaId` labels while
including their semantic schema digests.
It also excludes `previousRelease` in full and the complete
`physicalShardPolicy`; lineage and shard sizing cannot change logical state.
Retained blob content remains identified by the canonical logical records and
active-layer digests that reference it; moving or repacking those bytes cannot
change logical state.

Transient failures, attempts, workers, timestamps, resource use, scheduler
events, the execution profile, opaque profile state references, the sealed
`DocumentStore` receipt-set digest, run receipt, catalog-commit receipt, and all
physical reference fields are artifact evidence, not logical-state inputs. They
remain required in `release.json`; Rulespec membership and `artifactDigest` bind
their exact bytes. The exact SourceCatalog `artifactDigest` also remains in its
Rulespec input pin and `release.json` reference for admission, while
`documentStateDigest` consumes the verified catalog logical digest and
`catalogStateDigest` instead.
Changing only that evidence MAY change the physical artifact digest but MUST NOT
change `documentStateDigest` or the release logical ID. Changing any logical
layer, retained blob content identified by a logical record, semantic
disposition, accepted product failure, coverage, or behavior policy MUST change
both.

The DocSpec verifier MUST recompute the ProcessingPlan logical ID and
`documentStateDigest`, then let Rulespec re-derive the root logical ID from the
product kind, logical input identities, and complete product `spec`. This order
is acyclic and refuses different logical output under one logical ID without a
duplicate relation description.

The release MUST describe complete current logical state. It MAY reuse objects,
record files, manifests, database pages, or backing snapshots from prior
releases, but a consumer MUST NOT replay release history to discover current
membership.

The reconciler MUST verify every staged layer against its selected profile
before it conditionally publishes the shared Rulespec root. Workers MUST NOT
activate catalog state for individual documents or stores. If a profile updates
several tables, files, or services, those updates are staging data until the
shared root publishes. Unpublished state MAY be collected only after its
retention period.

Release verification MUST cover every referenced active layer, sealed store,
profile state root, captured source object, retained representation object, and
retained segment object. A verifier MAY reuse a digest-bound verification
receipt or disposable verification cache. It MUST fail when an authoritative
object is absent or differs, regardless of cache availability.

### 9.7 Format-neutral layer requirements

The core MUST define layer membership, logical schemas, evidence, and identity
without requiring a physical record layout. Every `RecordStorageProfile` MUST
map its physical members to those logical records and prove bounded streaming,
partition pruning, closed-schema validation, incremental partition reuse, and
independent verification.

Every `BlobStorageProfile` MUST support immutable, content-addressed objects and
SHA-256 integrity. Every saved receipt and layer root MUST remain small and
bounded; a profile MUST move large membership or record lists into declared
members rather than one expanding root object.

An Iceberg profile MAY pin table and snapshot identifiers. A manifest profile
MAY pin immutable record members. A Parquet profile MAY store closed-schema
record layers. These are adapter choices, not DocSpec domain types.

DocSpec MUST ship one optional registered Parquet record-storage profile as the
portable queryable-layer reference. It writes one or more bounded immutable
Parquet members per `(layerKind, partitionId)` and records each member's schema,
digest, byte size, row count, and partition identity through the Rulespec
manifest. It MUST support projection and partition pruning, bounded streaming,
independent verification, and lazy optional imports. A conformance query MUST
read one selected partition without materializing the complete layer. The same
logical rows written through a local JSON Lines profile and the Parquet profile
MUST have identical logical layer and `documentStateDigest` inputs even when
their artifact digests differ.

### 9.8 Partitioning

The partition policy MUST assign stable logical buckets from source-item
identity. A catalog or source partition MAY provide an additional prefix.

Each saved layer root MUST list the complete active partition map. An
incremental run MUST reference unchanged partitions by digest and replace only
affected partitions. It MUST NOT rewrite every partition to publish a small
update.

When the selected Rulespec container profile stores immutable members outside
the new artifact prefix, unchanged partitions MUST use Rulespec `blobRef`
descriptors whose value is the verified content digest. The release builder
must not copy those bytes merely to create a successor prefix. It still opens
and verifies every reference and recomputes complete logical layer state. New or
changed partitions, local manifests, producer evidence, receipts, and the root
are written as new publication bytes. The injected `BlobSource` resolves
content; DocSpec domain code never constructs an object-store client.

Physical shard targets MUST be configurable. Each record profile MUST declare a
recommended target and a hard safety limit, and MUST reject a member above that
limit. The scale profile MUST record the active values.

Partition identity MUST include schema, partition policy, and ordered logical
content. Physical row-group layout MAY vary without changing logical records.

### 9.9 Artifact integrity and publication

Every saved document store and layer MUST use a closed, versioned product
schema. Every published `DocumentRelease` MUST use the one Rulespec root and
MUST:

- derive its logical identity from canonical identity-bearing DocSpec content;
- declare every member, role, media type, byte size, digest, record count, and
  schema through Rulespec manifests;
- rely on Rulespec admission to reject missing, extra, duplicate, escaped, or
  symlinked members before DocSpec semantic verification;
- verify DocSpec meaning before publishing the shared root;
- refuse to replace an existing root; and
- remain complete without reading an earlier root.

Writers MUST write new objects and partitions into an unpublished namespace.
`DocumentCatalog` MUST verify the complete output before publishing the
`DocumentRelease` root with a conditional create. Duplicate, stale, or
conflicting attempts MUST NOT publish.

Garbage collection MUST be a separate, explicit maintenance operation. It MUST
produce a dry-run inventory, honor minimum retention, and preserve every object
reachable from a retained store, profile state reference, or `DocumentRelease`
root.

### 9.10 FR-Mirrulations v3 transition gate

> **Status 2026-08-30 — gate dropped by product ruling.** The v3 migration
> differential served migration fidelity, a goal the owner retired with the
> other migration ceremony; the 10k campaign checkpoint superseded the 1k v3
> corpus as the demonstration input. The sealed corpus remains archived at
> `~/Work/corpora/fr-mirrulations-1k-v1` (digest-verified, reversible), and
> per REF-048 the read-only adapter this gate would have required retires
> with it. Recorded in the platform plan and the 2026-08-29 document
> consolidation [R3].


The known-good 2026-08-10 FR-Mirrulations release is mandatory migration input,
not disposable compatibility debt. The old artifact is
`spicyregs-document-release/3.0` with release digest
`2def04c320d754b0e1291ad5b88c32851c1955ad95146e17619159f6080c88b4`.
It contains 28 files, and its pinned historical verifier proves all 25 members
and 13 schemas. The current namespace spelling does not authorize rewriting the
old root or declaring the corpus invalid.

One repository-local DocSpec migration command MUST:

1. materialize the exact predecessor Git revision in a temporary worktree and
   run its pinned serializer, schema vocabulary, reader, and verifier in an
   isolated process; none is copied into or shipped with DocSpec;
2. reproduce its root identity and verify every member from the artifact bytes
   without reading a mutable producer checkout;
3. emit one `docspec-document-release` through the canonical builder and exact
   pinned `SourceCatalog` and `ProcessingPlan` inputs;
4. compare every old and new logical document, file, representation, segment,
   layer record, source/rendition link, evidence coordinate, and retained byte
   digest; and
5. classify every difference as identity/schema representation, the sealed
   number-format transition, or an accepted DocSpec semantic mapping. An
   unclassified value, missing member, unresolved coordinate, or changed source
   byte fails.

The command emits one closed
`docspec-document-release-migration/1.0` receipt containing the exact old and
new artifact pins, historical Git and verifier implementation IDs,
SourceCatalog and ProcessingPlan pins, old/new member inventories and
logical-state digests,
sorted member- and field-level differences with classifications, counts,
command, verifier identity, and pass verdict. An independent verifier reopens
both releases and recomputes the receipt. The one-time command cannot write a
new v3 artifact and is never packaged as a DocSpec runtime surface.

The same gate reruns the exact sealed 1,000-document FR-Mirrulations campaign
end to end. It compares the complete expected document set, active logical
layers, evidence resolution, coverage, and output digests and runs the canonical
release through the local and qualified distributed paths. A skip, reduced
sample, changed input set, or unexplained result difference fails. The v3
corpus locator and historical Git reference remain available until the migration
receipt and campaign receipt both pass and the public-surface roster records
every consumer cutover. Current packages carry no v3 reader or compatibility
mode.

## 10. Execution and recovery

### 10.1 Work state

The control plane MUST distinguish:

- plan;
- base `DocumentRelease` and opened catalog snapshot;
- logical partition;
- planned `DocumentStore`;
- active store revision;
- attempt;
- verified entry and stage result;
- delivery receipt;
- sealed store receipt;
- run receipt;
- layer build;
- staged document-catalog update;
- `DocumentRelease` verification; and
- release-root publication.

Backing-profile staging and commit steps apply only when the result sink
persists data.
Every stateful run commits a `DocumentRelease`, including a run that retains only
metadata and delivery receipts.
These units MAY coincide in a small local run. The system MUST keep their
identities distinct.

### 10.2 Bounded work

The planner MUST bound each `DocumentStore` by entry count, estimated bytes,
pages or frames, expected segments, processor cost, memory, and duration.
Document count alone is insufficient.

Acquisition, extraction, segmentation, processing, and writing MUST use bounded
queues. Slow stages MUST apply backpressure rather than grow an unbounded
in-memory backlog.

Those queues MAY be supplied by the selected execution tool, object-store
client, or sink library. DocSpec MUST declare and verify the active bounds and
MUST keep its messages small; it need not implement the queue itself.

No coordinator operation may require all source items, files, segments, or
document stores in one in-memory object graph. It MAY keep bounded active-store
summaries and a partition-level run ledger.

### 10.3 Retry and resume

The control plane MUST classify failures as:

- transient external failure;
- transient resource failure;
- deterministic input failure;
- policy exclusion;
- artifact-integrity failure; or
- implementation defect.

Retries MUST have finite limits and bounded backoff. The `ProcessingPlan` owns
which failures may be retried and the maximum semantic stage attempts. The
execution tool MAY own task retry timing and worker replacement under the sealed
`ExecutionProfile`. A resumed run MUST verify a completed stage, entry, or store
checkpoint before reuse. Stale attempts and duplicate delivery MUST remain
idempotent.

The sealed store and run receipt MUST preserve terminal failures and accepted
exclusions. A durable sink MUST also preserve them in its release.

### 10.4 Execution backends

DocSpec MUST expose these scheduler-neutral application functions:

```text
plan_run(source_catalog_ref, base_document_release_ref, plan_ref)
    -> execution_handoff_ref
planned_stores(execution_handoff_ref) -> stream[planned_document_store_ref]
execute_store(planned_document_store_ref) -> processed_document_store_ref
deliver_store(processed_document_store_ref, sink_ref) -> sealed_document_store_ref
reconcile_run(execution_handoff_ref, stream[StoreTaskResult])
    -> run_receipt_ref
commit_release(base_document_release_ref, run_receipt_ref) -> document_release_ref
```

Each argument and result MUST be a small, serializable value or immutable
reference. Scheduler messages MUST NOT contain source files, complete result
sets, provider clients, open streams, or framework-specific objects.

`plan_run` MUST save the planned jobs before returning the one sealed handoff
reference. `planned_stores` streams that handoff's ledger; a scheduler adapter
MAY wrap each reference in a `StoreTask`. The complete handoff MUST have a small
sealed root that identifies:

- the `ProcessingPlan` and `ExecutionProfile`;
- the planned-store ledger and exact expected task count;
- the worker-composition profile;
- the result sink;
- the base `DocumentRelease`, if any; and
- the task and result schema versions.

The handoff root MUST reference a bounded, streamable member when the job list is
large. It MUST NOT inline millions of task objects.

The same functions and task semantics MUST run through:

- a local execution backend; and
- one maintained external scheduler adapter.

Backend selection MUST NOT change logical output. Each backend MUST accept the
same `StoreTask` and return the same `StoreTaskResult` shape. A result contains
the exact task identity and exactly one verified sealed-store reference or
persisted terminal-failure reference. Worker loss, coordinator restart,
duplicate delivery, and slow storage MUST affect only incomplete work.

An execution backend MUST accept a stream of `StoreTask` values and return a
stream of `StoreTaskResult` values. Results MAY arrive out of order and MAY be
replayed. DocSpec MUST match them to the sealed planned-store ledger, verify the
task identity and referenced store revision or terminal-failure record, and
deduplicate an identical replay. An unknown task, conflicting duplicate,
missing terminal task, result containing both terminal forms, or unsealed result
MUST fail run reconciliation. No facade or scheduler adapter may discard the
task identity or convert the stream to bare store references.

Workers MUST send source bytes, representations, segments, and large derived
records directly to the injected blob store or result sink. The scheduler or
queue carries only task references, terminal references, progress, and bounded
diagnostics. A returned-result sink MAY stream bounded records through the same
caller connection, but those records remain a sink stream with acknowledgement;
they are not scheduler task metadata.

A purely ephemeral invocation MAY stop after returning its sealed stores and run
receipt. Such an invocation is stateless: it MUST NOT advance `DocumentCatalog`
and MUST NOT serve as the base of an incremental run.

### 10.5 Scheduler portability

An external scheduler such as Dagster MAY map one `DocumentStore` reference to
one operation, dynamic task, or partition. The scheduler owns worker placement,
triggers, queues, concurrency, task retry timing, event storage, and monitoring.
DocSpec owns task identity, idempotency, checkpoint verification, dispositions,
delivery receipts, expected-task reconciliation, and release publication.

The portable seam MUST support three deployment capabilities without making
them domain dependencies: observing a newly admitted source-catalog identity,
mapping its sealed planned-store ledger to stable partition tasks, and injecting
blob/result resources that keep large bytes out of scheduler messages. An
adapter MAY implement the observation as a sensor, event subscription, poller,
or explicit invocation. Re-observing the same catalog and plan identity MUST be
idempotent. Retrying one failed partition MUST NOT rerun or rewrite verified
sibling partitions.

The registered Dagster adapter MUST demonstrate these capabilities with a native
sensor or equivalent event trigger, dynamic or partitioned work keyed by the
DocSpec task/partition identity, and injected blob/result resources. Dagster
asset, partition, sensor, event, and resource objects remain inside the adapter.
The core sees only the sealed handoff and DocSpec ports. Another maintained
scheduler can replace Dagster without changing the handoff, task, store,
release, or logical output schemas.

Adapters MUST NOT require DocSpec domain code to import the scheduler. A Dagster
job, queue consumer, Ray task, or local loop MUST call the same application
functions. A scheduler adapter SHOULD translate DocSpec task records to the
tool's native operations and translate its terminal events back; it MUST NOT
reimplement the tool's scheduler inside DocSpec.

`SCHEDULER-PORTABILITY` MUST execute one shared fixture graph through the local
backend and either Dagster or another maintained scheduler. Both runs MUST
produce equivalent sealed stores and run receipts. The test MUST also prove that
tasks and results survive serialization, out-of-order results reconcile, and
replaying one scheduled store does not duplicate saved output or returned data.
It MUST also prove that one catalog event creates exactly the expected task set,
replaying the event creates no second logical run, retrying one failed partition
preserves completed sibling outputs, and a large captured file travels through
the injected blob store rather than scheduler metadata.

An adapter test that calls an external scheduler's in-process helper proves only
the adapter's local composition. External-scheduler conformance remains pending
until the same serialized handoff crosses a real worker-process boundary through
deployment-owned Dagster resources and an injected executor. Dagster's run and
event storage records that qualification; DocSpec does not define a second
scheduler profile, deployment file, worker protocol, or qualification-receipt
schema.

## 11. Initial backfill and incremental operation

### 11.1 Initial backfill

The initial backfill MUST:

- seal the complete source-catalog input before work begins;
- partition work into bounded `DocumentStore` jobs before material execution;
- save each planned store before distributed execution;
- checkpoint verified entry stages and store revisions;
- seal every store and one reconciled run receipt;
- reconcile every selected item; and
- commit the initial `DocumentRelease` through `DocumentCatalog`.

The backfill MAY take hours or days under its approved profile. It MUST not
become a recurring requirement for normal updates.

### 11.2 Incremental update

An incremental update MUST:

- open and verify the prior `DocumentRelease` through `DocumentCatalog`;
- consume a new complete source-catalog snapshot;
- identify changed source items and invalidated descendants;
- create stores only for changed or explicitly repaired work;
- reuse unchanged objects, records, and partitions by digest;
- rebuild only affected logical buckets;
- record additions, changes, deletions, repairs, and exclusions; and
- seal a new run receipt and commit a successor `DocumentRelease` with a complete
  active catalog state.

`INCREMENTAL-EQUIVALENCE` MUST prove that the `DocumentCatalog` records opened
from an incremental `DocumentRelease` equal a clean build over the same current
source catalog and policies. Physical layout, backing snapshots, and reused
object locations may differ.

### 11.3 Targeted reprocessing

An operator MUST be able to create targeted `DocumentStore` jobs for one
extractor, segmenter, processor, failed population, source partition, or logical
bucket without reacquiring unrelated files.

The resulting run receipt MUST state the exact selected population. A stateful
rerun MUST commit a `DocumentRelease` that preserves complete catalog state by
referencing unchanged partitions or backing state references.

### 11.4 Compaction

For a durable sink, compaction MAY combine small physical shards. It MUST commit
new backing state and a new exact `DocumentRelease` artifact, preserve all
active logical records and evidence, and avoid source reacquisition or semantic
reprocessing. The compacted artifact records the prior exact release as
publication lineage, but preserves `documentStateDigest`, release `logicalId`,
and the two logical root inputs. Changed manifests, shard evidence, and lineage
move `artifactDigest`. Compaction therefore creates a new physical publication
of the same logical release, not a new logical release.

## 12. Operator interface

One `docspec` command MUST expose the lifecycle:

```text
docspec source-catalog build
docspec source-catalog verify
docspec profile list
docspec profile verify
docspec document-catalog open
docspec document-catalog compare
docspec plan create
docspec document-store create
docspec document-store verify
docspec run prepare
docspec run start
docspec run resume
docspec run reconcile
docspec run status
docspec task execute
docspec sink verify
docspec document-release commit
docspec document-release verify
docspec document-release diff
docspec document-release compact
docspec blob-store verify
docspec blob-store gc --dry-run
docspec conformance run
docspec conformance report
```

Every mutating command MUST accept an explicit destination, refuse replacement
by default, and emit a machine receipt. Read-only commands MUST make no external
state change.

`docspec source-catalog build` is the outer composition root. It accepts one or
more repeated exact `--source-native <locator>
--source-native-artifact-digest <digest> --source-native-blob-store <locator>
--source-native-profile <profile>` groups, one closed `--catalog-policy <path>`,
and explicit `--destination` and `--receipt` paths. The receipt path MUST name
`<destination>/source-catalog-build-command-receipt.json` so the required
machine receipt and immutable store cross one atomic publication boundary. The
handler lazily constructs the DocSpec adapter from the
installed SpicyRegs `SourceNativeReleaseReader` and injects it as
`SourceNativeRecordSource`; catalog policy code never imports SpicyRegs. The
command has no storage-mode or base-catalog options. It derives all logical IDs
and digests; no caller supplies them. It refuses an unknown profile, missing
producer package, unknown policy or source schema, incomplete semantic proof,
and an existing destination. A catalog accounts for every row in its exact
requested universe whether an upstream release proves a complete source
snapshot or explicitly reports an observed crawl; it preserves that scope in
the input identity and does not broaden the source claim.

The command builds one complete snapshot in an unpublished staging location,
reuses verified content-addressed partition blobs, runs `rulespec-artifacts`
structural and DocSpec semantic verification, writes the closed build receipt
described in §5.1 plus the command receipt into that staged destination, and
conditionally creates the immutable destination only after both pass. It MUST
NOT delete a published destination to compensate for a later operation.
The atomically published command receipt pins every input, policy, destination,
resulting logical and artifact IDs, byte-write measurements, verifier, and pass
verdict. A read-only `source-catalog verify` call supplies that emitted
`receiptId` as the expected command-receipt identity, so a different
self-consistent receipt is not accepted as the original build record. A
clean-wheel end-to-end test builds an initial and changed successor snapshot
from published source-native artifacts with every sibling checkout absent. It
proves exact reuse of unchanged `blobRef` values, a new logical identity for
changed state, bounded newly written bytes, replacement refusal, machine-receipt
verification, and base-package operation without SpicyRegs installed.

The local reference adapter follows the Rulespec local-filesystem trust
boundary. Kernel conditional creation and persistent advisory locks coordinate
cooperative writers using the adapter; deployments give mutually untrusted
writers separate operating-system accounts, filesystem permissions, or object
store credentials. Readers still pin directory identity and verify complete
distribution and blob digests at every admission and open.

An abrupt process exit before the final conditional rename may leave a hidden,
unpublished staging directory beside the requested destination. That directory
has no published name or authority, does not block a retry, and MUST NOT be
admitted as a catalog. Deployment housekeeping may remove such staging only
after it has established that no writer using the same destination parent is
active.

Commands MUST use explicit files, immutable locators, or released packages.
They MUST NOT select sibling worktrees or mutable producer state implicitly.

`run prepare` MUST save the bounded jobs and emit the sealed execution-handoff
reference without executing them. `task execute` MUST accept one serialized task
and emit one serialized task result, so an external tool can use it as a worker
entry point. `run reconcile` MUST consume the exact sealed handoff reference plus
a bounded result stream or saved result ledger. It MUST compare terminal results
with that handoff's complete planned-store ledger and expected task count; stream
exhaustion alone never proves completeness. `run start` MAY compose those
operations as a local convenience. None of these commands may print bulk file or
record payloads as scheduler metadata.

## 13. Scale profile

### 13.1 Sealed profile

Before a scale run, DocSpec MUST seal a `ScaleProfile` that identifies:

- exact corpus or deterministic generator;
- real input-shape sample and sampling method;
- file, image, page, byte, representation, and segment distributions;
- extractor, segmenter, and processor graph;
- worker and coordinator resources;
- document-store sizing policy and result sink;
- selected storage profile set, document-catalog adapter, and base release;
- storage and network placement;
- cache state;
- partition and task policy;
- wall-time and resource targets; and
- acceptance authority.

The `ScaleProfile` MUST pin the `ProcessingPlan`, `ExecutionProfile`, execution
tool's configuration digest, and exact storage and sink profiles. The execution
tool and storage packages MAY supply scheduling, queue, cache, network, and
resource measurements. DocSpec MUST preserve their evidence references and the
DocSpec task, store, byte, and release counts needed to verify the claim.

A changed corpus, processor graph, resource allocation, or target creates a new
profile.

The same `ScaleProfile` family governs catalog construction and document
processing; DocSpec MUST NOT add a second source-catalog scale-profile format.
The profile model MUST identify a closed workload kind. For a
`source-catalog` workload it MUST replace processing-only fields with one
closed catalog-workload section that pins the source-native input artifact set,
catalog policy, requested universe, builder and verifier,
bounded join/order/set-proof strategy, output profile, command, reference
machine and resources, cold/warm cache state, measurement method, absolute
ceilings, and acceptance authority. The checked-in schema, parser, generator,
and tests MUST evolve together before such a profile can be called sealed.

Each scale run MUST emit one closed, content-addressed `ScaleResult`. The result
MUST bind the complete `ScaleProfile` pin, including its locator; the workload
kind; the exact input and output artifact pins; item, partition, task, store,
release, and byte counts; wall time and peak resource measures; evidence pins;
the first failure when present; and a `pass` or `fail` verdict. A `pass` MUST
remain within the profile's declared targets, resources, and absolute ceilings.
This evidence model records a completed run; it does not add scheduler state or
campaign execution machinery to DocSpec.

### 13.2 Ordered campaigns

The reference implementation MUST pass these campaigns in order:

1. 100,000 representative image or page units;
2. 1,000,000 representative image or page units; and
3. at least 5,000,000 representative image or page units.

Samples MUST cover byte-size, page-count, segment-count, media-type, and
observed-cost ranges. A convenient prefix is not representative.

### 13.3 Base-platform targets

With source bytes colocated with the processing store, the five-million-unit
base campaign SHOULD complete within 24 hours on no more than 256 effective
worker CPUs. The profile MUST set an absolute deadline before execution.

The base campaign includes file verification, reference extraction,
segmentation, partition writing, document-catalog commit, and release
verification. Source-limited download time MUST be measured separately.

Injected processors have independent throughput targets because their cost may
range from a local calculation to model inference. Each processor required by a
scale campaign MUST declare its own deadline, concurrency, provider limits, and
cost estimate before the run.

Scale conformance measures the composed deployment; it does not require DocSpec
to provide its own distributed scheduler, cache server, object store, or table
engine. A campaign MAY use Dagster for scheduling, Redis for disposable
coordination or lookup acceleration, an object store for bytes, and a maintained
Parquet or Iceberg implementation for records, provided all selected adapters
meet DocSpec's identity, boundedness, and verification rules.

The reference profile has these hard bounds:

<!-- markdownlint-disable MD013 -->

| Measure | Bound |
| --- | --- |
| Worker peak resident memory | 8 GiB or less |
| Coordinator peak resident memory | 16 GiB or less |
| Coordinator item growth | Bounded by active stores, workers, and partition count, not corpus size |
| Unexplained item loss | Zero |
| Unsealed store reported as successful | Zero |
| Worker-initiated catalog activation | Zero |
| Partial root publication | Zero |
| Reacquisition during processor-only rerun | Zero unchanged files |
| Reprocessing during a metadata-only update | Zero unchanged content |

<!-- markdownlint-enable MD013 -->

### 13.4 Incremental target

After the initial backfill, an update of at most 10,000 changed source items and
10 GiB of changed bytes SHOULD complete delivery within 30 minutes on the
reference hardware, excluding source-imposed delay and processor profiles with
longer sealed deadlines.

The update MUST report bytes and records reused, rewritten, and newly produced.
It MUST prove that unaffected partitions were referenced rather than rewritten.

### 13.5 Prediction and recovery

The million-unit campaign MUST predict full-campaign worker-hours, wall time,
final storage, and temporary storage. The full campaign MUST land within 25% for
worker-hours, 30% for wall time, and 20% for each storage measure.

Before the million-unit campaign, a two-worker-node test MUST prove safe recovery
from worker loss, coordinator restart, duplicate delivery, slow storage, full
local disk, oversized input, and one deterministic processor failure.

## 14. Security and policy

Readers MUST reject path traversal, symlink escape, unsafe archive members,
decompression beyond configured limits, and objects above the active byte limit.

Logs and receipts MUST exclude credentials and provider secrets. Store profiles
MUST declare access, encryption, region, retention, and redistribution rules.
Unknown policy MUST stop publication.

An external processor MUST receive only fields allowed by the sealed data-use
policy. The system MUST record or digest its requests and responses as that
policy requires. Failure MUST NOT trigger an undeclared provider or processor.

## 15. Machine evidence and conformance

### 15.1 Evidence report

Each conformance run MUST emit a closed JSON report containing:

- specification ID and normative source path;
- conformance class;
- the exact clean Git commit containing the normative specification, matrix,
  implementation, tests, and dependency lock;
- exact input and output identities and digests;
- plan and configuration identity;
- command and environment identity;
- required test identifiers and verdicts;
- document-store counts, dispositions, retries, and failures;
- bytes read, reused, written, delivered, and published;
- wall time and peak memory when applicable;
- verifier identity and version;
- first registered failure code; and
- overall `pass` or `fail`.

A required test that is absent, skipped, or xfailed MUST fail the class.
A release conformance run MUST also fail when its source is dirty or is not a Git
checkout. The commit and normative path pin repository source; the runner MUST
NOT manually walk and hash selected repository files. Runtime packages, inputs,
outputs, and other bytes exchanged outside Git retain their normal content
digests.

### 15.2 Required tests

<!-- markdownlint-disable MD013 -->

| Test ID | Required proof |
| --- | --- |
| `CORE-INSTALL` | A built wheel installs, imports, and shows help in an empty environment |
| `BOUNDARY-IMPORT` | Core code depends only on DocSpec ports and domain records; vendor and concrete processing types remain in adapters; the optional SpicyRegs adapter loads only at the CLI composition root |
| `BOUNDARY-CODE` | Git history, package boundaries, static checks, and focused behavior fixtures prove production code contains no copied predecessor implementation; no project-owned source archive or fingerprint corpus exists |
| `SOURCE-CATALOG-CONTRACT` | The DocSpec builder, reader, and semantic verifier pass the sole complete-snapshot form, closed input/payload roles, multi-source sets, policy, migration-fixture identity, every field and disposition, source-URL and immutable-object candidates, exact decision-order/sampling/join/rendition/topic-recovery differential, Federal Register malformed-RIN and missing-agency/rendition isolation, the pinned 30-row empty-topic interpretation, exact shared framed `catalogStateDigest`, `U`, and `S` projections including empty-set and duplicate cases, deterministic partitions, unchanged `blobRef` reuse, reconciled byte writes, changed-state identity, legacy change-set refusal, consumer receipt verification without a second full semantic pass or prior-catalog replay, logical-versus-physical identity, immutability, and bounded streaming |
| `PROFILE-DESCRIPTION` | Every selected profile has a closed, versioned, digest-pinned description with declared capabilities and limits |
| `RELEASE-MANIFEST` | Every catalog profile reads and publishes the shared Rulespec root with one `release.json` semantic member, enforces the closed product-role vocabulary from DocSpec authority, and rejects unknown or incomplete state |
| `DOCUMENT-CATALOG-CONTRACT` | Every registered catalog profile opens, compares, stages, and commits the same logical release fixtures |
| `RECORD-STORAGE-CONTRACT` | Every registered record profile streams, partitions, prunes, reuses, and verifies the same logical layer fixtures; the optional Parquet profile reads one selected partition without materializing the complete layer |
| `PROFILE-COMPATIBILITY` | Each supported profile set composes successfully or fails before work begins with a registered incompatibility |
| `DOCUMENT-STORE` | Planned, stage-checkpointed, ephemeral, rejected, accepted-failure, and completed store fixtures reconcile with complete resource and disposition counts |
| `BLOB-STORE-CONTRACT` | Local, S3, and S3-compatible blob stores pass one immutable-object suite |
| `ACQUISITION` | Capture, deduplication, deletion, retry, byte-limit, and disposition paths reconcile |
| `REPRESENTATION` | Supported content produces identified, receipted representations |
| `SEGMENTATION` | Deterministic segments and coverage pass for every supported content type |
| `EVIDENCE-ROUNDTRIP` | Every fixture segment resolves to its representation and exact source file |
| `COMPLETE-SEARCH-CORPUS` | Every selected source maps to one searchable document with gapless visible-text accounting, exact evidence, no accepted failure, and clean-package verification |
| `PROCESSOR-CONTRACT` | Fake and real processor adapters pass reference-based input, dependency-output, limit, cache-outage, retry, data-use, output, and provenance tests |
| `RESULT-SINK` | Durable, returned-result, and hybrid sinks pass complete receipt, delivery, replay, acknowledgement, and backpressure tests |
| `SCHEDULER-PORTABILITY` | The same serialized task handoff crosses local and external process boundaries, maps one catalog event to the exact stable partition task set, isolates partition retry, routes large bytes through injected stores, and produces equivalent sealed stores and run receipts |
| `DOCUMENT-RELEASE-INTEGRITY` | Layer and DocumentRelease fixtures verify every retained authoritative object, recompute the ProcessingPlan logical ID and `documentStateDigest`, reject different output under one logical ID, preserve logical ID across execution-only or storage-profile changes with identical logical rows, and reject invalid snapshots or missing source bytes |
| `INCREMENTAL-EQUIVALENCE` | Initial, incremental, targeted, and compacted active states reconcile; unchanged partitions retain exact `blobRef` values, and physical-only compaction preserves `documentStateDigest` and logical ID while moving the exact artifact digest |
| `RECOVERY` | Interruption and duplicate-delivery scenarios reuse verified work safely |
| `SCALE` | Ordered campaigns satisfy the sealed profile |
| `PACKAGE-RELEASE` | Published package and fixtures install and verify by version and digest |

<!-- markdownlint-enable MD013 -->

Each registered optional profile adds its own required test. For example, an
Iceberg catalog profile MUST pass `ICEBERG-DOCUMENT-CATALOG`, including staged
files, batched commits, pinned snapshots, conflicts, and abandoned-staging
reconciliation. An unregistered optional profile does not add that test to the
core conformance class.

`BLOB-STORE-CONTRACT` is the single required ID for local, Amazon S3, and
S3-compatible adapters; provider-specific duplicate IDs are forbidden. The
FR-Mirrulations migration remains the one-time retirement gate in §9.10 and is
not a permanent core conformance ID.

### 15.3 Negative fixtures

Artifact fixtures MUST include:

- unknown format or schema version;
- changed identity-bearing content under an existing identity;
- missing and extra members;
- size or digest mismatch;
- duplicate logical identity;
- invalid foreign key or evidence coordinate;
- incomplete disposition population;
- path escape or symlink;
- unsupported extractor, segmenter, processor, or policy identity;
- unknown, unpinned, capability-mismatched, or digest-mismatched profile;
- unpinned, missing, or mismatched backing state reference;
- stale base release or conflicting catalog commit;
- missing or unsealed document-store receipt;
- missing, unknown, duplicate, conflicting, or unsealed execution task result;
- missing retained captured, representation, or segment object;
- unknown execution profile, worker-composition profile, or policy identity;
- a partial successor containing only changed rows;
- legacy `storageMode`, `baseCatalog`, `changeKind`, or `source-item-changes`;
- a missing, mismatched, unverified, or falsely reported reused `blobRef`; and
- attempted in-place replacement.

Producer and independent verifier tests MUST share exact fixture bytes, not a
fixture generator.

## 16. Implementation sequence

Each step MUST leave DocSpec installable and independently testable.

Before adding infrastructure, the implementer MUST inspect maintained packages
and the pinned predecessor Git history for direct reuse or a proven algorithm.
Prefer a thin adapter when a maintained package supplies most of the required
behavior. Git history and focused executable fixtures supply tests, invariants,
fixture identities, and algorithm pointers without copying the predecessor tree
into DocSpec. Predecessor schemas, product identities, sibling-package coupling,
and production modules MUST NOT remain as a checked-in quarantine.

DocSpec MUST NOT create a homegrown scheduler, distributed queue, cache server,
object store, table engine, or analytics engine as part of this sequence.

### 16.1 Keep the standalone kernel independently verifiable

- Maintain the clean package boundary, Git-native predecessor reference, focused
  static checks, and sibling-free installation checks.
- Make application services depend only on DocSpec domain records and ports;
  move concrete processing result and verification implementations behind those
  surfaces.
- Build shared sealed fixture distributions for catalogs, files,
  representations, segments, processors, stores, sinks, and releases.
- Run producers and independent verifiers against the same exact fixture bytes.
- Replace archive- and fingerprint-dependent tests with Git-native history,
  static boundaries, and focused behavior fixtures, then remove both checked-in
  predecessor corpora.

### 16.2 Complete integrity, receipts, and policy records

- Make `DocumentRelease` verification resolve and verify every retained source,
  representation, segment, active layer, sealed store, and profile-state object.
- Add complete identity-bearing delivery population, accepted, rejected,
  retried, undelivered, byte, resource-use, and final-verdict fields.
- Complete capture retry history and retention disposition; representation
  encoding and resource use; segment derivation identity; and derived-record
  processor-description and exact-input digests.
- Replace open retention and data-use dictionaries with closed, versioned policy
  records. Make profiles declare access, encryption, region, retention, and
  redistribution policy identities.
- Add credential and secret redaction tests for errors, logs, and receipts.
- Make every verification command state its exact scope. Only complete composed
  verification may emit a complete `pass` verdict.

### 16.3 Make stage execution reusable and processor-neutral

- Persist independently verifiable capture, extraction, segmentation, and
  processor-invocation checkpoints under exact work identities.
- Resume at the first incomplete stage and verify every reused output.
- Introduce closed, reference-based `ProcessorRequest` and `ProcessorResult`
  records that support declared file, representation, segment, and dependency
  outputs.
- Enforce processor item limits, retry classification, dependency order,
  dispositions, and data-use field filtering in DocSpec application services.
- Add a `ProcessorResultCache` or stage-result repository port. Provide a small
  local reference implementation; keep Redis or another shared cache optional,
  disposable, and non-authoritative.
- Adapt real processing libraries behind `Processor`; do not reproduce their
  engines.

### 16.4 Implement the portable job handoff

- Add closed `ExecutionProfile`, execution-handoff, `StoreTask`, and
  `StoreTaskResult` records with canonical identities and strict size limits.
- Make `run prepare` save all planned stores and emit a small handoff root plus
  a bounded task ledger.
- Make `task execute` reconstruct its worker from a pinned composition profile,
  execute and deliver one store idempotently, and return only a terminal
  reference or persisted failure reference.
- Make `run reconcile` stream results, accept out-of-order completion, deduplicate
  identical replay, and reject missing, unknown, conflicting, or unsealed tasks.
- Make the local runner use the same serialized tasks and results.
- Implement one thin maintained-scheduler adapter, preferably Dagster, using its
  native dynamic mapping, resources, retries, event log, and process launcher.
  Do not close over an arbitrary local callable or implement scheduling inside
  DocSpec.
- Pass `SCHEDULER-PORTABILITY` across a real process boundary.

### 16.5 Compose scalable storage and reconciliation profiles

- Retain local JSON Lines, SQLite, files, and threads as portable reference
  adapters and bounded test defaults.
- Build complete source snapshots with stable logical partitions and
  content-addressed `blobRef` reuse. Measure actual reused payload bytes, new
  payload writes, and local publication writes.
- Add a Parquet `RecordStorageProfile` through a maintained Arrow/Parquet
  library as the first production-oriented record profile when required by a
  deployment.
- Add Iceberg, Delta, or database catalog profiles only when their transaction
  and history capabilities are needed; adapt their libraries instead of
  rebuilding them.
- Keep S3, R2, and compatible object services behind `BlobStore` adapters.
- Bound durable sink memory independently of store size by streaming or using
  the selected record writer's bounded batches.
- Reconcile partitions independently and finalize from small sealed partition
  receipts so an initial build is not one corpus-sized coordinator task.
- Use digest-bound verified-reader and partition-verification receipts to avoid
  repeated corpus-wide scans during one commit. A disposable verification cache
  MAY accelerate this path but cannot authorize publication.

### 16.6 Complete incremental and maintenance operations

- Enumerate a source snapshot once per plan and use verified reader handles to
  avoid redundant complete scans.
- Prove a clean build and an incremental build over the same state expose the
  same logical records.
- Support targeted extractor, segmenter, processor, failed-population, source-
  partition, and logical-bucket runs without reacquiring unrelated content.
- Implement format-neutral compaction behind `RecordStorage` and
  `DocumentCatalog`, preserving logical state and committing a successor
  release.
- Build garbage-collection retention sets from verified reachability across
  retained stores, profile state, and releases before reporting collectable
  objects.
- Pass `INCREMENTAL-EQUIVALENCE` for initial, incremental, targeted, and
  compacted state.

### 16.7 Finish the independent product surface

- Expose prepare, task execution, reconciliation, commit, inspection, and
  conformance through one `docspec` command and the same application services.
- Implement the section 12 source-catalog composition root and optional
  SpicyRegs adapter without restating that command contract elsewhere.
- Enable clean installation, continuous integration, package builds, and sealed
  fixture publication.
- Publish the package, profile descriptions, and fixture releases by version and
  digest when a release destination is selected.
- Keep conformance fail-closed: an absent, skipped, partial, or fixture-free
  required check does not pass.

### 16.8 Qualify the composed deployment

- Seal the exact processing, execution, storage, sink, and scale profiles.
- Run local and maintained-scheduler recovery campaigns, including worker loss,
  coordinator restart, duplicate result delivery, slow storage, full scratch
  disk, and deterministic processor failure.
- Run the ordered representative scale campaigns and record the execution
  tool's event evidence with DocSpec's task, byte, partition, store, and release
  evidence.
- Publish machine-readable conformance reports. Keep unrun external campaigns
  distinct from locally verified implementation.

## 17. Completion decision

DocSpec is complete only when:

- it builds and atomically publishes one complete immutable `SourceCatalog`
  snapshot through injected source and storage ports;
- every successor declares complete current state and requires no previous
  catalog to read;
- unchanged partitions retain exact verified `blobRef` values while changed
  state receives a new logical identity;
- its receipt reconciles reused payload bytes, new payload writes, and
  publication writes;
- its installed catalog semantic verifier rejects unknown source columns,
  schema drift, invalid roles, legacy change-set fields, bad blob references,
  incomplete `U` or `S`, and policy or receipt mismatch before streaming a row;
- its bounded reader streams stable ordered partitions without loading the
  catalog or replaying prior state;
- its Git-pinned migration differential corpus preserves decision order,
  sampling, joins, interpretations, dispositions, reasons, and rendition choices,
  with every intentional difference named by policy;
- the optional installed SpicyRegs adapter is selected only by the CLI
  composition root, while DocSpec core imports no SpicyRegs module;
- its sealed source-catalog `ScaleProfile` and dated result pass the declared
  multi-million-row resource and determinism gate;
- `DocumentCatalog` opens, compares, and advances corpus state only through
  explicit `DocumentRelease` identities;
- each stateful run commits one immutable `DocumentRelease`;
- physical storage remains behind pinned profiles and never changes DocSpec's
  logical records;
- its optional Parquet record profile supports bounded, independently verified,
  partition-pruned queries over the same logical layers as the local profile;
- at least one portable profile set and one scale-qualified profile set produce
  equivalent logical releases;
- it divides catalog entries into bounded `DocumentStore` jobs;
- it emits those jobs as a sealed bounded task ledger, lets a replaceable local
  or external tool consume them, and reconciles streamed terminal references;
- every successful job has a sealed, independently verifiable store receipt;
- it preserves and verifies exact whole files and every retained derived object;
- it derives representations and source-grounded segments;
- it executes injected processors without owning their meaning;
- an injected sink can save immutable partitioned datasets, return bounded
  result streams, or do both;
- it updates only changed data and invalidated descendants;
- it resumes without repeating verified work;
- scheduler, queue, cache, object-store, record-format, and table-engine
  capabilities remain delegated behind DocSpec ports and profiles;
- it installs and runs without sibling worktrees;
- its local and external-scheduler backends produce equivalent logical output;
- a catalog event maps idempotently to the exact stable partition task set,
  isolated partition retry preserves completed siblings, and large bytes remain
  outside scheduler messages;
- its package, fixtures, and conformance reports are independently verifiable;
  and
- the ordered scale and recovery campaigns pass.

No Markdown status statement may substitute for these results.

## Appendix A. Suggested package layout

```text
src/docspec/domain/
src/docspec/ports/
src/docspec/application/
src/docspec/profiles/
src/docspec/adapters/source_catalogs/
src/docspec/adapters/document_catalogs/
src/docspec/adapters/record_storage/
src/docspec/adapters/stores/
src/docspec/adapters/extractors/
src/docspec/adapters/segmenters/
src/docspec/adapters/processors/
src/docspec/adapters/execution/
src/docspec/adapters/sinks/
src/docspec/artifacts/
src/docspec/conformance/
```

This layout is informative. Import-direction and executable conformance checks
govern the installed package.

## Appendix B. Required machine files

```text
conformance/specification.json
conformance/test-matrix.json
conformance/scale-profile.schema.json
conformance/scale-result.schema.json
profiles/
fixtures/execution-handoffs/
fixtures/source-catalogs/
fixtures/storage-profiles/
fixtures/document-catalogs/
fixtures/document-stores/
fixtures/files/
fixtures/representations/
fixtures/segments/
fixtures/processors/
fixtures/sinks/
fixtures/document-releases/
```

The JSON files and executable validators govern their own shapes. This appendix
does not authorize empty placeholder files.
