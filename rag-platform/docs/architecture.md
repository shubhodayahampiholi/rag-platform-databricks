# RAG Platform on Databricks — Architecture & Design Decisions

Status: **Decisions locked — ready for implementation**
Scope: Layer 3 (RAG) of the broader knowledge-platform vision, built production-grade on Databricks, native stack.

This doc exists so we agree on schemas and module boundaries *before* writing code that's expensive to change later (per the harness-engineering principle: structural primitives — registry, ACL enforcement, metadata schema — should be designed deliberately, not evolved ad hoc).

---

## 1. Why this design (recap of grounding principles)

From the prior articles, four requirements are treated as non-negotiable, not nice-to-haves:

1. **Idempotent ingestion** — re-running ingestion must not create duplicate vectors. Detection is by content hash, not filename or timestamp.
2. **ACL enforced at the metadata layer, not bolted on after retrieval** — access control must be a filter the vector search applies at query time, because retrofitting ACL into an existing index requires full re-ingestion.
3. **Migration-ready** — original chunk text and embedding model version are stored alongside vectors, so re-embedding (model upgrade) is a background job, not a crisis.
4. **Full observability from the first ingestion run** — every ingestion and retrieval event is traced in MLflow from day one, not added retroactively.

The "structural vs compensatory" lens: the registry, the ACL metadata schema, and the observability instrumentation are **structural** — build these with confidence, version them explicitly. Chunk size, rerank thresholds, and specific embedding model choice are **compensatory/tunable** — config-driven, expected to change as models and corpus evolve.

---

## 2. Source of truth: the ingestion volume

Per your direction, documents are not loaded from a static notebook path. They live in a Unity Catalog Volume:

```
/Volumes/knowledge_platform/raw/raw_documents/
    internal-employees/
    external-workers/
    finance/
    hr/
```

Subfolder = a coarse-grained signal for ACL group tagging at ingestion time (refined further by per-file metadata if needed). This volume is the thing a "team" would actually drop files into in a real org.

**Decided:** folder-per-ACL-group for v1. ACL groups simulated: `internal-employees`, `external-workers`, `finance`, `hr`. No implied hierarchy between groups — all four are peers; membership in one does not grant visibility into another. Corpus-to-group assignment is arbitrary/random for this demo and will be clearly documented as synthetic, not representative of real document sensitivity.

---

## 3. The Document Registry (idempotency, change detection, lifecycle)

A Delta table in Unity Catalog — the equivalent of your Postgres `DocumentRegistry`, native to this stack.

```sql
CREATE TABLE <catalog>.<schema>.document_registry (
    doc_id              STRING NOT NULL,   -- stable hash of source path (not content) — survives content updates
    source_path          STRING NOT NULL,   -- full Volume path
    source_hash           STRING NOT NULL,   -- content hash (sha256) — detects changes
    acl_groups            ARRAY<STRING>,      -- ['engineering'], ['hr'], etc. — can be multi-group
    doc_type              STRING,             -- pdf, txt, md, etc. — drives chunking strategy
    schema_version        STRING NOT NULL,   -- VectorMetadata schema version at ingestion time
    embedding_model       STRING,             -- model id used to embed current vectors
    embedding_model_version STRING,
    chunk_count            INT,
    ingestion_status       STRING,             -- pending | embedded | failed | deleted
    first_ingested_at      TIMESTAMP,
    last_updated_at        TIMESTAMP,
    last_checked_at         TIMESTAMP,          -- last time the ingestion job looked at this file, even if unchanged
    deleted_at              TIMESTAMP            -- soft delete marker, null if active
)
USING DELTA;
```

**Idempotency logic** (`needs_ingestion()` equivalent), evaluated per file found in the volume on each ingestion run:

| Registry state | File state | Action |
|---|---|---|
| Not in registry | Exists | `new_document` → ingest, register |
| In registry, hash matches | Exists | `unchanged` → skip, update `last_checked_at` only |
| In registry, hash differs | Exists | `updated_document` → delete old vectors for this `doc_id`, re-chunk, re-embed, update registry |
| In registry, not deleted | File missing from volume | `deleted_document` → delete vectors for this `doc_id`, set `deleted_at`, `ingestion_status = deleted` |

**Registry-last write ordering** (per your LangGraph article's self-healing insight): the vector upsert to AI Search happens *before* the registry record is written/updated. If the upsert fails partway, the registry never reflects success, so the next run safely retries as `new_document` or `updated_document` rather than silently skipping a half-ingested file.

---

## 4. VectorMetadata schema (the contract)

Every chunk synced to AI Search carries this metadata. This is versioned (`schema_version`) — a breaking change here means a planned migration, not a silent drift.

```python
{
    "chunk_id": str,            # stable id: f"{doc_id}::{chunk_index}"
    "doc_id": str,               # FK to document_registry
    "chunk_text": str,            # the actual chunk content — non-negotiable, per the v3->v4 lesson
    "chunk_index": int,
    "source_path": str,
    "doc_type": str,
    "acl_groups": list[str],      # filtered on at query time
    "embedding_model": str,
    "embedding_model_version": str,
    "schema_version": str,
    "ingested_at": str,           # ISO timestamp
}
```

`acl_groups` is the field AI Search will filter on at query time — the query-time filter is the actual enforcement mechanism, not a post-hoc check in application code.

---

## 5. ACL enforcement mechanism

Enforcement happens via **metadata filtering at query time**, passed as a `filters` parameter to the AI Search query — this is the AI Search-native equivalent of the Pinecone metadata filter pattern from your article.

Simulated groups: `internal-employees`, `external-workers`, `finance`, `hr`.

A retrieval call always requires a `requesting_user_groups: list[str]` parameter. The retrieval module constructs a filter equivalent to "chunk's `acl_groups` intersects with the caller's groups" — no query executes without this filter applied. This is enforced inside the retrieval module itself (not optional per-caller), so there's no code path that can accidentally skip it.

**Decided:** no implied hierarchy. All four groups (`internal-employees`, `external-workers`, `finance`, `hr`) are peers with no inheritance — membership must be explicit per group, matching real RBAC group semantics.

---

## 6. Module boundaries (`src/rag_platform/`)

| Module | Responsibility | Structural or compensatory? |
|---|---|---|
| `config/` | Loads `settings.yaml` — chunk size per doc_type, embedding model id, ACL group list, AI Search endpoint name | Structural (the separation), compensatory (the values) |
| `registry/` | Delta-backed `DocumentRegistry` — idempotency, change detection, lifecycle states | Structural |
| `acl/` | ACL group taxonomy, filter-construction helpers used by retrieval | Structural |
| `ingestion/` | Volume scanning, extraction (pdf/txt/md), normalization, source-aware chunking | Mostly structural (the pipeline shape); chunk size values are compensatory |
| `embedding/` | Wraps Foundation Model API calls, tags vectors with model/version | Structural wrapper, compensatory model choice |
| `retrieval/` | AI Search query construction, hybrid logic, mandatory ACL filter injection | Structural |
| `eval/` | Recall@K, MRR, nDCG, Precision@K against golden dataset; drift comparison vs. baseline | Structural |
| `observability/` | MLflow tracing decorators/helpers used by every other module | Structural — per harness article, observability does not decay |

---

## 7. Unity Catalog structure (decided)

Important distinction: Unity Catalog's native RBAC (catalog/schema/table GRANTs) governs which *workspace users* can see which *tables*. The four ACL groups above are a separate, simulated attribute-based filter applied at the chunk/vector level inside AI Search — finer-grained, and not the same mechanism as UC GRANTs. We do not create a catalog or schema per ACL group; ACL lives in the data (`acl_groups` column), not in catalog structure.

Per current Databricks guidance, catalogs should not be created per team/project — use schemas for that separation instead. The structure is:

```
Catalog:  knowledge_platform
  ├── Schema: raw           → Volume: raw_documents (folder-per-ACL-group inside)
  ├── Schema: registry      → Table: document_registry
  ├── Schema: curated       → Table: chunk_metadata
  ├── Schema: eval          → Tables: golden_dataset, eval_runs, drift_baseline
  └── Schema: observability → derived/aggregated metric tables (MLflow owns its own tracking backend separately)
```

`knowledge_platform` names the data substrate (reusable beyond this RAG build); the repo/package `rag_platform` names this specific capability built on top of it.

## 8. Resolved decisions (formerly open questions)

1. Folder-per-ACL-group, no sidecar override mechanism for v1 — confirmed.
2. No implied hierarchy between ACL groups — confirmed.
3. Catalog: `knowledge_platform`, schema structure as above — confirmed.
4. Corpus-to-ACL-group assignment is arbitrary/random, documented as synthetic — confirmed.