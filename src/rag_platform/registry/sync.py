"""
registry.sync
 
The Spark-dependent layer that ties scanner + lifecycle together:
queries the current document_registry table, classifies the Volume scan
against it, and writes the results back via MERGE.
 
This is the only module in the registry package that touches Spark/Delta
directly — scanner and lifecycle stay framework-free and independently
testable, per the same separation-of-concerns principle used throughout
this design.
"""
 
from datetime import datetime, timezone
 
from pyspark.sql import SparkSession
 
from .lifecycle import ClassifiedFile, LifecycleState, RegistryRecord, classify_files
from .scanner import scan_volume
 
REGISTRY_TABLE = "knowledge_platform.registry.document_registry"
SCHEMA_VERSION = "v1"  # bump this when VectorMetadata schema changes; see architecture.md section 4
 
 
def _load_existing_registry(spark: SparkSession) -> dict[str, RegistryRecord]:
    """Reads the full registry table into a plain dict, keyed by doc_id."""
    rows = spark.sql(
        f"SELECT doc_id, source_hash, deleted_at FROM {REGISTRY_TABLE}"
    ).collect()
 
    return {
        row["doc_id"]: RegistryRecord(
            doc_id=row["doc_id"],
            source_hash=row["source_hash"],
            deleted_at=row["deleted_at"],
        )
        for row in rows
    }
 
 
def _build_merge_rows(classified: list[ClassifiedFile]) -> list[dict]:
    """
    Converts ClassifiedFile records into flat dicts ready to be written as
    a Spark DataFrame and merged into document_registry. ingestion_status
    here reflects the registry's view, not the embedding pipeline's —
    a NEW or UPDATED file is marked 'pending' because chunking/embedding
    happens in a later pipeline stage, not in this sync step.
    """
    now = datetime.now(timezone.utc)
    rows = []
 
    for item in classified:
        if item.state == LifecycleState.DELETED:
            rows.append({
                "doc_id": item.doc_id,
                "source_path": None,
                "source_hash": None,
                "acl_groups": None,
                "doc_type": None,
                "schema_version": SCHEMA_VERSION,
                "embedding_model": None,
                "embedding_model_version": None,
                "chunk_count": None,
                "ingestion_status": LifecycleState.DELETED.value,
                "first_ingested_at": None,
                "last_updated_at": None,
                "last_checked_at": now,
                "deleted_at": now,
            })
            continue
 
        scanned = item.scanned
        rows.append({
            "doc_id": scanned.doc_id,
            "source_path": scanned.source_path,
            "source_hash": scanned.source_hash,
            "acl_groups": [scanned.acl_group],  # wrapped into array per registry schema; see scanner notes
            "doc_type": scanned.doc_type,
            "schema_version": SCHEMA_VERSION,
            "embedding_model": None,             # set by the embedding pipeline stage, not here
            "embedding_model_version": None,
            "chunk_count": None,                  # set by the embedding pipeline stage, not here
            "ingestion_status": "pending",
            "first_ingested_at": now if item.state == LifecycleState.NEW else None,
            "last_updated_at": now if item.state in (LifecycleState.NEW, LifecycleState.UPDATED) else None,
            "last_checked_at": now,
            "deleted_at": None,
        })
 
    return rows
 
 
def sync_registry(spark: SparkSession, volume_root: str) -> dict[str, int]:
    """
    Runs a full scan-classify-merge cycle. Returns a count of files in each
    lifecycle state, for logging/observability at the call site.
    """
    scanned_files = scan_volume(volume_root)
    existing_registry = _load_existing_registry(spark)
    classified = classify_files(scanned_files, existing_registry)
 
    merge_rows = _build_merge_rows(classified)
 
    if merge_rows:
        updates_df = spark.createDataFrame(merge_rows)
        updates_df.createOrReplaceTempView("_registry_updates")
 
        spark.sql(f"""
            MERGE INTO {REGISTRY_TABLE} AS target
            USING _registry_updates AS source
            ON target.doc_id = source.doc_id
            WHEN MATCHED THEN UPDATE SET
                source_path = COALESCE(source.source_path, target.source_path),
                source_hash = COALESCE(source.source_hash, target.source_hash),
                acl_groups = COALESCE(source.acl_groups, target.acl_groups),
                doc_type = COALESCE(source.doc_type, target.doc_type),
                schema_version = source.schema_version,
                ingestion_status = source.ingestion_status,
                last_updated_at = COALESCE(source.last_updated_at, target.last_updated_at),
                last_checked_at = source.last_checked_at,
                deleted_at = source.deleted_at
            WHEN NOT MATCHED THEN INSERT *
        """)
 
    counts: dict[str, int] = {}
    for item in classified:
        key = item.state.value
        counts[key] = counts.get(key, 0) + 1
 
    return counts