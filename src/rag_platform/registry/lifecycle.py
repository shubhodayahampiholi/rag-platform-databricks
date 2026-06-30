"""
registry.lifecycle
 
Compares the current Volume scan (from registry.scanner) against the
existing state of the document_registry table, and classifies every file
into one of four lifecycle states. This module only classifies — it does
not write to the registry or trigger ingestion. That happens in the next
piece (registry.sync), which consumes this module's output.
"""
 
from dataclasses import dataclass
from enum import Enum
 
from .scanner import ScannedFile
 
 
class LifecycleState(Enum):
    NEW = "new_document"
    UNCHANGED = "unchanged"
    UPDATED = "updated_document"
    DELETED = "deleted_document"
 
 
@dataclass
class ClassifiedFile:
    doc_id: str
    state: LifecycleState
    scanned: ScannedFile | None  # None when state is DELETED (file no longer exists)
 
 
@dataclass
class RegistryRecord:
    """Minimal view of an existing document_registry row, just what's needed for classification."""
    doc_id: str
    source_hash: str
    deleted_at: str | None  # non-null means already soft-deleted; ignored for re-classification unless file reappears
 
 
def classify_files(
    scanned_files: list[ScannedFile],
    existing_registry: dict[str, RegistryRecord],
) -> list[ClassifiedFile]:
    """
    existing_registry: a dict keyed by doc_id, representing the current
    document_registry table contents. Built by the caller from a query
    against the table — this module has no Spark/SQL dependency, to keep
    it independently testable.
 
    Returns one ClassifiedFile per file that needs attention: every
    scanned file (new/unchanged/updated), plus every registry doc_id that
    was active but is now missing from the scan (deleted).
    """
    classified: list[ClassifiedFile] = []
    seen_doc_ids: set[str] = set()
 
    for scanned in scanned_files:
        seen_doc_ids.add(scanned.doc_id)
        existing = existing_registry.get(scanned.doc_id)
 
        if existing is None:
            state = LifecycleState.NEW
        elif existing.source_hash != scanned.source_hash:
            state = LifecycleState.UPDATED
        else:
            state = LifecycleState.UNCHANGED
 
        classified.append(ClassifiedFile(doc_id=scanned.doc_id, state=state, scanned=scanned))
 
    # Anything active in the registry but not seen in this scan has been removed from the Volume
    for doc_id, record in existing_registry.items():
        if doc_id not in seen_doc_ids and record.deleted_at is None:
            classified.append(ClassifiedFile(doc_id=doc_id, state=LifecycleState.DELETED, scanned=None))
 
    return classified