"""
registry.scanner

Scans the raw documents Volume and computes a stable identity + content hash
for each file found. This is the raw input to the registry's idempotency
logic (see registry.lifecycle, built next) — this module does not read or
write the document_registry table itself.
"""

import hashlib
import os
from dataclasses import dataclass


@dataclass
class ScannedFile:
    doc_id: str
    source_path: str
    acl_group: str
    doc_type: str
    source_hash: str


def _compute_content_hash(file_path: str) -> str:
    """SHA-256 hash of file content, read in chunks to avoid loading large files fully into memory."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            sha256.update(block)
    return sha256.hexdigest()


def _doc_id_from_path(file_path: str) -> str:
    """
    Stable identity derived from the source path itself (not content).
    Using the filename stem as doc_id assumes filenames are unique across
    the corpus, which holds for our synthetic corpus (doc_00000.md, ...).
    This is the one assumption worth revisiting if this scanner is ever
    pointed at a real, multi-source corpus where filenames may collide.
    """
    filename = os.path.basename(file_path)
    stem, _ext = os.path.splitext(filename)
    return stem


def scan_volume(volume_root: str) -> list[ScannedFile]:
    """
    Walks volume_root, expecting the folder-per-ACL-group structure
    decided in architecture.md (one subfolder per ACL group, files directly
    inside each).

    Returns a ScannedFile for every file found. Does not consult the
    registry — this is a pure filesystem scan, idempotency logic is layered
    on top separately.
    """
    scanned: list[ScannedFile] = []

    for acl_group in sorted(os.listdir(volume_root)):
        group_path = os.path.join(volume_root, acl_group)
        if not os.path.isdir(group_path):
            continue  # skip stray files directly under volume_root, if any

        for filename in sorted(os.listdir(group_path)):
            file_path = os.path.join(group_path, filename)
            if not os.path.isfile(file_path):
                continue

            _stem, ext = os.path.splitext(filename)
            doc_type = ext.lstrip(".").lower() or "unknown"

            scanned.append(
                ScannedFile(
                    doc_id=_doc_id_from_path(file_path),
                    source_path=file_path,
                    acl_group=acl_group,
                    doc_type=doc_type,
                    source_hash=_compute_content_hash(file_path),
                )
            )

    return scanned
