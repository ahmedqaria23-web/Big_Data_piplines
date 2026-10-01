from typing import List, Dict, Any, Tuple, Optional
import logging
from pymongo import ReplaceOne, UpdateOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError

import json
import hashlib
from datetime import datetime, timezone

from config.settings import (
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    COLLECTION_META_STATE
)

logger = logging.getLogger(__name__)


def compute_idempotency_key(raw_record: Dict[str, Any], id_order_val: Any = None) -> str:
    """
    Computes a deterministic, stable idempotency key (_id) for orders_raw.
    1. If valid id_order/order_id present after trimming: returns trimmed id_order string.
    2. Otherwise: returns 'HASH_' + SHA-256 hash of canonical raw content.
    """
    if id_order_val is not None:
        trimmed = str(id_order_val).strip()
        if trimmed:
            return trimmed

    raw_dict = raw_record.get("record_raw", raw_record) if isinstance(raw_record, dict) and isinstance(raw_record.get("record_raw"), dict) else raw_record

    if isinstance(raw_dict, dict):
        order_key = raw_dict.get("id_order") or raw_dict.get("order_id")
        if order_key is not None:
            trimmed = str(order_key).strip()
            if trimmed:
                return trimmed

        meta_keys = {"_id", "id_run", "at_ingested", "number_row_source", "engine_used", "file_source"}
        clean_dict = {k: str(v) for k, v in raw_dict.items() if k not in meta_keys}
        canonical_str = json.dumps(clean_dict, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
        sha = hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
        return f"HASH_{sha}"

    canonical_str = str(raw_dict)
    sha = hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    return f"HASH_{sha}"


# --- Checkpoint Management Functions ---

def get_ingestion_checkpoint(db: Database, file_fingerprint: str, id_run: Optional[str] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {"file_fingerprint": file_fingerprint}
    if id_run:
        query["id_run"] = id_run
    return db[COLLECTION_META_STATE].find_one(query)


def save_ingestion_checkpoint(db: Database, checkpoint_doc: Dict[str, Any]):
    file_fingerprint = checkpoint_doc["file_fingerprint"]
    id_run = checkpoint_doc.get("id_run", "")
    checkpoint_doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    key = f"{file_fingerprint}_{id_run}" if id_run else file_fingerprint
    if "pipeline" not in checkpoint_doc:
        checkpoint_doc["pipeline"] = f"ingest_checkpoint_{key}"
    filter_query = {"file_fingerprint": file_fingerprint, "id_run": id_run} if id_run else {"file_fingerprint": file_fingerprint}
    db[COLLECTION_META_STATE].replace_one(
        filter_query,
        checkpoint_doc,
        upsert=True
    )


# --- Raw Repository Functions ---

def insert_raw_batch(db: Database, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    bulk_ops = []
    for rec in records:
        if "_id" not in rec or not rec["_id"]:
            id_run = rec.get("id_run", "run")
            row_num = rec.get("number_row_source", 0)
            rec["_id"] = f"{id_run}:{row_num}"
        if "id_order" in rec and rec["id_order"]:
            rec["id_order"] = str(rec["id_order"]).strip()
        bulk_ops.append(ReplaceOne({"_id": rec["_id"]}, rec, upsert=True))
    db[COLLECTION_RAW].bulk_write(bulk_ops, ordered=False)
    return len(records)


def count_raw(db: Database, filter_query: Dict[str, Any] = None) -> int:
    return db[COLLECTION_RAW].count_documents(filter_query or {})


def find_raw_sample(db: Database, limit: int = 100, filter_query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    cursor = db[COLLECTION_RAW].find(filter_query or {}, {"_id": 0})
    if limit and limit > 0:
        cursor = cursor.limit(limit)
    return list(cursor)


# --- Helper for True Business Idempotency Comparison ---

def _clean_for_comparison(val: Any) -> Any:
    """Recursively strip dynamic execution timestamps and run IDs for business state comparison."""
    if isinstance(val, dict):
        return {
            k: _clean_for_comparison(v)
            for k, v in val.items()
            if k not in ["timestamp", "processed_at", "at_ingested", "id_run", "quarantined_at", "file_source", "number_row_source", "engine_used"]
        }
    elif isinstance(val, list):
        return [_clean_for_comparison(x) for x in val]
    return val


def is_business_state_equal(new_record: Dict[str, Any], existing_record: Dict[str, Any]) -> bool:
    """
    Compares two records solely on stable Business State attributes,
    ignoring execution-level tracking metadata (id_run, timestamps, etc.).
    """
    ignore_keys = {
        "_id", "id_run", "at_ingested", "processed_at", "quarantined_at",
        "ingest_timestamp", "file_source", "number_row_source", "engine_used"
    }
    all_keys = (set(new_record.keys()) | set(existing_record.keys())) - ignore_keys

    for k in all_keys:
        val1 = _clean_for_comparison(new_record.get(k))
        val2 = _clean_for_comparison(existing_record.get(k))
        if val1 != val2:
            return False
    return True


# --- Validated Repository Functions ---

def upsert_validated_batch(db: Database, records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Directly upserts records into orders_validated using id_order as the business key.
    Queries by business key {"id_order": id_order} with ReplaceOne(..., upsert=True).
    Intra-batch records with duplicate id_order are consolidated to retain the latest state,
    preventing concurrent E11000 bulk race conditions on the unique index.
    Derives (inserted_count, updated_count, unchanged_count) directly from BulkWriteResult.

    Returns: (inserted_count, updated_count, unchanged_count)
    """
    if not records:
        return 0, 0, 0

    dedup_map: Dict[str, Dict[str, Any]] = {}
    for record in records:
        raw_id = record.get("id_order") or record.get("order_id")
        if raw_id is None:
            continue
        id_order = str(raw_id).strip()
        if not id_order:
            continue

        record["id_order"] = id_order
        rec_to_insert = record.copy()
        rec_to_insert.pop("_id", None)
        dedup_map[id_order] = rec_to_insert

    if not dedup_map:
        return 0, 0, 0

    order_ids = list(dedup_map.keys())
    existing_docs = {
        doc["id_order"]: doc
        for doc in db[COLLECTION_VALIDATED].find(
            {"id_order": {"$in": order_ids}}
        )
    }

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    bulk_operations = []

    for id_order, rec in dedup_map.items():
        if id_order not in existing_docs:
            inserted_count += 1
            bulk_operations.append(
                ReplaceOne(
                    {"id_order": id_order},
                    rec,
                    upsert=True
                )
            )
        else:
            existing_doc = existing_docs[id_order]
            if is_business_state_equal(rec, existing_doc):
                unchanged_count += 1
            else:
                updated_count += 1
                bulk_operations.append(
                    ReplaceOne(
                        {"id_order": id_order},
                        rec,
                        upsert=True
                    )
                )

    if bulk_operations:
        try:
            db[COLLECTION_VALIDATED].bulk_write(bulk_operations, ordered=False)
        except BulkWriteError as bwe:
            details = bwe.details or {}
            write_errors = details.get("writeErrors", [])
            failed_count = len(write_errors)
            succeeded_count = len(bulk_operations) - failed_count

            logger.error(
                f"[BulkWriteError in orders_validated] Total Ops: {len(bulk_operations)} | "
                f"Succeeded: {succeeded_count} | Failed: {failed_count}"
            )

            for err_info in write_errors:
                op_idx = err_info.get("index")
                err_code = err_info.get("code")
                errmsg = err_info.get("errmsg")
                key_val = err_info.get("keyValue") or err_info.get("op", {}).get("q")
                logger.error(
                    f"[BulkWriteError Detail] Op Index: {op_idx} | "
                    f"Business Key: {key_val} | Error Code: {err_code} | Message: {errmsg}"
                )

            raise RuntimeError(
                f"BulkWriteError in upsert_validated_batch: {failed_count} write operations failed out of {len(bulk_operations)}."
            ) from bwe

    return inserted_count, updated_count, unchanged_count


def count_validated(db: Database, filter_query: Dict[str, Any] = None) -> int:
    return db[COLLECTION_VALIDATED].count_documents(filter_query or {})


def find_validated_sample(db: Database, limit: int = 100, filter_query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    return list(db[COLLECTION_VALIDATED].find(filter_query or {}, {"_id": 0}).limit(limit))


# --- Quarantine Repository Functions ---

def insert_quarantine_batch(db: Database, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    bulk_ops = []
    for rec in records:
        id_order = rec.get("id_order")
        rec_to_insert = rec.copy()
        rec_to_insert.pop("_id", None)
        if id_order:
            filter_q = {"id_order": id_order}
        else:
            id_run = rec.get("id_run", "no_run")
            source_row = rec.get("source_row_number", 0)
            filter_q = {"id_run": id_run, "source_row_number": source_row}
        bulk_ops.append(ReplaceOne(filter_q, rec_to_insert, upsert=True))

    try:
        db[COLLECTION_QUARANTINE].bulk_write(bulk_ops, ordered=False)
        return len(records)
    except BulkWriteError as bwe:
        details = bwe.details or {}
        write_errors = details.get("writeErrors", [])
        failed_count = len(write_errors)
        logger.error(
            f"[BulkWriteError in quarantine_orders] Total Ops: {len(bulk_ops)} | Failed: {failed_count}"
        )
        for err_info in write_errors:
            logger.error(
                f"[Quarantine BulkWriteError Detail] Index: {err_info.get('index')} | "
                f"Key: {err_info.get('keyValue')} | Error: {err_info.get('errmsg')}"
            )
        raise RuntimeError(
            f"BulkWriteError in insert_quarantine_batch: {failed_count} write operations failed."
        ) from bwe


def count_quarantine(db: Database, filter_query: Dict[str, Any] = None) -> int:
    return db[COLLECTION_QUARANTINE].count_documents(filter_query or {})


def find_quarantine_sample(db: Database, limit: int = 100, filter_query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    return list(db[COLLECTION_QUARANTINE].find(filter_query or {}, {"_id": 0}).limit(limit))
