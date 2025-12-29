"""
marketo_dedupe.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Remove duplicate record from Marketo
"""

import argparse
import csv
import glob
import requests
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from etl_utilities.utils import send_teams_message
from diagnostics import DiagnosticsManager
from global_settings import DIAGNOSTICS_PATH, bi_db, CONTACTS_LIMIT, DOWNLOADS_PATH, DRY_RUN, log, mc, SQLITE_DB_PATH
from mkto_data_downloader import download_jobs, enqueue_jobs, monitor_queued_jobs, populate_sqlite_table, submit_export_jobs
from query_functions import (double_check_not_found_in_marketo, get_contact_type_categories, get_contact_type_category_id, get_dupes_from_dump, 
                             get_records_from_email_marketing_f, is_deleted_contact, show_EMF_record, soft_delete_contact)
from stage_counts import StageCounts

total_dupes = 0
successful_merges = 0

class DupeRecField(Enum):
    MARKETO_ID = 0
    CONTACT_ID = 1
    NAME = 2
    EMAIL = 3
    CONTACT_TYPE_CATEGORY = 4
    SOURCE = 5
    ACTION = 6


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


CRM_NO_MATCH_FIELDS = [
    "run_id",
    "captured_at_utc",
    "contact_id",
    "group_email",
    "group_key",
    "mkto_rows_in_group",
    "mkto_marketo_ids_in_group",
    "mkto_fullnames_sample",
    "note",
]

INCONSISTENT_EMAIL_FIELDS = [
    "run_id",
    "captured_at_utc",
    "contact_id",
    "group_email",
    "crm_marketo_id",
    "crm_email",
    "reason_code",
    "mkto_emails_for_that_marketo_id",
    "mkto_rows_for_that_marketo_id",
    "mkto_marketo_ids_in_group",
    "explanation",
]

_crm_no_match_logged_keys: Set[str] = set()
_inconsistent_email_logged_keys: Set[str] = set()


def _gather_csv_stage_metrics(downloads_path: str) -> Dict[str, object]:
    csv_files = [
        os.path.join(downloads_path, f)
        for f in os.listdir(downloads_path)
        if f.lower().endswith(".csv") and os.path.isfile(os.path.join(downloads_path, f))
    ]
    rows_per_file: Dict[str, int] = {}
    empty_or_tiny_files: List[str] = []
    total_bytes = 0
    tiny_threshold = 1

    for csv_path in sorted(csv_files):
        file_name = os.path.basename(csv_path)
        total_bytes += os.path.getsize(csv_path)
        row_count = 0
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                next(reader, None)
                for _ in reader:
                    row_count += 1
        except Exception as exc:
            log.error(f"Failed to read CSV for counting ({csv_path}): {exc}")
        rows_per_file[file_name] = row_count
        if row_count <= tiny_threshold:
            empty_or_tiny_files.append(file_name)

    csv_total_rows = sum(rows_per_file.values())
    csv_file_count = len(rows_per_file)
    metrics = {
        "csv_file_count": csv_file_count,
        "csv_total_rows": csv_total_rows,
        "csv_rows_per_file": rows_per_file,
        "empty_or_tiny_files": empty_or_tiny_files,
        "total_bytes": total_bytes,
        "created_at": _now_iso(),
    }
    return metrics


def _gather_sqlite_stage_metrics(db_file_name: str) -> Dict[str, object]:
    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()
    mkto_dump_row_count = cursor.execute("SELECT COUNT(*) FROM mkto_dump").fetchone()[0]
    unique_contact_ids = cursor.execute("SELECT COUNT(DISTINCT ContactID) FROM mkto_dump").fetchone()[0]
    unique_contact_email_pairs = cursor.execute("SELECT COUNT(DISTINCT ContactID || '|' || Email) FROM mkto_dump").fetchone()[0]
    null_or_blank_emails = cursor.execute(
        "SELECT COUNT(*) FROM mkto_dump WHERE Email IS NULL OR TRIM(Email) = '' OR LOWER(Email) = 'null';"
    ).fetchone()[0]
    sqlite_conn.close()
    return {
        "mkto_dump_row_count": mkto_dump_row_count,
        "unique_contact_ids": unique_contact_ids,
        "unique_contact_email_pairs": unique_contact_email_pairs,
        "null_or_blank_emails": null_or_blank_emails,
        "captured_at": _now_iso(),
    }


def _gather_duplicate_metrics(db_file_name: str, dupes_from_dump: tuple) -> Dict[str, object]:
    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()
    duplicate_row_count = cursor.execute(
        """
        SELECT SUM(dupe_count)
        FROM (
            SELECT COUNT(*) as dupe_count
            FROM mkto_dump
            GROUP BY ContactID, FullName, Email
            HAVING COUNT(*) > 1
        );
        """
    ).fetchone()[0]
    sqlite_conn.close()
    duplicate_row_count = duplicate_row_count or 0
    unique_contact_ids = len({dupe[0] for dupe in dupes_from_dump})
    return {
        "duplicate_group_count": len(dupes_from_dump),
        "duplicate_row_count": duplicate_row_count,
        "duplicate_unique_contact_ids": unique_contact_ids,
        "captured_at": _now_iso(),
    }


def _append_diagnostic_csv(diagnostics: Optional[DiagnosticsManager], filename: str, fieldnames: List[str], row: Dict[str, object]) -> None:
    if diagnostics is None:
        return
    try:
        diagnostics.append_csv_row(filename, fieldnames, row)
    except Exception as exc:
        log.error(f"Failed to append diagnostic row to {filename}: {exc}")


def _record_crm_no_match_group(
    diagnostics: Optional[DiagnosticsManager],
    contact_id: str,
    group_email: str,
    dupe_recs: List[List[object]],
) -> None:
    if diagnostics is None:
        return
    group_email = group_email or ""
    group_key = f"{contact_id}|{group_email}"
    if group_key in _crm_no_match_logged_keys:
        return
    _crm_no_match_logged_keys.add(group_key)

    marketo_ids_in_group = sorted({str(rec[DupeRecField.MARKETO_ID.value]) for rec in dupe_recs if rec[DupeRecField.MARKETO_ID.value] is not None})
    fullname_sample = []
    for rec in dupe_recs:
        name_value = rec[DupeRecField.NAME.value]
        if name_value and name_value not in fullname_sample:
            fullname_sample.append(name_value)
        if len(fullname_sample) >= 3:
            break

    row = {
        "run_id": diagnostics.run_id,
        "captured_at_utc": _now_iso_utc(),
        "contact_id": contact_id,
        "group_email": group_email,
        "group_key": group_key,
        "mkto_rows_in_group": len(dupe_recs),
        "mkto_marketo_ids_in_group": "|".join(marketo_ids_in_group),
        "mkto_fullnames_sample": "|".join(fullname_sample),
        "note": "No CRM rows found for this (contact_id,email)",
    }
    _append_diagnostic_csv(diagnostics, "crm_no_match_groups.csv", CRM_NO_MATCH_FIELDS, row)


def _fetch_marketo_email_evidence(sqlite_conn: sqlite3.Connection, marketo_id: str) -> Tuple[List[str], int]:
    emails: List[str] = []
    row_count = 0
    try:
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT DISTINCT Email FROM mkto_dump WHERE Id = ?", (marketo_id,))
        emails = [row[0] for row in cursor.fetchall() if row and row[0]]
        row_count = cursor.execute("SELECT COUNT(*) FROM mkto_dump WHERE Id = ?", (marketo_id,)).fetchone()[0]
    except Exception as exc:
        log.error(f"Failed to collect mkto_dump evidence for marketo_id {marketo_id}: {exc}")
    return sorted(set(emails)), row_count


def _record_inconsistent_email_case(
    diagnostics: Optional[DiagnosticsManager],
    sqlite_conn: sqlite3.Connection,
    contact_id: str,
    group_email: str,
    dupe_CRM: List[object],
    group_marketo_ids: List[object],
) -> None:
    if diagnostics is None:
        return
    group_email = group_email or ""
    crm_marketo_id = dupe_CRM[DupeRecField.MARKETO_ID.value]
    unique_key = f"{contact_id}|{group_email}|{crm_marketo_id}"
    if unique_key in _inconsistent_email_logged_keys:
        return
    _inconsistent_email_logged_keys.add(unique_key)

    mkto_emails, mkto_row_count = _fetch_marketo_email_evidence(sqlite_conn, crm_marketo_id)
    marketo_ids_in_group = sorted({str(mid) for mid in group_marketo_ids if mid is not None})
    explanation = (
        f"Marketo ID {crm_marketo_id} exists in mkto_dump under email(s) {', '.join(mkto_emails) or 'N/A'}, "
        f"which does not match group email '{group_email}'."
    )

    row = {
        "run_id": diagnostics.run_id,
        "captured_at_utc": _now_iso_utc(),
        "contact_id": contact_id,
        "group_email": group_email,
        "crm_marketo_id": crm_marketo_id,
        "crm_email": dupe_CRM[DupeRecField.EMAIL.value],
        "reason_code": "PRESENT_IN_EXPORT_DIFFERENT_EMAIL",
        "mkto_emails_for_that_marketo_id": "|".join(mkto_emails),
        "mkto_rows_for_that_marketo_id": mkto_row_count,
        "mkto_marketo_ids_in_group": "|".join(marketo_ids_in_group),
        "explanation": explanation,
    }
    _append_diagnostic_csv(diagnostics, "inconsistent_email_cases.csv", INCONSISTENT_EMAIL_FIELDS, row)


def _initialize_crm_metrics() -> Dict[str, object]:
    return {
        "groups_checked_in_crm": 0,
        "groups_with_crm_match": 0,
        "crm_rows_matched": 0,
        "crm_records_carried_forward": 0,
        "unique_marketo_ids_from_crm": set(),
        "missing_marketo_id_validation_checked": 0,
        "missing_marketo_id_cases": 0,
        "missing_marketo_ids_total": 0,
        "missing_marketo_id_examples": [],
        "inconsistent_email_cases": 0,
        "inconsistent_email_marketo_ids_total": 0,
        "inconsistent_email_examples": [],
    }


def _finalize_crm_metrics(metrics: Dict[str, object]) -> Dict[str, object]:
    finalized = dict(metrics)
    finalized["unique_marketo_ids_from_crm"] = sorted(list(metrics.get("unique_marketo_ids_from_crm", set())))
    finalized["missing_marketo_id_examples"] = metrics.get("missing_marketo_id_examples", [])[:25]
    finalized["inconsistent_email_examples"] = metrics.get("inconsistent_email_examples", [])[:25]
    finalized["captured_at"] = _now_iso()
    return finalized


def _initialize_run_stats() -> Dict[str, object]:
    return {
        "total_duplicate_groups_found": 0,
        "groups_entered_stage5": 0,
        "groups_winner_selected": 0,
        "groups_skipped_no_winner": 0,
        "groups_skipped_no_marketo_losers": 0,
        "groups_merge_attempted": 0,
        "groups_merge_succeeded": 0,
        "groups_merge_failed": 0,
        "groups_aborted_due_to_inconsistent_email": 0,
        "groups_aborted_other_reasons": 0,
        "groups_aborted_reasons": {},
        "total_marketo_losers_attempted": 0,
        "total_marketo_losers_merged": 0,
        "inconsistent_email_event_count": 0,
        "inconsistent_email_unique_marketo_ids": set(),
        "inconsistent_email_groups": set(),
        "unique_winner_marketo_ids": set(),
        "unique_loser_marketo_ids": set(),
        "unique_loser_crm_ids": set(),
    }


def _finalize_run_stats(stats: Dict[str, object]) -> Dict[str, object]:
    finalized = dict(stats)
    finalized["inconsistent_email_unique_marketo_ids"] = len(stats.get("inconsistent_email_unique_marketo_ids", set()))
    finalized["inconsistent_email_groups"] = len(stats.get("inconsistent_email_groups", set()))
    finalized["unique_winner_marketo_ids"] = len(stats.get("unique_winner_marketo_ids", set()))
    finalized["unique_loser_marketo_ids"] = len(stats.get("unique_loser_marketo_ids", set()))
    finalized["unique_loser_crm_ids"] = len(stats.get("unique_loser_crm_ids", set()))
    finalized["dry_run"] = DRY_RUN
    return finalized


def _log_stats_summary(stats: Dict[str, object]) -> None:
    log.info("=" * 60)
    log.info("Stats Summary:")
    ordered_keys = [
        "dry_run",
        "total_duplicate_groups_found",
        "groups_entered_stage5",
        "groups_winner_selected",
        "groups_skipped_no_winner",
        "groups_aborted_due_to_inconsistent_email",
        "groups_skipped_no_marketo_losers",
        "groups_merge_attempted",
        "groups_merge_succeeded",
        "groups_merge_failed",
        "groups_aborted_other_reasons",
        "total_marketo_losers_attempted",
        "total_marketo_losers_merged",
        "unique_winner_marketo_ids",
        "unique_loser_marketo_ids",
        "unique_loser_crm_ids",
        "inconsistent_email_event_count",
        "inconsistent_email_unique_marketo_ids",
        "inconsistent_email_groups",
    ]
    for key in ordered_keys:
        if key in stats:
            log.info(f"  {key}: {stats[key]}")
    if "groups_aborted_reasons" in stats:
        log.info(f"  groups_aborted_reasons: {stats['groups_aborted_reasons']}")
    log.info("=" * 60)


def create_directories(downloads_path=DOWNLOADS_PATH, sqlite_path=SQLITE_DB_PATH, diagnostics_path=DIAGNOSTICS_PATH):
    """ Create all the required directories """
    os.makedirs(downloads_path, exist_ok=True)
    os.makedirs(sqlite_path, exist_ok=True)
    os.makedirs(diagnostics_path, exist_ok=True)

def remove_files_in_dir(directory):
    """ Removes all the files in the specified directory"""
    files = glob.glob(os.path.join(directory, "*"))
    for file in files:
        if os.path.isfile(file):
            log.info(f"Removing previous run file: {file}")
            os.remove(file)
        
        if os.path.exists(file):
            log.error("Error removing file. Aborting.")
            sys.exit(1)

def _is_recoverable(error: Exception) -> bool:
    """
    Determine if an exception is recoverable (transient error).
    We treat typical network errors and known HTTP status codes as retryable.
    """

    # Set of HTTP status codes considered recoverable (e.g., timeouts, rate-limiting, server errors)
    RECOVERABLE_STATUS = {408, 429} | set(range(500, 600))  # 408 = Timeout, 429 = Rate limit, 5xx = Server errors

    # Set of recoverable exceptions, mainly from the `requests` library
    RECOVERABLE_EXC = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
    )

    # Case 1: It's a network-level exception
    if isinstance(error, RECOVERABLE_EXC):
        return True

    # Case 2: Some exceptions expose HTTP status code attributes
    status = getattr(error, "status", None) or getattr(error, "code", None)
    if status and status in RECOVERABLE_STATUS:
        return True

    # Otherwise, consider it non-recoverable
    return False

def call_marketo_API_merge_lead(
    winner,
    losers,
    retries: int = 3,
    base_delay: float = 10.0,
    backoff_factor: float = 2.0,
    diagnostics: DiagnosticsManager = None,
):
    """
    Calls Marketo's 'merge_lead' API method with retry logic.

    Parameters:
    ----------
    winner : str or int
        The ID of the winner lead

    losers : list
        List of IDs from the loser leads

    retries : int
        Number of retry attempts before giving up. Default is 3.

    base_delay : float
        Initial delay before the first retry (in seconds). Default is 5.

    backoff_factor : float
        Multiplier used for exponential backoff (e.g., 2 = 5s, 10s, 20s). Default is 2.
    """

    attempt = 1
    while attempt <= retries:
        try:
            if diagnostics:
                result_merge_marketo = diagnostics.execute_marketo(mc, method='merge_lead', export_id=None, id=winner, leadIds=losers)
            else:
                result_merge_marketo = mc.execute(method='merge_lead', id=winner, leadIds=losers)
            return result_merge_marketo 
        
        except Exception as exc:
            recoverable = _is_recoverable(exc)

            if not recoverable:
                log.error(f"[call_marketo_API_merge_lead] Permanent error (non-retryable). {exc}")
                raise

            log.warning(f"[call_marketo_API_merge_lead] Recoverable error ({exc}) on attempt {attempt}/{retries}]")

            if attempt == retries:
                log.error(f"[call_marketo_API_merge_lead] Exhausted retries ({retries}). Raising last error.")
                raise

            # Wait before retrying (exponential backoff)
            delay = base_delay * (backoff_factor ** (attempt - 1))
            log.info(f"[call_marketo_API_merge_lead] Waiting {delay:.1f}s before next retry")
            time.sleep(delay)

        attempt += 1 

def perform_dedupe(db_file_name, category_types, dupes_from_dump, diagnostics: DiagnosticsManager = None,
                  stage_metrics: Optional[Dict[str, object]] = None, stats: Optional[Dict[str, object]] = None, stop_after_validation: bool = False):
    """ Execute dedupe procedure over every contactID in the contact_ids list """

    if DRY_RUN:
        log.info("*" * 100)
        log.info("DRY RUN MODE. NO CHANGES WILL BE PERFORMED IN MARKETO ")
        log.info("*" * 100)

    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()

    GET_MKTO_DUMP_RECORDS_QUERY = """
        SELECT Id, ContactID, FullName, Email, ContactTypeCategory
        FROM mkto_dump
        WHERE ContactID = %s AND Email = "%s"
    """
    
    global total_dupes, successful_merges
    counter = 0
    total_dupes = len(dupes_from_dump)
     
    for dupe_contact in dupes_from_dump: 
        counter += 1
        contact_id = dupe_contact[0]
        dupe_email = dupe_contact[1] if len(dupe_contact) > 1 else ""
        abort_this_dupe = False
        if stats is not None:
            stats["groups_entered_stage5"] += 1
        log.info("-" * 100)
        if stop_after_validation:
            log.info(f"Instrumentation-only: CRM cross-check for contactId {contact_id} {dupe_contact[1]} (no merges will run)")
        else:
            log.info(f"Deduping contactId: {contact_id} {dupe_contact[1]}")

        # Build dupe lists
        dupe_recs = [[*x, "Marketo", ""] for x in cursor.execute(GET_MKTO_DUMP_RECORDS_QUERY % dupe_contact).fetchall()]
        dupe_recs_marketo_id_only = [x[0] for x in dupe_recs]
        dupes_CRM = get_records_from_email_marketing_f(*dupe_contact)
        if stage_metrics is not None:
            stage_metrics["groups_checked_in_crm"] += 1
            stage_metrics["crm_rows_matched"] += len(dupes_CRM)
            if dupes_CRM:
                stage_metrics["groups_with_crm_match"] += 1
            stage_metrics["unique_marketo_ids_from_crm"].update(
                [dupe_CRM[DupeRecField.MARKETO_ID.value] for dupe_CRM in dupes_CRM if len(dupe_CRM) > DupeRecField.MARKETO_ID.value]
            )
        if not dupes_CRM:
            _record_crm_no_match_group(
                diagnostics,
                contact_id=str(contact_id),
                group_email=dupe_email,
                dupe_recs=dupe_recs,
            )
        missing_marketo_ids: List[str] = []
        inconsistent_email_case_recorded = False
 
        for dupe_CRM in dupes_CRM:
            idx = dupe_recs_marketo_id_only.index(dupe_CRM[0]) if dupe_CRM[0] in dupe_recs_marketo_id_only else None
            if idx is not None: 
                dupe_recs[idx][DupeRecField.SOURCE.value] = "Both"
            else:
                # Double check that the appended marketo_id absolutely don't have any record in Marketo. (In case there are mixed email addresses)
                if double_check_not_found_in_marketo(sqlite_conn, dupe_CRM[DupeRecField.MARKETO_ID.value]): 
                    dupe_recs.append([*dupe_CRM, "CRM", ""])
                    if stage_metrics is not None:
                        stage_metrics["crm_records_carried_forward"] += 1
                        if len(stage_metrics["missing_marketo_id_examples"]) < 25:
                            stage_metrics["missing_marketo_id_examples"].append(
                                {
                                    "contact_id": contact_id,
                                    "email": dupe_CRM[DupeRecField.EMAIL.value],
                                    "marketo_id": dupe_CRM[DupeRecField.MARKETO_ID.value],
                                }
                            )
                    missing_marketo_ids.append(dupe_CRM[DupeRecField.MARKETO_ID.value])
                else:
                    log.error(f"---> Inconsistent email address between Marketo and CRM in marketo_id {dupe_CRM[DupeRecField.MARKETO_ID.value]}. Requires human intervention.")
                    _record_inconsistent_email_case(
                        diagnostics,
                        sqlite_conn,
                        contact_id=str(contact_id),
                        group_email=dupe_email,
                        dupe_CRM=dupe_CRM,
                        group_marketo_ids=dupe_recs_marketo_id_only,
                    )
                    abort_this_dupe = True
                    if stats is not None:
                        stats["inconsistent_email_event_count"] += 1
                        stats["inconsistent_email_unique_marketo_ids"].add(dupe_CRM[DupeRecField.MARKETO_ID.value])
                        stats["inconsistent_email_groups"].add(contact_id)
                    if stage_metrics is not None:
                        if not inconsistent_email_case_recorded:
                            stage_metrics["inconsistent_email_cases"] += 1
                            inconsistent_email_case_recorded = True
                        stage_metrics["inconsistent_email_marketo_ids_total"] += 1
                        if len(stage_metrics["inconsistent_email_examples"]) < 25:
                            stage_metrics["inconsistent_email_examples"].append(
                                {
                                    "contact_id": contact_id,
                                    "email": dupe_contact[1],
                                    "marketo_id": dupe_CRM[DupeRecField.MARKETO_ID.value],
                                    "found_in_dump_with_different_email": True,
                                }
                            )

        if stage_metrics is not None:
            stage_metrics["missing_marketo_id_validation_checked"] += 1
            if missing_marketo_ids:
                stage_metrics["missing_marketo_id_cases"] += 1
                stage_metrics["missing_marketo_ids_total"] += len([mid for mid in missing_marketo_ids if mid is not None])

        if abort_this_dupe:
            # As part of the abort action, we'll show the data collected so far
            if stats is not None:
                stats["groups_aborted_due_to_inconsistent_email"] += 1
                stats["groups_aborted_reasons"]["inconsistent_email"] = stats["groups_aborted_reasons"].get("inconsistent_email", 0) + 1
            for dupe_rec in dupe_recs:
                log.debug(dupe_rec) 
            continue
        
        if stop_after_validation:
            log.info("Stop after validation flag enabled; skipping merge operations for this contact.")
            continue
        
        # Step 0: Determine if the contact is deleted. For now this is only informative
        if is_deleted_contact(contact_id):
            log.info(f"ContactId {contact_id} is marked as deleted.")
    
        # Step 1: Determine what is the current ContactTypeCategory. 
        actual_contact_type_category_id = get_contact_type_category_id(contact_id)
        log.info(f"ContactId {contact_id} has contact_type_category_id {category_types[actual_contact_type_category_id]}")

        winner_marketo_id = determine_winner(dupe_recs, contact_id, category_types, actual_contact_type_category_id)
        
        if not winner_marketo_id:
            log.error("Unable to determine a winner. Skipping...")
            if stats is not None:
                stats["groups_skipped_no_winner"] += 1
                stats["groups_aborted_other_reasons"] += 1
                stats["groups_aborted_reasons"]["no_winner"] = stats["groups_aborted_reasons"].get("no_winner", 0) + 1
            continue
        if stats is not None:
            stats["groups_winner_selected"] += 1
            stats["unique_winner_marketo_ids"].add(winner_marketo_id)

        for dupe_rec in dupe_recs:
            if dupe_rec[DupeRecField.MARKETO_ID.value] == winner_marketo_id:
                dupe_rec[DupeRecField.ACTION.value] = "Winner"

        winner_source = next(
            (dupe_rec[DupeRecField.SOURCE.value] for dupe_rec in dupe_recs if dupe_rec[DupeRecField.MARKETO_ID.value] == winner_marketo_id),
            "Unknown",
        )
        losers_marketo_preview = [
            dupe_rec[DupeRecField.MARKETO_ID.value]
            for dupe_rec in dupe_recs
            if dupe_rec[DupeRecField.SOURCE.value] in {"Marketo", "Both"} and dupe_rec[DupeRecField.ACTION.value] != "Winner"
        ]
        losers_crm_preview = [
            dupe_rec[DupeRecField.MARKETO_ID.value]
            for dupe_rec in dupe_recs
            if dupe_rec[DupeRecField.SOURCE.value] in {"CRM", "Both"} and dupe_rec[DupeRecField.ACTION.value] != "Winner"
        ]
        log.info(
            f"[Stage5] contact_id={contact_id} email={dupe_email} winner={winner_marketo_id} "
            f"winner_source={winner_source} losers_marketo_count={len(losers_marketo_preview)} "
            f"losers_crm_count={len(losers_crm_preview)}"
        )

        # logging the list for debugging purposes
        for dupe_rec in dupe_recs:
            log.debug(dupe_rec) 
        
        merge_result = perform_dedupe_single_contact(contact_id, dupe_recs, diagnostics=diagnostics, stats=stats)
        if merge_result and not DRY_RUN:
            successful_merges += 1
        
        log.info(f"Completed: {counter / total_dupes * 100:.2f}%   ({counter}/{total_dupes})")

    sqlite_conn.close()
    
def perform_dedupe_single_contact(contact_id, dupe_recs, diagnostics: DiagnosticsManager = None, stats: Optional[Dict[str, object]] = None):
    """ Processes all the duplicate record on a given dupe_recs list """

    winner = None
    losers_marketo = []
    losers_CRM = []

    for dupe_rec in dupe_recs:
        if dupe_rec[DupeRecField.ACTION.value] == "Winner":
            winner = dupe_rec[DupeRecField.MARKETO_ID.value]
        
        elif dupe_rec[DupeRecField.SOURCE.value] == "Marketo":
            losers_marketo.append(dupe_rec[DupeRecField.MARKETO_ID.value])
        
        elif dupe_rec[DupeRecField.SOURCE.value] == "CRM":
            losers_CRM.append(dupe_rec[DupeRecField.MARKETO_ID.value])

        elif dupe_rec[DupeRecField.SOURCE.value] == "Both":
            losers_marketo.append(dupe_rec[DupeRecField.MARKETO_ID.value])
            losers_CRM.append(dupe_rec[DupeRecField.MARKETO_ID.value])

    log.debug (f"[perform_dedupe_single_contact] winner = {winner} losers in Marketo = {losers_marketo}, losers in CRM = {losers_CRM}")
    if stats is not None:
        stats["unique_loser_marketo_ids"].update(losers_marketo)
        stats["unique_loser_crm_ids"].update(losers_CRM)
    log.info(f"[Stage6.1] contact_id={contact_id} winner={winner} losers_marketo_count={len(losers_marketo)} losers_crm_count={len(losers_CRM)}")
    if not losers_marketo:
        log.info("[Stage6.1] No Marketo losers to merge; skipping merge step.")
        if stats is not None:
            stats["groups_skipped_no_marketo_losers"] += 1
            stats["groups_aborted_other_reasons"] += 1
            stats["groups_aborted_reasons"]["no_marketo_losers"] = stats["groups_aborted_reasons"].get("no_marketo_losers", 0) + 1
        return False
    
    # Call Marketo API to merge contacts. API returns boolean. True means successful.
    log.debug(f"Calling Marketo API: method='merge_lead', id={winner}, leadIds={losers_marketo}")
    
    result_merge_marketo = True
    if stats is not None:
        stats["groups_merge_attempted"] += 1
        stats["total_marketo_losers_attempted"] += len(losers_marketo)
    if not DRY_RUN:
        try:
            result_merge_marketo = call_marketo_API_merge_lead(winner, losers_marketo, diagnostics=diagnostics)
        except Exception as exc:
            log.error(f"Merge in Marketo raised an error: {exc}")
            if stats is not None:
                stats["groups_merge_failed"] += 1
                stats["groups_aborted_other_reasons"] += 1
                stats["groups_aborted_reasons"]["merge_failed_exception"] = stats["groups_aborted_reasons"].get("merge_failed_exception", 0) + 1
            return False
    
    if not result_merge_marketo:
        log.error("Merge in Marketo failed. Skipping contact...")
        if stats is not None:
            stats["groups_merge_failed"] += 1
            stats["groups_aborted_other_reasons"] += 1
            stats["groups_aborted_reasons"]["merge_failed"] = stats["groups_aborted_reasons"].get("merge_failed", 0) + 1
        return False

    if DRY_RUN:
        log.info("DRY RUN: Contact merge simulated in Marketo (no changes performed).")
    else:
        log.info("Contact successfully merged in Marketo")
        if stats is not None:
            stats["groups_merge_succeeded"] += 1
            stats["total_marketo_losers_merged"] += len(losers_marketo)

    if DRY_RUN:
        log.info("DRY RUN mode. No changes performed.")
    
    return True
  

def determine_winner(dupe_recs, contact_id, category_types, actual_contact_type_category_id):
    """ Identifies the winner 

        The rules for the winner are
            - Contact is present in BOTH, Marketo and CRM.
            - DupeRecField.CONTACT_TYPE_CATEGORY = actual_contact_type_category_id
            - If there are many matches, the lower marketo_id wins
            - If there are no matches (none has "Both" in source), create a new record in Marketo, update its marketo_id with the new one and then set it as winner
              This last rule isn't implemented yet.
    """

    for dupe_rec in dupe_recs: 
        if dupe_rec[DupeRecField.SOURCE.value] == "Both" \
        and category_types[actual_contact_type_category_id] == dupe_rec[DupeRecField.CONTACT_TYPE_CATEGORY.value]:
            dupe_rec[DupeRecField.ACTION.value] = "WC"
        
    winner_candidates_marketo_ids = [dupe_rec[DupeRecField.MARKETO_ID.value] for dupe_rec in dupe_recs if dupe_rec[DupeRecField.ACTION.value] == "WC"]
    winner_candidates_marketo_ids.sort()

    if winner_candidates_marketo_ids:
        return winner_candidates_marketo_ids[0]

    log.error(f"---> There are no records in email_marketing_f associated with the contact_id {contact_id} with category_type {category_types[actual_contact_type_category_id]}"
              f" and submitted email: {dupe_recs[0][DupeRecField.EMAIL.value]}")
    return None

def _count_rows_in_sqlite(db_file_name: str) -> int:
    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()
    total_rows = cursor.execute("SELECT COUNT(*) FROM mkto_dump").fetchone()[0]
    sqlite_conn.close()
    return total_rows


def _is_export_only_mode(args_export_only: bool) -> bool:
    env_flag = os.getenv("EXPORT_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
    return args_export_only or env_flag


def parse_args():
    parser = argparse.ArgumentParser(description="Marketo export and dedupe pipeline")
    parser.add_argument("--export-only", "--stop-after-load", action="store_true", help="Stop after SQLite load and skip dedupe/merge")
    return parser.parse_args()


def main(export_only: bool = False):
    start_ts = time.time()
    diagnostics = DiagnosticsManager(DIAGNOSTICS_PATH, log)
    stage_counts = StageCounts(diagnostics.root, logger=log, run_id=diagnostics.run_id)
    run_stats = _initialize_run_stats()
    downloads_path = diagnostics.downloads_dir
    sqlite_path = diagnostics.sqlite_dir
    export_ids = []
    export_only_summary = None
    crm_stage_metrics = _initialize_crm_metrics()
    try: 
        create_directories(downloads_path, sqlite_path, DIAGNOSTICS_PATH)
        remove_files_in_dir(downloads_path)
        log.info(f"Diagnostics artifacts will be written to: {diagnostics.root}")

        diagnostics.mark_phase_start("create_jobs")
        export_ids = submit_export_jobs(mc, 2017, 1, datetime.now().year, datetime.now().month, diagnostics=diagnostics)  
        diagnostics.mark_phase_end("create_jobs")
        diagnostics.mark_phase_start("enqueue_jobs")
        enqueue_jobs(mc, export_ids, diagnostics=diagnostics)
        diagnostics.mark_phase_end("enqueue_jobs")
        
        diagnostics.mark_phase_start("monitor_jobs")
        if not monitor_queued_jobs(mc, export_ids, diagnostics=diagnostics):
            raise Exception("Error downloading data from Marketo")
        diagnostics.mark_phase_end("monitor_jobs")
        
        diagnostics.mark_phase_start("download_jobs")
        download_jobs(mc, export_ids, diagnostics=diagnostics, download_dir=downloads_path)
        diagnostics.mark_phase_end("download_jobs")
        diagnostics.write_counts_summary()
        log.info("Stage CSV_DOWNLOADS: computing counts for downloaded CSV files.")
        csv_metrics = _gather_csv_stage_metrics(downloads_path)
        log.info(f"Stage CSV_DOWNLOADS metrics: {csv_metrics}")
        stage_counts.add("CSV_DOWNLOADS", csv_metrics)

        diagnostics.mark_phase_start("sqlite_load")
        db_file_name = populate_sqlite_table(export_ids, diagnostics=diagnostics, download_dir=downloads_path, sqlite_dir=sqlite_path)
        diagnostics.mark_phase_end("sqlite_load")
        sqlite_row_count = _count_rows_in_sqlite(db_file_name)
        log.info(f"Rows currently in mkto_dump: {sqlite_row_count}")
        log.info("Stage SQLITE_LOAD: capturing row counts from SQLite.")
        sqlite_metrics = _gather_sqlite_stage_metrics(db_file_name)
        log.info(f"Stage SQLITE_LOAD metrics: {sqlite_metrics}")
        stage_counts.add("SQLITE_LOAD", sqlite_metrics)

        if export_only:
            export_only_summary = {
                "mode": "export_only",
                "export_jobs_created": len(export_ids),
                "export_jobs_enqueued": len(export_ids),
                "export_jobs_completed": len(export_ids),
                "csvs_downloaded": len(export_ids),
                "rows_per_csv": {record.get("export_id"): record.get("row_count") for record in diagnostics.download_records},
                "total_rows_across_csvs": sum(record.get("row_count", 0) for record in diagnostics.download_records),
                "sqlite_db_path": db_file_name,
                "sqlite_mkto_dump_rowcount": sqlite_row_count,
            }
            diagnostics.export_only_summary = export_only_summary
            log.info("Export-only mode enabled. SQLite load complete; skipping dedupe/merge steps.")
            log.info(f"SQLite DB: {db_file_name}")
            log.info(f"mkto_dump rowcount: {sqlite_row_count}")
            return 0

        category_types = get_contact_type_categories()
        dupes_from_dump = get_dupes_from_dump(db_file_name)
        if CONTACTS_LIMIT is not None:
            dupes_from_dump = dupes_from_dump[0:CONTACTS_LIMIT]
        log.info("Stage DUPLICATE_IDENTIFICATION: recording duplicate metrics.")
        duplicate_metrics = _gather_duplicate_metrics(db_file_name, dupes_from_dump)
        log.info(f"Stage DUPLICATE_IDENTIFICATION metrics: {duplicate_metrics}")
        stage_counts.add("DUPLICATE_IDENTIFICATION", duplicate_metrics)
        run_stats["total_duplicate_groups_found"] = duplicate_metrics.get("duplicate_group_count", 0)

        log.info("Stage CRM_CROSS_CHECK: starting CRM cross-check instrumentation.")
        perform_dedupe(
            db_file_name,
            category_types,
            dupes_from_dump,
            diagnostics=diagnostics,
            stage_metrics=crm_stage_metrics,
            stop_after_validation=True,
        )
        finalized_crm_metrics = _finalize_crm_metrics(crm_stage_metrics)
        crm_cross_check_metrics = {
            "groups_checked_in_crm": finalized_crm_metrics.get("groups_checked_in_crm"),
            "groups_with_crm_match": finalized_crm_metrics.get("groups_with_crm_match"),
            "crm_rows_matched": finalized_crm_metrics.get("crm_rows_matched"),
            "crm_records_carried_forward": finalized_crm_metrics.get("crm_records_carried_forward"),
            "unique_marketo_ids_from_crm": finalized_crm_metrics.get("unique_marketo_ids_from_crm"),
            "captured_at": finalized_crm_metrics.get("captured_at"),
        }
        stage_counts.add("CRM_CROSS_CHECK", crm_cross_check_metrics)
        log.info(f"Stage CRM_CROSS_CHECK metrics: {crm_cross_check_metrics}")

        validation_metrics = {
            "missing_marketo_id_validation_checked": finalized_crm_metrics.get("missing_marketo_id_validation_checked"),
            "missing_marketo_id_cases": finalized_crm_metrics.get("missing_marketo_id_cases"),
            "missing_marketo_ids_total": finalized_crm_metrics.get("missing_marketo_ids_total"),
            "missing_marketo_id_examples": finalized_crm_metrics.get("missing_marketo_id_examples"),
            "inconsistent_email_cases": finalized_crm_metrics.get("inconsistent_email_cases"),
            "inconsistent_email_marketo_ids_total": finalized_crm_metrics.get("inconsistent_email_marketo_ids_total"),
            "inconsistent_email_examples": finalized_crm_metrics.get("inconsistent_email_examples"),
            "captured_at": finalized_crm_metrics.get("captured_at"),
        }
        stage_counts.add("CRM_MARKETO_ID_MISSING_VALIDATION", validation_metrics)
        log.info(f"Stage CRM_MARKETO_ID_MISSING_VALIDATION metrics: {validation_metrics}")
        log.info("Stage DEDUPE_AND_MERGE: executing Stage 5 winner selection + Stage 6.1 Marketo merges only.")
        perform_dedupe(
            db_file_name,
            category_types,
            dupes_from_dump,
            diagnostics=diagnostics,
            stats=run_stats,
            stop_after_validation=False,
        )
        final_stats = _finalize_run_stats(run_stats)
        _log_stats_summary(final_stats)
        stage_counts.add("DEDUPLICATION_AND_STAGE6_1", final_stats)
        return 0

    except SystemExit:
        raise
    except Exception as e:
        log.critical("Critical error has occurred. Error info: {e}".format(e=e))
        log.error(traceback.print_exc())
        send_teams_message(summary="Marketo_Dedupe", activityTitle="Critical error occurred",
                           activitySubtitle=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
        sys.exit(1)
    
    finally:
        # show summary
        log.info("-" * 100)
        log.info (f"Contacts with duplicates found: {total_dupes}. Contacts successfully processed: {successful_merges}")
        log.info("-" * 100)
        try:
            diagnostics.write_run_summary(total_export_jobs=len(export_ids), extra_summary=export_only_summary)
            log.info(f"Run summary written to {diagnostics.paths['run_summary']}")
        except Exception as diag_exc:
            log.error(f"Failed to write run summary: {diag_exc}")
        try:
            stage_counts.write()
            log.info(f"Stage counts written to {stage_counts.json_path}")
        except Exception as stage_exc:
            log.error(f"Failed to write stage counts: {stage_exc}")

        log.info("Closing database connection...")
        bi_db.close()
        log.info("Database connection closed.")
        end_ts = time.time()
        final_time = end_ts - start_ts
        log.info(f"Processing finished.  Time: {round(final_time / 60, 3)} minutes ({round(final_time, 3)} seconds)")
        log.info("-" * 100)


if __name__ == "__main__":
    args = parse_args()
    export_only_mode = _is_export_only_mode(args.export_only)
    sys.exit(main(export_only=export_only_mode))
