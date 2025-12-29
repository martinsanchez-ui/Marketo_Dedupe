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
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set

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
                  stage_metrics: Optional[Dict[str, object]] = None, stop_after_validation: bool = False):
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
        abort_this_dupe = False
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
                    abort_this_dupe = True
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
            continue

        for dupe_rec in dupe_recs:
            if dupe_rec[DupeRecField.MARKETO_ID.value] == winner_marketo_id:
                dupe_rec[DupeRecField.ACTION.value] = "Winner"

        # logging the list for debugging purposes
        for dupe_rec in dupe_recs:
            log.debug(dupe_rec) 
        
        if perform_dedupe_single_contact(contact_id, dupe_recs, diagnostics=diagnostics):
            successful_merges += 1
        
        log.info(f"Completed: {counter / total_dupes * 100:.2f}%   ({counter}/{total_dupes})")

    sqlite_conn.close()
    
def perform_dedupe_single_contact(contact_id, dupe_recs, diagnostics: DiagnosticsManager = None):
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
    
    # Call Marketo API to merge contacts. API returns boolean. True means successful.
    log.debug(f"Calling Marketo API: method='merge_lead', id={winner}, leadIds={losers_marketo}")
    
    result_merge_marketo = True
    if not DRY_RUN:
        result_merge_marketo = call_marketo_API_merge_lead(winner, losers_marketo, diagnostics=diagnostics)
    
    if not result_merge_marketo:
        log.error("Merge in Marketo failed. Skipping contact...")
        return False

    log.info(f"{'DRY RUN:' if DRY_RUN else ''} Contact successfully merged in Marketo")

    # Perform Merge in email_marketing_f
    for loser_CRM in losers_CRM:
        log.debug(f"Calling soft_delete_contact contact_id={contact_id}, loser_CRM={loser_CRM}, dry_run={DRY_RUN}")
        soft_delete_contact(contact_id, loser_CRM, DRY_RUN)
        show_EMF_record(contact_id, loser_CRM)

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
        log.info("Forcing hard stop immediately after CRM MarketoID missing validation stage.")
        stage_counts.write()
        raise SystemExit("INTENTIONAL_STOP_AFTER_STAGE_5")

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
