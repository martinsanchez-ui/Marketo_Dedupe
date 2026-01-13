"""
marketo_dedupe.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Remove duplicate record from Marketo
"""

import glob
import requests
import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime
from enum import Enum

from etl_utilities.utils import send_teams_message
from global_settings import (CONTACTS_LIMIT, DOWNLOADS_PATH, DRY_RUN, SQLITE_DB_PATH,
                             bi_db, generate_run_id, log, mc)
from mkto_data_downloader import download_jobs, enqueue_jobs, monitor_queued_jobs, populate_sqlite_table, submit_export_jobs
from query_functions import (double_check_not_found_in_marketo, get_contact_type_categories, get_contact_type_category_id, get_dupes_from_dump, 
                             get_records_from_email_marketing_f, is_deleted_contact, show_EMF_record, soft_delete_contact)

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


def create_directories():
    """ Create all the required directories """
    os.makedirs(DOWNLOADS_PATH, exist_ok=True)
    os.makedirs(SQLITE_DB_PATH, exist_ok=True)

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

def perform_dedupe(db_file_name, category_types, dupes_from_dump):
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
        log.info(f"Deduping contactId: {contact_id} {dupe_contact[1]}")

        # Build dupe lists
        dupe_recs = [[*x, "Marketo", ""] for x in cursor.execute(GET_MKTO_DUMP_RECORDS_QUERY % dupe_contact).fetchall()]
        dupe_recs_marketo_id_only = [x[0] for x in dupe_recs]
        dupes_CRM = get_records_from_email_marketing_f(*dupe_contact)
 
        for dupe_CRM in dupes_CRM:
            idx = dupe_recs_marketo_id_only.index(dupe_CRM[0]) if dupe_CRM[0] in dupe_recs_marketo_id_only else None
            if idx is not None: 
                dupe_recs[idx][DupeRecField.SOURCE.value] = "Both"
            else:
                # Double check that the appended marketo_id absolutely don't have any record in Marketo. (In case there are mixed email addresses)
                if double_check_not_found_in_marketo(sqlite_conn, dupe_CRM[DupeRecField.MARKETO_ID.value]): 
                    dupe_recs.append([*dupe_CRM, "CRM", ""])
                else:
                    log.error(f"---> Inconsistent email address between Marketo and CRM in marketo_id {dupe_CRM[DupeRecField.MARKETO_ID.value]}. Requires human intervention.")
                    abort_this_dupe = True

        if abort_this_dupe:
            # As part of the abort action, we'll show the data collected so far
            for dupe_rec in dupe_recs:
                log.debug(dupe_rec) 
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
        
        if perform_dedupe_single_contact(contact_id, dupe_recs):
            successful_merges += 1
        
        log.info(f"Completed: {counter / total_dupes * 100:.2f}%   ({counter}/{total_dupes})")

    sqlite_conn.close()
    
def perform_dedupe_single_contact(contact_id, dupe_recs):
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
        result_merge_marketo = call_marketo_API_merge_lead(winner, losers_marketo)
    
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

def main():
    start_ts = time.time()
    try: 
        run_id = generate_run_id()
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=main action=start")
        create_directories()
        remove_files_in_dir(DOWNLOADS_PATH)

        start_year = 2017
        start_month = 1
        end_year = datetime.now().year
        end_month = datetime.now().month
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=submit_export_jobs action=before start_year={start_year} "
                 f"start_month={start_month} end_year={end_year} end_month={end_month}")
        export_ids = submit_export_jobs(mc, start_year, start_month, end_year, end_month, run_id=run_id)
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=submit_export_jobs action=after export_ids_count={len(export_ids)}")
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=before total_jobs={len(export_ids)}")
        enqueue_jobs(mc, export_ids, run_id=run_id)
        
        if not monitor_queued_jobs(mc, export_ids, run_id=run_id):
            raise Exception("Error downloading data from Marketo")
        
        download_jobs(mc, export_ids, run_id=run_id)
        db_file_name = populate_sqlite_table(export_ids)
        category_types = get_contact_type_categories()
        dupes_from_dump = get_dupes_from_dump(db_file_name)[0:CONTACTS_LIMIT]
        perform_dedupe(db_file_name, category_types, dupes_from_dump)

    except Exception as e:
        log.critical(f"[MKTO_DEDUPE] run_id={run_id} step=main action=error exc_str={str(e)} exc_repr={repr(e)}")
        log.error(traceback.print_exc())
        send_teams_message(summary="Marketo_Dedupe", activityTitle="Critical error occurred",
                           activitySubtitle=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
        sys.exit(1)
    
    finally:
        # show summary
        log.info("-" * 100)
        log.info (f"Contacts with duplicates found: {total_dupes}. Contacts successfully processed: {successful_merges}")
        log.info("-" * 100)

        log.info("Closing database connection...")
        bi_db.close()
        log.info("Database connection closed.")
        end_ts = time.time()
        final_time = end_ts - start_ts
        log.info(f"Processing finished.  Time: {round(final_time / 60, 3)} minutes ({round(final_time, 3)} seconds)")
        log.info("-" * 100)


if __name__ == "__main__":
    main()
