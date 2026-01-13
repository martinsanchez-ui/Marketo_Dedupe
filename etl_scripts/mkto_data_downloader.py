"""
mkto_data_downloader.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Marketo Data Downloader 
"""

import calendar
import csv
import json
import os
import requests
import time
import sqlite3

from global_settings import (DOWNLOADS_PATH, SECRETS, SQLITE_DB_PATH,
                             extract_marketo_error_info, log, safe_json_dump)


RAW_RESPONSE_MAX_CHARS = 3000
REQUEST_TIMEOUT_SECONDS = 30


def _truncate_value(value, limit=RAW_RESPONSE_MAX_CHARS):
    if value is None:
        return value
    value = str(value)
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<truncated>"


def _get_marketo_access_token():
    munchkin = SECRETS.get("marketo_munchkin_id")
    client_id = SECRETS.get("marketo_client_id")
    client_secret = SECRETS.get("marketo_client_secret")

    if not munchkin or not client_id or not client_secret:
        raise ValueError("Missing Marketo credentials for access token request.")

    token_url = f"https://{munchkin}.mktorest.com/identity/oauth/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.get(token_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    return payload.get("access_token")


def _log_enqueue_raw_response(export_id, idx, total, run_id=None):
    munchkin = SECRETS.get("marketo_munchkin_id")
    if not munchkin:
        raise ValueError("Missing Marketo munchkin id for enqueue diagnostics.")

    access_token = _get_marketo_access_token()
    if not access_token:
        raise ValueError("No access token returned from Marketo.")

    url = f"https://{munchkin}.mktorest.com/bulk/v1/leads/export/{export_id}/enqueue.json"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    http_status = response.status_code

    raw_json = None
    raw_text = None
    try:
        payload = response.json()
        raw_json = _truncate_value(json.dumps(payload, ensure_ascii=False, default=str))
    except ValueError:
        raw_text = _truncate_value(response.text)

    log_line = (f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=raw_response idx={idx} total={total} "
                f"export_id={export_id} http_status={http_status}")
    if raw_json is not None:
        log.error(f"{log_line} raw_json={raw_json}")
    else:
        log.error(f"{log_line} raw_text={raw_text}")


def produce_start_end_dates(year, month):
   """ Produces a list with a pair of dates, the initial date and final date of a given month """

   start = f"{year}-{month}-01T00:00:00Z"
   end = f"{year}-{month}-{calendar.monthrange(year, month)[1]}T23:59:99Z"
   return [start, end] 

def create_dates_for_data_chunks(start_year, start_month, end_year, end_month):
    """ Creates a list of with the start and end date of every chunk for the given time range """

    dates_list = []
    m = start_month
    y = start_year

    while True:
        dates_list.append(produce_start_end_dates(y, m))
        m += 1

        if y == end_year and m > end_month:
            break
    
        if m == 13:
            m = 1
            y += 1

    return dates_list


def submit_export_jobs(mc, start_year, start_month, end_year, end_month, dry_run=False, run_id=None):
    """ Creates the chunks from the supplied start and end dates and submit the job creation to Marketo """

    FIELDS = ["Id", "ContactID", "FullName", "Email", "ContactTypeCategory"]
    export_ids = []
    
    # obtain the list of chunks
    chunk_dates = create_dates_for_data_chunks(start_year, start_month, end_year, end_month)
    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=submit_export_jobs action=start start_year={start_year} "
             f"start_month={start_month} end_year={end_year} end_month={end_month} total_chunks={len(chunk_dates)} "
             f"fields={FIELDS}")
    log.debug(f"[MKTO_DEDUPE] run_id={run_id} step=submit_export_jobs action=chunk_list chunks={safe_json_dump(chunk_dates)}")

    for idx, chunk_date in enumerate(chunk_dates, start=1):
        filters = {
            "createdAt": {
                "startAt": chunk_date[0],
                "endAt": chunk_date[1]
            }
        }

        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=create_export_job action=request idx={idx} total={len(chunk_dates)} "
                 f"start_at={chunk_date[0]} end_at={chunk_date[1]} filters={safe_json_dump(filters)}")

        if not dry_run:
            try:
                export_job = mc.execute(method="create_leads_export_job", fields=FIELDS, filters=filters)
                export_id = export_job[0]["exportId"]
                log.info(f"[MKTO_DEDUPE] run_id={run_id} step=create_export_job action=success idx={idx} total={len(chunk_dates)} "
                         f"export_id={export_id} start_at={chunk_date[0]} end_at={chunk_date[1]}")
                log.debug(f"[MKTO_DEDUPE] run_id={run_id} step=create_export_job action=response export_id={export_id} "
                          f"response={safe_json_dump(export_job)}")
                export_ids.append(export_id)
            except Exception as exc:
                code, message, payload = extract_marketo_error_info(exc)
                log.error(f"[MKTO_DEDUPE] run_id={run_id} step=create_export_job action=error idx={idx} total={len(chunk_dates)} "
                          f"start_at={chunk_date[0]} end_at={chunk_date[1]} exc_repr={repr(exc)} exc_str={str(exc)} "
                          f"exc_args={safe_json_dump(getattr(exc, 'args', None))} response={safe_json_dump(payload)}")
                if code or message:
                    log.error(f"[MKTO_DEDUPE] run_id={run_id} step=create_export_job MKTO_ERROR parsed_code={code} "
                              f"parsed_message={message}")
                raise
    
    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=submit_export_jobs action=complete export_ids_count={len(export_ids)}")
    return export_ids
    

def enqueue_jobs(mc, export_ids, run_id=None):
    """ Enqueue the jobs recently created """
    
    total = len(export_ids)
    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=start total={total}")
    for idx, export_id in enumerate(export_ids, start=1):
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=request idx={idx} total={total} export_id={export_id}")
        try:
            response = mc.execute(method='enqueue_leads_export_job', job_id=export_id)
            log.info(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=success idx={idx} total={total} export_id={export_id}")
            log.debug(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=response export_id={export_id} "
                      f"response={safe_json_dump(response)}")
        except Exception as exc:
            code, message, payload = extract_marketo_error_info(exc)
            log.error(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=error idx={idx} total={total} export_id={export_id} "
                      f"exc_repr={repr(exc)} exc_str={str(exc)} exc_args={safe_json_dump(getattr(exc, 'args', None))} "
                      f"response={safe_json_dump(payload)}")
            try:
                _log_enqueue_raw_response(export_id, idx, total, run_id)
            except Exception as diag_exc:
                log.error(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue action=raw_response_failed idx={idx} total={total} "
                          f"export_id={export_id} exc_repr={repr(diag_exc)}")
            if code or message:
                log.error(f"[MKTO_DEDUPE] run_id={run_id} step=enqueue MKTO_ERROR parsed_code={code} parsed_message={message}")
            raise

def monitor_queued_jobs(mc, export_ids, run_id=None):
    """ Monitors the list of queued jobs and waits until all the of them are ready to be downloaded """
    
    COMPLETED = "Completed"
    ALL_COMPLETED_LIST = [COMPLETED] * len(export_ids)

    checks_remaining = 180 # 180 checks every 10 seconds each is 30 minutes
    export_ids_status = {k:"" for k in export_ids}
    
    total = len(export_ids)
    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=monitor action=start total={total} checks_remaining={checks_remaining}")
    while checks_remaining and list(export_ids_status.values()) != ALL_COMPLETED_LIST:
        checks_remaining -= 1

        for export_id, status in export_ids_status.items():
            if status == COMPLETED:
                continue

            job_status = mc.execute(method='get_leads_export_job_status', job_id=export_id)[0]['status']
            log.info(f"[MKTO_DEDUPE] run_id={run_id} step=monitor action=status export_id={export_id} status={job_status} "
                     f"checks_remaining={checks_remaining}")

            if job_status == COMPLETED:
                export_ids_status[export_id] = COMPLETED

        if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
            log.info(f"[MKTO_DEDUPE] run_id={run_id} step=monitor action=wait seconds=10 checks_remaining={checks_remaining}")
            time.sleep(10)  

    if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
        incomplete = [k for k, v in export_ids_status.items() if v != COMPLETED]
        log.error(f"[MKTO_DEDUPE] run_id={run_id} step=monitor action=timeout total={total} "
                  f"incomplete_count={len(incomplete)} incomplete_export_ids={safe_json_dump(incomplete)}")
        return False

    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=monitor action=complete total={total}")
    return True

    
def download_jobs(mc, export_ids, run_id=None):
    """ Downloads the jobs from Marketo """

    total = len(export_ids)
    log.info(f"[MKTO_DEDUPE] run_id={run_id} step=download action=start total={total}")
    for idx, export_id in enumerate(export_ids, start=1):
        file_name = os.path.join(DOWNLOADS_PATH, f"{export_id}.csv")
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=download action=request idx={idx} total={total} "
                 f"export_id={export_id} file={file_name}")
        try:
            file_content = mc.execute(method="get_leads_export_job_file", job_id=export_id)
        except Exception as exc:
            code, message, payload = extract_marketo_error_info(exc)
            log.error(f"[MKTO_DEDUPE] run_id={run_id} step=download action=error idx={idx} total={total} export_id={export_id} "
                      f"file={file_name} exc_repr={repr(exc)} exc_str={str(exc)} exc_args={safe_json_dump(getattr(exc, 'args', None))} "
                      f"response={safe_json_dump(payload)}")
            if code or message:
                log.error(f"[MKTO_DEDUPE] run_id={run_id} step=download MKTO_ERROR parsed_code={code} parsed_message={message}")
            raise
                
        with open(file_name, "wb") as f:
            f.write(file_content)
        log.info(f"[MKTO_DEDUPE] run_id={run_id} step=download action=success idx={idx} total={total} export_id={export_id} "
                 f"bytes={len(file_content)} file={file_name}")

    
def populate_sqlite_table(export_ids):
    """ Populate sqlite table with the export files downloaded """
    
    db_file_name = os.path.join(SQLITE_DB_PATH, f"{int(time.time())}.db")
    log.info(f"sqlite db file name: {db_file_name}")
    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mkto_dump (
            Id INTEGER PRIMARY KEY,
            ContactID TEXT,
            FullName TEXT,
            Email TEXT,
            ContactTypeCategory TEXT
        )
        """)

    for export_id in export_ids:
        export_file = os.path.join(DOWNLOADS_PATH, f"{export_id}.csv")
        log.info(f"Loading to sqlite db: {export_file}")
        
        with open(export_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            cursor.executemany("INSERT INTO mkto_dump (Id, ContactID, FullName, Email, ContactTypeCategory) VALUES (?, ?, ?, ?, ?)", reader)

    sqlite_conn.commit()
    sqlite_conn.close()

    return db_file_name
