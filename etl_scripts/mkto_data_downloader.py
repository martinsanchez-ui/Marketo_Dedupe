"""
mkto_data_downloader.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Marketo Data Downloader 
"""

import calendar
import csv
import os
import time
import sqlite3

from global_settings import DOWNLOADS_PATH, log, SQLITE_DB_PATH
from diagnostics import DiagnosticsManager


def produce_start_end_dates(year, month):
   """ Produces a list with a pair of dates, the initial date and final date of a given month """

   last_day = calendar.monthrange(year, month)[1]
   start = f"{year:04d}-{month:02d}-01T00:00:00Z"
   end = f"{year:04d}-{month:02d}-{last_day:02d}T23:59:59Z"
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


def submit_export_jobs(mc, start_year, start_month, end_year, end_month, diagnostics: DiagnosticsManager = None, dry_run=False):
    """ Creates the chunks from the supplied start and end dates and submit the job creation to Marketo """

    FIELDS = ["Id", "ContactID", "FullName", "Email", "ContactTypeCategory"]
    export_ids = []
    
    # obtain the list of chunks
    chunk_dates = create_dates_for_data_chunks(start_year, start_month, end_year, end_month)
    log.info(f"Dates requested: {start_year}/{start_month} to {end_year}/{end_month}")
    log.debug(f"List of chunks: {chunk_dates}")

    for chunk_date in chunk_dates:
        log.info(f"Chunk {chunk_date[0]} to {chunk_date[1]}")
        filters = {
            "createdAt": {
                "startAt": chunk_date[0],
                "endAt": chunk_date[1]
            }
        }

        log.debug(filters)

        if not dry_run:
            export_job = mc.execute(method="create_leads_export_job", fields=FIELDS, filters=filters) if diagnostics is None else diagnostics.execute_marketo(mc, method="create_leads_export_job", fields=FIELDS, filters=filters)

            export_id = export_job[0]["exportId"]
            log.info(f"Export ID for chunk {chunk_date}: {export_id}")
            export_ids.append(export_id)
            if diagnostics:
                diagnostics.log_filter(export_id, chunk_date[0], chunk_date[1], year=int(chunk_date[0][0:4]), month=int(chunk_date[0][5:7]))
    
    return export_ids
    

def enqueue_jobs(mc, export_ids, diagnostics: DiagnosticsManager = None):
    """ Enqueue the jobs recently created """
    
    for export_id in export_ids:
        log.info(f"Enqueuing job: {export_id}")
        if diagnostics:
            diagnostics.execute_marketo(mc, method='enqueue_leads_export_job', export_id=export_id, job_id=export_id)
        else:
            mc.execute(method='enqueue_leads_export_job', job_id=export_id)

def monitor_queued_jobs(mc, export_ids, diagnostics: DiagnosticsManager = None):
    """ Monitors the list of queued jobs and waits until all the of them are ready to be downloaded """
    
    COMPLETED = "Completed"
    ALL_COMPLETED_LIST = [COMPLETED] * len(export_ids)

    checks_remaining = 180 # 180 checks every 10 seconds each is 30 minutes
    export_ids_status = {k:"" for k in export_ids}
    
    while checks_remaining and list(export_ids_status.values()) != ALL_COMPLETED_LIST:
        checks_remaining -= 1

        for export_id, status in export_ids_status.items():
            if status == COMPLETED:
                continue

            response = mc.execute(method='get_leads_export_job_status', job_id=export_id) if diagnostics is None else diagnostics.execute_marketo(mc, method='get_leads_export_job_status', export_id=export_id, job_id=export_id)
            job_status_payload = response[0] if isinstance(response, list) and response else {}
            job_status = job_status_payload.get('status', 'Unknown')
            log.info(f"Export Job {export_id} Status: {job_status}")
            if diagnostics:
                diagnostics.record_job_status(export_id, job_status_payload)

            if job_status == COMPLETED:
                export_ids_status[export_id] = COMPLETED
                if diagnostics:
                    diagnostics.write_final_status(export_id)

        if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
            log.info("Waiting for 10 seconds...")
            time.sleep(10)  

    if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
        log.error("Marketo couldn't export all the chunks on time. Aborting.")
        return False

    return True

    
def download_jobs(mc, export_ids, diagnostics: DiagnosticsManager = None, download_dir: str = None):
    """ Downloads the jobs from Marketo """

    download_destination = download_dir or (diagnostics.downloads_dir if diagnostics else DOWNLOADS_PATH)
    for export_id in export_ids:
        file_name = os.path.join(download_destination, f"{export_id}.csv")
        log.info(f"Downloading file: {file_name}")
        file_content = mc.execute(method="get_leads_export_job_file", job_id=export_id) if diagnostics is None else diagnostics.execute_marketo(mc, method="get_leads_export_job_file", export_id=export_id, job_id=export_id)
                
        with open(file_name, "wb") as f:
            f.write(file_content)

        file_size_on_disk = os.path.getsize(file_name)
        anomalies = []
        bytes_downloaded = len(file_content) if hasattr(file_content, "__len__") else file_size_on_disk
        if diagnostics:
            expected_size = diagnostics.job_status_final.get(export_id, {}).get("fileSize")
            expected_record_count = diagnostics.job_status_final.get(export_id, {}).get("recordCount")
        else:
            expected_size = None
            expected_record_count = None
        try:
            expected_size = int(expected_size) if expected_size is not None else None
        except (ValueError, TypeError):
            expected_size = None
        try:
            expected_record_count = int(expected_record_count) if expected_record_count is not None else None
        except (ValueError, TypeError):
            expected_record_count = None

        header = []
        row_count = 0
        created_at_range = {"min": None, "max": None}
        created_at_index = None
        repeated_header = False
        missing_header = False

        try:
            with open(file_name, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                try:
                    header = next(reader)
                except StopIteration:
                    missing_header = True
                    anomalies.append("missing_header")
                    header = []
                else:
                    if "createdAt" in header:
                        created_at_index = header.index("createdAt")
                for row in reader:
                    if header and row == header:
                        repeated_header = True
                    if created_at_index is not None and len(row) > created_at_index:
                        current_value = row[created_at_index]
                        if created_at_range["min"] is None or current_value < created_at_range["min"]:
                            created_at_range["min"] = current_value
                        if created_at_range["max"] is None or current_value > created_at_range["max"]:
                            created_at_range["max"] = current_value
                    row_count += 1
        except Exception as exc:
            anomalies.append(f"read_error:{exc}")

        if repeated_header:
            anomalies.append("repeated_header")
            log.warning(f"Export {export_id}: repeated header row detected")
        if missing_header:
            log.error(f"Export {export_id}: missing header row")
        if bytes_downloaded != file_size_on_disk:
            anomalies.append("byte_length_mismatch")
        log.info(f"Export {export_id}: downloaded {bytes_downloaded} bytes to {file_name} (on disk: {file_size_on_disk})")
        if expected_size:
            if expected_size != file_size_on_disk:
                anomalies.append("expected_file_size_mismatch")
                log.error(f"Export {export_id}: expected fileSize {expected_size}, got {file_size_on_disk}")
            else:
                log.info(f"Export {export_id}: fileSize matches expected metadata ({expected_size})")
        if expected_record_count is not None:
            log.info(f"Export {export_id}: Marketo recordCount metadata = {expected_record_count}")
        log.info(f"Export {export_id}: row_count (excluding header) = {row_count}")

        if diagnostics:
            diagnostics.record_download_result(
                export_id=export_id,
                file_path=file_name,
                bytes_downloaded=bytes_downloaded,
                file_size_on_disk=file_size_on_disk,
                header=header,
                row_count=row_count,
                anomalies=anomalies,
                created_at_range=created_at_range,
                expected_file_size=expected_size,
                expected_record_count=expected_record_count,
            )

    
def populate_sqlite_table(export_ids, diagnostics: DiagnosticsManager = None, download_dir: str = None, sqlite_dir: str = None):
    """ Populate sqlite table with the export files downloaded """
    
    sqlite_destination = sqlite_dir or SQLITE_DB_PATH
    db_file_name = os.path.join(sqlite_destination, f"{int(time.time())}.db")
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
        export_file = os.path.join(download_dir or DOWNLOADS_PATH, f"{export_id}.csv")
        log.info(f"Loading to sqlite db: {export_file}")
        
        with open(export_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            rows = [row for row in reader]
            cursor.executemany("INSERT INTO mkto_dump (Id, ContactID, FullName, Email, ContactTypeCategory) VALUES (?, ?, ?, ?, ?)", rows)
            if diagnostics:
                diagnostics.record_sqlite_insert(export_id, len(rows))

    sqlite_conn.commit()
    if diagnostics:
        total_rows = cursor.execute("SELECT COUNT(*) FROM mkto_dump").fetchone()[0]
        diagnostics.set_sqlite_total_rows(total_rows)
        diagnostics.write_sqlite_metrics()
        log.info(f"Rows inserted into sqlite so far: {total_rows}")
    sqlite_conn.close()

    return db_file_name
