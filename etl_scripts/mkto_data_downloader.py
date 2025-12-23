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


def submit_export_jobs(mc, start_year, start_month, end_year, end_month, dry_run=False):
    """ Creates the chunks from the supplied start and end dates and submit the job creation to Marketo """

    FIELDS = ["Id", "ContactID", "FullName", "Email", "ContactTypeCategory"]
    export_ids = []
    
    # obtain the list of chunks
    chunk_dates = create_dates_for_data_chunks(start_year, start_month, end_year, end_month)
    log.info(f"Dates requested: {start_year}/{start_month} to {end_year}/{end_month}")
    log.debug(f"List of chunks: {chunk_dates}")

    for chunk_date in chunk_dates:
        filters = {
            "createdAt": {
                "startAt": chunk_date[0],
                "endAt": chunk_date[1]
            }
        }

        log.debug(filters)

        if not dry_run:
            export_job = mc.execute(method="create_leads_export_job", fields=FIELDS, filters=filters)

            export_id = export_job[0]["exportId"]
            log.info(f"Export ID for chunk {chunk_date}: {export_id}")
            export_ids.append(export_id)
    
    return export_ids
    

def enqueue_jobs(mc, export_ids):
    """ Enqueue the jobs recently created """
    
    for export_id in export_ids:
        log.info(f"Enqueuing job: {export_id}")
        mc.execute(method='enqueue_leads_export_job', job_id=export_id)

def monitor_queued_jobs(mc, export_ids):
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

            job_status = mc.execute(method='get_leads_export_job_status', job_id=export_id)[0]['status']
            log.info(f"Export Job {export_id} Status: {job_status}")

            if job_status == COMPLETED:
                export_ids_status[export_id] = COMPLETED

        if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
            log.info("Waiting for 10 seconds...")
            time.sleep(10)  

    if list(export_ids_status.values()) != ALL_COMPLETED_LIST:
        log.error("Marketo couldn't export all the chunks on time. Aborting.")
        return False

    return True

    
def download_jobs(mc, export_ids):
    """ Downloads the jobs from Marketo """

    for export_id in export_ids:
        file_name = os.path.join(DOWNLOADS_PATH, f"{export_id}.csv")
        log.info(f"Downloading file: {file_name}")
        file_content = mc.execute(method="get_leads_export_job_file", job_id=export_id)
                
        with open(file_name, "wb") as f:
            f.write(file_content)

    
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
