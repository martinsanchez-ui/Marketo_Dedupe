"""
gobal_settings.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Global settings for Marketo Dedupe
"""
import os

from marketorestpython.client import MarketoClient
import MySQLdb 

from etl_utilities.utils import load_secrets
from etl_utilities.RMLogger import Logger


ENV_FILE_PATH = os.path.join(os.path.sep, "datafiles", "app_credentials.env")
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_PATH = os.path.join(SCRIPT_PATH, "downloads")
SQLITE_DB_PATH = os.path.join(SCRIPT_PATH, "sqlite_dbs")
LOG_LOCATION = os.path.join(SCRIPT_PATH, "logs", "log.log")

SECRETS = load_secrets(ENV_FILE_PATH,
                       ["bi_hostname", "bi_username", "bi_password", "bi_database",
                        "marketo_munchkin_id", "marketo_client_id", "marketo_client_secret"])

DRY_RUN = False
CONTACTS_LIMIT = None       # None = no limit

log = Logger(log_file_location=LOG_LOCATION, log_file_backup_count=150, logging_level="DEBUG")
mc = MarketoClient(SECRETS.get("marketo_munchkin_id"), SECRETS.get("marketo_client_id"), SECRETS.get("marketo_client_secret"), requests_timeout=120)
bi_db = MySQLdb.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"))