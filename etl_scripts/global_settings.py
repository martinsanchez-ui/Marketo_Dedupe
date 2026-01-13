"""
gobal_settings.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Global settings for Marketo Dedupe
"""
import json
import os
import uuid

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

SENSITIVE_KEYS = {
    "access_token",
    "authorization",
    "client_secret",
    "refresh_token",
    "token",
}
PARTIAL_MASK_KEYS = {"client_id", "munchkin", "marketo_munchkin_id", "marketo_client_id"}


def generate_run_id():
    """Generates a simple correlation id for a run."""
    return f"{int(uuid.uuid4().int % 10**12):012d}"


def mask_value(value, visible=4):
    """Mask a value keeping only the first `visible` characters."""
    if value is None:
        return value
    value = str(value)
    if len(value) <= visible:
        return f"{value}***"
    return f"{value[:visible]}***"


def mask_email(email):
    """Mask an email address to avoid logging full PII."""
    if not email or "@" not in str(email):
        return email
    name, domain = str(email).split("@", 1)
    name_mask = f"{name[0]}***" if name else "***"
    return f"{name_mask}@{domain}"


def sanitize_payload(payload):
    """Return a sanitized payload safe for logging."""
    if isinstance(payload, dict):
        sanitized = {}
        for key, value in payload.items():
            key_lower = str(key).lower()
            if key_lower in SENSITIVE_KEYS:
                sanitized[key] = "***"
            elif key_lower in PARTIAL_MASK_KEYS:
                sanitized[key] = mask_value(value)
            else:
                sanitized[key] = sanitize_payload(value)
        return sanitized
    if isinstance(payload, list):
        return [sanitize_payload(item) for item in payload]
    if isinstance(payload, tuple):
        return tuple(sanitize_payload(item) for item in payload)
    if isinstance(payload, bytes):
        return f"<bytes length={len(payload)}>"
    if isinstance(payload, str) and len(payload) > 500:
        return f"{payload[:500]}...<truncated>"
    return payload


def safe_json_dump(payload):
    """Serialize payload safely for logs."""
    try:
        sanitized = sanitize_payload(payload)
        return json.dumps(sanitized, ensure_ascii=False, default=str)
    except Exception:
        return repr(payload)


def extract_marketo_error_info(error):
    """Extract code/message/error payload details from a Marketo exception or payload."""
    payload = None
    code = None
    message = None

    if isinstance(error, Exception):
        if getattr(error, "args", None):
            payload = error.args[0] if len(error.args) == 1 else list(error.args)
        if getattr(error, "code", None) is not None:
            code = str(getattr(error, "code", None))
        if getattr(error, "message", None) is not None:
            message = str(getattr(error, "message", None))
    else:
        payload = error

    if isinstance(payload, dict):
        if payload.get("errors"):
            err0 = payload.get("errors")[0]
            code = code or str(err0.get("code", ""))
            message = message or str(err0.get("message", ""))
        code = code or str(payload.get("code", "")) if payload.get("code", None) is not None else code
        message = message or str(payload.get("message", "")) if payload.get("message", None) is not None else message

    return code, message, payload
