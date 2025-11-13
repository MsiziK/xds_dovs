import time
from xml.etree import ElementTree as ET
import requests
import sqlite3
import os
import db_access
import base64
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from typing import Tuple
from loggin.handlers import RotatingFileHandler

# --- Logging setup ---
handler = RotatingFileHandler(
    "xds_dovs.log", maxBytes=5_000_000, backupCount=3
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[handler]
)

# Load environment variables
load_dotenv()

# === Safeguards & Paths ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "verifications.db")
LOG_FILE = os.path.join(BASE_DIR, "dov_audit_log.txt")
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(DB_FILE), "xds_dovs.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


def ensure_database() -> None:
    logger.info("Checking database at: %s", DB_FILE)
    recreate = False
    if not os.path.exists(DB_FILE):
        recreate = True
    else:
        try:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='verifications'")
            if not cur.fetchone():
                recreate = True
            conn.close()
        except sqlite3.DatabaseError:
            recreate = True

    if recreate:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                client_id TEXT,
                status TEXT,
                details TEXT,
                name TEXT,
                id_number TEXT,
                email TEXT,
                id_photo TEXT,
                selfie_photo TEXT
            )
            """
        )
        conn.commit()
        conn.close()
        logger.info("Database created with `verifications` table.")


def migrate_database_schema(db_path: str = DB_FILE) -> None:
    """Ensure the verifications table has all required columns. Adds missing columns if needed."""
    required_columns = {
        "id_photo": "TEXT",
        "selfie_photo": "TEXT",
        # Add future required columns here
    }

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Get existing column names
        cur.execute("PRAGMA table_info(verifications)")
        existing_cols = {row[1] for row in cur.fetchall()}

        # Add any missing columns
        for col, col_type in required_columns.items():
            if col not in existing_cols:
                logger.info("Adding missing column '%s' (%s) to verifications", col, col_type)
                cur.execute(f"ALTER TABLE verifications ADD COLUMN {col} {col_type}")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.exception("Failed to migrate database schema: %s", e)


def ensure_audit_log() -> None:
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.write("")
        else:
            with open(LOG_FILE, "a", encoding="utf-8"):
                pass
    except Exception:
        logger.exception("Could not ensure audit log")


def verified_within_last_3_months(id_number: str) -> bool:
    if not os.path.exists(DB_FILE):
        return False
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            "SELECT timestamp FROM verifications WHERE id_number = ? ORDER BY timestamp DESC LIMIT 1",
            (id_number,),
        )
        row = cur.fetchone()
        conn.close()
    except sqlite3.DatabaseError:
        return False
    if row:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                last_verified = datetime.strptime(row[0], fmt)
                return last_verified >= datetime.now() - timedelta(days=90)
            except Exception:
                continue
    return False


ensure_database()
migrate_database_schema()
ensure_audit_log()

# Constants from .env
XDS_URL = os.getenv("XDS_URL", "https://www.web.xds.co.za/xdsconnect/XDSConnectWS.asmx?WSDL")
DEFAULT_PRODUCT_ID = os.getenv("DEFAULT_PRODUCT_ID", "194")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
XDS_USER = os.getenv("XDS_USER", "TestUser_DOVS")
XDS_PASS = os.getenv("XDS_PASS", "xds100")

# --- HTTP helper ---
_session = requests.Session()
_session.headers.update({"Accept": "*/*"})


def _post_soap(url: str, body: str, headers: dict | None = None) -> requests.Response:
    h = {"Content-Type": "text/xml; charset=utf-8"}
    if headers:
        h.update(headers)
    resp = _session.post(url, data=body, headers=h, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


def save_photo_from_base64(data: str, filename: str) -> str | None:
    if not data:
        return None
    try:
        img_path = os.path.join(UPLOADS_DIR, filename)
        # ensure subdirectories exist (e.g., uploads/ids, uploads/selfies)
        os.makedirs(os.path.dirname(img_path), exist_ok=True)
        with open(img_path, "wb") as f:
            f.write(base64.b64decode(data))
        # return web-friendly relative path
        return f"uploads/{filename}".replace("\\", "/")
    except Exception:
        logger.exception("Failed to save photo %s", filename)
        return None


def log_verification_result(enquiry_id, enquiry_result_id, summary_dict, status, logfile=None):
    logfile = logfile or LOG_FILE
    try:
        with open(logfile, "a", encoding="utf-8") as log:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.write(f"Timestamp: {timestamp}\n")
            log.write(f"Enquiry ID: {enquiry_id}\n")
            log.write(f"Enquiry Result ID: {enquiry_result_id}\n")
            log.write(f"Status: {status}\n")
            if isinstance(summary_dict, dict):
                id_no = summary_dict.get("ID Number") or summary_dict.get("id_number") or ""
                if id_no:
                    log.write(f"ID Number: {id_no}\n")
                for k, v in summary_dict.items():
                    log.write(f"  {k}: {v}\n")
            log.write("--- Verification Session ---\n")
    except Exception:
        logger.exception("Could not write to audit log")


def print_verification_status(status, summary=None):
    logging.info("\n📣 Verification Status:", status)
    if summary:
        logging.info("🔍 Summary:")
        for k, v in summary.items():
            logging.info(f"  {k}: {v}")


def login_to_xds(username: str | None = None, password: str | None = None) -> str:
    username = username or XDS_USER
    password = password or XDS_PASS
    headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <Login xmlns="http://www.web.xds.co.za/XDSConnectWS">
      <strUser>{username}</strUser>
      <strPwd>{password}</strPwd>
    </Login>
  </soap12:Body>
</soap12:Envelope>"""
    resp = _post_soap(XDS_URL, body, headers)
    logging.info("XDS Login raw response:\n", resp.text)
    tree = ET.fromstring(resp.content)
    ticket = tree.find(".//{http://www.web.xds.co.za/XDSConnectWS}LoginResult")
    return ticket.text if ticket is not None else ""


def is_ticket_valid(ticket):
    headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <IsTicketValid xmlns="http://www.web.xds.co.za/XDSConnectWS">
      <XDSConnectTicket>{ticket}</XDSConnectTicket>
    </IsTicketValid>
  </soap12:Body>
</soap12:Envelope>"""
    resp = _post_soap(XDS_URL, body, headers)
    tree = ET.fromstring(resp.content)
    result = tree.find(".//{http://www.web.xds.co.za/XDSConnectWS}IsTicketValidResult")
    return result.text if result is not None else ""


def match_consumer(ticket, id_number, cell_number, reference="", voucher_code=""):
    # Build the SOAP 1.2 envelope first
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <ConnectConsumerMatchDOVS xmlns="http://www.web.xds.co.za/XDSConnectWS">
      <ConnectTicket>{ticket}</ConnectTicket>
      <ProductId>{DEFAULT_PRODUCT_ID}</ProductId>
      <IdNumber>{id_number}</IdNumber>
      <CellNumber>{cell_number}</CellNumber>
      <YourReference>{reference}</YourReference>
      <VoucherCode>{voucher_code}</VoucherCode>
    </ConnectConsumerMatchDOVS>
  </soap12:Body>
</soap12:Envelope>"""

    # Enforce SOAP 1.2 content type
    headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
    resp = _post_soap(XDS_URL, body, headers)

    # Parse the response exactly as you already do
    tree = ET.fromstring(resp.content)
    result_node = tree.find(".//{http://www.web.xds.co.za/XDSConnectWS}ConnectConsumerMatchDOVSResult")
    if result_node is None:
        return {"error": "No result found"}
    result_xml = ET.fromstring(result_node.text)
    enquiry_id = result_xml.findtext(".//EnquiryID")
    enquiry_result_id = result_xml.findtext(".//EnquiryResultID")
    return {"xml": result_node.text, "enquiry_id": enquiry_id, "enquiry_result_id": enquiry_result_id}


def request_facial_verification(ticket, enquiry_id, enquiry_result_id, redirect_url=""):
    """
    Initiates the DOVS facial verification request with XDS.
    Per XDS production spec, RedirectURL must be blank.
    """
    # --- SOAP 1.2 header (preferred) ---
    headers = {"Content-Type": "application/soap+xml; charset=utf-8"}

    # --- SOAP 1.2 envelope ---
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                     xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <ConnectDOVRequest xmlns="http://www.web.xds.co.za/XDSConnectWS">
          <ConnectTicket>{ticket}</ConnectTicket>
          <EnquiryID>{enquiry_id}</EnquiryID>
          <EnquiryResultID>{enquiry_result_id}</EnquiryResultID>
          <ProductID>{DEFAULT_PRODUCT_ID}</ProductID>
          <RedirectURL>{redirect_url}</RedirectURL>
        </ConnectDOVRequest>
      </soap12:Body>
    </soap12:Envelope>"""

    resp = _post_soap(XDS_URL, body, headers)
    resp.raise_for_status()
    xml_resp = resp.text
    logging.info("XDS Facial Verification Response:\n", xml_resp)

    tree = ET.fromstring(xml_resp)
    result = tree.find(".//{http://www.web.xds.co.za/XDSConnectWS}ConnectDOVRequestResult")
    return result.text if result is not None else ""


def get_dov_result(ticket, enquiry_id):
    headers = {"SOAPAction": "http://www.web.xds.co.za/XDSConnectWS/ConnectGetDOVResult"}
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ConnectGetDOVResult xmlns="http://www.web.xds.co.za/XDSConnectWS">
      <ConnectTicket>{ticket}</ConnectTicket>
      <EnquiryID>{enquiry_id}</EnquiryID>
    </ConnectGetDOVResult>
  </soap:Body>
</soap:Envelope>"""
    resp = _post_soap(XDS_URL, body, headers)
    tree = ET.fromstring(resp.content)
    result = tree.find(".//{http://www.web.xds.co.za/XDSConnectWS}ConnectGetDOVResultResult")
    return result.text if result is not None else ""


def summarize_consumer_info(xml_data):
    try:
        root = ET.fromstring(xml_data)
        details = root.find(".//ConsumerDetails")
        if details is None:
            return {}
        summary = {
            "Name": f"{details.findtext('FirstName', '')} {details.findtext('SecondName', '')} {details.findtext('Surname', '')}".strip(),
            "ID Number": details.findtext('IDNo', 'N/A'),
            "Date of Birth": details.findtext('BirthDate', 'N/A'),
            "Gender": details.findtext('Gender', 'N/A'),
            "Marital Status": details.findtext('MaritalStatusDesc', 'N/A'),
            "Contact Number": details.findtext('CellularNo', 'N/A'),
            "Email": details.findtext('EmailAddress', 'N/A'),
            "Address": details.findtext('ResidentialAddress', 'N/A'),
            "Employer": details.findtext('EmployerDetail', 'N/A'),
            "Privacy Status": details.findtext('PrivacyStatus', 'N/A'),
            "Verification Reference": details.findtext('ReferenceNo', 'N/A'),
        }
        logging.info("\n✅ Consumer Summary:")
        for k, v in summary.items():
            logging.info(f"{k}: {v}")
        return summary
    except Exception:
        logger.exception("Error parsing XML for consumer info")
        return {}


def poll_dov_result(ticket, enquiry_id, max_attempts=30, interval=10):
    logging.info("🔄 Polling DOV result...")
    for attempt in range(max_attempts):
        try:
            dov_result = get_dov_result(ticket, enquiry_id)
        except Exception:
            logger.exception("Error calling get_dov_result")
            dov_result = ""
        if dov_result and "<NoResult>" not in dov_result:
            logging.info(f"DOV Result found on attempt {attempt + 1}:")
            return dov_result
        logging.info(f"Attempt {attempt + 1}: No result yet. Retrying in {interval} seconds...")
        time.sleep(interval)
    return "⛔ DOV Result polling timed out after multiple attempts."


def insert_verification_to_db(enquiry_id, summary_data, id_photo_data=None, selfie_photo_data=None):
    """Insert verification into DB using db_access, with normalized photo paths."""
    id_photo_path = save_photo_from_base64(id_photo_data, f"ids/id_{enquiry_id}.jpg") if id_photo_data else None
    selfie_photo_path = save_photo_from_base64(selfie_photo_data, f"selfies/selfie_{enquiry_id}.jpg") if selfie_photo_data else None

    db_access.insert_verification(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        client_id=summary_data.get("Client ID", enquiry_id or "N/A"),
        status=summary_data.get("Status", "Success"),
        details=summary_data.get("Details", f"Verification for {summary_data.get('Name', 'N/A')} - Success"),
        name=summary_data.get("Name", "N/A"),
        id_number=summary_data.get("ID Number", "N/A"),
        email=summary_data.get("Email", "N/A"),
        id_photo=id_photo_path,
        selfie_photo=selfie_photo_path
    )

    logging.info("✅ Verification inserted into database with photos via db_access.")


def extract_photos_from_xml(xml: str) -> Tuple[str | None, str | None]:
    """Return (id_photo_b64, selfie_b64) from a DOV result XML."""
    try:
        root = ET.fromstring(xml)
        id_b64 = root.findtext(".//ConsumerIDPhoto", "") or None
        selfie_b64 = root.findtext(".//ConsumerCapturedPhoto", "") or None
        return id_b64, selfie_b64
    except Exception:
        logger.exception("Failed to parse photos from XML")
        return None, None


def insert_verification_with_xml(enquiry_id, summary_data, xml_data):
    """Extract photos from XML and insert into DB using db_access."""
    id_b64, selfie_b64 = extract_photos_from_xml(xml_data)
    id_photo_path = save_photo_from_base64(id_b64, f"ids/id_{enquiry_id}.jpg") if id_b64 else None
    selfie_photo_path = save_photo_from_base64(selfie_b64, f"selfies/selfie_{enquiry_id}.jpg") if selfie_b64 else None

    db_access.insert_verification(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        client_id=summary_data.get("Client ID", enquiry_id or "N/A"),
        status=summary_data.get("Status", "Success"),
        details=summary_data.get("Details", f"Verification for {summary_data.get('Name', 'N/A')} - Success"),
        name=summary_data.get("Name", "N/A"),
        id_number=summary_data.get("ID Number", "N/A"),
        email=summary_data.get("Email", "N/A"),
        id_photo=id_photo_path,
        selfie_photo=selfie_photo_path
    )


if __name__ == "__main__":
    id_number = "9104036161082"
    cell_number = "0732563864"
    ticket = login_to_xds()
    safe_ticket = ticket[:8] + "..." + ticket[-8:] if ticket else "None"
    logging.info(f"XDS Ticket: {safe_ticket}")
    validation_result = is_ticket_valid(ticket)
    logging.info("Ticket Validation Result:", validation_result)

    id_number = "9104036161082"
    if verified_within_last_3_months(id_number):
        logging.info(f"ID {id_number} already verified in the last 3 months.")
    else:
        logging.info(f"Proceeding with new verification for {id_number}.")

        match_result = match_consumer(ticket, id_number, cell_number)
        enquiry_id = match_result.get("enquiry_id")
        enquiry_result_id = match_result.get("enquiry_result_id")

        if not (enquiry_id and enquiry_result_id):
            logging.error("❌ Failed to get enquiry IDs from XDS:", match_result)
            exit(1)

        logging.info(f"✅ Enquiry IDs received: EnquiryID={enquiry_id}, EnquiryResultID={enquiry_result_id}")

        link = request_facial_verification(ticket, enquiry_id, enquiry_result_id, redirect_url="")
        if link:
            logging.info("📩 SMS verification link requested successfully!")
            logging.info("🔗 Verification link (for testing):", link)
        else:
            logging.error("❌ Failed to request facial verification.")
