import db_access

DB_FILE = "verifications.db"
LOG_FILE = "dov_audit_log.txt"

def ensure_db_columns():
    """Ensure id_photo and selfie_photo columns exist in verifications table."""
    conn = db_access.get_conn()
    c = conn.cursor()
    c.execute("PRAGMA table_info(verifications)")
    existing_cols = [row[1] for row in c.fetchall()]

    if "id_photo" not in existing_cols:
        c.execute("ALTER TABLE verifications ADD COLUMN id_photo TEXT;")
    if "selfie_photo" not in existing_cols:
        c.execute("ALTER TABLE verifications ADD COLUMN selfie_photo TEXT;")

    conn.commit()
    conn.close()

def parse_session_block(block):
    """Extract verification session details from log block."""
    session = {
        "timestamp": None,
        "client_id": None,
        "status": None,
        "details": None,
        "name": None,
        "id_number": None,
        "email": None,
        "id_photo": None,
        "selfie_photo": None
    }

    for line in block.split("\n"):
        line = line.strip()
        if line.startswith("Timestamp:"):
            session["timestamp"] = line.split("Timestamp:")[1].strip()
        elif line.startswith("ClientID:"):
            session["client_id"] = line.split("ClientID:")[1].strip()
        elif line.startswith("Verification Status:"):
            session["status"] = line.split("Verification Status:")[1].strip()
        elif line.startswith("Details:"):
            session["details"] = line.split("Details:")[1].strip()
        elif line.startswith("Name:"):
            session["name"] = line.split("Name:")[1].strip()
        elif line.startswith("ID Number:"):
            session["id_number"] = line.split("ID Number:")[1].strip()
        elif line.startswith("Email:"):
            session["email"] = line.split("Email:")[1].strip()
        elif line.startswith("ConsumerIDPhoto:"):
            session["id_photo"] = line.split("ConsumerIDPhoto:")[1].strip()
        elif line.startswith("ConsumerCapturedPhoto:"):
            session["selfie_photo"] = line.split("ConsumerCapturedPhoto:")[1].strip()

    return session

def insert_into_db(session):
    """Insert a parsed session into DB using db_access."""
    db_access.insert_verification(
        timestamp=session.get("timestamp"),
        client_id=session.get("client_id"),
        status=session.get("status"),
        details=session.get("details"),
        name=session.get("name"),
        id_number=session.get("id_number"),
        email=session.get("email"),
        id_photo=session.get("id_photo"),
        selfie_photo=session.get("selfie_photo")
    )


def retrofill_photos():
    """Update old DB records with photos from the log if missing."""
    logs = db_access.fetch_all_verifications()
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        blocks = f.read().split("--- Verification Session ---")

    updated_count = 0
    for block in blocks:
        if "Timestamp:" in block and "ClientID:" in block:
            session = parse_session_block(block)
            for log in logs:
                if log['timestamp'] == session['timestamp'] and log['client_id'] == session['client_id']:
                    new_id_photo = log['id_photo'] or session['id_photo']
                    new_selfie_photo = log['selfie_photo'] or session['selfie_photo']
                    if new_id_photo != log['id_photo'] or new_selfie_photo != log['selfie_photo']:
                        conn = db_access.get_conn()
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE verifications
                            SET id_photo=?, selfie_photo=?
                            WHERE timestamp=? AND client_id=?
                        """, (new_id_photo, new_selfie_photo, log['timestamp'], log['client_id']))
                        conn.commit()
                        conn.close()
                        updated_count += 1

    print(f"Retrofill complete. Updated {updated_count} records.")


def process_log_file():
    """Insert new log sessions into DB using db_access."""
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    blocks = content.split("--- Verification Session ---")
    for block in blocks:
        if "Timestamp:" in block and "ClientID:" in block:
            session = parse_session_block(block)
            # Check if already exists
            exists = any(log['timestamp'] == session['timestamp'] and log['client_id'] == session['client_id']
                         for log in db_access.fetch_all_verifications())
            if not exists:
                insert_into_db(session)


if __name__ == "__main__":
    ensure_db_columns()
    process_log_file()
    retrofill_photos()
    print("Log processing complete. Data inserted and retrofilled.")
