import os
import sqlite3
from typing import List, Dict, Any

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "verifications.db")
LOG_FILE = os.path.join(BASE_DIR, "dov_audit_log.txt")


def normalize_path(path: str) -> str | None:
    """Ensure stored photo path is relative to /uploads."""
    if not path:
        return None
    p = str(path).replace("\\", "/").strip()
    if p.startswith("uploads/") or p.startswith("/uploads/"):
        return p.lstrip("/")
    return f"uploads/{os.path.basename(p)}"


def _delete_audit_blocks_by_id_number(id_number: str, log_path: str = None) -> int:
    """Remove all audit log blocks containing given ID number."""
    path = log_path or LOG_FILE
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        blocks = content.split("=" * 50 + "\n\n")
        kept, removed = [], 0
        for b in blocks:
            if not b.strip():
                continue
            if f"ID Number: {id_number}" in b:
                removed += 1
                continue
            kept.append(b)
        new_content = ""
        for b in kept:
            if not b.endswith("\n"):
                b += "\n"
            new_content += b + "=" * 50 + "\n\n"
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_content)
        return removed
    except Exception as e:
        print(f"[DEBUG] Audit log cleanup failed: {e}")
        return 0


def ensure_database():
    """Ensure DB and table exist."""
    recreate = False
    if not os.path.exists(DB_FILE):
        print("[DEBUG] Database file not found — creating new one.")
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
        cur.execute("""
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
        """)
        conn.commit()
        conn.close()
        print("[DEBUG] Database recreated successfully.")


def ensure_db_columns():
    """Ensure id_photo and selfie_photo columns exist in verifications table."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(verifications)")
    existing_cols = [row[1] for row in cur.fetchall()]

    if "id_photo" not in existing_cols:
        cur.execute("ALTER TABLE verifications ADD COLUMN id_photo TEXT;")
        print("[DEBUG] Added missing column: id_photo")

    if "selfie_photo" not in existing_cols:
        cur.execute("ALTER TABLE verifications ADD COLUMN selfie_photo TEXT;")
        print("[DEBUG] Added missing column: selfie_photo")

    conn.commit()
    conn.close()


# Run safety checks at import
ensure_database()
ensure_db_columns()


def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = dict_factory
    return conn


def insert_verification(timestamp: str, client_id: str, status: str,
                        details: str = None, name: str = None, id_number: str = None,
                        email: str = None, id_photo: str = None, selfie_photo: str = None) -> int:
    """Insert a new verification into DB."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO verifications (timestamp, client_id, status, details, name, id_number, email, id_photo, selfie_photo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        timestamp, client_id, status, details, name, id_number, email,
        normalize_path(id_photo), normalize_path(selfie_photo)
    ))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id


def fetch_all_verifications() -> List[Dict[str, Any]]:
    """Fetch all verifications as dicts, ordered by timestamp DESC."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, timestamp, client_id, status, details, name, id_number, email, id_photo, selfie_photo
        FROM verifications
        ORDER BY datetime(timestamp) DESC, id DESC
    """)
    rows = cur.fetchall()
    conn.close()

    logs: List[Dict[str, Any]] = []
    for r in rows:
        r["date_str"] = r.get("timestamp", "")
        r["id_photo"] = normalize_path(r.get("id_photo"))
        r["selfie_photo"] = normalize_path(r.get("selfie_photo"))
        logs.append(r)
    return logs


def delete_verification(rec_id: int) -> None:
    """Delete a single verification by row ID, remove linked files if they exist."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id_photo, selfie_photo FROM verifications WHERE id=?", (rec_id,))
    row = cur.fetchone()

    if row:
        for col in ("id_photo", "selfie_photo"):
            f = row.get(col)
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                    print(f"[DEBUG] Removed file: {f}")
                except Exception as e:
                    print(f"[DEBUG] Failed to remove {f}: {e}")

    cur.execute("DELETE FROM verifications WHERE id=?", (rec_id,))
    conn.commit()
    conn.close()


def delete_by_id_number(id_number: str) -> bool:
    """Delete verification(s) by ID number, and prune audit log entries + linked files."""
    print(f"[DEBUG] Attempting to delete ID number: {id_number}")

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id_photo, selfie_photo FROM verifications WHERE TRIM(id_number) = ?", (id_number.strip(),))
    rows = cur.fetchall()

    # Delete linked files
    for r in rows:
        for f in r:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                    print(f"[DEBUG] Removed file: {f}")
                except Exception as e:
                    print(f"[DEBUG] Failed to remove {f}: {e}")

    cur.execute("DELETE FROM verifications WHERE TRIM(id_number) = ?", (id_number.strip(),))
    deleted_count = cur.rowcount
    conn.commit()
    conn.close()
    print(f"[DEBUG] Rows deleted from DB: {deleted_count}")

    removed_blocks = _delete_audit_blocks_by_id_number(id_number)
    print(f"[DEBUG] Removed {removed_blocks} matching audit log blocks.")
    return deleted_count > 0 or removed_blocks > 0
