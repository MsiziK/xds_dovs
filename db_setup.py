
import sqlite3

DB_FILE = "verifications.db"

SCHEMA_SQL = '''
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
);
'''


def init_db(db_path: str = DB_FILE):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path}")


if __name__ == "__main__":
    init_db()
