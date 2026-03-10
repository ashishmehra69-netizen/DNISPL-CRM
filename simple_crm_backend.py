import csv
import os
import sqlite3
from datetime import datetime
from io import StringIO

from flask import Flask, jsonify, request


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


DB_PATH = os.environ.get(
    "CRM_DB_PATH",
    os.path.join(os.getcwd(), "data", "simple_crm.db"),
)

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

app = Flask(__name__)


@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, OPTIONS"
    return resp


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                name TEXT,
                role TEXT DEFAULT 'account_manager',
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT UNIQUE,
                account_manager_id INTEGER,
                created_at TEXT,
                updated_at TEXT,
                FOREIGN KEY(account_manager_id) REFERENCES users(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activities (
                id TEXT PRIMARY KEY,
                type TEXT,
                subject TEXT,
                notes TEXT,
                date TEXT,
                owner TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_passwords (
                email TEXT PRIMARY KEY,
                password TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


init_db()


def ensure_user(manager_value: str) -> int:
    """
    Resolve manager by email first, then by name.
    If missing, create as account_manager.
    """
    value = (manager_value or "").strip()
    if not value:
        raise ValueError("account_manager is required")

    conn = get_db()
    try:
        # Try email match
        if "@" in value:
            row = conn.execute(
                "SELECT id FROM users WHERE lower(email)=lower(?)",
                (value,),
            ).fetchone()
            if row:
                return int(row["id"])
            cur = conn.execute(
                "INSERT INTO users (email, name, role, created_at) VALUES (?, ?, 'account_manager', ?)",
                (value, value.split("@")[0], utc_now()),
            )
            conn.commit()
            return int(cur.lastrowid)

        # Try exact name match
        row = conn.execute(
            "SELECT id FROM users WHERE lower(name)=lower(?)",
            (value,),
        ).fetchone()
        if row:
            return int(row["id"])

        # Create placeholder user from name
        placeholder_email = f"{value.lower().replace(' ', '.')}@local.crm"
        cur = conn.execute(
            "INSERT OR IGNORE INTO users (email, name, role, created_at) VALUES (?, ?, 'account_manager', ?)",
            (placeholder_email, value, utc_now()),
        )
        conn.commit()
        if cur.lastrowid:
            return int(cur.lastrowid)

        row = conn.execute(
            "SELECT id FROM users WHERE lower(name)=lower(?)",
            (value,),
        ).fetchone()
        if not row:
            raise ValueError(f"Could not resolve manager: {value}")
        return int(row["id"])
    finally:
        conn.close()


def upsert_account(account_name: str, manager_id: int) -> str:
    name = (account_name or "").strip()
    if not name:
        raise ValueError("account_name is required")

    conn = get_db()
    try:
        now = utc_now()
        row = conn.execute(
            "SELECT id FROM accounts WHERE lower(account_name)=lower(?)",
            (name,),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE accounts SET account_manager_id=?, updated_at=? WHERE id=?",
                (manager_id, now, int(row["id"])),
            )
            conn.commit()
            return "updated"

        conn.execute(
            "INSERT INTO accounts (account_name, account_manager_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (name, manager_id, now, now),
        )
        conn.commit()
        return "created"
    finally:
        conn.close()


@app.route("/api/health", methods=["GET"])
def health():
    conn = get_db()
    try:
        users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        accounts = conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()["c"]
        return jsonify(
            {
                "status": "ok",
                "db_path": DB_PATH,
                "users": users,
                "accounts": accounts,
            }
        )
    finally:
        conn.close()


@app.route("/api/users", methods=["GET"])
def list_users():
    conn = get_db()
    try:
        role = (request.args.get("role") or "").strip().lower()
        if role:
            rows = conn.execute(
                "SELECT id, email, name, role FROM users WHERE lower(role)=lower(?) ORDER BY name",
                (role,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, email, name, role FROM users ORDER BY name"
            ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/accounts", methods=["GET"])
def list_accounts():
    """
    Visibility:
    - supervisor/admin -> all accounts
    - account_manager -> only assigned accounts
    via query params: viewer_email, viewer_role
    """
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_db()
    try:
        if viewer_role in ("supervisor", "admin"):
            rows = conn.execute(
                """
                SELECT a.id, a.account_name, a.created_at, a.updated_at,
                       u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                FROM accounts a
                LEFT JOIN users u ON u.id = a.account_manager_id
                ORDER BY a.account_name
                """
            ).fetchall()
            return jsonify([dict(r) for r in rows])

        if not viewer_email:
            return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

        manager = conn.execute(
            "SELECT id FROM users WHERE lower(email)=lower(?)",
            (viewer_email,),
        ).fetchone()
        if not manager:
            return jsonify([])

        rows = conn.execute(
            """
            SELECT a.id, a.account_name, a.created_at, a.updated_at,
                   u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
            FROM accounts a
            LEFT JOIN users u ON u.id = a.account_manager_id
            WHERE a.account_manager_id = ?
            ORDER BY a.account_name
            """,
            (int(manager["id"]),),
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/accounts", methods=["POST"])
def create_or_update_account():
    data = request.get_json(silent=True) or {}
    account_name = (data.get("account_name") or "").strip()
    account_manager = (data.get("account_manager") or "").strip()
    if not account_name or not account_manager:
        return jsonify({"error": "account_name and account_manager are required"}), 400

    try:
        manager_id = ensure_user(account_manager)
        result = upsert_account(account_name, manager_id)
        return jsonify({"status": result, "account_name": account_name}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/accounts/import", methods=["POST"])
def import_accounts():
    """
    Multipart upload:
    - form-data key: file (CSV with account_name,account_manager)
    """
    if "file" not in request.files:
        return jsonify({"error": "Missing file in form-data"}), 400

    file_obj = request.files["file"]
    if not file_obj or not file_obj.filename:
        return jsonify({"error": "Invalid file"}), 400

    try:
        content = file_obj.read().decode("utf-8", errors="replace")
        reader = csv.DictReader(StringIO(content))
    except Exception as exc:
        return jsonify({"error": f"Could not read CSV: {exc}"}), 400

    created = 0
    updated = 0
    failed = []

    for idx, row in enumerate(reader, start=2):
        name = (row.get("account_name") or "").strip()
        manager = (row.get("account_manager") or "").strip()
        if not name or not manager:
            failed.append({"row": idx, "error": "Missing account_name/account_manager"})
            continue
        try:
            manager_id = ensure_user(manager)
            result = upsert_account(name, manager_id)
            if result == "created":
                created += 1
            else:
                updated += 1
        except Exception as exc:
            failed.append({"row": idx, "error": str(exc), "account_name": name})

    return jsonify(
        {
            "created": created,
            "updated": updated,
            "failed_count": len(failed),
            "failed_rows": failed[:50],
        }
    )


@app.route("/api/activities", methods=["GET"])
def list_activities():
    """
    Visibility:
    - supervisor/admin -> all activities
    - others -> only own activities by viewer_email
    query params: viewer_email, viewer_role
    """
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_db()
    try:
        if viewer_role in ("supervisor", "admin"):
            rows = conn.execute(
                """
                SELECT id, type, subject, notes, date, owner, created_at, updated_at
                FROM activities
                ORDER BY datetime(date) DESC, datetime(updated_at) DESC
                """
            ).fetchall()
            return jsonify([dict(r) for r in rows])

        if not viewer_email:
            return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

        rows = conn.execute(
            """
            SELECT id, type, subject, notes, date, owner, created_at, updated_at
            FROM activities
            WHERE lower(owner)=lower(?)
            ORDER BY datetime(date) DESC, datetime(updated_at) DESC
            """,
            (viewer_email,),
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/activities", methods=["POST"])
def upsert_activity():
    data = request.get_json(silent=True) or {}
    activity_id = (data.get("id") or "").strip()
    activity_type = (data.get("type") or "").strip()
    subject = (data.get("subject") or "").strip()
    notes = (data.get("notes") or "").strip()
    date = (data.get("date") or "").strip()
    owner = (data.get("owner") or "").strip()

    if not activity_type or not subject or not date or not owner:
        return jsonify({"error": "type, subject, date, owner are required"}), 400

    now = utc_now()
    if not activity_id:
        activity_id = f"act_{int(datetime.utcnow().timestamp() * 1000)}"

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, owner, created_at FROM activities WHERE id=?",
            (activity_id,),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE activities
                SET type=?, subject=?, notes=?, date=?, owner=?, updated_at=?
                WHERE id=?
                """,
                (activity_type, subject, notes, date, owner, now, activity_id),
            )
            status = "updated"
        else:
            conn.execute(
                """
                INSERT INTO activities (id, type, subject, notes, date, owner, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (activity_id, activity_type, subject, notes, date, owner, now, now),
            )
            status = "created"
        conn.commit()
        return jsonify({"status": status, "id": activity_id})
    finally:
        conn.close()


@app.route("/api/activities/<activity_id>", methods=["DELETE"])
def delete_activity(activity_id: str):
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, owner FROM activities WHERE id=?",
            (activity_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "activity not found"}), 404

        if viewer_role not in ("supervisor", "admin"):
            if not viewer_email:
                return jsonify({"error": "viewer_email is required"}), 400
            if (row["owner"] or "").lower() != viewer_email.lower():
                return jsonify({"error": "not allowed"}), 403

        conn.execute("DELETE FROM activities WHERE id=?", (activity_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": activity_id})
    finally:
        conn.close()


@app.route("/api/passwords/<email>", methods=["GET"])
def get_password(email: str):
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    if viewer_role not in ("supervisor", "admin") and viewer_email != (email or "").strip().lower():
        return jsonify({"error": "not allowed"}), 403
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT email, password, updated_at FROM user_passwords WHERE lower(email)=lower(?)",
            (email,),
        ).fetchone()
        if not row:
            return jsonify({"email": email, "password": None})
        return jsonify(dict(row))
    finally:
        conn.close()


@app.route("/api/passwords", methods=["POST"])
def set_password():
    data = request.get_json(silent=True) or {}
    viewer_role = (data.get("viewer_role") or "account_manager").strip().lower()
    viewer_email = (data.get("viewer_email") or "").strip().lower()
    email = (data.get("email") or "").strip()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"error": "email and password required"}), 400
    if viewer_role not in ("supervisor", "admin"):
        if not viewer_email or viewer_email != email.lower():
            return jsonify({"error": "not allowed"}), 403
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO user_passwords (email, password, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(email) DO UPDATE SET password=excluded.password, updated_at=excluded.updated_at",
            (email, password, utc_now()),
        )
        conn.commit()
        return jsonify({"status": "ok", "email": email})
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"Simple CRM backend running on port {port}")
    print(f"DB path: {DB_PATH}")
    app.run(host="0.0.0.0", port=port, debug=True)
