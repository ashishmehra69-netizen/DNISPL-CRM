import csv
import fitz
import json
import base64
import hashlib
import hmac
import re
import urllib.parse
import urllib.request
import os
import smtplib
import threading
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from io import StringIO
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def parse_iso_dt(value: str):
    s = (value or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("SUPABASE_DB_URL")
    or os.environ.get("SUPABASE_DATABASE_URL")
)
PRESALES_OWNER = os.environ.get("PRESALES_OWNER", "vinod.v@dnispl.com")
SALES_OPS_OWNER = os.environ.get("SALES_OPS_OWNER", "soumya.m@dnispl.com")
SUPERVISOR_EMAIL = os.environ.get("SUPERVISOR_EMAIL", "ashish.mehra@dnispl.com").strip().lower()
SUPERVISOR_EMAILS_ALL = [SUPERVISOR_EMAIL, 'a.gupta@dnispl.com', 'rakesh.uniyal@dnispl.com']
ESCALATION_EMAILS = [
    e.strip().lower()
    for e in os.environ.get("ESCALATION_EMAILS", "ashish.mehra@dnispl.com,a.gupta@dnispl.com").split(",")
    if e.strip()
]
SMTP_HOST = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER or "noreply@dnispl.com")

MS_CLIENT_ID = os.environ.get("MS_CLIENT_ID", "").strip()
MS_CLIENT_SECRET = os.environ.get("MS_CLIENT_SECRET", "").strip()
MS_TENANT_ID = os.environ.get("MS_TENANT_ID", "common").strip() or "common"
MS_REDIRECT_URI = os.environ.get("MS_REDIRECT_URI", "").strip()
MS_OAUTH_SCOPES = os.environ.get("MS_OAUTH_SCOPES", "offline_access openid profile email Mail.Send")
OAUTH_STATE_SECRET = os.environ.get("OAUTH_STATE_SECRET", os.environ.get("PASSWORD", "crm2026")).strip() or "crm2026"

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is required (set it to your Supabase Postgres connection string)."
    )


def _strip_sslmode(url: str) -> str:
    """Remove sslmode from the URL so we can pass it as a kwarg instead."""
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query.pop("sslmode", None)
    return urlunparse(parsed._replace(query=urlencode(query)))


DATABASE_URL = _strip_sslmode(DATABASE_URL)

app = Flask(__name__)
_db_init_done = False
_db_init_lock = threading.Lock()
_write_limits = {}
_write_limits_lock = threading.Lock()


@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return resp


def get_conn():
    last_exc = None
    for attempt in range(2):
        try:
            return psycopg2.connect(
                DATABASE_URL,
                sslmode="require",
                connect_timeout=3,
                options="-c statement_timeout=30000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=10000",
                application_name="dnispl-crm",
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
        except Exception as exc:
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    raise last_exc


def init_db() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email TEXT UNIQUE,
                    name TEXT,
                    role TEXT DEFAULT 'account_manager',
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    account_name TEXT UNIQUE,
                    account_manager_id INTEGER REFERENCES users(id),
                    industry TEXT,
                    tier TEXT,
                    location TEXT,
                    company_size TEXT,
                    annual_spend TEXT,
                    mode TEXT,
                    suspect_q1 TEXT,
                    suspect_q2 TEXT,
                    suspect_q3 TEXT,
                    suspect_q4 TEXT,
                    suspect_q5 TEXT,
                    suspect_q6 TEXT,
                    suspect_q7 TEXT,
                    suspect_q8 TEXT,
                    suspect_q9 TEXT,
                    suspect_q10 TEXT,
                    suspect_score INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS industry TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS tier TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS location TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS company_size TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS annual_spend TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS mode TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q1 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q2 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q3 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q4 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q5 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q6 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q7 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q8 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q9 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_q10 TEXT;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS suspect_score INTEGER DEFAULT 0;")
            cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_manager_id INTEGER REFERENCES users(id);")
            cur.execute(
                """
                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='accounts' AND column_name='account_manager'
                  ) THEN
                    INSERT INTO users (email, name, role, created_at)
                    SELECT DISTINCT
                      lower(trim(account_manager)) AS email,
                      split_part(lower(trim(account_manager)), '@', 1) AS name,
                      'account_manager',
                      now()
                    FROM accounts
                    WHERE account_manager_id IS NULL
                      AND account_manager IS NOT NULL
                      AND trim(account_manager) <> ''
                      AND position('@' in account_manager) > 1
                    ON CONFLICT (email) DO NOTHING;

                    UPDATE accounts a
                    SET account_manager_id = u.id
                    FROM users u
                    WHERE a.account_manager_id IS NULL
                      AND lower(trim(a.account_manager)) = lower(u.email);
                  END IF;

                  IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='accounts' AND column_name='account_manager_email'
                  ) THEN
                    INSERT INTO users (email, name, role, created_at)
                    SELECT DISTINCT
                      lower(trim(account_manager_email)) AS email,
                      split_part(lower(trim(account_manager_email)), '@', 1) AS name,
                      'account_manager',
                      now()
                    FROM accounts
                    WHERE account_manager_id IS NULL
                      AND account_manager_email IS NOT NULL
                      AND trim(account_manager_email) <> ''
                      AND position('@' in account_manager_email) > 1
                    ON CONFLICT (email) DO NOTHING;

                    UPDATE accounts a
                    SET account_manager_id = u.id
                    FROM users u
                    WHERE a.account_manager_id IS NULL
                      AND lower(trim(a.account_manager_email)) = lower(u.email);
                  END IF;
                END $$;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS activities (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    subject TEXT,
                    notes TEXT,
                    date TEXT,
                    owner TEXT,
                    account_id TEXT,
                    account_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS type TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS subject TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS notes TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS date TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS owner TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS account_id TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS account_name TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS mom_sent_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS mom_sent_to TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS mom_send_status TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS mom_send_error TEXT;")
            cur.execute("ALTER TABLE activities ADD COLUMN IF NOT EXISTS mom_payload TEXT;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    company TEXT,
                    email TEXT,
                    phone TEXT,
                    source TEXT,
                    status TEXT,
                    notes TEXT,
                    owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contacts (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    title TEXT,
                    email TEXT,
                    phone TEXT,
                    role_type TEXT,
                    influence_level TEXT,
                    emotion TEXT,
                    account_id TEXT,
                    owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS opportunities (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    account_id TEXT,
                    value NUMERIC DEFAULT 0,
                    stage TEXT,
                    deal_type TEXT,
                    owner TEXT,
                    sales_owner TEXT,
                    workflow_stage TEXT,
                    assigned_presales TEXT,
                    assigned_salesops TEXT,
                    assigned_purchase TEXT,
                    sales_comments TEXT,
                    sales_ops_comments TEXT,
                    requirements TEXT,
                    presales_architecture TEXT,
                    presales_questions TEXT,
                    boq TEXT,
                    purchase_costing TEXT,
                    costing_tat TEXT,
                    final_pricing_proposal TEXT,
                    presales_assigned_at TEXT,
                    presales_due_at TEXT,
                    salesops_assigned_at TEXT,
                    salesops_due_at TEXT,
                    purchase_assigned_at TEXT,
                    purchase_due_at TEXT,
                    costing_returned_at TEXT,
                    final_proposal_at TEXT,
                    assignment_due_at TEXT,
                    sales_submitted_at TEXT,
                    presales_escalated_at TEXT,
                    oem_pricing_required BOOLEAN DEFAULT FALSE,
                    intake_problem_statement TEXT,
                    intake_why_now TEXT,
                    intake_business_impact TEXT,
                    intake_current_state TEXT,
                    intake_budget_range TEXT,
                    intake_decision_timeline TEXT,
                    intake_risk_if_not_solved TEXT,
                    intake_key_stakeholders TEXT,
                    intake_in_scope TEXT,
                    intake_out_of_scope TEXT,
                    intake_current_environment TEXT,
                    intake_pain_points TEXT,
                    intake_compliance_requirements TEXT,
                    intake_integration_requirements TEXT,
                    intake_competitors TEXT,
                    intake_win_strategy TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS presales_escalated_at TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS deal_type TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS assigned_salesops TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS sales_ops_comments TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS salesops_assigned_at TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS salesops_due_at TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS oem_pricing_required BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_problem_statement TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_why_now TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_business_impact TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_current_state TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_budget_range TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_decision_timeline TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_risk_if_not_solved TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_key_stakeholders TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_in_scope TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_out_of_scope TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_current_environment TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_pain_points TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_compliance_requirements TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_integration_requirements TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_competitors TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS intake_win_strategy TEXT;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aop_plans (
                    account_id TEXT NOT NULL,
                    fy_year TEXT NOT NULL,
                    plan_data JSONB DEFAULT '{}'::jsonb,
                    owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (account_id, fy_year)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aop_actuals (
                    account_id TEXT NOT NULL,
                    fy_year TEXT NOT NULL,
                    month TEXT NOT NULL,
                    hardware NUMERIC DEFAULT 0,
                    software NUMERIC DEFAULT 0,
                    managed_services NUMERIC DEFAULT 0,
                    owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (account_id, fy_year, month)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_passwords (
                    email TEXT PRIMARY KEY,
                    password TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_salary (
                    email TEXT PRIMARY KEY,
                    salary_data JSONB DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            
            # Performance indexes for concurrent access patterns.
            cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_manager_id ON accounts(account_manager_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_activities_owner ON activities(lower(owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_owner ON leads(lower(owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_owner ON contacts(lower(owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_account_id ON contacts(account_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_owner ON opportunities(lower(owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_sales_owner ON opportunities(lower(sales_owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_assigned_presales ON opportunities(lower(assigned_presales));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_assigned_salesops ON opportunities(lower(assigned_salesops));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_assigned_purchase ON opportunities(lower(assigned_purchase));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_account_id ON opportunities(account_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_aop_plans_fy ON aop_plans(fy_year);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_aop_actuals_fy ON aop_actuals(fy_year);")
            # Create Ayushi and Mandeep if not exist
            cur.execute("""INSERT INTO users (email, name, role, created_at) VALUES ('ayushi.v@dnispl.com','Ayushi V','account_manager',now()) ON CONFLICT (email) DO NOTHING;""")
            cur.execute("""INSERT INTO users (email, name, role, created_at) VALUES ('mandeep.kaur@dnispl.com','Mandeep Kaur','account_manager',now()) ON CONFLICT (email) DO NOTHING;""")
            cur.execute("""INSERT INTO user_passwords (email, password) VALUES ('ayushi.v@dnispl.com','crm2026') ON CONFLICT (email) DO NOTHING;""")
            cur.execute("""INSERT INTO user_passwords (email, password) VALUES ('mandeep.kaur@dnispl.com','crm2026') ON CONFLICT (email) DO NOTHING;""")
        conn.commit()
    finally:
        conn.close()


def ensure_db_initialized():
    global _db_init_done
    if _db_init_done:
        return
    with _db_init_lock:
        if _db_init_done:
            return
        init_db()
        _db_init_done = True


@app.errorhandler(Exception)
def _json_exception_handler(exc):
    if isinstance(exc, HTTPException):
        return jsonify({"error": exc.description}), exc.code
    return jsonify({"error": f"internal server error: {exc}"}), 500


@app.before_request
def _ensure_init_once():
    pass  # Schema already initialized


def _client_ip() -> str:
    xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if xff:
        return xff
    xrip = (request.headers.get("x-real-ip") or "").strip()
    if xrip:
        return xrip
    return (request.remote_addr or "").strip()


def _is_scanner_path(path: str) -> bool:
    p = (path or "").lower()
    if p.startswith("/wp-") or p.startswith("/wordpress") or p.startswith("/xmlrpc"):
        return True
    if p.endswith(".php") or "/wp-content/" in p or "/wp-admin/" in p:
        return True
    bad = ("/av.php", "/dx.php", "/ms-edit.php", "/admin.php")
    return p in bad


@app.before_request
def _shield_scanner_noise_and_log():
    path = request.path or ""
    if _is_scanner_path(path):
        print(
            f"[BOT_BLOCK] ip={_client_ip()} method={request.method} path={path} "
            f"ua={(request.headers.get('user-agent') or '-')[:160]}"
        )
        return jsonify({"error": "not found"}), 404


def _rate_limit_write(route_key: str, per_60s: int = 40):
    ip = _client_ip() or "unknown"
    now = time.time()
    key = f"{route_key}:{ip}"
    with _write_limits_lock:
        bucket = _write_limits.get(key, [])
        bucket = [t for t in bucket if now - t < 60]
        if len(bucket) >= per_60s:
            return False, ip
        bucket.append(now)
        _write_limits[key] = bucket
    return True, ip


def compute_suspect_score(data: dict) -> int:
    score = 0
    for idx in range(1, 11):
        if str(data.get(f"suspect_q{idx}") or "").strip():
            score += 1
    return score


def _is_supervisor(viewer_role: str) -> bool:
    return (viewer_role or "").strip().lower() in ("supervisor", "admin")


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return cur.fetchone() is not None


def _split_emails(value: str):
    if not value:
        return []
    out = []
    for part in str(value).replace(";", ",").split(","):
        e = _normalize_email(part)
        if e and "@" in e and e not in out:
            out.append(e)
    return out


def _build_oauth_state(email: str) -> str:
    payload = f"{_normalize_email(email)}|{int(time.time())}"
    sig = hmac.new(OAUTH_STATE_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def _verify_oauth_state(state: str, max_age_sec: int = 1800) -> str:
    try:
        decoded = base64.urlsafe_b64decode((state or "").encode("utf-8")).decode("utf-8")
        email, ts, sig = decoded.split("|", 2)
        payload = f"{email}|{ts}"
        expected = hmac.new(OAUTH_STATE_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return ""
        if abs(time.time() - int(ts)) > max_age_sec:
            return ""
        return _normalize_email(email)
    except Exception:
        return ""


def _http_form_post(url: str, form_data: dict):
    body = urllib.parse.urlencode(form_data).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_json_request(url: str, method: str = "GET", data=None, headers=None):
    payload = None if data is None else json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    if data is not None and "Content-Type" not in {k.title(): v for k, v in (headers or {}).items()}:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _get_user_row_by_email(email: str):
    email = _normalize_email(email)
    if not email:
        return None
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, email, name, role FROM users WHERE lower(email)=lower(%s)", (email,))
            row = cur.fetchone()
            if row:
                return row
            cur.execute(
                "INSERT INTO users (email, name, role, created_at) VALUES (%s, %s, 'account_manager', now()) RETURNING id, email, name, role",
                (email, email.split("@")[0]),
            )
            row = cur.fetchone()
        conn.commit()
        return row
    finally:
        conn.close()


def _upsert_o365_tokens(user_id: int, email: str, token_data: dict):
    conn = get_conn()
    try:
        expires_in = int(token_data.get("expires_in") or 3600)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_o365_tokens (user_id, email, tenant_id, refresh_token, access_token, expires_at, connected_at, status)
                VALUES (%s, %s, %s, %s, %s, now() + (%s || ' seconds')::interval, now(), 'active')
                ON CONFLICT (user_id)
                DO UPDATE SET
                    email=EXCLUDED.email,
                    tenant_id=EXCLUDED.tenant_id,
                    refresh_token=EXCLUDED.refresh_token,
                    access_token=EXCLUDED.access_token,
                    expires_at=EXCLUDED.expires_at,
                    connected_at=now(),
                    status='active'
                """,
                (
                    int(user_id),
                    _normalize_email(email),
                    MS_TENANT_ID,
                    token_data.get("refresh_token") or "",
                    token_data.get("access_token") or "",
                    str(expires_in),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _get_o365_token_row(email: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT user_id, email, tenant_id, refresh_token, access_token, expires_at, status FROM user_o365_tokens WHERE lower(email)=lower(%s)",
                (_normalize_email(email),),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _refresh_graph_token_if_needed(token_row: dict):
    if not token_row:
        raise ValueError("Microsoft 365 is not connected for this account manager")

    expires_at = token_row.get("expires_at")
    if expires_at and isinstance(expires_at, datetime):
        if expires_at > datetime.now(timezone.utc) + timedelta(minutes=5):
            return token_row.get("access_token") or ""

    refresh_token = (token_row.get("refresh_token") or "").strip()
    if not refresh_token:
        raise ValueError("Refresh token missing. Reconnect Microsoft 365.")

    token_url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    refreshed = _http_form_post(
        token_url,
        {
            "client_id": MS_CLIENT_ID,
            "client_secret": MS_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": MS_REDIRECT_URI,
            "scope": MS_OAUTH_SCOPES,
        },
    )

    _upsert_o365_tokens(int(token_row["user_id"]), token_row.get("email") or "", refreshed)
    return refreshed.get("access_token") or ""


def _send_graph_mail(sender_email: str, to_emails, cc_emails, subject: str, html_body: str):
    token_row = _get_o365_token_row(sender_email)
    access_token = _refresh_graph_token_if_needed(token_row)
    if not access_token:
        raise ValueError("Could not get Microsoft access token")

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in to_emails],
            "ccRecipients": [{"emailAddress": {"address": e}} for e in cc_emails],
            "replyTo": [{"emailAddress": {"address": _normalize_email(sender_email)}}],
        },
        "saveToSentItems": True,
    }

    return _http_json_request(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        method="POST",
        data=payload,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
    )


def _format_bullets(text: str):
    lines = [ln.strip(" -	") for ln in str(text or "").splitlines() if ln.strip()]
    if not lines:
        return "<li>NA</li>"
    return "".join([f"<li>{ln}</li>" for ln in lines])


def _format_action_rows(text: str):
    rows = []
    for ln in str(text or "").splitlines():
        raw = ln.strip()
        if not raw:
            continue
        parts = [p.strip() for p in raw.split("|")]
        while len(parts) < 4:
            parts.append("")
        rows.append(parts[:4])
    if not rows:
        rows = [["NA", "", "", "Open"]]
    return "".join([f"<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td></tr>" for r in rows])


def _build_mom_html(payload: dict):
    account_name = payload.get("account_name") or "Account"
    meeting_date = payload.get("meeting_date") or datetime.now().strftime("%d-%b-%Y")
    client_name = payload.get("client_name") or "Team"
    intro = payload.get("mom_intro") or ""
    discussion = payload.get("mom_discussion") or ""
    actions = payload.get("mom_actions") or ""
    next_steps = payload.get("mom_next_steps") or ""
    am_name = payload.get("account_manager_name") or "Account Manager"
    am_email = payload.get("account_manager_email") or ""

    return f"""
    <p>Hi {client_name},</p>
    <p>Thank you for your time today. Please find the minutes of meeting below.</p>
    <p><b>Introduction:</b><br>{intro}</p>
    <p><b>Discussion Points:</b></p>
    <ul>{_format_bullets(discussion)}</ul>
    <p><b>Action Points:</b></p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
      <tr><th>Action Item</th><th>Owner</th><th>Due Date</th><th>Status</th></tr>
      {_format_action_rows(actions)}
    </table>
    <p><b>Next Steps:</b><br>{next_steps}</p>
    <p>Please let us know if any point needs correction.</p>
    <p>Regards,<br>{am_name}<br>{am_email}<br>DNISPL</p>
    """


def send_email_smtp(to_emails, subject: str, body: str, cc_emails=None) -> bool:
    to_list = [e.strip().lower() for e in (to_emails or []) if (e or "").strip() and "@" in e]
    cc_list = [e.strip().lower() for e in (cc_emails or []) if (e or "").strip() and "@" in e]
    print(f"[CRM SMTP] Attempting send to={to_list} cc={cc_list} subject={subject}")
    if not to_list and not cc_list:
        print(f"[CRM SMTP] BLOCKED — no valid recipients")
        return False
    if not (SMTP_HOST and SMTP_USER and SMTP_PASSWORD):
        print(f"[CRM SMTP] BLOCKED — SMTP not configured. HOST={SMTP_HOST} USER={SMTP_USER}")
        return False
    msg = EmailMessage()
    msg["From"] = SMTP_FROM
    if to_list:
        msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"[CRM SMTP] Email sent successfully to={to_list} cc={cc_list}")
        return True
    except Exception as exc:
        print(f"[CRM SMTP] Email send FAILED: {exc}")
        return False

def send_presales_escalation_email(row, presales_due_iso: str) -> None:
    subject = f"[CRM Escalation] Presales SLA Breached: {row.get('name') or row.get('id')}"
    body = (
        f"Opportunity: {row.get('name') or ''}\n"
        f"Opportunity ID: {row.get('id') or ''}\n"
        f"Account ID: {row.get('account_id') or ''}\n"
        f"Sales Owner: {row.get('sales_owner') or row.get('owner') or ''}\n"
        f"Assigned Presales: {row.get('assigned_presales') or PRESALES_OWNER}\n"
        f"Current Workflow Stage: {row.get('workflow_stage') or ''}\n"
        f"Presales Due At (72h SLA): {presales_due_iso}\n\n"
        "Action Needed: Please review and expedite proposal submission."
    )
    send_email_smtp(ESCALATION_EMAILS, subject, body)


def send_presales_assignment_email(opportunity_name: str, opp_id: str, presales_email: str, presales_due_iso: str) -> None:
    target = (presales_email or "").strip().lower()
    if "@" not in target:
        return
    subject = f"[CRM] New Opportunity Assigned: {opportunity_name or opp_id}"
    body = (
        f"Opportunity: {opportunity_name or ''}\n"
        f"Opportunity ID: {opp_id}\n"
        f"Assigned To (Presales): {target}\n"
        f"Presales Due At (72h SLA): {presales_due_iso}\n\n"
        "Please review requirements and submit solution/proposal within SLA."
    )
    send_email_smtp([target], subject, body)


def send_opportunity_assignment_email(opportunity_name: str, opp_id: str, presales_email: str, sales_email: str, presales_due_iso: str, account_manager_email: str = "") -> None:
    presales_target = (presales_email or "").strip().lower()
    print(f"[CRM EMAIL] send_opportunity_assignment_email called: to={presales_target} cc_candidates={[SUPERVISOR_EMAIL, sales_email, account_manager_email]}")
    if "@" not in presales_target:
        print(f"[CRM EMAIL] Aborted — no valid presales email")
        return
    cc_list = []
    for e in SUPERVISOR_EMAILS_ALL + [sales_email, account_manager_email]:
        e = (e or "").strip().lower()
        if e and "@" in e and e != presales_target and e not in cc_list:
            cc_list.append(e)
    subject = f"[CRM] Opportunity Assigned to Presales: {opportunity_name or opp_id}"
    body = (
        f"Opportunity: {opportunity_name or ''}\n"
        f"Opportunity ID: {opp_id}\n"
        f"Sales Owner: {sales_email or 'NA'}\n"
        f"Assigned Presales: {presales_target}\n"
        f"Presales Due At (72h SLA): {presales_due_iso}\n\n"
        "Please review requirements and submit solution/proposal within SLA."
    )
    # Try Graph first (uses connected Microsoft 365), fall back to SMTP
    sent = False
    for sender in [SUPERVISOR_EMAIL] + cc_list:
        try:
            html_body = f"<pre style='font-family:sans-serif'>{body}</pre>"
            _send_graph_mail(sender, [presales_target], cc_list, subject, html_body)
            sent = True
            print(f"[CRM EMAIL] Sent via Graph as {sender}")
            break
        except Exception as eg:
            print(f"[CRM EMAIL] Graph failed for {sender}: {eg}")
    if not sent:
        send_email_smtp([presales_target], subject, body, cc_emails=cc_list)


def send_salesops_assignment_email(
    opportunity_name: str,
    opp_id: str,
    salesops_email: str,
    sales_email: str = "",
    presales_email: str = "",
    deal_type: str = "",
    due_iso: str = "",
    account_manager_email: str = "",
) -> None:
    target = (salesops_email or "").strip().lower()
    if "@" not in target:
        return
    cc_list = []
    for e in SUPERVISOR_EMAILS_ALL + [sales_email, presales_email, account_manager_email]:
        e = (e or "").strip().lower()
        if e and "@" in e and e != target and e not in cc_list:
            cc_list.append(e)
    subject = f"[CRM] Sales Ops Pricing Request: {opportunity_name or opp_id}"
    body = (
        f"Opportunity: {opportunity_name or ''}\n"
        f"Opportunity ID: {opp_id}\n"
        f"Sales Owner: {sales_email or 'NA'}\n"
        f"Assigned Sales Ops: {target}\n"
        f"Assigned Presales: {presales_email or 'NA'}\n"
        f"Deal Type: {deal_type or 'NA'}\n"
        f"Pricing Due At: {due_iso or 'NA'}\n\n"
        "Please coordinate with OEM / distributor and update pricing support in CRM."
    )
    # Try Graph first (uses connected Microsoft 365), fall back to SMTP
    sent = False
    for sender in [SUPERVISOR_EMAIL] + cc_list:
        try:
            html_body = f"<pre style='font-family:sans-serif'>{body}</pre>"
            _send_graph_mail(sender, [target], cc_list, subject, html_body)
            sent = True
            print(f"[CRM EMAIL] SalesOps mail sent via Graph as {sender}")
            break
        except Exception as eg:
            print(f"[CRM EMAIL] Graph failed for {sender}: {eg}")
    if not sent:
        send_email_smtp([target], subject, body, cc_emails=cc_list)

def enforce_opportunity_sla(conn, rows):
    now = datetime.now(timezone.utc)
    changed = False
    with conn.cursor() as cur:
        for row in rows:
            workflow_stage = (row.get("workflow_stage") or "Sales Review").strip()
            sales_submitted = parse_iso_dt(row.get("sales_submitted_at")) or parse_iso_dt(str(row.get("created_at") or ""))
            if not sales_submitted:
                sales_submitted = now
            assignment_due = parse_iso_dt(row.get("assignment_due_at")) or (sales_submitted + timedelta(hours=4))
            presales_due = parse_iso_dt(row.get("presales_due_at")) or (sales_submitted + timedelta(hours=72))

            updates = {}
            if not (row.get("sales_submitted_at") or "").strip():
                updates["sales_submitted_at"] = sales_submitted.isoformat().replace("+00:00", "Z")
            if not (row.get("assignment_due_at") or "").strip():
                updates["assignment_due_at"] = assignment_due.isoformat().replace("+00:00", "Z")
            if not (row.get("presales_due_at") or "").strip():
                updates["presales_due_at"] = presales_due.isoformat().replace("+00:00", "Z")

            if workflow_stage == "Sales Review" and now >= assignment_due:
                updates["workflow_stage"] = "Assigned to Presales"
                updates["assigned_presales"] = (row.get("assigned_presales") or "").strip() or PRESALES_OWNER
                updates["presales_assigned_at"] = (
                    parse_iso_dt(row.get("presales_assigned_at")) or assignment_due
                ).isoformat().replace("+00:00", "Z")
                send_presales_assignment_email(
                    row.get("name") or "",
                    row.get("id") or "",
                    updates["assigned_presales"],
                    presales_due.isoformat().replace("+00:00", "Z"),
                )

            has_proposal = bool((row.get("final_pricing_proposal") or "").strip())
            if has_proposal and workflow_stage != "Final Proposal Shared":
                updates["workflow_stage"] = "Final Proposal Shared"
                updates["final_proposal_at"] = (
                    parse_iso_dt(row.get("final_proposal_at")) or now
                ).isoformat().replace("+00:00", "Z")
            elif (
                not has_proposal
                and workflow_stage in ("Assigned to Presales", "Awaiting Sales Ops Pricing", "Pricing Returned to Presales")
                and now > presales_due
                and not (row.get("presales_escalated_at") or "").strip()
            ):
                updates["workflow_stage"] = "Presales Overdue"
                updates["presales_escalated_at"] = now.isoformat().replace("+00:00", "Z")
                send_presales_escalation_email(
                    row, presales_due.isoformat().replace("+00:00", "Z")
                )

            if updates:
                sets = ", ".join([f"{k}=%s" for k in updates.keys()] + ["updated_at=now()"])
                params = list(updates.values()) + [row["id"]]
                cur.execute(f"UPDATE opportunities SET {sets} WHERE id=%s", params)
                changed = True

    if changed:
        conn.commit()
    return changed


def ensure_user(manager_value: str) -> int:
    value = (manager_value or "").strip()
    if not value:
        raise ValueError("account_manager is required")

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if "@" in value:
                cur.execute(
                    "SELECT id FROM users WHERE lower(email)=lower(%s)",
                    (value,),
                )
                row = cur.fetchone()
                if row:
                    return int(row["id"])
                cur.execute(
                    "INSERT INTO users (email, name, role, created_at) VALUES (%s, %s, 'account_manager', now()) RETURNING id",
                    (value, value.split("@")[0]),
                )
                new_id = cur.fetchone()["id"]
                conn.commit()
                return int(new_id)

            cur.execute(
                "SELECT id FROM users WHERE lower(name)=lower(%s)",
                (value,),
            )
            row = cur.fetchone()
            if row:
                return int(row["id"])

            placeholder_email = f"{value.lower().replace(' ', '.')}@local.crm"
            cur.execute(
                "INSERT INTO users (email, name, role, created_at) VALUES (%s, %s, 'account_manager', now()) RETURNING id",
                (placeholder_email, value),
            )
            new_id = cur.fetchone()["id"]
            conn.commit()
            return int(new_id)
    finally:
        conn.close()


def upsert_account(data: dict, manager_id: int) -> str:
    name = (data.get("account_name") or "").strip()
    if not name:
        raise ValueError("account_name is required")
    account_id = str(data.get("id") or "").strip()

    industry = (data.get("industry") or "").strip()
    tier = (data.get("tier") or "").strip()
    location = (data.get("location") or "").strip()
    company_size = (data.get("company_size") or data.get("companySize") or "").strip()
    annual_spend = (data.get("annual_spend") or data.get("annualSpend") or "").strip()
    mode = (data.get("mode") or "").strip()
    suspect_answers = {
        f"suspect_q{i}": (data.get(f"suspect_q{i}") or "").strip()
        for i in range(1, 11)
    }
    suspect_score = compute_suspect_score(suspect_answers)

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if account_id.isdigit():
                cur.execute("SELECT id FROM accounts WHERE id=%s", (int(account_id),))
            else:
                cur.execute(
                    "SELECT id FROM accounts WHERE lower(account_name)=lower(%s)",
                    (name,),
                )
            row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    UPDATE accounts
                    SET account_manager_id=%s,
                        industry=%s,
                        tier=%s,
                        location=%s,
                        company_size=%s,
                        annual_spend=%s,
                        mode=%s,
                        suspect_q1=%s,
                        suspect_q2=%s,
                        suspect_q3=%s,
                        suspect_q4=%s,
                        suspect_q5=%s,
                        suspect_q6=%s,
                        suspect_q7=%s,
                        suspect_q8=%s,
                        suspect_q9=%s,
                        suspect_q10=%s,
                        suspect_score=%s,
                        updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        manager_id,
                        industry,
                        tier,
                        location,
                        company_size,
                        annual_spend,
                        mode,
                        suspect_answers["suspect_q1"],
                        suspect_answers["suspect_q2"],
                        suspect_answers["suspect_q3"],
                        suspect_answers["suspect_q4"],
                        suspect_answers["suspect_q5"],
                        suspect_answers["suspect_q6"],
                        suspect_answers["suspect_q7"],
                        suspect_answers["suspect_q8"],
                        suspect_answers["suspect_q9"],
                        suspect_answers["suspect_q10"],
                        suspect_score,
                        int(row["id"]),
                    ),
                )
                conn.commit()
                return "updated"

            cur.execute(
                """
                INSERT INTO accounts (
                    account_name, account_manager_id, industry, tier, location, company_size, annual_spend, mode,
                    suspect_q1, suspect_q2, suspect_q3, suspect_q4, suspect_q5,
                    suspect_q6, suspect_q7, suspect_q8, suspect_q9, suspect_q10, suspect_score,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    now(), now()
                )
                """,
                (
                    name,
                    manager_id,
                    industry,
                    tier,
                    location,
                    company_size,
                    annual_spend,
                    mode,
                    suspect_answers["suspect_q1"],
                    suspect_answers["suspect_q2"],
                    suspect_answers["suspect_q3"],
                    suspect_answers["suspect_q4"],
                    suspect_answers["suspect_q5"],
                    suspect_answers["suspect_q6"],
                    suspect_answers["suspect_q7"],
                    suspect_answers["suspect_q8"],
                    suspect_answers["suspect_q9"],
                    suspect_answers["suspect_q10"],
                    suspect_score,
                ),
            )
            conn.commit()
            return "created"
    finally:
        conn.close()


@app.route("/api/health", methods=["GET"])
def health():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM users")
            users = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM accounts")
            accounts = cur.fetchone()["c"]
        return jsonify(
            {
                "status": "ok",
                "db_host": urlparse(DATABASE_URL).hostname,
                "users": users,
                "accounts": accounts,
            }
        )
    finally:
        conn.close()


@app.route("/api/users", methods=["GET"])
def list_users():
    conn = get_conn()
    try:
        role = (request.args.get("role") or "").strip().lower()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if role:
                cur.execute(
                    "SELECT id, email, name, role FROM users WHERE lower(role)=lower(%s) ORDER BY name",
                    (role,),
                )
            else:
                cur.execute("SELECT id, email, name, role FROM users ORDER BY name")
            rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/api/accounts", methods=["GET"])
def list_accounts():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT a.id, a.account_name, a.created_at, a.updated_at,
                           a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                           a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                           a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10, a.suspect_score,
                           u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                    FROM accounts a
                    LEFT JOIN users u ON u.id = a.account_manager_id
                    ORDER BY a.account_name
                    """
                )
                return jsonify(cur.fetchall())

            if not viewer_email:
                return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

            cur.execute(
                "SELECT id FROM users WHERE lower(email)=lower(%s)",
                (viewer_email,),
            )
            manager = cur.fetchone()
            if not manager:
                return jsonify([])

            cur.execute(
                """
                SELECT a.id, a.account_name, a.created_at, a.updated_at,
                       a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                       a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                       a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10, a.suspect_score,
                       u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                FROM accounts a
                LEFT JOIN users u ON u.id = a.account_manager_id
                WHERE a.account_manager_id = %s
                ORDER BY a.account_name
                """,
                (int(manager["id"]),),
            )
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/bootstrap", methods=["GET"])
def bootstrap_data():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    payload = {
        "accounts": [],
        "activities": [],
        "leads": [],
        "contacts": [],
        "opportunities": [],
    }
    _cache_key = f"bootstrap:{viewer_email}:{viewer_role}"
    _now = time.time()
    with _write_limits_lock:
        _cached = _write_limits.get(_cache_key)
        if _cached and _now - _cached[0] < 10:
            return jsonify(_cached[1])
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT a.id, a.account_name, a.created_at, a.updated_at,
                           a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                           a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                           a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10, a.suspect_score,
                           u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                    FROM accounts a
                    LEFT JOIN users u ON u.id = a.account_manager_id
                    ORDER BY a.account_name
                    """
                )
                payload["accounts"] = cur.fetchall()
            elif viewer_email:
                cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (viewer_email,))
                manager = cur.fetchone()
                if manager:
                    cur.execute(
                        """
                        SELECT a.id, a.account_name, a.created_at, a.updated_at,
                               a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                               a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                               a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10, a.suspect_score,
                               u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                        FROM accounts a
                        LEFT JOIN users u ON u.id = a.account_manager_id
                        WHERE a.account_manager_id = %s
                        ORDER BY a.account_name
                        """,
                        (int(manager["id"]),),
                    )
                    payload["accounts"] = cur.fetchall()

            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, type, subject, notes, date, owner, account_id, account_name, created_at, updated_at
                    FROM activities
                    ORDER BY date DESC, updated_at DESC
                    """
                )
                payload["activities"] = cur.fetchall()
            elif viewer_email:
                cur.execute(
                    """
                    SELECT id, type, subject, notes, date, owner, account_id, account_name, created_at, updated_at
                    FROM activities
                    WHERE lower(owner)=lower(%s)
                    ORDER BY date DESC, updated_at DESC
                    """,
                    (viewer_email,),
                )
                payload["activities"] = cur.fetchall()

            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, name, company, email, phone, source, status, notes, owner, created_at, updated_at
                    FROM leads
                    ORDER BY updated_at DESC
                    """
                )
                payload["leads"] = cur.fetchall()
            elif viewer_email:
                cur.execute(
                    """
                    SELECT id, name, company, email, phone, source, status, notes, owner, created_at, updated_at
                    FROM leads
                    WHERE lower(owner)=lower(%s)
                    ORDER BY updated_at DESC
                    """,
                    (viewer_email,),
                )
                payload["leads"] = cur.fetchall()

            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, name, title, email, phone, role_type, influence_level, emotion, account_id, owner, created_at, updated_at
                    FROM contacts
                    ORDER BY updated_at DESC
                    """
                )
                payload["contacts"] = cur.fetchall()
            elif viewer_email:
                cur.execute(
                    """
                    SELECT id, name, title, email, phone, role_type, influence_level, emotion, account_id, owner, created_at, updated_at
                    FROM contacts
                    WHERE lower(owner)=lower(%s)
                    ORDER BY updated_at DESC
                    """,
                    (viewer_email,),
                )
                payload["contacts"] = cur.fetchall()

            opp_all = """
                SELECT id, name, account_id, value, stage, deal_type, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_salesops, assigned_purchase, sales_comments, sales_ops_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at, salesops_assigned_at, salesops_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at, oem_pricing_required,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                ORDER BY updated_at DESC
            """
            opp_scoped = """
                SELECT id, name, account_id, value, stage, deal_type, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_salesops, assigned_purchase, sales_comments, sales_ops_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at, salesops_assigned_at, salesops_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at, oem_pricing_required,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(owner)=lower(%s)
                   OR lower(sales_owner)=lower(%s)
                   OR lower(assigned_presales)=lower(%s)
                   OR lower(assigned_salesops)=lower(%s)
                   OR lower(assigned_purchase)=lower(%s)
                ORDER BY updated_at DESC
            """

            if _is_supervisor(viewer_role):
                cur.execute(opp_all)
                rows = cur.fetchall()
                if enforce_opportunity_sla(conn, rows):
                    cur.execute(opp_all)
                    rows = cur.fetchall()
                payload["opportunities"] = rows
            elif viewer_email:
                cur.execute(opp_scoped, (viewer_email, viewer_email, viewer_email, viewer_email, viewer_email))
                rows = cur.fetchall()
                if enforce_opportunity_sla(conn, rows):
                    cur.execute(opp_scoped, (viewer_email, viewer_email, viewer_email, viewer_email, viewer_email))
                    rows = cur.fetchall()
                payload["opportunities"] = rows
        with _write_limits_lock:
            _write_limits[f"bootstrap:{viewer_email}:{viewer_role}"] = (time.time(), payload)        
        return jsonify(payload)
        
    except Exception as exc:
        return jsonify({"error": f"bootstrap failed: {exc}", **payload}), 200
    finally:
        conn.close()


@app.route("/api/accounts", methods=["POST"])
def create_or_update_account():
    allowed, ip = _rate_limit_write("accounts_post", per_60s=35)
    if not allowed:
        print(f"[RATE_LIMIT] route=/api/accounts ip={ip}")
        return jsonify({"error": "too many requests"}), 429

    data = request.get_json(silent=True) or {}
    account_name = (data.get("account_name") or "").strip()
    account_manager = (data.get("account_manager") or "").strip()
    if not account_name or not account_manager:
        return jsonify({"error": "account_name and account_manager are required"}), 400

    try:
        manager_id = ensure_user(account_manager)
        result = upsert_account(data, manager_id)
        return jsonify({"status": result, "account_name": account_name}), 200
    except ValueError as exc:
        print(f"[ACCOUNTS_POST_BAD_REQUEST] ip={ip} err={exc}")
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        print(f"[ACCOUNTS_POST_ERROR] ip={ip} err={exc}")
        return jsonify({"error": f"account save failed: {exc}"}), 500


@app.route("/api/accounts/<account_id>", methods=["DELETE"])
def delete_account(account_id: str):
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    if not str(account_id).isdigit():
        return jsonify({"error": "invalid account id"}), 400

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, account_manager_id FROM accounts WHERE id=%s", (int(account_id),))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "account not found"}), 404

            if not _is_supervisor(viewer_role):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (viewer_email,))
                manager = cur.fetchone()
                if not manager or int(manager["id"]) != int(row.get("account_manager_id") or -1):
                    return jsonify({"error": "not allowed"}), 403

            cur.execute("DELETE FROM accounts WHERE id=%s", (int(account_id),))
        conn.commit()
        return jsonify({"status": "deleted", "id": int(account_id)})
    finally:
        conn.close()


@app.route("/api/accounts/import", methods=["POST"])
def import_accounts():
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
            result = upsert_account(
                {
                    "account_name": name,
                    "account_manager": manager,
                },
                manager_id,
            )
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
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, type, subject, notes, date, owner, account_id, account_name, created_at, updated_at
                    FROM activities
                    ORDER BY date DESC, updated_at DESC
                    """
                )
                return jsonify(cur.fetchall())

            if not viewer_email:
                return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

            cur.execute(
                """
                SELECT id, type, subject, notes, date, owner, account_id, account_name, created_at, updated_at
                FROM activities
                WHERE lower(owner)=lower(%s)
                ORDER BY date DESC, updated_at DESC
                """,
                (viewer_email,),
            )
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/leads", methods=["GET"])
def list_leads():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, name, company, email, phone, source, status, notes, owner, created_at, updated_at
                    FROM leads
                    ORDER BY updated_at DESC
                    """
                )
                return jsonify(cur.fetchall())

            if not viewer_email:
                return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

            cur.execute(
                """
                SELECT id, name, company, email, phone, source, status, notes, owner, created_at, updated_at
                FROM leads
                WHERE lower(owner)=lower(%s)
                ORDER BY updated_at DESC
                """,
                (viewer_email,),
            )
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/leads", methods=["POST"])
def upsert_lead():
    data = request.get_json(silent=True) or {}
    lead_id = (data.get("id") or "").strip() or f"lead_{int(datetime.utcnow().timestamp() * 1000)}"
    owner = (data.get("owner") or "").strip()
    if not owner:
        return jsonify({"error": "owner is required"}), 400

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM leads WHERE id=%s", (lead_id,))
            exists = cur.fetchone()
            if exists:
                cur.execute(
                    """
                    UPDATE leads
                    SET name=%s, company=%s, email=%s, phone=%s, source=%s, status=%s, notes=%s, owner=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        (data.get("name") or "").strip(),
                        (data.get("company") or "").strip(),
                        (data.get("email") or "").strip(),
                        (data.get("phone") or "").strip(),
                        (data.get("source") or "").strip(),
                        (data.get("status") or "").strip(),
                        (data.get("notes") or "").strip(),
                        owner,
                        lead_id,
                    ),
                )
                status = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO leads (id, name, company, email, phone, source, status, notes, owner, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    """,
                    (
                        lead_id,
                        (data.get("name") or "").strip(),
                        (data.get("company") or "").strip(),
                        (data.get("email") or "").strip(),
                        (data.get("phone") or "").strip(),
                        (data.get("source") or "").strip(),
                        (data.get("status") or "").strip(),
                        (data.get("notes") or "").strip(),
                        owner,
                    ),
                )
                status = "created"
        conn.commit()
        return jsonify({"status": status, "id": lead_id})
    finally:
        conn.close()


@app.route("/api/leads/<lead_id>", methods=["DELETE"])
def delete_lead(lead_id: str):
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, owner FROM leads WHERE id=%s", (lead_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "lead not found"}), 404
            if not _is_supervisor(viewer_role):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                if (row["owner"] or "").lower() != viewer_email.lower():
                    return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM leads WHERE id=%s", (lead_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": lead_id})
    finally:
        conn.close()


@app.route("/api/contacts", methods=["GET"])
def list_contacts():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT id, name, title, email, phone, role_type, influence_level, emotion, account_id, owner, created_at, updated_at
                    FROM contacts
                    ORDER BY updated_at DESC
                    """
                )
                return jsonify(cur.fetchall())

            if not viewer_email:
                return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

            cur.execute(
                """
                SELECT id, name, title, email, phone, role_type, influence_level, emotion, account_id, owner, created_at, updated_at
                FROM contacts
                WHERE lower(owner)=lower(%s)
                ORDER BY updated_at DESC
                """,
                (viewer_email,),
            )
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/contacts", methods=["POST"])
def upsert_contact():
    data = request.get_json(silent=True) or {}
    contact_id = (data.get("id") or "").strip() or f"con_{int(datetime.utcnow().timestamp() * 1000)}"
    owner = (data.get("owner") or "").strip()
    if not owner:
        return jsonify({"error": "owner is required"}), 400

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM contacts WHERE id=%s", (contact_id,))
            exists = cur.fetchone()
            if exists:
                cur.execute(
                    """
                    UPDATE contacts
                    SET name=%s, title=%s, email=%s, phone=%s, role_type=%s, influence_level=%s, emotion=%s,
                        account_id=%s, owner=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        (data.get("name") or "").strip(),
                        (data.get("title") or "").strip(),
                        (data.get("email") or "").strip(),
                        (data.get("phone") or "").strip(),
                        (data.get("role_type") or data.get("roleType") or "").strip(),
                        (data.get("influence_level") or data.get("influenceLevel") or "").strip(),
                        (data.get("emotion") or "").strip(),
                        (data.get("account_id") or data.get("accountId") or "").strip(),
                        owner,
                        contact_id,
                    ),
                )
                status = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO contacts (id, name, title, email, phone, role_type, influence_level, emotion, account_id, owner, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    """,
                    (
                        contact_id,
                        (data.get("name") or "").strip(),
                        (data.get("title") or "").strip(),
                        (data.get("email") or "").strip(),
                        (data.get("phone") or "").strip(),
                        (data.get("role_type") or data.get("roleType") or "").strip(),
                        (data.get("influence_level") or data.get("influenceLevel") or "").strip(),
                        (data.get("emotion") or "").strip(),
                        (data.get("account_id") or data.get("accountId") or "").strip(),
                        owner,
                    ),
                )
                status = "created"
        conn.commit()
        return jsonify({"status": status, "id": contact_id})
    finally:
        conn.close()


@app.route("/api/contacts/<contact_id>", methods=["DELETE"])
def delete_contact(contact_id: str):
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, owner FROM contacts WHERE id=%s", (contact_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "contact not found"}), 404
            if not _is_supervisor(viewer_role):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                if (row["owner"] or "").lower() != viewer_email.lower():
                    return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM contacts WHERE id=%s", (contact_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": contact_id})
    finally:
        conn.close()


@app.route("/api/opportunities", methods=["GET"])
def list_opportunities():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query_all = """
                    SELECT id, name, account_id, value, stage, deal_type, owner, sales_owner, workflow_stage,
                           assigned_presales, assigned_salesops, assigned_purchase, sales_comments, sales_ops_comments, requirements,
                           presales_architecture, presales_questions, boq, purchase_costing,
                           costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at, salesops_assigned_at, salesops_due_at,
                           purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                           assignment_due_at, sales_submitted_at, presales_escalated_at, oem_pricing_required,
                           intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                           intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                           intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                           intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                           intake_competitors, intake_win_strategy, created_at, updated_at
                    FROM opportunities
                    ORDER BY updated_at DESC
                    """
            query_scoped = """
                SELECT id, name, account_id, value, stage, deal_type, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_salesops, assigned_purchase, sales_comments, sales_ops_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at, salesops_assigned_at, salesops_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at, oem_pricing_required,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(owner)=lower(%s)
                   OR lower(sales_owner)=lower(%s)
                   OR lower(assigned_presales)=lower(%s)
                   OR lower(assigned_salesops)=lower(%s)
                   OR lower(assigned_purchase)=lower(%s)
                ORDER BY updated_at DESC
            """
            if _is_supervisor(viewer_role):
                cur.execute(query_all)
                rows = cur.fetchall()
                if enforce_opportunity_sla(conn, rows):
                    cur.execute(query_all)
                    rows = cur.fetchall()
                return jsonify(rows)

            if not viewer_email:
                return jsonify({"error": "viewer_email is required for non-supervisor access"}), 400

            cur.execute(query_scoped, (viewer_email, viewer_email, viewer_email, viewer_email, viewer_email))
            rows = cur.fetchall()
            if enforce_opportunity_sla(conn, rows):
                cur.execute(query_scoped, (viewer_email, viewer_email, viewer_email, viewer_email, viewer_email))
                rows = cur.fetchall()
            return jsonify(rows)
    finally:
        conn.close()

def clean_ts(value):
    v = (value or "").strip()
    return v or None
    
@app.route("/api/opportunities", methods=["POST"])
def upsert_opportunity():
    data = request.get_json(silent=True) or {}
    opp_id = (data.get("id") or "").strip() or f"opp_{int(datetime.utcnow().timestamp() * 1000)}"
    owner = (data.get("owner") or "").strip()
    if not owner:
        return jsonify({"error": "owner is required"}), 400

    payload = {
        "name": (data.get("name") or "").strip(),
        "account_id": (data.get("account_id") or data.get("accountId") or "").strip(),
        "value": float(data.get("value") or 0),
        "stage": (data.get("stage") or "").strip(),
        "deal_type": (data.get("deal_type") or data.get("dealType") or "").strip(),
        "owner": owner,
        "sales_owner": (data.get("sales_owner") or data.get("salesOwner") or owner).strip(),
        "workflow_stage": (data.get("workflow_stage") or data.get("workflowStage") or "").strip(),
        "assigned_presales": (data.get("assigned_presales") or data.get("assignedPresales") or "").strip(),
        "assigned_salesops": (data.get("assigned_salesops") or data.get("assignedSalesOps") or "").strip(),
        "assigned_purchase": (data.get("assigned_purchase") or data.get("assignedPurchase") or "").strip(),
        "sales_comments": (data.get("sales_comments") or data.get("salesComments") or "").strip(),
        "sales_ops_comments": (data.get("sales_ops_comments") or data.get("salesOpsComments") or "").strip(),
        "requirements": (data.get("requirements") or "").strip(),
        "presales_architecture": (data.get("presales_architecture") or data.get("presalesArchitecture") or "").strip(),
        "presales_questions": (data.get("presales_questions") or data.get("presalesQuestions") or "").strip(),
        "boq": (data.get("boq") or "").strip(),
        "purchase_costing": (data.get("purchase_costing") or data.get("purchaseCosting") or "").strip(),
        "costing_tat": (data.get("costing_tat") or data.get("costingTat") or "").strip(),
        "final_pricing_proposal": (data.get("final_pricing_proposal") or data.get("finalPricingProposal") or "").strip(),
        "presales_assigned_at": clean_ts(data.get("presales_assigned_at") or data.get("presalesAssignedAt")),
        "presales_due_at": clean_ts(data.get("presales_due_at") or data.get("presalesDueAt")),
        "purchase_assigned_at": clean_ts(data.get("purchase_assigned_at") or data.get("purchaseAssignedAt")),
        "purchase_due_at": clean_ts(data.get("purchase_due_at") or data.get("purchaseDueAt")),
        "costing_returned_at": clean_ts(data.get("costing_returned_at") or data.get("costingReturnedAt")),
        "final_proposal_at": clean_ts(data.get("final_proposal_at") or data.get("finalProposalAt")),
        "assignment_due_at": clean_ts(data.get("assignment_due_at") or data.get("assignmentDueAt")),
        "sales_submitted_at": clean_ts(data.get("sales_submitted_at") or data.get("salesSubmittedAt")),
        "presales_escalated_at": clean_ts(data.get("presales_escalated_at") or data.get("presalesEscalatedAt")),
        "oem_pricing_required": str(data.get("oem_pricing_required") or data.get("oemPricingRequired") or "").strip().lower() in ("1", "true", "yes", "y"),
        "intake_problem_statement": (data.get("intake_problem_statement") or data.get("intakeProblemStatement") or "").strip(),
        "intake_why_now": (data.get("intake_why_now") or data.get("intakeWhyNow") or "").strip(),
        "intake_business_impact": (data.get("intake_business_impact") or data.get("intakeBusinessImpact") or "").strip(),
        "intake_current_state": (data.get("intake_current_state") or data.get("intakeCurrentState") or "").strip(),
        "intake_budget_range": (data.get("intake_budget_range") or data.get("intakeBudgetRange") or "").strip(),
        "intake_decision_timeline": (data.get("intake_decision_timeline") or data.get("intakeDecisionTimeline") or "").strip(),
        "intake_risk_if_not_solved": (data.get("intake_risk_if_not_solved") or data.get("intakeRiskIfNotSolved") or "").strip(),
        "intake_key_stakeholders": (data.get("intake_key_stakeholders") or data.get("intakeKeyStakeholders") or "").strip(),
        "intake_in_scope": (data.get("intake_in_scope") or data.get("intakeInScope") or "").strip(),
        "intake_out_of_scope": (data.get("intake_out_of_scope") or data.get("intakeOutOfScope") or "").strip(),
        "intake_current_environment": (data.get("intake_current_environment") or data.get("intakeCurrentEnvironment") or "").strip(),
        "intake_pain_points": (data.get("intake_pain_points") or data.get("intakePainPoints") or "").strip(),
        "intake_compliance_requirements": (data.get("intake_compliance_requirements") or data.get("intakeComplianceRequirements") or "").strip(),
        "intake_integration_requirements": (data.get("intake_integration_requirements") or data.get("intakeIntegrationRequirements") or "").strip(),
        "intake_competitors": (data.get("intake_competitors") or data.get("intakeCompetitors") or "").strip(),
        "intake_win_strategy": (data.get("intake_win_strategy") or data.get("intakeWinStrategy") or "").strip(),
    }

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, assigned_presales, assigned_salesops, workflow_stage, sales_owner, owner FROM opportunities WHERE id=%s", (opp_id,))
            exists = cur.fetchone()
            prev_assigned_presales = ((exists or {}).get("assigned_presales") or "").strip().lower()
            prev_assigned_salesops = ((exists or {}).get("assigned_salesops") or "").strip().lower()
            prev_workflow_stage = ((exists or {}).get("workflow_stage") or "").strip()
            prev_sales_owner = ((exists or {}).get("sales_owner") or (exists or {}).get("owner") or "").strip().lower()
            deal_type_now = (payload.get("deal_type") or "").strip().lower()
            if deal_type_now in ("hardware", "mixed") or (payload.get("assigned_presales") or "").strip():
                payload["assigned_salesops"] = SALES_OPS_OWNER
                payload["oem_pricing_required"] = True
                payload["salesops_assigned_at"] = payload.get("salesops_assigned_at") or utc_now()
                payload["salesops_due_at"] = payload.get("salesops_due_at") or (datetime.utcnow() + timedelta(hours=24)).isoformat(timespec="seconds") + "Z"

            required_intake_fields = [
                ("intake_problem_statement", "Problem Statement"),
                ("intake_why_now", "Why Now (Trigger Event)"),
                ("intake_business_impact", "Business Impact"),
                ("intake_current_state", "Current State Summary"),
                ("intake_budget_range", "Budget Range"),
                ("intake_decision_timeline", "Decision Timeline"),
            ]
            workflow_now = (payload.get("workflow_stage") or "").strip()
            # Only enforce intake when Sales is creating/editing — not for workflow stage transitions
            # by presales or salesops (they move stages without filling intake forms)
            viewer_role_now = (payload.get("viewer_role") or "").strip().lower()
            is_workflow_transition = viewer_role_now in ("presales", "salesops", "purchase")
            requires_intake = False
            missing_intake = [label for key, label in required_intake_fields if not payload.get(key)]
            if requires_intake and missing_intake:
                return jsonify({"error": "Mandatory presales intake fields missing", "missing_fields": missing_intake}), 400

            if exists:
                cur.execute(
                    """
                    UPDATE opportunities
                    SET name=%s, account_id=%s, value=%s, stage=%s, deal_type=%s, owner=%s, sales_owner=%s, workflow_stage=%s,
                        assigned_presales=%s, assigned_salesops=%s, assigned_purchase=%s, sales_comments=%s, sales_ops_comments=%s, requirements=%s,
                        presales_architecture=%s, presales_questions=%s, boq=%s, purchase_costing=%s,
                        costing_tat=%s, final_pricing_proposal=%s, presales_assigned_at=%s, presales_due_at=%s, salesops_assigned_at=%s, salesops_due_at=%s,
                        purchase_assigned_at=%s, purchase_due_at=%s, costing_returned_at=%s, final_proposal_at=%s,
                        assignment_due_at=%s, sales_submitted_at=%s, presales_escalated_at=%s, oem_pricing_required=%s,
                        intake_problem_statement=%s, intake_why_now=%s, intake_business_impact=%s, intake_current_state=%s,
                        intake_budget_range=%s, intake_decision_timeline=%s, intake_risk_if_not_solved=%s,
                        intake_key_stakeholders=%s, intake_in_scope=%s, intake_out_of_scope=%s, intake_current_environment=%s,
                        intake_pain_points=%s, intake_compliance_requirements=%s, intake_integration_requirements=%s,
                        intake_competitors=%s, intake_win_strategy=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        payload["name"], payload["account_id"], payload["value"], payload["stage"], payload["deal_type"], payload["owner"],
                        payload["sales_owner"], payload["workflow_stage"], payload["assigned_presales"], payload["assigned_salesops"],
                        payload["assigned_purchase"], payload["sales_comments"], payload["sales_ops_comments"], payload["requirements"],
                        payload["presales_architecture"], payload["presales_questions"], payload["boq"],
                        payload["purchase_costing"], payload["costing_tat"], payload["final_pricing_proposal"],
                        payload["presales_assigned_at"], payload["presales_due_at"], payload["salesops_assigned_at"], payload["salesops_due_at"], payload["purchase_assigned_at"],
                        payload["purchase_due_at"], payload["costing_returned_at"], payload["final_proposal_at"],
                        payload["assignment_due_at"], payload["sales_submitted_at"], payload["presales_escalated_at"], payload["oem_pricing_required"],
                        payload["intake_problem_statement"], payload["intake_why_now"], payload["intake_business_impact"], payload["intake_current_state"],
                        payload["intake_budget_range"], payload["intake_decision_timeline"], payload["intake_risk_if_not_solved"],
                        payload["intake_key_stakeholders"], payload["intake_in_scope"], payload["intake_out_of_scope"], payload["intake_current_environment"],
                        payload["intake_pain_points"], payload["intake_compliance_requirements"], payload["intake_integration_requirements"],
                        payload["intake_competitors"], payload["intake_win_strategy"], opp_id,
                    ),
                )
                status = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO opportunities (
                        id, name, account_id, value, stage, deal_type, owner, sales_owner, workflow_stage,
                        assigned_presales, assigned_salesops, assigned_purchase, sales_comments, sales_ops_comments, requirements,
                        presales_architecture, presales_questions, boq, purchase_costing,
                        costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at, salesops_assigned_at, salesops_due_at,
                        purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                        assignment_due_at, sales_submitted_at, presales_escalated_at, oem_pricing_required,
                        intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                        intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                        intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                        intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                        intake_competitors, intake_win_strategy, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, now(), now()
                    )
                    """,
                    (
                        opp_id, payload["name"], payload["account_id"], payload["value"], payload["stage"], payload["deal_type"], payload["owner"],
                        payload["sales_owner"], payload["workflow_stage"], payload["assigned_presales"], payload["assigned_salesops"],
                        payload["assigned_purchase"], payload["sales_comments"], payload["sales_ops_comments"], payload["requirements"],
                        payload["presales_architecture"], payload["presales_questions"], payload["boq"],
                        payload["purchase_costing"], payload["costing_tat"], payload["final_pricing_proposal"],
                        payload["presales_assigned_at"], payload["presales_due_at"], payload["salesops_assigned_at"], payload["salesops_due_at"], payload["purchase_assigned_at"],
                        payload["purchase_due_at"], payload["costing_returned_at"], payload["final_proposal_at"],
                        payload["assignment_due_at"], payload["sales_submitted_at"], payload["presales_escalated_at"], payload["oem_pricing_required"],
                        payload["intake_problem_statement"], payload["intake_why_now"], payload["intake_business_impact"], payload["intake_current_state"],
                        payload["intake_budget_range"], payload["intake_decision_timeline"], payload["intake_risk_if_not_solved"], payload["intake_key_stakeholders"],
                        payload["intake_in_scope"], payload["intake_out_of_scope"], payload["intake_current_environment"], payload["intake_pain_points"],
                        payload["intake_compliance_requirements"], payload["intake_integration_requirements"], payload["intake_competitors"], payload["intake_win_strategy"],
                    ),
                )
                status = "created"
        conn.commit()

        current_assigned = (payload.get("assigned_presales") or "").strip().lower()
        current_salesops = (payload.get("assigned_salesops") or "").strip().lower()
        workflow_now = (payload.get("workflow_stage") or "").strip()
        sales_now = (payload.get("sales_owner") or payload.get("owner") or "").strip().lower()

        # Fire email when:
        # 1. Presales is assigned AND workflow just moved to "Assigned to Presales"
        # 2. OR presales assignee changed while already in that stage
        presales_just_assigned = (
            workflow_now == "Assigned to Presales"
            and bool(current_assigned)
            and (
                prev_workflow_stage != "Assigned to Presales"
                or current_assigned != prev_assigned_presales
                or status == "created"
            )
        )

        if presales_just_assigned:
            due_iso = (payload.get("presales_due_at") or "").strip()
            if not due_iso:
                base_dt = parse_iso_dt((payload.get("sales_submitted_at") or "").strip()) or datetime.now(timezone.utc)
                due_iso = (base_dt + timedelta(hours=72)).isoformat().replace("+00:00", "Z")

            account_manager_email = ""
            acc_id = (payload.get("account_id") or "").strip()
            if acc_id:
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur_am:
                        cur_am.execute(
                            """
                            SELECT u.email FROM accounts a
                            LEFT JOIN users u ON u.id = a.account_manager_id
                            WHERE CAST(a.id AS TEXT) = %s
                            """,
                            (acc_id,),
                        )
                        am_row = cur_am.fetchone()
                        if am_row:
                            account_manager_email = (am_row.get("email") or "").strip().lower()
                except Exception as e:
                    print(f"[CRM] Could not fetch account manager for email CC: {e}")

            send_opportunity_assignment_email(
                payload.get("name") or "",
                opp_id,
                current_assigned,
                sales_now,
                due_iso,
                account_manager_email,
            )
            if current_salesops:
                send_salesops_assignment_email(
                    payload.get("name") or "",
                    opp_id,
                    current_salesops,
                    sales_now,
                    current_assigned,
                    payload.get("deal_type") or "",
                    (payload.get("salesops_due_at") or "").strip(),
                    account_manager_email,
                )
        elif current_salesops and (
            current_salesops != prev_assigned_salesops
            or status == "created"
            or str(payload.get("oem_pricing_required") or "").lower() in ("true", "1")
        ):
            account_manager_email = ""
            acc_id = (payload.get("account_id") or "").strip()
            if acc_id:
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur_am:
                        cur_am.execute(
                            """
                            SELECT u.email FROM accounts a
                            LEFT JOIN users u ON u.id = a.account_manager_id
                            WHERE CAST(a.id AS TEXT) = %s
                            """,
                            (acc_id,),
                        )
                        am_row = cur_am.fetchone()
                        if am_row:
                            account_manager_email = (am_row.get("email") or "").strip().lower()
                except Exception as e:
                    print(f"[CRM] Could not fetch account manager for sales ops CC: {e}")
            send_salesops_assignment_email(
                payload.get("name") or "",
                opp_id,
                current_salesops,
                sales_now,
                current_assigned,
                payload.get("deal_type") or "",
                (payload.get("salesops_due_at") or "").strip(),
                account_manager_email,
            )
        # Clear bootstrap cache for all affected users so supervisor/presales/salesops see fresh data
        affected_emails = set()
        for e in [
            payload.get("owner"), payload.get("sales_owner"),
            payload.get("assigned_presales"), payload.get("assigned_salesops"),
            SUPERVISOR_EMAIL,
        ]:
            if e and "@" in str(e):
                affected_emails.add(str(e).strip().lower())
        with _write_limits_lock:
            keys_to_clear = [k for k in _write_limits if any(e in k for e in affected_emails) or k.startswith("bootstrap:")]
            for k in keys_to_clear:
                _write_limits.pop(k, None)

        return jsonify({"status": status, "id": opp_id})
    finally:
        conn.close()


@app.route("/api/opportunities/<opp_id>", methods=["DELETE"])
def delete_opportunity(opp_id: str):
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, owner, sales_owner, assigned_salesops FROM opportunities WHERE id=%s", (opp_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "opportunity not found"}), 404
            if not _is_supervisor(viewer_role):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                allowed = {
                    (row.get("owner") or "").lower(),
                    (row.get("sales_owner") or "").lower(),
                    (row.get("assigned_salesops") or "").lower(),
                }
                if viewer_email.lower() not in allowed:
                    return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM opportunities WHERE id=%s", (opp_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": opp_id})
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
    account_id = (data.get("account_id") or "").strip()
    account_name = (data.get("account_name") or "").strip()

    if not activity_type or not subject or not date or not owner:
        return jsonify({"error": "type, subject, date, owner are required"}), 400

    if not activity_id:
        activity_id = f"act_{int(datetime.utcnow().timestamp() * 1000)}"

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM activities WHERE id=%s",
                (activity_id,),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    UPDATE activities
                    SET type=%s, subject=%s, notes=%s, date=%s, owner=%s, account_id=%s, account_name=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        activity_type,
                        subject,
                        notes,
                        date,
                        owner,
                        account_id,
                        account_name,
                        activity_id,
                    ),
                )
                status = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO activities (id, type, subject, notes, date, owner, account_id, account_name, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    """,
                    (
                        activity_id,
                        activity_type,
                        subject,
                        notes,
                        date,
                        owner,
                        account_id,
                        account_name,
                    ),
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

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, owner FROM activities WHERE id=%s",
                (activity_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "activity not found"}), 404

            if viewer_role not in ("supervisor", "admin"):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                if (row["owner"] or "").lower() != viewer_email.lower():
                    return jsonify({"error": "not allowed"}), 403

            cur.execute("DELETE FROM activities WHERE id=%s", (activity_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": activity_id})
    except Exception as exc:
        return jsonify({"error": f"activity delete failed: {exc}"}), 500
    finally:
        conn.close()


@app.route("/api/passwords/<email>", methods=["GET"])
def get_password(email: str):
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    email = (email or "").strip().lower()
    if viewer_email != email and not _is_supervisor(viewer_role):
        return jsonify({"error": "not allowed"}), 403
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT email, password, updated_at FROM user_passwords WHERE lower(email)=lower(%s)",
                (email,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"email": email, "password": None})
            return jsonify(row)
    finally:
        conn.close()


@app.route("/api/passwords", methods=["POST"])
def set_password():
    data = request.get_json(silent=True) or {}
    viewer_role = (data.get("viewer_role") or "account_manager").strip().lower()
    viewer_email = (data.get("viewer_email") or "").strip().lower()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"error": "email and password required"}), 400
    if viewer_role not in ("supervisor", "admin"):
        if not viewer_email or viewer_email != email:
            return jsonify({"error": "not allowed"}), 403

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_passwords (email, password, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT(email)
                DO UPDATE SET password=EXCLUDED.password, updated_at=EXCLUDED.updated_at
                """,
                (email, password),
            )
        conn.commit()
        return jsonify({"status": "ok", "email": email})
    finally:
        conn.close()


@app.route("/api/aop", methods=["GET"])
def list_aop_plans():
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    fy_year = (request.args.get("fy_year") or "2025-26").strip()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base_cols = """p.account_id, p.account_name, p.account_manager, p.fy_year,
                p.current_revenue, p.target_growth, p.updated_by, p.updated_at,
                p.apr_hardware, p.apr_software, p.apr_managed_services,
                p.may_hardware, p.may_software, p.may_managed_services,
                p.jun_hardware, p.jun_software, p.jun_managed_services,
                p.jul_hardware, p.jul_software, p.jul_managed_services,
                p.aug_hardware, p.aug_software, p.aug_managed_services,
                p.sep_hardware, p.sep_software, p.sep_managed_services,
                p.oct_hardware, p.oct_software, p.oct_managed_services,
                p.nov_hardware, p.nov_software, p.nov_managed_services,
                p.dec_hardware, p.dec_software, p.dec_managed_services,
                p.jan_hardware, p.jan_software, p.jan_managed_services,
                p.feb_hardware, p.feb_software, p.feb_managed_services,
                p.mar_hardware, p.mar_software, p.mar_managed_services"""
            if _is_supervisor(viewer_role):
                cur.execute(f"SELECT {base_cols} FROM aop_plans p WHERE p.fy_year=%s", (fy_year,))
            else:
                if not viewer_email:
                    return jsonify([])
                cur.execute(f"SELECT {base_cols} FROM aop_plans p WHERE p.fy_year=%s AND lower(p.account_manager)=lower(%s)", (fy_year, viewer_email))
            rows = cur.fetchall()
            month_map = {"apr":"april","may":"may","jun":"june","jul":"july","aug":"august","sep":"september","oct":"october","nov":"november","dec":"december","jan":"january","feb":"february","mar":"march"}
            out = []
            for r in rows:
                win = float(r.get("target_growth") or 0)
                months = {}
                for px, mname in month_map.items():
                    raw = float(r.get(f"{px}_hardware") or 0) + float(r.get(f"{px}_software") or 0) + float(r.get(f"{px}_managed_services") or 0)
                    months[mname] = round(raw * win * 100, 4)  # convert Cr to Lakhs
                row_out = {
                    "account_id": r.get("account_id"),
                    "account_name": r.get("account_name"),
                    "owner": r.get("account_manager"),
                    "fy_year": r.get("fy_year"),
                    "win_pct": win,
                    "aop_cr": float(r.get("current_revenue") or 0),
                    "updated_at": str(r.get("updated_at") or ""),
                    "months": months,
                }
                # Add flat fields in format the frontend expects: april_hardware, april_software etc.
                for px, mname in month_map.items():
                    raw = float(r.get(f"{px}_hardware") or 0) * win
                    row_out[f"{mname}_hardware"] = round(float(r.get(f"{px}_hardware") or 0) * win * 100, 4)
                    row_out[f"{mname}_software"] = round(float(r.get(f"{px}_software") or 0) * win * 100, 4)
                    row_out[f"{mname}_managed_services"] = round(float(r.get(f"{px}_managed_services") or 0) * win * 100, 4)
                out.append(row_out)
            return jsonify(out)
    except Exception as exc:
        return jsonify([])
    finally:
        conn.close()


@app.route("/api/aop", methods=["POST"])
def upsert_aop_plan():
    data = request.get_json(silent=True) or {}
    account_id = str(data.get("account_id") or "").strip()
    fy_year = str(data.get("fy_year") or "2025-26").strip()
    owner = str(data.get("owner") or data.get("viewer_email") or "").strip().lower()
    if not account_id:
        return jsonify({"error": "account_id required"}), 400

    plan_data = {k: v for k, v in data.items() if k not in {"account_id", "fy_year", "owner", "viewer_email", "viewer_role"}}

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aop_plans (account_id, fy_year, plan_data, owner, created_at, updated_at)
                VALUES (%s, %s, %s::jsonb, %s, now(), now())
                ON CONFLICT (account_id, fy_year)
                DO UPDATE SET plan_data=EXCLUDED.plan_data, owner=EXCLUDED.owner, updated_at=now()
                """,
                (account_id, fy_year, json.dumps(plan_data), owner),
            )
        conn.commit()
        return jsonify({"status": "ok", "account_id": account_id, "fy_year": fy_year})
    finally:
        conn.close()


@app.route("/api/aop/actuals", methods=["GET"])
def list_aop_actuals():
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    fy_year = (request.args.get("fy_year") or "2025-26").strip()

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute(
                    """
                    SELECT account_id, fy_year, month,
                           COALESCE(hardware,0) AS hardware,
                           COALESCE(software,0) AS software,
                           COALESCE(managed_services,0) AS managed_services,
                           owner, updated_at
                    FROM aop_actuals
                    WHERE fy_year=%s
                    """,
                    (fy_year,),
                )
            else:
                if not viewer_email:
                    return jsonify([])
                cur.execute(
                    """
                    SELECT x.account_id, x.fy_year, x.month,
                           COALESCE(x.hardware,0) AS hardware,
                           COALESCE(x.software,0) AS software,
                           COALESCE(x.managed_services,0) AS managed_services,
                           x.owner, x.updated_at
                    FROM aop_actuals x
                    JOIN accounts a ON CAST(a.id AS TEXT) = x.account_id
                    JOIN users u ON u.id = a.account_manager_id
                    WHERE x.fy_year=%s AND lower(u.email)=lower(%s)
                    """,
                    (fy_year, viewer_email),
                )
            return jsonify(cur.fetchall())
    except Exception as exc:
        return jsonify([])
    finally:
        conn.close()


@app.route("/api/aop/actuals", methods=["POST"])
def upsert_aop_actual():
    data = request.get_json(silent=True) or {}
    account_id = str(data.get("account_id") or "").strip()
    fy_year = str(data.get("fy_year") or "2025-26").strip()
    month = str(data.get("month") or "").strip()
    owner = str(data.get("owner") or data.get("viewer_email") or "").strip().lower()
    if not account_id or not month:
        return jsonify({"error": "account_id and month required"}), 400

    hardware = float(data.get("hardware") or 0)
    software = float(data.get("software") or 0)
    managed_services = float(data.get("managed_services") or 0)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aop_actuals (account_id, fy_year, month, hardware, software, managed_services, owner, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (account_id, fy_year, month)
                DO UPDATE SET hardware=EXCLUDED.hardware,
                              software=EXCLUDED.software,
                              managed_services=EXCLUDED.managed_services,
                              owner=EXCLUDED.owner,
                              updated_at=now()
                """,
                (account_id, fy_year, month, hardware, software, managed_services, owner),
            )
        conn.commit()
        return jsonify({"status": "ok", "account_id": account_id, "fy_year": fy_year, "month": month})
    finally:
        conn.close()


@app.route("/api/oauth/microsoft/start", methods=["GET"])
def microsoft_oauth_start():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    if not viewer_email:
        return jsonify({"error": "viewer_email required"}), 400
    if not (MS_CLIENT_ID and MS_CLIENT_SECRET and MS_REDIRECT_URI):
        return jsonify({"error": "Microsoft OAuth env vars missing"}), 500

    state = _build_oauth_state(viewer_email)
    params = {
        "client_id": MS_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": MS_REDIRECT_URI,
        "response_mode": "query",
        "scope": MS_OAUTH_SCOPES,
        "state": state,
    }
    url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/authorize?" + urllib.parse.urlencode(params)
    return jsonify({"url": url})


@app.route("/api/oauth/microsoft/callback", methods=["GET"])
def microsoft_oauth_callback():
    code = (request.args.get("code") or "").strip()
    state = (request.args.get("state") or "").strip()
    if not code:
        return "Missing code", 400

    email = _verify_oauth_state(state)
    if not email:
        return "Invalid or expired OAuth state", 400

    token_url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    try:
        token_data = _http_form_post(
            token_url,
            {
                "client_id": MS_CLIENT_ID,
                "client_secret": MS_CLIENT_SECRET,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": MS_REDIRECT_URI,
                "scope": MS_OAUTH_SCOPES,
            },
        )

        user = _get_user_row_by_email(email)
        _upsert_o365_tokens(int(user["id"]), email, token_data)

        return """
        <html><body style='font-family:Arial;padding:20px'>
        <h3>Microsoft 365 connected successfully.</h3>
        <p>You can close this window and return to CRM.</p>
        <script>
          if (window.opener) {
            window.opener.postMessage({ type: 'ms_o365_connected' }, '*');
            window.close();
          }
        </script>
        </body></html>
        """
    except Exception as exc:
        return f"OAuth failed: {exc}", 500


@app.route("/api/oauth/microsoft/status", methods=["GET"])
def microsoft_oauth_status():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    if not viewer_email:
        return jsonify({"connected": False, "error": "viewer_email required"}), 400
    row = _get_o365_token_row(viewer_email)
    return jsonify({"connected": bool(row and (row.get("status") or "") == "active")})


@app.route("/api/mom/send", methods=["POST"])
def send_mom_mail_endpoint():
    data = request.get_json(silent=True) or {}
    viewer_email = _normalize_email(data.get("viewer_email") or "")
    viewer_role = (data.get("viewer_role") or "account_manager").strip().lower()

    account_id = str(data.get("account_id") or "").strip()
    to_emails = _split_emails(data.get("to_emails") or "")
    cc_emails = _split_emails(data.get("cc_emails") or "")

    if not to_emails:
        return jsonify({"error": "to_emails required"}), 400

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            account_manager_email = viewer_email
            account_manager_name = (viewer_email.split("@")[0] if viewer_email else "Account Manager")
            account_name = data.get("account_name") or ""

            if account_id:
                cur.execute(
                    """
                    SELECT a.account_name, u.email AS manager_email, u.name AS manager_name
                    FROM accounts a
                    LEFT JOIN users u ON u.id = a.account_manager_id
                    WHERE CAST(a.id AS TEXT) = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
                if row:
                    account_name = row.get("account_name") or account_name
                    if row.get("manager_email"):
                        account_manager_email = _normalize_email(row.get("manager_email"))
                    if row.get("manager_name"):
                        account_manager_name = row.get("manager_name")

            if not _is_supervisor(viewer_role) and viewer_email and account_manager_email and viewer_email != account_manager_email:
                return jsonify({"error": "not allowed to send MoM for this account"}), 403

            if not account_manager_email:
                return jsonify({"error": "account manager email not found"}), 400

            meeting_date = (data.get("meeting_date") or "").strip() or datetime.now().strftime("%d-%b-%Y")
            subject = (data.get("subject") or "").strip() or f"Minutes of Meeting | {account_name or 'Account'} | {meeting_date}"

            payload = {
                "account_name": account_name,
                "meeting_date": meeting_date,
                "client_name": data.get("client_name") or "Team",
                "mom_intro": data.get("mom_intro") or "",
                "mom_discussion": data.get("mom_discussion") or "",
                "mom_actions": data.get("mom_actions") or "",
                "mom_next_steps": data.get("mom_next_steps") or "",
                "account_manager_name": account_manager_name,
                "account_manager_email": account_manager_email,
            }
            html = _build_mom_html(payload)

            _send_graph_mail(account_manager_email, to_emails, cc_emails, subject, html)

            activity_id = str(data.get("activity_id") or "").strip()
            if activity_id:
                cur.execute(
                    """
                    UPDATE activities
                    SET mom_sent_at=now(),
                        mom_sent_to=%s,
                        mom_send_status='sent',
                        mom_send_error=NULL,
                        mom_payload=%s,
                        updated_at=now()
                    WHERE id=%s
                    """,
                    (", ".join(to_emails), json.dumps({**payload, "to_emails": to_emails, "cc_emails": cc_emails, "subject": subject}), activity_id),
                )
                conn.commit()

            return jsonify({"status": "sent", "from": account_manager_email, "to": to_emails, "cc": cc_emails})
    except Exception as exc:
        return jsonify({"error": f"MoM send failed: {exc}"}), 500
    finally:
        conn.close()

def _pdf_to_base64_images(pdf_bytes: bytes, max_pages: int = 5, dpi: int = 150):
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        page_count = min(len(doc), max_pages)
        for i in range(page_count):
            page = doc[i]
            pix = page.get_pixmap(dpi=dpi, alpha=False)
            img_bytes = pix.tobytes("jpeg")
            img_b64 = base64.b64encode(img_bytes).decode("utf-8")
            images.append(img_b64)
    finally:
        doc.close()
    return images


def _coerce_ai_json_text(text: str) -> str:
    source = (text or "").strip()
    if not source:
        return source

    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", source, re.IGNORECASE)
    if fenced:
        candidate = fenced.group(1).strip()
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass

    start = source.find("{")
    end = source.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = source[start:end + 1].strip()
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass

    return source


ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()


@app.route("/api/ai-extract", methods=["POST"])
def ai_extract():
    """Proxy AI extraction requests to Anthropic Claude so the API key stays server-side."""
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "AI extraction not configured (ANTHROPIC_API_KEY missing)"}), 503

    data = request.get_json(silent=True) or {}
    prompt = (data.get("prompt") or "").strip()
    image_b64 = (data.get("image_b64") or "").strip()
    media_type = (data.get("media_type") or "image/jpeg").strip()

    if not image_b64 or not prompt:
        return jsonify({"error": "image_b64 and prompt are required"}), 400

    if "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]

    try:
        content_parts = [{"type": "text", "text": prompt}]

        if media_type == "application/pdf":
            pdf_bytes = base64.b64decode(image_b64)
            pdf_images = _pdf_to_base64_images(pdf_bytes, max_pages=5, dpi=150)

            if not pdf_images:
                return jsonify({"error": "Could not read PDF pages"}), 400

            for img_b64 in pdf_images:
                content_parts.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
                })
        else:
            content_parts.append({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": image_b64}
            })

        payload = {
            "model": "claude-sonnet-4-5",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": content_parts}],
        }

        result = _http_json_request(
            "https://api.anthropic.com/v1/messages",
            method="POST",
            data=payload,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
        )

        text = ""
        for block in (result.get("content") or []):
            if block.get("type") == "text":
                text += block.get("text") or ""

        text = _coerce_ai_json_text(text)

        if not text:
            text = "Could not extract details."

        return jsonify({"text": text})

    except Exception as exc:
        return jsonify({"error": f"AI extraction failed: {exc}"}), 500


@app.route("/api/po-notify", methods=["POST"])
def po_notify():
    """Send approval request or approval notification emails for POs via SMTP."""
    data = request.get_json(silent=True) or {}
    event = (data.get("event") or "").strip()   # "submitted" | "approved" | "ceo_approved"
    po_number = (data.get("po_number") or "Draft PO").strip()
    po_id = (data.get("po_id") or "").strip()
    account_name = (data.get("account_name") or "").strip()
    stage = (data.get("stage") or "").strip()
    creator_email = _normalize_email(data.get("creator_email") or "")
    approved_by = _normalize_email(data.get("approved_by") or "")
    value = float(data.get("value") or 0)

    def _val():
        return f"₹{value/100000:.1f}L" if value >= 100000 else f"₹{int(value):,}"

    if event == "submitted":
        # Notify presales + finance + supervisor
        to = [
            "vinod.v@dnispl.com",        # presales approver
            "rakesh.uniyal@dnispl.com",  # finance approver
        ]
        cc = [SUPERVISOR_EMAIL]
        if creator_email and creator_email not in to and creator_email not in cc:
            cc.append(creator_email)
        subject = f"[PO Approval Required] {po_number} | {account_name} | {_val()}"
        body = (
            f"A Purchase Order has been submitted for approval.\n\n"
            f"PO Number  : {po_number}\n"
            f"Account    : {account_name}\n"
            f"Value      : {_val()}\n"
            f"Current Stage: {stage}\n"
            f"Created By : {creator_email}\n\n"
            f"Action Required: Please log in to CRM and approve / reject this PO.\n"
            f"Both Presales and Finance approvals are needed in parallel before it moves forward."
        )
        send_email_smtp(to, subject, body, cc_emails=cc)

    elif event == "approved":
        # Notify creator + supervisor about the approval step
        to = [e for e in [creator_email, SUPERVISOR_EMAIL] if e and "@" in e]
        subject = f"[PO Approved] {po_number} — {stage}"
        body = (
            f"Purchase Order stage update:\n\n"
            f"PO Number  : {po_number}\n"
            f"Account    : {account_name}\n"
            f"Value      : {_val()}\n"
            f"New Stage  : {stage}\n"
            f"Approved By: {approved_by}\n\n"
        )
        # Notify next approver
        if stage == "Both Approved - Pending Implementation":
            to.append("pokhraj.yadav@dnispl.com")
            body += "Action Required: Implementation approval needed from pokhraj.yadav@dnispl.com."
        elif stage == "Pending CEO Approval":
            to.append("ashish.mehra@dnispl.com")
            body += "Action Required: P&L / CEO approval needed from ashish.mehra@dnispl.com."
        send_email_smtp(list(dict.fromkeys(to)), subject, body)

    elif event == "ceo_approved":
        to = [e for e in [creator_email, SUPERVISOR_EMAIL, "vinod.v@dnispl.com", "rakesh.uniyal@dnispl.com"] if e and "@" in e]
        subject = f"[PO Fully Approved] {po_number} — Ready to Issue"
        body = (
            f"Purchase Order has been fully approved and is ready to issue to vendor.\n\n"
            f"PO Number  : {po_number}\n"
            f"Account    : {account_name}\n"
            f"Value      : {_val()}\n"
            f"Approved By (CEO): {approved_by}\n"
        )
        send_email_smtp(list(dict.fromkeys(to)), subject, body)

    else:
        return jsonify({"error": f"unknown event: {event}"}), 400

    return jsonify({"status": "ok", "event": event})



# AOP raw data (win_pct applied at query time)
AOP_RAW_DATA = [
  {"am_email": "om.prakash@dnispl.com", "account_name": "Vishal Pipes Limited", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Uflex", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "TECHBOOKS INTERNATIONAL (Aptara)", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "SIMPA ENERGY INDIA PVT LTD", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "SANSPAREILS GREENLANDS (SG)", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "SANGAM INDIA LTD", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Rose IT Solutions", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "AGNISYS TECHNOLOGY Private Limited", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "RMSI PRIVATE LTD", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "R SYSTEMS", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "PPAP Automotive Ltd", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "PHYSICS WALLAH PVT LTD", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "PGS (PEPO GLOBAL SOURCING)", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Metro Hospitals & Heart Inst.", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Manav Rachna Institutions", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "MAGICBRICKS.COM (Times Internet)", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "LOGIX GROUP", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "KRIBHCO", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "JIL Information Technology (Jaypee)", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "JAIPRAKASH POWER VENTURES", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "INTERARCH BUILDING PRODUCTS", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "INOX WIND LTD", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "INDIA GLYCOLS LTD", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Honda Cars India Ltd", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "HAVELLS INDIA LIMITED", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Dharampal Satyapal Limited", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "CTA APPARELS", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "BROOKFIELD INDIA OFFICE PARKS", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "BIBA APPARELS", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "MITHILA PLYWOOD Private Limited", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "JAKSON ENGINEERS LIMITED", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Sinch India", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Somany Ceramics", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Rx-Logix", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "UNICLOUD LABS PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "GUJARAT FLUOROCHEMICALS LIMITED", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Jagran Prakashan Limited", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "INNOVATIVE VIEW INDIA PVT LTD", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "VECTUS INDUSTRIES LIMITED", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "ONEXTEL TECHNOLOGIES / TELSPIEL COMMUNICATIONS", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "SHARDA UNIVERSITY", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Addverbb", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "YoekiSoft Pvt Ltd", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Magic Software Pvt Ltd(Magic Edtech)", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "ESRI INDIA TECHNOLOGIES PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.17, "september": 0.17, "october": 0.17, "november": 0.52, "december": 0.23, "january": 0.23, "february": 0.23, "march": 0.7}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "INDIAN ENERGY EXCHANGE LIMITED", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "QRG Enterprises", "win_pct": 0.25, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.35, "september": 0.35, "october": 0.35, "november": 1.04, "december": 0.47, "january": 0.47, "february": 0.47, "march": 1.4}},
  {"am_email": "om.prakash@dnispl.com", "account_name": "Go2Cloud Solutions Pvt Ltd", "win_pct": 0.25, "aop_cr": 0.5, "months_raw": {"april": 0.03, "may": 0.03, "june": 0.03, "july": 0.08, "august": 0.04, "september": 0.04, "october": 0.04, "november": 0.13, "december": 0.06, "january": 0.06, "february": 0.06, "march": 0.18}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Phoenix Contact (I) Pvt Ltd", "win_pct": 0.18, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Akums Drugs and Pharmaceuticals Ltd", "win_pct": 0.19, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "LUMAX INDUSTRIES LIMITED", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "GREENLAM INDUSTRIES LIMITED", "win_pct": 0.1, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Ashiana Housing", "win_pct": 0.1, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Poly Medicure Limited", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "SANT NIRANKARI MISSON", "win_pct": 0.15, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "STONEX INDIA PRIVATE LIMITED", "win_pct": 0.12, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.025, "september": 0.025, "october": 0.025, "november": 0.075, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Hindustan Power", "win_pct": 0.15, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "SATYA MICROCAPITAL LIMITED", "win_pct": 0.2, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Rockwell Automation", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "AWFIS SPACE SOLUTIONS PRIVATE LIMITED", "win_pct": 0.15, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.025, "september": 0.025, "october": 0.025, "november": 0.075, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Bikanervala Foods Private Limited", "win_pct": 0.2, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Save Financial Services Pvt Ltd", "win_pct": 0.12, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "HUNCH CIRCLE PRIVATE LIMITED", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "FENA PRIVATE LIMITED", "win_pct": 0.15, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "INFOMERICS VALUATION AND RATING PRIVATE LIMITED", "win_pct": 0.15, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.025, "september": 0.025, "october": 0.025, "november": 0.075, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ANAND Group", "win_pct": 0.18, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "orient Bell", "win_pct": 0.1, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "Trinity Touch Pvt Ltd", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "KAPOOR WATCH COMPANY PRIVATE LTD", "win_pct": 0.1, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.025, "september": 0.025, "october": 0.025, "november": 0.075, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "DARK MATTER TECHNOLOGIES INDIA PRIVATE Limited", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "PHOENIX FAMILY OFFICE ADVISERS PVT LTD", "win_pct": 0.1, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "INGENUITY BUSINESS SERVICES PVT LTD", "win_pct": 0.12, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "IDVB RECYCLING PRIVATE LTD", "win_pct": 0.35, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "CEASEFIRE INDUSTRIES PVT LTD", "win_pct": 0.15, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "PATH ORG", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ZEACLOUD SERVICES PRIVATE LTD", "win_pct": 0.15, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.025, "september": 0.025, "october": 0.025, "november": 0.075, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ORIENTAL STRUCTURAL ENGINEERS PVT LTD", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "HEALTHQUAD CAPITAL ADVISORS PRIVATE LTD", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ADRIANNA PAPELL KD INDIA PRIVATE LTD", "win_pct": 0.18, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "CHRYS CAPITAL", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "SPRIX MANABIE EDUCATION PRIVATE LTD", "win_pct": 0.1, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "HASKONINGDHV CONSULTING PRIVATE LTD", "win_pct": 0.12, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "AGARWAL PACKERS AND MOVERS LTD", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "THE CHILDREN S INVESTMENT FUND FOUNDATION", "win_pct": 0.15, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "FJR INDIA PRIVATE Limited", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ALTF COWORKING", "win_pct": 0.15, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0208, "september": 0.0208, "october": 0.0208, "november": 0.0625, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "EAII ADVISORS PRIVATE LTD", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "DELHI STATE COOPERATIVE BANK LTD", "win_pct": 0.18, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ANUSHA TECHNOVISION PVT LTD", "win_pct": 0.15, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "WHALE CLOUD TECHNOLOGY INDIA PVT LTD", "win_pct": 0.15, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "KHD HUMBOLDT WEDAG INDIA PVT LTD", "win_pct": 0.12, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "DEVANSH AJUKESHAN AND WELFEYAR SOSAITY", "win_pct": 0.05, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "HPPL HINDUSTAN POWER PROJECTS PRIVATE Limited", "win_pct": 0.15, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "AREA27 Private Limited", "win_pct": 0.1, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "ORIFLAME INDIA PRIVATE LTD", "win_pct": 0.1, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "MANSUKH SECURITIES AND FINANCE Limited", "win_pct": 0.15, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0083, "september": 0.0083, "october": 0.0083, "november": 0.025, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "KOCHHAR AND COMPANY", "win_pct": 0.15, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "navneet.k@dnispl.com", "account_name": "OGI SOFTWARE PRIVATE Limited", "win_pct": 0.12, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0167, "september": 0.0167, "october": 0.0167, "november": 0.05, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Airtel", "win_pct": 0.7, "aop_cr": 20.0, "months_raw": {"april": 1.0, "may": 1.0, "june": 1.0, "july": 3.0, "august": 1.666, "september": 1.666, "october": 1.666, "november": 4.998, "december": 2.334, "january": 2.334, "february": 2.334, "march": 7.002}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Airtel Payment Bank", "win_pct": 0.8, "aop_cr": 3.0, "months_raw": {"april": 0.15, "may": 0.15, "june": 0.15, "july": 0.45, "august": 0.2499, "september": 0.2499, "october": 0.2499, "november": 0.7497, "december": 0.3501, "january": 0.3501, "february": 0.3501, "march": 1.0503}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Altius", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Anonet", "win_pct": 0.8, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Crest", "win_pct": 0.6, "aop_cr": 7.0, "months_raw": {"april": 0.35, "may": 0.35, "june": 0.35, "july": 1.05, "august": 0.5831, "september": 0.5831, "october": 0.5831, "november": 1.7493, "december": 0.8169, "january": 0.8169, "february": 0.8169, "march": 2.4507}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Ctrl S", "win_pct": 0.5, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Den", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Exitel", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Hathway", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Huges", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "NTT", "win_pct": 0.4, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Nxtra by Airtel", "win_pct": 0.6, "aop_cr": 5.0, "months_raw": {"april": 0.25, "may": 0.25, "june": 0.25, "july": 0.75, "august": 0.4165, "september": 0.4165, "october": 0.4165, "november": 1.2495, "december": 0.5835, "january": 0.5835, "february": 0.5835, "march": 1.7505}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Sify", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Tata", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Tikona", "win_pct": 0.1, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "soumya.m@dnispl.com", "account_name": "Yotta", "win_pct": 0.5, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Delhivery", "win_pct": 0.75, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Cinepolis", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Luminous Power", "win_pct": 0.75, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Medanta The Medicity", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "CENTRIENT PHARMACEUTICALS INDIA PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Infogain India Private Limited", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SMARTWORLD DEVELOPERS PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "OAKNORTH GLOBAL PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "G4S SECURITY SYSTEMS (INDIA) PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "XEBIA IT ARCHITECTS INDIA PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SKH Metals", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "YATRA ONLINE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SANDHAR TECHNOLOGIES LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Jaquar group", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "BLUE TOKAI(Muhavra Enterprises Pvt Ltd)", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "VLCC", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "UNICHARM INDIA PVT LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Innovative Facility/AIHP", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Smart Works", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "EasyRewardz Software Services Pvt Ltd", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "ANADRONE SYSTEMS PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Bharat Seats Ltd", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Krishna Maruti", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Park Plus", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "JUNIPER GREEN ENERGY PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "AdGlobal360 India Pvt Ltd", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Green panel Industries Ltd", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "BLUPINE ENERGY PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Xceedance", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "CE SERVICED INDIA PVT LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "INTECH ORGANICS LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "TRIDENT HILL PVT LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "MAHARASHTRA SEAMLESS LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "KRISUMI CORPORATION PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "CTAP SYSTEMS", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "ROOP AUTOMOTIVES LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "ANAQUA INDIA LLP", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "JAE INDIA PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SONI AUTO & ALLIED INDUSTRIES Limited", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "IDP EDUCATION INDIA PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "FLUID3 INFOTECH PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "ELECTRO RENT INDIA PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SANKYU INDIA LOGISTICS &ENGINEERING", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "UNIQUS CONSULTECH INC", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "PRECISION PYRAMID PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "XEBIA IT ARCHITECTS INDIA PRIVATE LIMITED", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "SHIMODA TRADING INDIA PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "XP INDIA", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "NISSHINBO COMPREHENSIVE PRECISION MACHINING GURGAON PRIVATE LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "NEXXBASE MARKETING Private Limited", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "ORBIS FINANCIAL CORPORATION LTD", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "PENTAX MEDICAL INDIA Private Limited", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Univo Education", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Barista", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Brookfield Properties", "win_pct": 0.5, "aop_cr": 4.0, "months_raw": {"april": 0.175, "may": 0.175, "june": 0.175, "july": 0.525, "august": 0.3034, "september": 0.3034, "october": 0.3034, "november": 0.9103, "december": 0.4084, "january": 0.4084, "february": 0.4084, "march": 1.2253}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Azure Power", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Gentari", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "aakriti.v@dnispl.com", "account_name": "Appinventiv", "win_pct": 0.25, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "L M PUBLIC SCHOOL", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "LOCON SOLUTIONS PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "IDEMITSU FINE COMPOSITES INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.5, "months_raw": {"april": 0.025, "may": 0.025, "june": 0.025, "july": 0.075, "august": 0.0433, "september": 0.0433, "october": 0.0433, "november": 0.13, "december": 0.0583, "january": 0.0583, "february": 0.0583, "march": 0.175}},
  {"am_email": "yash.k@dnispl.com", "account_name": "JATO DYNAMICS", "win_pct": 0.5, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.06, "august": 0.0347, "september": 0.0347, "october": 0.0347, "november": 0.104, "december": 0.0467, "january": 0.0467, "february": 0.0467, "march": 0.14}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ROOTSTRONG TECHNOLOGIES PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "CALLAWAY GOLF Private Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "EYE Q VISION", "win_pct": 0.5, "aop_cr": 0.7, "months_raw": {"april": 0.035, "may": 0.035, "june": 0.035, "july": 0.105, "august": 0.0607, "september": 0.0607, "october": 0.0607, "november": 0.1821, "december": 0.0817, "january": 0.0817, "february": 0.0817, "march": 0.2451}},
  {"am_email": "yash.k@dnispl.com", "account_name": "MGF SOURCING", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "INDORAMA SYNTHETICS I Limited", "win_pct": 0.5, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "yash.k@dnispl.com", "account_name": "NETWORK BULLSTUDY Private Limited", "win_pct": 0.5, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.06, "august": 0.0347, "september": 0.0347, "october": 0.0347, "november": 0.104, "december": 0.0467, "january": 0.0467, "february": 0.0467, "march": 0.14}},
  {"am_email": "yash.k@dnispl.com", "account_name": "NAVIGA GLOBAL", "win_pct": 0.5, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.06, "august": 0.0347, "september": 0.0347, "october": 0.0347, "november": 0.104, "december": 0.0467, "january": 0.0467, "february": 0.0467, "march": 0.14}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SYAC INNOVATIONS PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.06, "august": 0.0347, "september": 0.0347, "october": 0.0347, "november": 0.104, "december": 0.0467, "january": 0.0467, "february": 0.0467, "march": 0.14}},
  {"am_email": "yash.k@dnispl.com", "account_name": "IDP EDUCATION INDIA Private Limited", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ISON XPERIENCES INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "MACE PROJECT & COST MANAGEMENT Private Limited", "win_pct": 0.5, "aop_cr": 0.85, "months_raw": {"april": 0.0425, "may": 0.0425, "june": 0.0425, "july": 0.1275, "august": 0.0737, "september": 0.0737, "october": 0.0737, "november": 0.2211, "december": 0.0992, "january": 0.0992, "february": 0.0992, "march": 0.2976}},
  {"am_email": "yash.k@dnispl.com", "account_name": "TRAVEL CORPORATION OF INDIA", "win_pct": 0.5, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ALP AEROFLEX INDIA Private Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ROADSTAR TRUCKING", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "UCHIYAMA INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ASTI INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "CRS GLOBAL SERVICES PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "VYGON INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SSDN TECHNOLOGIES Private Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SUEZ ENVIRONMENT INDIA Limited", "win_pct": 0.5, "aop_cr": 1.5, "months_raw": {"april": 0.075, "may": 0.075, "june": 0.075, "july": 0.225, "august": 0.13, "september": 0.13, "october": 0.13, "november": 0.3901, "december": 0.175, "january": 0.175, "february": 0.175, "march": 0.5252}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SCA TECHNOLOGIES INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "LEX IP CARE LLP", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "MASTERS UNION SCHOOL OF BUSINESS", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "OLUMPUS CORPORATION", "win_pct": 0.5, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "yash.k@dnispl.com", "account_name": "Ireo", "win_pct": 0.5, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0867, "september": 0.0867, "october": 0.0867, "november": 0.2601, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "yash.k@dnispl.com", "account_name": "Wizfair", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "Paap Automotive", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "Polymedicure", "win_pct": 0.5, "aop_cr": 0.5, "months_raw": {"april": 0.025, "may": 0.025, "june": 0.025, "july": 0.075, "august": 0.0433, "september": 0.0433, "october": 0.0433, "november": 0.13, "december": 0.0583, "january": 0.0583, "february": 0.0583, "march": 0.175}},
  {"am_email": "yash.k@dnispl.com", "account_name": "Aye Finance", "win_pct": 0.5, "aop_cr": 0.5, "months_raw": {"april": 0.025, "may": 0.025, "june": 0.025, "july": 0.075, "august": 0.0433, "september": 0.0433, "october": 0.0433, "november": 0.13, "december": 0.0583, "january": 0.0583, "february": 0.0583, "march": 0.175}},
  {"am_email": "yash.k@dnispl.com", "account_name": "HBA CPAS AND CONSULTANTS", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "MRS BECTOR'S FOOD SPECIALTIES Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SISTEMA SHYAM TELESERVICES Limited", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ILOG SOLUTIONS INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "MINISTRY OF MINES", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "CLARION INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "BENGAL AEROTROPOLIS PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SML LABEL", "win_pct": 0.5, "aop_cr": 0.5, "months_raw": {"april": 0.025, "may": 0.025, "june": 0.025, "july": 0.075, "august": 0.0433, "september": 0.0433, "october": 0.0433, "november": 0.13, "december": 0.0583, "january": 0.0583, "february": 0.0583, "march": 0.175}},
  {"am_email": "yash.k@dnispl.com", "account_name": "PINKERTON INDIA", "win_pct": 0.5, "aop_cr": 0.5, "months_raw": {"april": 0.025, "may": 0.025, "june": 0.025, "july": 0.075, "august": 0.0433, "september": 0.0433, "october": 0.0433, "november": 0.13, "december": 0.0583, "january": 0.0583, "february": 0.0583, "march": 0.175}},
  {"am_email": "yash.k@dnispl.com", "account_name": "BALBIX INDIA Private Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SEAMLESS INFOTECH PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SAFFRON NETWORKS Private Limited", "win_pct": 0.5, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.015, "august": 0.0087, "september": 0.0087, "october": 0.0087, "november": 0.026, "december": 0.0117, "january": 0.0117, "february": 0.0117, "march": 0.035}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SANKYU INDIA LOGISTICS & ENGINEERING PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SAGACIOUS RESEARCH Private Limited", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ZABIN INDIA PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "PACE STOCK BROKING SERVICES Private Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "SAVANNAHSEEDSPrivate Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "CAPARO MARUTI Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "AUSTRALIA INDIA INSTITUTE PRIVATE Limited", "win_pct": 0.5, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.03, "august": 0.0173, "september": 0.0173, "october": 0.0173, "november": 0.052, "december": 0.0233, "january": 0.0233, "february": 0.0233, "march": 0.07}},
  {"am_email": "yash.k@dnispl.com", "account_name": "ADGLOBAL360 INDIA PVT LTD", "win_pct": 0.5, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.045, "august": 0.026, "september": 0.026, "october": 0.026, "november": 0.078, "december": 0.035, "january": 0.035, "february": 0.035, "march": 0.105}},
  {"am_email": "yash.k@dnispl.com", "account_name": "DYNATA", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "yash.k@dnispl.com", "account_name": "INDIAN NATIONAL ACADEMY OF ENGINEERING", "win_pct": 0.5, "aop_cr": 0.25, "months_raw": {"april": 0.0125, "may": 0.0125, "june": 0.0125, "july": 0.0375, "august": 0.0217, "september": 0.0217, "october": 0.0217, "november": 0.065, "december": 0.0292, "january": 0.0292, "february": 0.0292, "march": 0.0875}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Caparo Maruti", "win_pct": 0.3, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Hellmann Worldwide Logistics", "win_pct": 0.28, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "JumpCloud", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "JB Jewels and Metals", "win_pct": 0.4, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Voice Communication Delhi", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "SEB Administrative Services", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Ministry of Steel", "win_pct": 0.2, "aop_cr": 0.35, "months_raw": {"april": 0.0175, "may": 0.0175, "june": 0.0175, "july": 0.0292, "august": 0.0292, "september": 0.0292, "october": 0.0408, "november": 0.0408, "december": 0.0408, "january": 0.0292, "february": 0.0292, "march": 0.0292}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Telecraft E Solutions", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "CIMMYT", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Energia Systems", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Eglo India Production", "win_pct": 0.36, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Astrantia Real Estate", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Asia Pragati Capfin", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Damcosoft", "win_pct": 0.4, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "SMK Petrochemicals", "win_pct": 0.32, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Shri Guru Ram Dass Ed. Society", "win_pct": 0.32, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "FIBS Logistics", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Kailash Healthcare", "win_pct": 0.25, "aop_cr": 0.35, "months_raw": {"april": 0.0175, "may": 0.0175, "june": 0.0175, "july": 0.0292, "august": 0.0292, "september": 0.0292, "october": 0.0408, "november": 0.0408, "december": 0.0408, "january": 0.0292, "february": 0.0292, "march": 0.0292}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Jupiter Aluminium Industries", "win_pct": 0.36, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Veolia Water India", "win_pct": 0.22, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Aamor Inox", "win_pct": 0.36, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Remfry & Sagar", "win_pct": 0.4, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Envirocare Infrasolutions", "win_pct": 0.38, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Neurolytica Consulting", "win_pct": 0.4, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Brys Hotels", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "FICCI", "win_pct": 0.28, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Hughes Communications India", "win_pct": 0.22, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.0333, "august": 0.0333, "september": 0.0333, "october": 0.0467, "november": 0.0467, "december": 0.0467, "january": 0.0333, "february": 0.0333, "march": 0.0333}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "DEE Development Engineers", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Yatra Online", "win_pct": 0.25, "aop_cr": 0.35, "months_raw": {"april": 0.0175, "may": 0.0175, "june": 0.0175, "july": 0.0292, "august": 0.0292, "september": 0.0292, "october": 0.0408, "november": 0.0408, "december": 0.0408, "january": 0.0292, "february": 0.0292, "march": 0.0292}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "MMTC", "win_pct": 0.18, "aop_cr": 0.0, "months_raw": {"april": 0.0, "may": 0.0, "june": 0.0, "july": 0.0, "august": 0.0, "september": 0.0, "october": 0.0, "november": 0.0, "december": 0.0, "january": 0.0, "february": 0.0, "march": 0.0}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "OLX India", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Medanta The Medicity", "win_pct": 0.28, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Dhanuka Agritech", "win_pct": 0.3, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "E4E / Savista Global", "win_pct": 0.25, "aop_cr": 0.0, "months_raw": {"april": 0.0, "may": 0.0, "june": 0.0, "july": 0.0, "august": 0.0, "september": 0.0, "october": 0.0, "november": 0.0, "december": 0.0, "january": 0.0, "february": 0.0, "march": 0.0}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Urban Company", "win_pct": 0.25, "aop_cr": 0.0, "months_raw": {"april": 0.0, "may": 0.0, "june": 0.0, "july": 0.0, "august": 0.0, "september": 0.0, "october": 0.0, "november": 0.0, "december": 0.0, "january": 0.0, "february": 0.0, "march": 0.0}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "CP Wholesale India", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "AWFIS", "win_pct": 0.28, "aop_cr": 0.35, "months_raw": {"april": 0.0175, "may": 0.0175, "june": 0.0175, "july": 0.0292, "august": 0.0292, "september": 0.0292, "october": 0.0408, "november": 0.0408, "december": 0.0408, "january": 0.0292, "february": 0.0292, "march": 0.0292}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "BharatPe", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Varun Group", "win_pct": 0.22, "aop_cr": 0.4, "months_raw": {"april": 0.02, "may": 0.02, "june": 0.02, "july": 0.0333, "august": 0.0333, "september": 0.0333, "october": 0.0467, "november": 0.0467, "december": 0.0467, "january": 0.0333, "february": 0.0333, "march": 0.0333}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Delhi Waste Management", "win_pct": 0.25, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Cashify", "win_pct": 0.3, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "SGT University", "win_pct": 0.28, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Louis Dreyfus Commodities", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Qualfon Technology Support", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Caparo Group India", "win_pct": 0.22, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Estee Advisors", "win_pct": 0.38, "aop_cr": 10.0, "months_raw": {"april": 0.5, "may": 0.5, "june": 0.5, "july": 0.833, "august": 0.833, "september": 0.833, "october": 1.167, "november": 1.167, "december": 1.167, "january": 0.833, "february": 0.833, "march": 0.833}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Brookfield India Office Parks", "win_pct": 0.25, "aop_cr": 0.3, "months_raw": {"april": 0.015, "may": 0.015, "june": 0.015, "july": 0.025, "august": 0.025, "september": 0.025, "october": 0.035, "november": 0.035, "december": 0.035, "january": 0.025, "february": 0.025, "march": 0.025}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Flender Drives", "win_pct": 0.35, "aop_cr": 0.1, "months_raw": {"april": 0.005, "may": 0.005, "june": 0.005, "july": 0.0083, "august": 0.0083, "september": 0.0083, "october": 0.0117, "november": 0.0117, "december": 0.0117, "january": 0.0083, "february": 0.0083, "march": 0.0083}},
  {"am_email": "stephen.h@dnispl.com", "account_name": "Relaxo Footwears", "win_pct": 0.25, "aop_cr": 0.2, "months_raw": {"april": 0.01, "may": 0.01, "june": 0.01, "july": 0.0167, "august": 0.0167, "september": 0.0167, "october": 0.0233, "november": 0.0233, "december": 0.0233, "january": 0.0167, "february": 0.0167, "march": 0.0167}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "ANANT RAJ LIMITED", "win_pct": 0.25, "aop_cr": 2.0, "months_raw": {"april": 0.1, "may": 0.1, "june": 0.1, "july": 0.3, "august": 0.1666, "september": 0.1666, "october": 0.1666, "november": 0.4998, "december": 0.2334, "january": 0.2334, "february": 0.2334, "march": 0.7002}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "DEGANIA MEDICAL DEVICES", "win_pct": 0.25, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "Denso International India", "win_pct": 0.35, "aop_cr": 4.0, "months_raw": {"april": 0.2, "may": 0.2, "june": 0.2, "july": 0.6, "august": 0.3332, "september": 0.3332, "october": 0.3332, "november": 0.9996, "december": 0.4668, "january": 0.4668, "february": 0.4668, "march": 1.4004}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "DSY Creations", "win_pct": 0.25, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "HL Mando Anand", "win_pct": 0.3, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "Joyson Anand Abhishek Safety", "win_pct": 0.2, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "MUNJAL KIRIU INDUSTRIES", "win_pct": 0.25, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}},
  {"am_email": "vikrant.tolumbia@dnispl.com", "account_name": "Ambrane India Pvt Ltd", "win_pct": 0.6, "aop_cr": 1.0, "months_raw": {"april": 0.05, "may": 0.05, "june": 0.05, "july": 0.15, "august": 0.0833, "september": 0.0833, "october": 0.0833, "november": 0.2499, "december": 0.1167, "january": 0.1167, "february": 0.1167, "march": 0.3501}}
]

# ---------------------------------------------------------------------------
# SALARY API
# ---------------------------------------------------------------------------
@app.route("/api/salary", methods=["GET"])
def get_salary():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    if not viewer_email:
        return jsonify({})
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT salary_data FROM user_salary WHERE lower(email)=lower(%s)", (viewer_email,))
            row = cur.fetchone()
            if row:
                d = row.get("salary_data") or {}
                if isinstance(d, str):
                    try: d = json.loads(d)
                    except: d = {}
                return jsonify(d)
            return jsonify({})
    finally:
        conn.close()

@app.route("/api/salary", methods=["POST"])
def save_salary():
    data = request.get_json(silent=True) or {}
    viewer_email = _normalize_email(data.get("viewer_email") or "")
    if not viewer_email:
        return jsonify({"error": "viewer_email required"}), 400
    salary_data = {k: v for k, v in data.items() if k not in {"viewer_email", "viewer_role"}}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO user_salary (email, salary_data, updated_at) VALUES (%s, %s::jsonb, now()) ON CONFLICT (email) DO UPDATE SET salary_data=EXCLUDED.salary_data, updated_at=now()",
                (viewer_email, json.dumps(salary_data))
            )
        conn.commit()
        return jsonify({"status": "ok"})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# REPORTS TEAM
# ---------------------------------------------------------------------------
@app.route("/api/reports/team", methods=["GET"])
def reports_team():
    viewer_role = (request.args.get("viewer_role") or "").strip().lower()
    if not _is_supervisor(viewer_role):
        return jsonify({"error": "supervisor only"}), 403
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, email, name, role FROM users
                WHERE role IN ('account_manager','presales','salesops','sales','sales_head')
                ORDER BY name
            """)
            return jsonify(cur.fetchall())
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# KRA REPORT
# ---------------------------------------------------------------------------
@app.route("/api/reports/kra", methods=["GET"])
def kra_report():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    viewer_role  = (request.args.get("viewer_role") or "account_manager").strip().lower()
    target_email = _normalize_email(request.args.get("target_email") or viewer_email)
    # Frontend sends target_role as the authoritative role — no DB lookup needed
    target_role  = (request.args.get("target_role") or "").strip().lower()
    fy_year      = (request.args.get("fy_year") or "2025-26").strip()
    quarter      = (request.args.get("quarter") or "Q1").strip().upper()

    if not _is_supervisor(viewer_role) and viewer_email != target_email:
        return jsonify({"error": "not authorized"}), 403

    role_aliases = {
        "sales": "account_manager",
        "account_manager": "account_manager",
        "presales": "presales",
        "salesops": "salesops",
        "sales_ops": "salesops",
        "sales_head": "sales_head",
        "supervisor": "supervisor",
        "admin": "supervisor",
    }
    effective_role = role_aliases.get(target_role or viewer_role, target_role or viewer_role)

    q_month_map = {
        "Q1": ["apr", "may", "jun"],
        "Q2": ["jul", "aug", "sep"],
        "Q3": ["oct", "nov", "dec"],
        "Q4": ["jan", "feb", "mar"],
    }
    q_prefixes = q_month_map.get(quarter, ["apr", "may", "jun"])

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # PRESALES KRA
            if effective_role == "presales":
                cur.execute("""SELECT COUNT(*) AS total,
                    COUNT(CASE WHEN lower(workflow_stage) IN ('won','closed won') THEN 1 END) AS won,
                    COUNT(CASE WHEN final_pricing_proposal IS NOT NULL AND final_pricing_proposal<>'' THEN 1 END) AS proposals,
                    COALESCE(SUM(value),0) AS pipeline
                    FROM opportunities WHERE lower(assigned_presales)=lower(%s)""", (target_email,))
                r = cur.fetchone() or {}
                proposals = int(r.get("proposals") or 0)
                won_opps  = int(r.get("won") or 0)
                pipeline  = float(r.get("pipeline") or 0) / 10000000
                win_rate  = round(won_opps/proposals*100,1) if proposals > 0 else 0
                cur.execute("SELECT COUNT(*) AS c FROM opportunities WHERE lower(assigned_presales)=lower(%s) AND presales_architecture IS NOT NULL AND presales_architecture<>''", (target_email,))
                pocs = int((cur.fetchone() or {}).get("c") or 0)
                return jsonify({"role":"presales","target_email":target_email,"quarter":quarter,"fy_year":fy_year,
                    "kras":[
                        {"id":"win_rate","name":"Proposal & RFP Win Rate","weight":30,"target":">=75% win rate","actual_label":f"{win_rate}% ({won_opps} won / {proposals} submitted)","actual_value":win_rate,"target_value":75,"unit":"%","achievement_pct":min(round(win_rate/75*100,1) if proposals else 0,150)},
                        {"id":"solution_design","name":"Solution Design & Technical Accuracy","weight":25,"target":"<2 revision cycles; >=90% BOQ accuracy","actual_label":f"{pocs} architectures submitted","actual_value":pocs,"target_value":None,"unit":"count","achievement_pct":None,"manual":True},
                        {"id":"pipeline","name":"Revenue Pipeline Contribution","weight":10,"target":">=12 POCs/demos per quarter","actual_label":f"{pocs} POCs; Pipeline Rs{pipeline:.1f}Cr","actual_value":pocs,"target_value":12,"unit":"POCs","achievement_pct":min(round(pocs/12*100,1),150)},
                        {"id":"expertise","name":"Product & Technology Expertise","weight":10,"target":">=3 certs/year; 40hrs training/year","actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"collaboration","name":"Cross-functional Collaboration","weight":15,"target":"Stakeholder feedback; quarterly review","actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"innovation","name":"Innovation & Thought Leadership","weight":10,"target":"COE lab setup; case studies","actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                    ]})

            # SALESOPS KRA — OEM/Distributor Quote & Pricing Ops
            if effective_role == "salesops":
                cur.execute("""SELECT COUNT(*) AS total,
                    COUNT(CASE WHEN costing_returned_at IS NOT NULL AND costing_returned_at<>'' THEN 1 END) AS returned,
                    COALESCE(SUM(value),0) AS pipeline
                    FROM opportunities WHERE lower(assigned_salesops)=lower(%s)""", (target_email,))
                r = cur.fetchone() or {}
                total = int(r.get("total") or 0)
                returned = int(r.get("returned") or 0)
                pipeline = round(float(r.get("pipeline") or 0) / 10000000, 2)
                avg_tat = 0.0
                try:
                    cur.execute("""
                        SELECT costing_returned_at, salesops_assigned_at
                        FROM opportunities
                        WHERE lower(assigned_salesops)=lower(%s)
                          AND costing_returned_at IS NOT NULL AND trim(costing_returned_at) <> ''
                          AND salesops_assigned_at IS NOT NULL AND trim(salesops_assigned_at) <> ''
                    """, (target_email,))
                    tat_hours = []
                    for row in cur.fetchall() or []:
                        cr = parse_iso_dt(row.get("costing_returned_at"))
                        sa = parse_iso_dt(row.get("salesops_assigned_at"))
                        if cr and sa and cr >= sa:
                            tat_hours.append((cr - sa).total_seconds() / 3600.0)
                    if tat_hours:
                        avg_tat = round(sum(tat_hours) / len(tat_hours), 1)
                except Exception:
                    avg_tat = 0.0

                tat_ach = min(round(48 / avg_tat * 100, 1), 150) if avg_tat > 0 else (100 if returned == 0 else 0)
                crm_filled = 0
                crm_comment_column = None
                if _column_exists(cur, "opportunities", "sales_ops_comments"):
                    crm_comment_column = "sales_ops_comments"
                elif _column_exists(cur, "opportunities", "salesops_comments"):
                    crm_comment_column = "salesops_comments"
                if crm_comment_column:
                    cur.execute(f"""SELECT COUNT(*) AS c FROM opportunities
                        WHERE lower(assigned_salesops)=lower(%s)
                        AND {crm_comment_column} IS NOT NULL AND {crm_comment_column}<>''""", (target_email,))
                    crm_filled = int((cur.fetchone() or {}).get("c") or 0)
                crm_ach = min(round(crm_filled/total*100, 1) if total > 0 else 100, 100)
                return jsonify({"role":"salesops","target_email":target_email,"quarter":quarter,"fy_year":fy_year,
                    "kras":[
                        {"id":"oem_quote_tat","name":"OEM & Distributor Quote Follow-ups","weight":25,
                         "target":"Quote TAT <=48hrs; follow-up closure >=90%; zero lapses",
                         "actual_label":f"Avg TAT {avg_tat}h | {returned}/{total} quotes returned",
                         "actual_value":avg_tat,"target_value":48,"unit":"hours","achievement_pct":tat_ach},
                        {"id":"proposal_accuracy","name":"Proposal Preparation & Quote Accuracy","weight":20,
                         "target":">=95% pricing accuracy; <2 revision cycles/month; within TAT",
                         "actual_label":"Manual input required — track revision cycles",
                         "actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"vendor_coordination","name":"Third-Party & L3 Vendor Coordination","weight":20,
                         "target":"Vendor TAT <=72hrs; escalation rate <10%; >=2 price sources/req",
                         "actual_label":"Manual input required — track vendor responses",
                         "actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"crm_support","name":"Sales Pipeline & CRM Support","weight":15,
                         "target":"CRM accuracy >=98%; weekly pipeline reports on time; ops TAT <=24hrs",
                         "actual_label":f"{crm_filled}/{total} opps with pricing data in CRM ({crm_ach}%)",
                         "actual_value":crm_ach,"target_value":98,"unit":"%","achievement_pct":min(round(crm_ach/98*100,1),150)},
                        {"id":"order_processing","name":"Order Processing & Documentation Compliance","weight":10,
                         "target":"PO TAT <=24hrs; doc accuracy >=99%; zero compliance misses",
                         "actual_label":"Manual input required — track PO processing",
                         "actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"deal_registration","name":"OEM Partner Program & Deal Registration","weight":10,
                         "target":"100% eligible deals registered; rebate tracking >=98%; zero missed",
                         "actual_label":"Manual input required — track deal registrations",
                         "actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                    ]})

            if effective_role in ("supervisor", "sales_head"):
                # Auto-calculate from DB
                cur.execute("SELECT COALESCE(SUM(value),0) AS won FROM opportunities WHERE lower(workflow_stage) IN ('won','closed won')", ())
                won_val = round(float((cur.fetchone() or {}).get("won") or 0)/10000000, 2)
                revenue_target = 75.0
                revenue_ach = min(round(won_val/revenue_target*100, 1), 150)

                cur.execute("""
                    SELECT COUNT(DISTINCT account_id) AS accs
                    FROM opportunities
                    WHERE lower(workflow_stage) IN ('won','closed won')
                      AND account_id IS NOT NULL AND trim(account_id) <> ''
                """, ())
                won_accounts = int((cur.fetchone() or {}).get("accs") or 0)
                new_biz_ach = min(round(won_accounts/200*100, 1), 150)

                cur.execute("SELECT COALESCE(SUM(value),0) AS pipeline FROM opportunities WHERE lower(workflow_stage) NOT IN ('closed won','closed lost')", ())
                pipeline_cr = round(float((cur.fetchone() or {}).get("pipeline") or 0)/10000000, 2)

                cur.execute("""SELECT COUNT(DISTINCT lower(p.account_manager)) AS total_reps,
                    COUNT(DISTINCT CASE WHEN o.won_val >= p.q_target THEN lower(p.account_manager) END) AS on_target
                    FROM (SELECT lower(owner) AS account_manager,
                        SUM(CASE WHEN lower(workflow_stage) IN ('won','closed won') THEN value ELSE 0 END) AS won_val,
                        SUM(value)*0.25 AS q_target
                        FROM opportunities GROUP BY lower(owner)) o
                    JOIN (SELECT DISTINCT lower(account_manager) AS account_manager FROM aop_plans WHERE fy_year=%s) p
                    ON o.account_manager = p.account_manager""", (fy_year,))
                tr = cur.fetchone() or {}
                total_reps = int(tr.get("total_reps") or 0)
                on_target = int(tr.get("on_target") or 0)
                team_ach = min(round(on_target/total_reps*100, 1) if total_reps > 0 else 0, 150)

                return jsonify({"role":"supervisor","target_email":target_email,"quarter":quarter,"fy_year":fy_year,
                    "kras":[
                        {"id":"revenue","name":"Overall Revenue & Profitability","weight":30,
                         "target":"₹75Cr annual revenue target",
                         "actual_label":f"₹{won_val:.2f}Cr won | Target ₹75Cr",
                         "actual_value":won_val,"target_value":revenue_target,"unit":"Cr","achievement_pct":revenue_ach},
                        {"id":"new_biz","name":"New Business Acquisition","weight":20,
                         "target":"200 accounts with closed won deals",
                         "actual_label":f"{won_accounts} accounts with won deals | Target 200",
                         "actual_value":won_accounts,"target_value":200,"unit":"accounts","achievement_pct":new_biz_ach},
                        {"id":"pipeline","name":"Sales Pipeline & Forecast Accuracy","weight":10,
                         "target":"3x quota pipeline; win rate >= 35%",
                         "actual_label":f"₹{pipeline_cr:.2f}Cr active pipeline",
                         "actual_value":pipeline_cr,"target_value":None,"unit":"Cr","achievement_pct":None,"manual":True},
                        {"id":"team_perf","name":"Team Performance & Quota Attainment","weight":15,
                         "target":">= 70% of team at 100% quota",
                         "actual_label":f"{on_target}/{total_reps} reps on target ({team_ach}%)",
                         "actual_value":team_ach,"target_value":70,"unit":"%","achievement_pct":min(round(team_ach/70*100,1),150)},
                        {"id":"coaching","name":"Leadership, Coaching & Team Development","weight":10,
                         "target":"Structured PDPs for each team member",
                         "actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                        {"id":"strategic","name":"Strategic Thinking & Market Intelligence","weight":15,
                         "target":"Partnerships signed; Cisco/Fortinet/HP/Dell/Lenovo",
                         "actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                    ]})

            # ACCOUNT MANAGER / SALES KRA
            cur.execute("""
                SELECT COALESCE(SUM(value),0) AS won
                FROM opportunities
                WHERE (lower(owner)=lower(%s) OR lower(COALESCE(sales_owner, ''))=lower(%s))
                  AND lower(workflow_stage) IN ('won','closed won')
            """, (target_email, target_email))
            won_cr = float((cur.fetchone() or {}).get("won") or 0) / 10000000

            month_sum_expr = " + ".join([f"COALESCE(p.{px}_hardware,0)+COALESCE(p.{px}_software,0)+COALESCE(p.{px}_managed_services,0)" for px in q_prefixes])
            cur.execute(f"""
                SELECT COALESCE(SUM(({month_sum_expr}) * COALESCE(p.target_growth, 1)), 0) AS q_target
                FROM aop_plans p
                WHERE p.fy_year=%s AND lower(p.account_manager)=lower(%s)
            """, (fy_year, target_email))
            q_target = round(float((cur.fetchone() or {}).get("q_target") or 0), 4)
            rev_ach   = min(round(won_cr/q_target*100,1) if q_target > 0 else 0, 150)

            cur.execute("SELECT COUNT(DISTINCT a.id) AS total FROM accounts a JOIN users u ON u.id=a.account_manager_id WHERE lower(u.email)=lower(%s)", (target_email,))
            total_accts = int((cur.fetchone() or {}).get("total") or 0)
            cur.execute("SELECT COUNT(DISTINCT c.account_id) AS covered FROM contacts c JOIN accounts a ON CAST(a.id AS TEXT)=c.account_id JOIN users u ON u.id=a.account_manager_id WHERE lower(u.email)=lower(%s)", (target_email,))
            covered_accts = int((cur.fetchone() or {}).get("covered") or 0)
            cov_pct = round(covered_accts/total_accts*100,1) if total_accts > 0 else 0

            cur.execute("SELECT COUNT(DISTINCT o.account_id) AS c FROM opportunities o JOIN accounts a ON CAST(a.id AS TEXT)=o.account_id JOIN users u ON u.id=a.account_manager_id WHERE lower(u.email)=lower(%s) AND lower(o.workflow_stage) IN ('won','closed won')", (target_email,))
            new_act = int((cur.fetchone() or {}).get("c") or 0)
            new_act_ach = min(round(new_act/6*100,1), 150)

            cur.execute("""
                SELECT COALESCE(SUM(value),0) AS pipe
                FROM opportunities
                WHERE (lower(owner)=lower(%s) OR lower(COALESCE(sales_owner, ''))=lower(%s))
                  AND lower(workflow_stage) NOT IN ('won','closed won','lost','closed lost')
            """, (target_email, target_email))
            pipe_cr     = float((cur.fetchone() or {}).get("pipe") or 0) / 10000000
            pipe_target = q_target * 3
            pipe_ach    = min(round(pipe_cr/pipe_target*100,1) if pipe_target > 0 else 0, 150)

            cur.execute("""
                SELECT COUNT(*) AS c
                FROM opportunities
                WHERE (lower(owner)=lower(%s) OR lower(COALESCE(sales_owner, ''))=lower(%s))
                  AND value>=10000000
            """, (target_email, target_email))
            qual     = int((cur.fetchone() or {}).get("c") or 0)
            qual_ach = min(round(qual/9*100,1), 150)

            return jsonify({"role":"account_manager","target_email":target_email,"quarter":quarter,"fy_year":fy_year,
                "q_target_cr":round(q_target,2),"won_value_cr":round(won_cr,2),
                "kras":[
                    {"id":"revenue","name":"Revenue & Sales Target Achievement","weight":25,"target":f"Rs{q_target:.2f}Cr ({quarter})","actual_label":f"Rs{won_cr:.2f}Cr won","actual_value":won_cr,"target_value":q_target,"unit":"Cr","achievement_pct":rev_ach},
                    {"id":"account_coverage","name":"Account Coverage","weight":40,"target":"100% accounts with contacts in CRM","actual_label":f"{covered_accts}/{total_accts} accounts ({cov_pct}%)","actual_value":cov_pct,"target_value":100,"unit":"%","achievement_pct":min(cov_pct,150)},
                    {"id":"new_account","name":"New Account Activation","weight":20,"target":"6 new accounts with approved PO","actual_label":f"{new_act} accounts with PO","actual_value":new_act,"target_value":6,"unit":"accounts","achievement_pct":new_act_ach},
                    {"id":"pipeline","name":"Pipeline & Opportunity Management","weight":10,"target":f"3x quota (Rs{pipe_target:.1f}Cr)","actual_label":f"Rs{pipe_cr:.2f}Cr pipeline","actual_value":pipe_cr,"target_value":pipe_target,"unit":"Cr","achievement_pct":pipe_ach},
                    {"id":"qual_opps","name":"Qualified Opportunity Creation","weight":5,"target":"Min 3 opps/month >=Rs1Cr","actual_label":f"{qual} qualified opps","actual_value":qual,"target_value":9,"unit":"opps","achievement_pct":qual_ach},
                ]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# INCENTIVE REPORT
# ---------------------------------------------------------------------------
@app.route("/api/reports/incentive", methods=["GET"])
def incentive_report():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    viewer_role  = (request.args.get("viewer_role") or "account_manager").strip().lower()
    target_email = _normalize_email(request.args.get("target_email") or viewer_email)
    quarter      = (request.args.get("quarter") or "Q1").strip().upper()
    if not _is_supervisor(viewer_role) and viewer_email != target_email:
        return jsonify({"error": "not authorized"}), 403
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT salary_data FROM user_salary WHERE lower(email)=lower(%s)", (target_email,))
            row = cur.fetchone()
            sal = {}
            if row:
                d = row.get("salary_data") or {}
                if isinstance(d, str):
                    try: d = json.loads(d)
                    except: d = {}
                sal = d
        fixed = float(sal.get("fixed_annual_lakhs") or 0)
        var   = float(sal.get("variable_annual_lakhs") or 0)
        qfrac = {"Q1":0.15,"Q2":0.25,"Q3":0.35,"Q4":0.25}.get(quarter, 0.15)
        q_var = round(var * qfrac, 2)
        slabs = [
            {"label":"Below Threshold","min":0,"max":84.99,"payout_pct":0},
            {"label":"Threshold","min":85,"max":99.99,"payout_pct":70},
            {"label":"On Target","min":100,"max":109.99,"payout_pct":100},
            {"label":"Stretch","min":110,"max":119.99,"payout_pct":120},
            {"label":"Accelerator","min":120,"max":999,"payout_pct":150},
        ]
        return jsonify({"target_email":target_email,"quarter":quarter,"fixed_annual_lakhs":fixed,
            "variable_annual_lakhs":var,"quarterly_variable_lakhs":q_var,
            "salary_configured":bool(sal),"slabs":slabs})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# AOP BULK IMPORT
# ---------------------------------------------------------------------------
@app.route("/api/aop/bulk-import", methods=["POST"])
def aop_bulk_import():
    data = request.get_json(silent=True) or {}
    viewer_role = (data.get("viewer_role") or "").strip().lower()
    if not _is_supervisor(viewer_role):
        return jsonify({"error": "supervisor only"}), 403
    rows = data.get("rows") or []
    if not rows:
        rows = AOP_RAW_DATA
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ok = skipped = 0
            month_map = {
                "april":"apr","may":"may","june":"jun","july":"jul","august":"aug","september":"sep",
                "october":"oct","november":"nov","december":"dec","january":"jan","february":"feb","march":"mar"
            }
            for row in rows:
                account_name = (row.get("account_name") or "").strip()
                am_email     = (row.get("am_email") or "").strip().lower()
                win_pct      = float(row.get("win_pct") or 0)
                aop_cr       = float(row.get("aop_cr") or 0)
                months_raw   = row.get("months_raw") or {}
                if not account_name or not am_email:
                    skipped += 1; continue

                # Find account
                cur.execute("SELECT id FROM accounts WHERE lower(account_name)=lower(%s) LIMIT 1", (account_name,))
                acc = cur.fetchone()
                if not acc:
                    skipped += 1; continue

                account_id = str(acc["id"])
                # Build flat column values — distribute AOP evenly across HW/SW/SV (100%/0%/0% default since we only have total)
                # Store total in hardware column, win_pct in target_growth
                col_vals = {}
                for mname, prefix in month_map.items():
                    raw = float(months_raw.get(mname) or 0)
                    col_vals[f"{prefix}_hardware"] = round(raw, 6)
                    col_vals[f"{prefix}_software"] = 0.0
                    col_vals[f"{prefix}_managed_services"] = 0.0

                cols = ", ".join(col_vals.keys())
                placeholders = ", ".join(["%s"] * len(col_vals))
                update_set = ", ".join([f"{k}=EXCLUDED.{k}" for k in col_vals.keys()])
                if not col_vals:
                    skipped += 1
                    continue
                cur.execute(f"""
                    INSERT INTO aop_plans (account_id, account_name, account_manager, fy_year,
                        current_revenue, target_growth, updated_by, created_at, updated_at, {cols})
                    VALUES (%s, %s, %s, '2025-26', %s, %s, %s, now(), now(), {placeholders})
                    ON CONFLICT (account_id, fy_year) DO UPDATE SET
                        account_manager=EXCLUDED.account_manager,
                        current_revenue=EXCLUDED.current_revenue,
                        target_growth=EXCLUDED.target_growth,
                        updated_by=EXCLUDED.updated_by,
                        updated_at=now(), {update_set}
                """, (account_id, account_name, am_email, aop_cr, win_pct, am_email, *col_vals.values()))
                ok += 1
        conn.commit()
        return jsonify({"status":"ok","imported":ok,"skipped":skipped})
    except Exception as exc:
        return jsonify({"error":str(exc)}), 500
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"Simple CRM backend running on port {port}")
    print("DB host:", urlparse(DATABASE_URL).hostname)
    app.run(host="0.0.0.0", port=port, debug=True)
