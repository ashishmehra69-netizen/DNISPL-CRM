import csv
import json
import base64
import hashlib
import hmac
import importlib.util
import urllib.parse
import urllib.request
import os
import smtplib
import threading
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from io import StringIO
from pathlib import Path
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
SUPERVISOR_EMAIL = os.environ.get("SUPERVISOR_EMAIL", "ashish.mehra@dnispl.com").strip().lower()
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
_aop_module = None


def _load_aop_module():
    global _aop_module
    if _aop_module is not None:
        return _aop_module
    current_path = Path(__file__).resolve()
    aop_path = current_path.parents[1] / 'aop' / 'backend.py'
    if aop_path == current_path or not aop_path.exists():
        return None
    spec = importlib.util.spec_from_file_location('dnispl_aop_backend', aop_path)
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _aop_module = module
    return module


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
                options="-c statement_timeout=8000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=10000",
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
                    owner TEXT,
                    sales_owner TEXT,
                    workflow_stage TEXT,
                    assigned_presales TEXT,
                    assigned_purchase TEXT,
                    assigned_salesops TEXT,
                    sales_comments TEXT,
                    requirements TEXT,
                    presales_architecture TEXT,
                    presales_questions TEXT,
                    boq TEXT,
                    purchase_costing TEXT,
                    costing_tat TEXT,
                    final_pricing_proposal TEXT,
                    presales_assigned_at TEXT,
                    presales_due_at TEXT,
                    purchase_assigned_at TEXT,
                    purchase_due_at TEXT,
                    costing_returned_at TEXT,
                    final_proposal_at TEXT,
                    assignment_due_at TEXT,
                    sales_submitted_at TEXT,
                    presales_escalated_at TEXT,
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
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS assigned_salesops TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS presales_escalated_at TEXT;")
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
                CREATE TABLE IF NOT EXISTS purchase_orders (
                    id TEXT PRIMARY KEY,
                    po_number TEXT,
                    po_type TEXT,
                    stage TEXT,
                    opportunity_id TEXT,
                    account_id TEXT,
                    account_name TEXT,
                    vendor_name TEXT,
                    vendor_po_number TEXT,
                    oem TEXT,
                    deal_registration_no TEXT,
                    description TEXT,
                    value NUMERIC DEFAULT 0,
                    vendor_value NUMERIC DEFAULT 0,
                    payment_terms_customer TEXT,
                    payment_terms_vendor TEXT,
                    approval_level TEXT,
                    presales_approved_by TEXT,
                    presales_approved_at TEXT,
                    finance_approved_by TEXT,
                    finance_approved_at TEXT,
                    implementation_approved_by TEXT,
                    implementation_approved_at TEXT,
                    ceo_approved_by TEXT,
                    ceo_approved_at TEXT,
                    expected_delivery TEXT,
                    actual_delivery TEXT,
                    grn_number TEXT,
                    grn_date TEXT,
                    invoice_number TEXT,
                    invoice_date TEXT,
                    notes TEXT,
                    site_address TEXT,
                    is_site_work BOOLEAN DEFAULT FALSE,
                    site_completion_date TEXT,
                    scanned_po_data TEXT,
                    scanned_po_image TEXT,
                    owner TEXT,
                    created_by TEXT,
                    requestor_email TEXT,
                    sales_owner TEXT,
                    account_manager_email TEXT,
                    presales_approver TEXT,
                    finance_approver TEXT,
                    implementation_approver TEXT,
                    ceo_approver TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_assigned_purchase ON opportunities(lower(assigned_purchase));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_assigned_salesops ON opportunities(lower(assigned_salesops));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_account_id ON opportunities(account_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_aop_plans_fy ON aop_plans(fy_year);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_aop_actuals_fy ON aop_actuals(fy_year);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_po_account_id ON purchase_orders(account_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_po_stage ON purchase_orders(stage);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_po_owner ON purchase_orders(lower(owner));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_po_requestor ON purchase_orders(lower(requestor_email));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_po_sales_owner ON purchase_orders(lower(sales_owner));")
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
    ensure_db_initialized()


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
    with urllib.request.urlopen(req, timeout=25) as resp:
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
    for e in [SUPERVISOR_EMAIL, sales_email, account_manager_email]:
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
    send_email_smtp([presales_target], subject, body, cc_emails=cc_list)
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
                and workflow_stage in ("Assigned to Presales", "Awaiting Purchase Costing", "Costing Returned")
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
            if viewer_role in ("supervisor", "admin"):
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
                if viewer_role in ('presales', 'salesops', 'purchase'):
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
                else:
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
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                ORDER BY updated_at DESC
            """
            opp_sales_scoped = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(owner)=lower(%s)
                   OR lower(sales_owner)=lower(%s)
                ORDER BY updated_at DESC
            """
            opp_presales_scoped = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(assigned_presales)=lower(%s)
                  AND lower(coalesce(workflow_stage,'Sales Review')) IN ('assigned to presales', 'costing returned', 'pricing returned to presales', 'presales overdue')
                ORDER BY updated_at DESC
            """
            opp_salesops_scoped = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE (lower(assigned_salesops)=lower(%s) OR lower(assigned_purchase)=lower(%s))
                  AND lower(coalesce(workflow_stage,'Sales Review')) IN ('awaiting purchase costing', 'awaiting sales ops pricing')
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
                if viewer_role == "presales":
                    cur.execute(opp_presales_scoped, (viewer_email,))
                elif viewer_role in ("salesops", "purchase"):
                    cur.execute(opp_salesops_scoped, (viewer_email, viewer_email))
                else:
                    cur.execute(opp_sales_scoped, (viewer_email, viewer_email))
                rows = cur.fetchall()
                if enforce_opportunity_sla(conn, rows):
                    cur.execute(opp_scoped, (viewer_email, viewer_email, viewer_email, viewer_email))
                    rows = cur.fetchall()
                payload["opportunities"] = rows
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
            if viewer_role in ("supervisor", "admin"):
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
                    SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                           assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                           presales_architecture, presales_questions, boq, purchase_costing,
                           costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                           purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                           assignment_due_at, sales_submitted_at, presales_escalated_at,
                           intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                           intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                           intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                           intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                           intake_competitors, intake_win_strategy, created_at, updated_at
                    FROM opportunities
                    ORDER BY updated_at DESC
                    """
            query_sales = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(owner)=lower(%s)
                   OR lower(sales_owner)=lower(%s)
                ORDER BY updated_at DESC
                """
            query_presales = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE lower(assigned_presales)=lower(%s)
                  AND lower(coalesce(workflow_stage,'Sales Review')) IN ('assigned to presales', 'costing returned', 'pricing returned to presales', 'presales overdue')
                ORDER BY updated_at DESC
                """
            query_salesops = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at
                FROM opportunities
                WHERE (lower(assigned_salesops)=lower(%s) OR lower(assigned_purchase)=lower(%s))
                  AND lower(coalesce(workflow_stage,'Sales Review')) IN ('awaiting purchase costing', 'awaiting sales ops pricing')
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

            if viewer_role == 'presales':
                cur.execute(query_presales, (viewer_email,))
            elif viewer_role in ('salesops', 'purchase'):
                cur.execute(query_salesops, (viewer_email, viewer_email))
            else:
                cur.execute(query_sales, (viewer_email, viewer_email))
            rows = cur.fetchall()
            if enforce_opportunity_sla(conn, rows):
                if viewer_role == 'presales':
                    cur.execute(query_presales, (viewer_email,))
                elif viewer_role in ('salesops', 'purchase'):
                    cur.execute(query_salesops, (viewer_email, viewer_email))
                else:
                    cur.execute(query_sales, (viewer_email, viewer_email))
                rows = cur.fetchall()
            return jsonify(rows)
    finally:
        conn.close()


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
        "owner": owner,
        "sales_owner": (data.get("sales_owner") or data.get("salesOwner") or owner).strip(),
        "workflow_stage": (data.get("workflow_stage") or data.get("workflowStage") or "").strip(),
        "assigned_presales": (data.get("assigned_presales") or data.get("assignedPresales") or "").strip(),
        "assigned_purchase": (data.get("assigned_purchase") or data.get("assignedPurchase") or "").strip(),
        "assigned_salesops": (data.get("assigned_salesops") or data.get("assignedSalesOps") or data.get("assignedPurchase") or data.get("assigned_purchase") or "").strip(),
        "sales_comments": (data.get("sales_comments") or data.get("salesComments") or "").strip(),
        "requirements": (data.get("requirements") or "").strip(),
        "presales_architecture": (data.get("presales_architecture") or data.get("presalesArchitecture") or "").strip(),
        "presales_questions": (data.get("presales_questions") or data.get("presalesQuestions") or "").strip(),
        "boq": (data.get("boq") or "").strip(),
        "purchase_costing": (data.get("purchase_costing") or data.get("purchaseCosting") or "").strip(),
        "costing_tat": (data.get("costing_tat") or data.get("costingTat") or "").strip(),
        "final_pricing_proposal": (data.get("final_pricing_proposal") or data.get("finalPricingProposal") or "").strip(),
        "presales_assigned_at": (data.get("presales_assigned_at") or data.get("presalesAssignedAt") or "").strip(),
        "presales_due_at": (data.get("presales_due_at") or data.get("presalesDueAt") or "").strip(),
        "purchase_assigned_at": (data.get("purchase_assigned_at") or data.get("purchaseAssignedAt") or "").strip(),
        "purchase_due_at": (data.get("purchase_due_at") or data.get("purchaseDueAt") or "").strip(),
        "costing_returned_at": (data.get("costing_returned_at") or data.get("costingReturnedAt") or "").strip(),
        "final_proposal_at": (data.get("final_proposal_at") or data.get("finalProposalAt") or "").strip(),
        "assignment_due_at": (data.get("assignment_due_at") or data.get("assignmentDueAt") or "").strip(),
        "sales_submitted_at": (data.get("sales_submitted_at") or data.get("salesSubmittedAt") or "").strip(),
        "presales_escalated_at": (data.get("presales_escalated_at") or data.get("presalesEscalatedAt") or "").strip(),
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

    required_intake_fields = [
        ("intake_problem_statement", "Problem Statement"),
        ("intake_why_now", "Why Now (Trigger Event)"),
        ("intake_business_impact", "Business Impact"),
        ("intake_current_state", "Current State Summary"),
        ("intake_budget_range", "Budget Range"),
        ("intake_decision_timeline", "Decision Timeline"),
    ]
    missing_intake = [label for key, label in required_intake_fields if not payload.get(key)]
    if missing_intake:
        return jsonify({"error": "Mandatory presales intake fields missing", "missing_fields": missing_intake}), 400

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, assigned_presales, workflow_stage, sales_owner, owner FROM opportunities WHERE id=%s", (opp_id,))
            exists = cur.fetchone()
            prev_assigned_presales = ((exists or {}).get("assigned_presales") or "").strip().lower()
            prev_workflow_stage = ((exists or {}).get("workflow_stage") or "").strip()
            prev_sales_owner = ((exists or {}).get("sales_owner") or (exists or {}).get("owner") or "").strip().lower()
            if exists:
                cur.execute(
                    """
                    UPDATE opportunities
                    SET name=%s, account_id=%s, value=%s, stage=%s, owner=%s, sales_owner=%s, workflow_stage=%s,
                        assigned_presales=%s, assigned_purchase=%s, assigned_salesops=%s, sales_comments=%s, requirements=%s,
                        presales_architecture=%s, presales_questions=%s, boq=%s, purchase_costing=%s,
                        costing_tat=%s, final_pricing_proposal=%s, presales_assigned_at=%s, presales_due_at=%s,
                        purchase_assigned_at=%s, purchase_due_at=%s, costing_returned_at=%s, final_proposal_at=%s,
                        assignment_due_at=%s, sales_submitted_at=%s, presales_escalated_at=%s,
                        intake_problem_statement=%s, intake_why_now=%s, intake_business_impact=%s, intake_current_state=%s,
                        intake_budget_range=%s, intake_decision_timeline=%s, intake_risk_if_not_solved=%s,
                        intake_key_stakeholders=%s, intake_in_scope=%s, intake_out_of_scope=%s, intake_current_environment=%s,
                        intake_pain_points=%s, intake_compliance_requirements=%s, intake_integration_requirements=%s,
                        intake_competitors=%s, intake_win_strategy=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    (
                        payload["name"], payload["account_id"], payload["value"], payload["stage"], payload["owner"],
                        payload["sales_owner"], payload["workflow_stage"], payload["assigned_presales"],
                        payload["assigned_purchase"], payload["assigned_salesops"], payload["sales_comments"], payload["requirements"],
                        payload["presales_architecture"], payload["presales_questions"], payload["boq"],
                        payload["purchase_costing"], payload["costing_tat"], payload["final_pricing_proposal"],
                        payload["presales_assigned_at"], payload["presales_due_at"], payload["purchase_assigned_at"],
                        payload["purchase_due_at"], payload["costing_returned_at"], payload["final_proposal_at"],
                        payload["assignment_due_at"], payload["sales_submitted_at"], payload["presales_escalated_at"],
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
                        id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                        assigned_presales, assigned_purchase, assigned_salesops, sales_comments, requirements,
                        presales_architecture, presales_questions, boq, purchase_costing,
                        costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                        purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                        assignment_due_at, sales_submitted_at, presales_escalated_at,
                        intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                        intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                        intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                        intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                        intake_competitors, intake_win_strategy, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
                    )
                    """,
                    (
                        opp_id, payload["name"], payload["account_id"], payload["value"], payload["stage"], payload["owner"],
                        payload["sales_owner"], payload["workflow_stage"], payload["assigned_presales"],
                        payload["assigned_purchase"], payload["assigned_salesops"], payload["sales_comments"], payload["requirements"],
                        payload["presales_architecture"], payload["presales_questions"], payload["boq"],
                        payload["purchase_costing"], payload["costing_tat"], payload["final_pricing_proposal"],
                        payload["presales_assigned_at"], payload["presales_due_at"], payload["purchase_assigned_at"],
                        payload["purchase_due_at"], payload["costing_returned_at"], payload["final_proposal_at"],
                        payload["assignment_due_at"], payload["sales_submitted_at"], payload["presales_escalated_at"],
                        payload["intake_problem_statement"], payload["intake_why_now"], payload["intake_business_impact"], payload["intake_current_state"],
                        payload["intake_budget_range"], payload["intake_decision_timeline"], payload["intake_risk_if_not_solved"], payload["intake_key_stakeholders"],
                        payload["intake_in_scope"], payload["intake_out_of_scope"], payload["intake_current_environment"], payload["intake_pain_points"],
                        payload["intake_compliance_requirements"], payload["intake_integration_requirements"], payload["intake_competitors"], payload["intake_win_strategy"],
                    ),
                )
                status = "created"
        conn.commit()

        current_assigned = (payload.get("assigned_presales") or "").strip().lower()
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
            cur.execute("SELECT id, owner, sales_owner FROM opportunities WHERE id=%s", (opp_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "opportunity not found"}), 404
            if not _is_supervisor(viewer_role):
                if not viewer_email:
                    return jsonify({"error": "viewer_email is required"}), 400
                allowed = {
                    (row.get("owner") or "").lower(),
                    (row.get("sales_owner") or "").lower(),
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
    if viewer_role not in ("supervisor", "admin") and viewer_email != (email or "").strip().lower():
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
            if _is_supervisor(viewer_role):
                cur.execute(
                    "SELECT account_id, fy_year, plan_data, owner, updated_at FROM aop_plans WHERE fy_year=%s",
                    (fy_year,),
                )
            else:
                if not viewer_email:
                    return jsonify([])
                cur.execute(
                    """
                    SELECT p.account_id, p.fy_year, p.plan_data, p.owner, p.updated_at
                    FROM aop_plans p
                    JOIN accounts a ON CAST(a.id AS TEXT) = p.account_id
                    JOIN users u ON u.id = a.account_manager_id
                    WHERE p.fy_year=%s AND lower(u.email)=lower(%s)
                    """,
                    (fy_year, viewer_email),
                )
            rows = cur.fetchall()
            out = []
            for r in rows:
                item = {
                    "account_id": r.get("account_id"),
                    "fy_year": r.get("fy_year"),
                    "owner": r.get("owner"),
                    "updated_at": str(r.get("updated_at") or ""),
                }
                pd = r.get("plan_data") or {}
                if isinstance(pd, str):
                    try:
                        pd = json.loads(pd)
                    except Exception:
                        pd = {}
                if isinstance(pd, dict):
                    item.update(pd)
                out.append(item)
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


def _po_stage_recipients(po: dict):
    stage = (po.get("stage") or "").strip()
    to_emails = []
    cc_emails = []

    def add_to(value):
        e = _normalize_email(value)
        if e and e not in to_emails:
            to_emails.append(e)

    def add_cc(value):
        e = _normalize_email(value)
        if e and e not in to_emails and e not in cc_emails:
            cc_emails.append(e)

    if stage == 'Pending Presales+Finance Approval':
        add_to(po.get('presales_approver') or 'vinod.v@dnispl.com')
        add_to(po.get('finance_approver') or 'rakesh.uniyal@dnispl.com')
    elif stage == 'Both Approved - Pending Implementation':
        add_to(po.get('implementation_approver') or 'pokhraj.yadav@dnispl.com')
    elif stage == 'Pending CEO Approval':
        add_to(po.get('ceo_approver') or SUPERVISOR_EMAIL)
    else:
        add_to(po.get('requestor_email') or po.get('owner'))

    add_cc(po.get('requestor_email') or po.get('created_by') or po.get('owner'))
    add_cc(po.get('sales_owner'))
    add_cc(po.get('account_manager_email'))
    add_cc(SUPERVISOR_EMAIL)
    return to_emails, cc_emails


def _po_notification_subject(po: dict, event_name: str) -> str:
    po_number = po.get('po_number') or po.get('poNumber') or 'PO'
    account_name = po.get('account_name') or po.get('accountName') or 'Account'
    return f"[CRM PO] {event_name}: {po_number} | {account_name}"


def _po_notification_body(po: dict, event_name: str) -> str:
    return (
        f"Event: {event_name}\n"
        f"PO Number: {po.get('po_number') or po.get('poNumber') or ''}\n"
        f"Account: {po.get('account_name') or po.get('accountName') or ''}\n"
        f"Opportunity ID: {po.get('opportunity_id') or po.get('opportunityId') or ''}\n"
        f"Stage: {po.get('stage') or ''}\n"
        f"Customer PO Value: {po.get('value') or 0}\n"
        f"Vendor Cost: {po.get('vendor_value') or po.get('vendorValue') or 0}\n"
        f"Requestor: {po.get('requestor_email') or po.get('created_by') or po.get('owner') or ''}\n"
        f"Sales Owner: {po.get('sales_owner') or ''}\n"
        f"Account Manager: {po.get('account_manager_email') or ''}\n"
        f"Presales Approver: {po.get('presales_approver') or 'vinod.v@dnispl.com'}\n"
        f"Finance Approver: {po.get('finance_approver') or 'rakesh.uniyal@dnispl.com'}\n"
        f"Implementation Approver: {po.get('implementation_approver') or 'pokhraj.yadav@dnispl.com'}\n"
        f"CEO Approver: {po.get('ceo_approver') or SUPERVISOR_EMAIL}\n\n"
        f"Notes: {po.get('notes') or ''}\n"
    )


@app.route('/api/purchase-orders', methods=['GET'])
def list_purchase_orders():
    viewer_email = _normalize_email(request.args.get('viewer_email') or '')
    viewer_role = (request.args.get('viewer_role') or 'account_manager').strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute("SELECT * FROM purchase_orders ORDER BY updated_at DESC, created_at DESC")
            else:
                if not viewer_email:
                    return jsonify([])
                cur.execute(
                    """
                    SELECT * FROM purchase_orders
                    WHERE lower(COALESCE(owner,''))=lower(%s)
                       OR lower(COALESCE(created_by,''))=lower(%s)
                       OR lower(COALESCE(requestor_email,''))=lower(%s)
                       OR lower(COALESCE(sales_owner,''))=lower(%s)
                       OR lower(COALESCE(account_manager_email,''))=lower(%s)
                       OR (stage='Pending Presales+Finance Approval' AND (lower(COALESCE(presales_approver,''))=lower(%s) OR lower(COALESCE(finance_approver,''))=lower(%s)))
                       OR (stage='Both Approved - Pending Implementation' AND lower(COALESCE(implementation_approver,''))=lower(%s))
                       OR (stage='Pending CEO Approval' AND lower(COALESCE(ceo_approver,''))=lower(%s))
                    ORDER BY updated_at DESC, created_at DESC
                    """,
                    (viewer_email, viewer_email, viewer_email, viewer_email, viewer_email, viewer_email, viewer_email, viewer_email, viewer_email),
                )
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route('/api/purchase-orders', methods=['POST'])
def upsert_purchase_order():
    data = request.get_json(silent=True) or {}
    po_id = str(data.get('id') or '').strip()
    if not po_id:
        return jsonify({'error': 'id required'}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO purchase_orders (
                    id, po_number, po_type, stage, opportunity_id, account_id, account_name,
                    vendor_name, vendor_po_number, oem, deal_registration_no, description,
                    value, vendor_value, payment_terms_customer, payment_terms_vendor,
                    approval_level, presales_approved_by, presales_approved_at,
                    finance_approved_by, finance_approved_at, implementation_approved_by,
                    implementation_approved_at, ceo_approved_by, ceo_approved_at,
                    expected_delivery, actual_delivery, grn_number, grn_date,
                    invoice_number, invoice_date, notes, site_address, is_site_work,
                    site_completion_date, scanned_po_data, scanned_po_image, owner,
                    created_by, requestor_email, sales_owner, account_manager_email,
                    presales_approver, finance_approver, implementation_approver, ceo_approver,
                    created_at, updated_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    COALESCE(%s::timestamptz, now()), now()
                )
                ON CONFLICT (id) DO UPDATE SET
                    po_number=EXCLUDED.po_number,
                    po_type=EXCLUDED.po_type,
                    stage=EXCLUDED.stage,
                    opportunity_id=EXCLUDED.opportunity_id,
                    account_id=EXCLUDED.account_id,
                    account_name=EXCLUDED.account_name,
                    vendor_name=EXCLUDED.vendor_name,
                    vendor_po_number=EXCLUDED.vendor_po_number,
                    oem=EXCLUDED.oem,
                    deal_registration_no=EXCLUDED.deal_registration_no,
                    description=EXCLUDED.description,
                    value=EXCLUDED.value,
                    vendor_value=EXCLUDED.vendor_value,
                    payment_terms_customer=EXCLUDED.payment_terms_customer,
                    payment_terms_vendor=EXCLUDED.payment_terms_vendor,
                    approval_level=EXCLUDED.approval_level,
                    presales_approved_by=EXCLUDED.presales_approved_by,
                    presales_approved_at=EXCLUDED.presales_approved_at,
                    finance_approved_by=EXCLUDED.finance_approved_by,
                    finance_approved_at=EXCLUDED.finance_approved_at,
                    implementation_approved_by=EXCLUDED.implementation_approved_by,
                    implementation_approved_at=EXCLUDED.implementation_approved_at,
                    ceo_approved_by=EXCLUDED.ceo_approved_by,
                    ceo_approved_at=EXCLUDED.ceo_approved_at,
                    expected_delivery=EXCLUDED.expected_delivery,
                    actual_delivery=EXCLUDED.actual_delivery,
                    grn_number=EXCLUDED.grn_number,
                    grn_date=EXCLUDED.grn_date,
                    invoice_number=EXCLUDED.invoice_number,
                    invoice_date=EXCLUDED.invoice_date,
                    notes=EXCLUDED.notes,
                    site_address=EXCLUDED.site_address,
                    is_site_work=EXCLUDED.is_site_work,
                    site_completion_date=EXCLUDED.site_completion_date,
                    scanned_po_data=EXCLUDED.scanned_po_data,
                    scanned_po_image=EXCLUDED.scanned_po_image,
                    owner=EXCLUDED.owner,
                    created_by=EXCLUDED.created_by,
                    requestor_email=EXCLUDED.requestor_email,
                    sales_owner=EXCLUDED.sales_owner,
                    account_manager_email=EXCLUDED.account_manager_email,
                    presales_approver=EXCLUDED.presales_approver,
                    finance_approver=EXCLUDED.finance_approver,
                    implementation_approver=EXCLUDED.implementation_approver,
                    ceo_approver=EXCLUDED.ceo_approver,
                    updated_at=now()
                """,
                (
                    po_id,
                    data.get('poNumber') or data.get('po_number') or '',
                    data.get('poType') or data.get('po_type') or '',
                    data.get('stage') or 'Draft',
                    data.get('opportunityId') or data.get('opportunity_id') or '',
                    data.get('accountId') or data.get('account_id') or '',
                    data.get('accountName') or data.get('account_name') or '',
                    data.get('vendorName') or data.get('vendor_name') or '',
                    data.get('vendorPONumber') or data.get('vendor_po_number') or '',
                    data.get('oem') or '',
                    data.get('dealRegistrationNo') or data.get('deal_registration_no') or '',
                    data.get('description') or '',
                    data.get('value') or 0,
                    data.get('vendorValue') or data.get('vendor_value') or 0,
                    data.get('paymentTermsCustomer') or data.get('payment_terms_customer') or '',
                    data.get('paymentTermsVendor') or data.get('payment_terms_vendor') or '',
                    data.get('approvalLevel') or data.get('approval_level') or '',
                    data.get('presalesApprovedBy') or data.get('presales_approved_by') or '',
                    data.get('presalesApprovedAt') or data.get('presales_approved_at') or '',
                    data.get('financeApprovedBy') or data.get('finance_approved_by') or '',
                    data.get('financeApprovedAt') or data.get('finance_approved_at') or '',
                    data.get('implementationApprovedBy') or data.get('implementation_approved_by') or '',
                    data.get('implementationApprovedAt') or data.get('implementation_approved_at') or '',
                    data.get('ceoApprovedBy') or data.get('ceo_approved_by') or '',
                    data.get('ceoApprovedAt') or data.get('ceo_approved_at') or '',
                    data.get('expectedDelivery') or data.get('expected_delivery') or '',
                    data.get('actualDelivery') or data.get('actual_delivery') or '',
                    data.get('grnNumber') or data.get('grn_number') or '',
                    data.get('grnDate') or data.get('grn_date') or '',
                    data.get('invoiceNumber') or data.get('invoice_number') or '',
                    data.get('invoiceDate') or data.get('invoice_date') or '',
                    data.get('notes') or '',
                    data.get('siteAddress') or data.get('site_address') or '',
                    bool(data.get('isSiteWork') if data.get('isSiteWork') is not None else data.get('is_site_work')),
                    data.get('siteCompletionDate') or data.get('site_completion_date') or '',
                    data.get('scannedPOData') or data.get('scanned_po_data') or '',
                    data.get('scannedPOImage') or data.get('scanned_po_image') or '',
                    _normalize_email(data.get('owner') or ''),
                    _normalize_email(data.get('createdBy') or data.get('created_by') or ''),
                    _normalize_email(data.get('requestorEmail') or data.get('requestor_email') or ''),
                    _normalize_email(data.get('salesOwner') or data.get('sales_owner') or ''),
                    _normalize_email(data.get('accountManagerEmail') or data.get('account_manager_email') or ''),
                    _normalize_email(data.get('presalesApprover') or data.get('presales_approver') or 'vinod.v@dnispl.com'),
                    _normalize_email(data.get('financeApprover') or data.get('finance_approver') or 'rakesh.uniyal@dnispl.com'),
                    _normalize_email(data.get('implementationApprover') or data.get('implementation_approver') or 'pokhraj.yadav@dnispl.com'),
                    _normalize_email(data.get('ceoApprover') or data.get('ceo_approver') or SUPERVISOR_EMAIL),
                    data.get('createdDate') or data.get('created_at') or None,
                ),
            )
        conn.commit()
        return jsonify({'status': 'ok', 'id': po_id})
    finally:
        conn.close()


@app.route('/api/purchase-orders/<po_id>', methods=['DELETE'])
def delete_purchase_order(po_id: str):
    viewer_email = _normalize_email(request.args.get('viewer_email') or '')
    viewer_role = (request.args.get('viewer_role') or 'account_manager').strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, owner, created_by, requestor_email FROM purchase_orders WHERE id=%s", (po_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'error': 'purchase order not found'}), 404
            allowed = _is_supervisor(viewer_role) or viewer_email in {
                _normalize_email(row.get('owner') or ''),
                _normalize_email(row.get('created_by') or ''),
                _normalize_email(row.get('requestor_email') or ''),
            }
            if not allowed:
                return jsonify({'error': 'not allowed'}), 403
            cur.execute("DELETE FROM purchase_orders WHERE id=%s", (po_id,))
        conn.commit()
        return jsonify({'status': 'deleted', 'id': po_id})
    finally:
        conn.close()


@app.route('/api/purchase-orders/notify', methods=['POST'])
def notify_purchase_order():
    data = request.get_json(silent=True) or {}
    po = data.get('purchase_order') or data.get('po') or {}
    event_name = (data.get('event') or 'PO Update').strip() or 'PO Update'
    to_emails, cc_emails = _po_stage_recipients(po)
    if not to_emails and not cc_emails:
        return jsonify({'status': 'skipped', 'reason': 'no recipients'})
    subject = _po_notification_subject(po, event_name)
    body = _po_notification_body(po, event_name)
    sent = send_email_smtp(to_emails, subject, body, cc_emails=cc_emails)
    return jsonify({'status': 'sent' if sent else 'not_sent', 'to': to_emails, 'cc': cc_emails, 'subject': subject})


@app.route('/api/ai/extract-po', methods=['POST'])
def extract_po_with_ai():
    data = request.get_json(silent=True) or {}
    image_b64 = str(data.get('image_base64') or '').strip()
    mime_type = str(data.get('mime_type') or 'image/jpeg').strip() or 'image/jpeg'
    api_key = (os.environ.get('ANTHROPIC_API_KEY') or '').strip()
    if not image_b64:
        return jsonify({'error': 'image_base64 required'}), 400
    if not api_key:
        return jsonify({'error': 'ANTHROPIC_API_KEY not configured'}), 503
    payload = {
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 2000,
        'messages': [{
            'role': 'user',
            'content': [
                {
                    'type': 'image',
                    'source': {'type': 'base64', 'media_type': mime_type, 'data': image_b64}
                },
                {
                    'type': 'text',
                    'text': 'Extract from this Purchase Order document: PO Number, PO Date, Customer Name, Total Value (in INR), Payment Terms, Delivery Address, Scope of Work, OEM/Brand, Vendor/Distributor, line items, and any risk observations. Return a clean structured summary with clear section headers.'
                }
            ]
        }]
    }
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=json.dumps(payload).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode('utf-8')
        parsed = json.loads(raw)
        text = (((parsed.get('content') or [{}])[0]).get('text') or '').strip()
        text = _coerce_ai_json_text(text)
        return jsonify({'text': text, 'raw': parsed})
    except Exception as exc:
        return jsonify({'error': f'po extraction failed: {exc}'}), 500


def _coerce_ai_json_text(text: str) -> str:
    source = (text or '').strip()
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
    start = source.find('{')
    end = source.rfind('}')
    if start != -1 and end != -1 and end > start:
        candidate = source[start:end + 1].strip()
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass
    return source


def _call_standalone_aop(fn_name: str):
    module = _load_aop_module()
    if not module:
        return jsonify({'error': 'standalone aop module not found'}), 500
    return getattr(module, fn_name)()


@app.route('/api/aop/import-sales-xlsx', methods=['POST'])
def import_sales_aop_xlsx_route():
    return _call_standalone_aop('import_sales_aop_xlsx')


@app.route('/api/kra/users', methods=['GET'])
def kra_users_route():
    return _call_standalone_aop('kra_users')


@app.route('/api/aop/import-audit', methods=['GET'])
def aop_import_audit_route():
    return _call_standalone_aop('aop_import_audit')


@app.route('/api/kra/config', methods=['GET'])
def get_kra_config_route():
    return _call_standalone_aop('get_kra_config')


@app.route('/api/kra/scorecard', methods=['GET'])
def kra_scorecard_route():
    return _call_standalone_aop('kra_scorecard')


@app.route('/api/kra/leaderboard', methods=['GET'])
def kra_leaderboard_route():
    return _call_standalone_aop('kra_leaderboard')


@app.route('/api/kra/report.csv', methods=['GET'])
def kra_report_csv_route():
    return _call_standalone_aop('kra_report_csv')


@app.route('/api/presales/learning', methods=['POST'])
def add_presales_learning_route():
    return _call_standalone_aop('add_presales_learning')


@app.route('/api/presales/feedback', methods=['POST'])
def add_presales_feedback_route():
    return _call_standalone_aop('add_presales_feedback')


@app.route('/api/presales/innovation', methods=['POST'])
def add_presales_innovation_route():
    return _call_standalone_aop('add_presales_innovation')


@app.route('/api/kra/manual-metric', methods=['POST'])
def upsert_manual_metric_route():
    return _call_standalone_aop('upsert_manual_metric')


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"Simple CRM backend running on port {port}")
    print("DB host:", urlparse(DATABASE_URL).hostname)
    app.run(host="0.0.0.0", port=port, debug=True)
