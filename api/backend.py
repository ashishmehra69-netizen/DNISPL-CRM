import csv
import json
import base64
import hashlib
import hmac
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

# ── Month mapping for flat-column aop_plans table ──────────────────────────
# Maps frontend month abbreviation → column prefix used in DB
AOP_MONTH_COL = {
    "Apr": "apr", "May": "may", "Jun": "jun",
    "Jul": "jul", "Aug": "aug", "Sep": "sep",
    "Oct": "oct", "Nov": "nov", "Dec": "dec",
    "Jan": "jan", "Feb": "feb", "Mar": "mar",
}
AOP_CATS = ["Hardware", "Software", "Managed_Services"]
AOP_CAT_COL = {
    "Hardware": "hardware",
    "Software": "software",
    "Managed_Services": "managed_services",
}
# Full list of numeric columns in aop_plans (flat schema)
AOP_FLAT_COLS = []
for _m in AOP_MONTH_COL.values():
    for _c in AOP_CAT_COL.values():
        AOP_FLAT_COLS.append(f"{_m}_{_c}")

# Months in FY order (Apr-Mar)
AOP_FY_MONTHS = ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar"]


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
            # users
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email TEXT UNIQUE,
                    name TEXT,
                    role TEXT DEFAULT 'account_manager',
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # accounts
            cur.execute("""
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
                    suspect_q1 TEXT, suspect_q2 TEXT, suspect_q3 TEXT,
                    suspect_q4 TEXT, suspect_q5 TEXT, suspect_q6 TEXT,
                    suspect_q7 TEXT, suspect_q8 TEXT, suspect_q9 TEXT,
                    suspect_q10 TEXT,
                    suspect_score INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            for col in ["industry","tier","location","company_size","annual_spend","mode",
                        "suspect_q1","suspect_q2","suspect_q3","suspect_q4","suspect_q5",
                        "suspect_q6","suspect_q7","suspect_q8","suspect_q9","suspect_q10",
                        "suspect_score","account_manager_id"]:
                try:
                    if col == "suspect_score":
                        cur.execute(f"ALTER TABLE accounts ADD COLUMN IF NOT EXISTS {col} INTEGER DEFAULT 0;")
                    elif col == "account_manager_id":
                        cur.execute(f"ALTER TABLE accounts ADD COLUMN IF NOT EXISTS {col} INTEGER REFERENCES users(id);")
                    else:
                        cur.execute(f"ALTER TABLE accounts ADD COLUMN IF NOT EXISTS {col} TEXT;")
                except Exception:
                    pass
            # activities
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activities (
                    id TEXT PRIMARY KEY,
                    type TEXT, subject TEXT, notes TEXT, date TEXT,
                    owner TEXT, account_id TEXT, account_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            for col in ["mom_sent_at","mom_sent_to","mom_send_status","mom_send_error","mom_payload"]:
                try:
                    if col == "mom_sent_at":
                        cur.execute(f"ALTER TABLE activities ADD COLUMN IF NOT EXISTS {col} TIMESTAMPTZ;")
                    else:
                        cur.execute(f"ALTER TABLE activities ADD COLUMN IF NOT EXISTS {col} TEXT;")
                except Exception:
                    pass
            # leads
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id TEXT PRIMARY KEY,
                    name TEXT, company TEXT, email TEXT, phone TEXT,
                    source TEXT, status TEXT, notes TEXT, owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # contacts
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id TEXT PRIMARY KEY,
                    name TEXT, title TEXT, email TEXT, phone TEXT,
                    role_type TEXT, influence_level TEXT, emotion TEXT,
                    account_id TEXT, owner TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # opportunities
            cur.execute("""
                CREATE TABLE IF NOT EXISTS opportunities (
                    id TEXT PRIMARY KEY,
                    name TEXT, account_id TEXT, value NUMERIC DEFAULT 0,
                    stage TEXT, owner TEXT, sales_owner TEXT, workflow_stage TEXT,
                    assigned_presales TEXT, assigned_purchase TEXT,
                    sales_comments TEXT, requirements TEXT,
                    presales_architecture TEXT, presales_questions TEXT,
                    boq TEXT, purchase_costing TEXT, costing_tat TEXT,
                    final_pricing_proposal TEXT,
                    presales_assigned_at TEXT, presales_due_at TEXT,
                    purchase_assigned_at TEXT, purchase_due_at TEXT,
                    costing_returned_at TEXT, final_proposal_at TEXT,
                    assignment_due_at TEXT, sales_submitted_at TEXT,
                    presales_escalated_at TEXT,
                    intake_problem_statement TEXT, intake_why_now TEXT,
                    intake_business_impact TEXT, intake_current_state TEXT,
                    intake_budget_range TEXT, intake_decision_timeline TEXT,
                    intake_risk_if_not_solved TEXT, intake_key_stakeholders TEXT,
                    intake_in_scope TEXT, intake_out_of_scope TEXT,
                    intake_current_environment TEXT, intake_pain_points TEXT,
                    intake_compliance_requirements TEXT, intake_integration_requirements TEXT,
                    intake_competitors TEXT, intake_win_strategy TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # aop_plans — flat column schema matching existing Supabase table
            # We do NOT try to add plan_data JSONB; we use the flat columns that already exist.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aop_plans (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    account_name TEXT,
                    account_manager TEXT,
                    fy_year TEXT NOT NULL,
                    current_revenue NUMERIC DEFAULT 0,
                    target_growth NUMERIC DEFAULT 0,
                    key_solutions TEXT,
                    oem TEXT,
                    updated_by TEXT,
                    apr_hardware NUMERIC DEFAULT 0, apr_software NUMERIC DEFAULT 0, apr_managed_services NUMERIC DEFAULT 0,
                    may_hardware NUMERIC DEFAULT 0, may_software NUMERIC DEFAULT 0, may_managed_services NUMERIC DEFAULT 0,
                    jun_hardware NUMERIC DEFAULT 0, jun_software NUMERIC DEFAULT 0, jun_managed_services NUMERIC DEFAULT 0,
                    jul_hardware NUMERIC DEFAULT 0, jul_software NUMERIC DEFAULT 0, jul_managed_services NUMERIC DEFAULT 0,
                    aug_hardware NUMERIC DEFAULT 0, aug_software NUMERIC DEFAULT 0, aug_managed_services NUMERIC DEFAULT 0,
                    sep_hardware NUMERIC DEFAULT 0, sep_software NUMERIC DEFAULT 0, sep_managed_services NUMERIC DEFAULT 0,
                    oct_hardware NUMERIC DEFAULT 0, oct_software NUMERIC DEFAULT 0, oct_managed_services NUMERIC DEFAULT 0,
                    nov_hardware NUMERIC DEFAULT 0, nov_software NUMERIC DEFAULT 0, nov_managed_services NUMERIC DEFAULT 0,
                    dec_hardware NUMERIC DEFAULT 0, dec_software NUMERIC DEFAULT 0, dec_managed_services NUMERIC DEFAULT 0,
                    jan_hardware NUMERIC DEFAULT 0, jan_software NUMERIC DEFAULT 0, jan_managed_services NUMERIC DEFAULT 0,
                    feb_hardware NUMERIC DEFAULT 0, feb_software NUMERIC DEFAULT 0, feb_managed_services NUMERIC DEFAULT 0,
                    mar_hardware NUMERIC DEFAULT 0, mar_software NUMERIC DEFAULT 0, mar_managed_services NUMERIC DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(account_id, fy_year)
                );
            """)
            # aop_actuals — flat schema
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aop_actuals (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    account_name TEXT,
                    account_manager TEXT,
                    fy_year TEXT NOT NULL,
                    month TEXT NOT NULL,
                    hardware NUMERIC DEFAULT 0,
                    software NUMERIC DEFAULT 0,
                    managed_services NUMERIC DEFAULT 0,
                    notes TEXT,
                    updated_by TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(account_id, fy_year, month)
                );
            """)
            # passwords & salary
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_passwords (
                    email TEXT PRIMARY KEY,
                    password TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_salary (
                    email TEXT PRIMARY KEY,
                    salary_data JSONB DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # Indexes
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_accounts_manager_id ON accounts(account_manager_id);",
                "CREATE INDEX IF NOT EXISTS idx_activities_owner ON activities(lower(owner));",
                "CREATE INDEX IF NOT EXISTS idx_leads_owner ON leads(lower(owner));",
                "CREATE INDEX IF NOT EXISTS idx_contacts_owner ON contacts(lower(owner));",
                "CREATE INDEX IF NOT EXISTS idx_opps_owner ON opportunities(lower(owner));",
                "CREATE INDEX IF NOT EXISTS idx_opps_sales_owner ON opportunities(lower(sales_owner));",
                "CREATE INDEX IF NOT EXISTS idx_opps_presales ON opportunities(lower(assigned_presales));",
                "CREATE INDEX IF NOT EXISTS idx_opps_purchase ON opportunities(lower(assigned_purchase));",
                "CREATE INDEX IF NOT EXISTS idx_aop_plans_fy ON aop_plans(fy_year);",
                "CREATE INDEX IF NOT EXISTS idx_aop_actuals_fy ON aop_actuals(fy_year);",
            ]:
                try:
                    cur.execute(idx_sql)
                except Exception:
                    pass
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
    return (request.remote_addr or "").strip()


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
    return sum(1 for i in range(1, 11) if str(data.get(f"suspect_q{i}") or "").strip())


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


# ── AOP flat-schema helpers ────────────────────────────────────────────────

def _aop_month_col_prefix(month_abbr: str) -> str:
    """Convert 'Apr'/'Apr' frontend abbreviation to DB column prefix 'apr'."""
    return AOP_MONTH_COL.get(month_abbr, month_abbr.lower()[:3])


def _aop_row_to_frontend(row: dict) -> dict:
    """Convert a flat aop_plans DB row to the dict format the frontend expects."""
    out = {
        "account_id": str(row.get("account_id") or ""),
        "account_name": row.get("account_name") or "",
        "account_manager": row.get("account_manager") or "",
        "fy_year": row.get("fy_year") or "",
        "current_revenue": float(row.get("current_revenue") or 0),
        "target_growth": float(row.get("target_growth") or 0),
        "key_solutions": row.get("key_solutions") or "",
        "oem": row.get("oem") or "",
        "updated_by": row.get("updated_by") or "",
        "updated_at": str(row.get("updated_at") or ""),
    }
    # Expand month/category columns → frontend key format e.g. "Apr_Hardware"
    for month in AOP_FY_MONTHS:
        mp = _aop_month_col_prefix(month)
        for cat in AOP_CATS:
            cp = AOP_CAT_COL[cat]
            db_col = f"{mp}_{cp}"
            fe_key = f"{month}_{cat}"
            out[fe_key] = float(row.get(db_col) or 0)
    return out


def _frontend_to_aop_flat(data: dict) -> dict:
    """Extract flat column values from frontend payload."""
    flat = {}
    for month in AOP_FY_MONTHS:
        mp = _aop_month_col_prefix(month)
        for cat in AOP_CATS:
            cp = AOP_CAT_COL[cat]
            fe_key = f"{month}_{cat}"
            db_col = f"{mp}_{cp}"
            flat[db_col] = float(data.get(fe_key) or 0)
    return flat


# ── OAuth helpers ──────────────────────────────────────────────────────────

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
    if data is not None:
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


def _get_o365_token_row(email: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    "SELECT user_id, email, tenant_id, refresh_token, access_token, expires_at, status FROM user_o365_tokens WHERE lower(email)=lower(%s)",
                    (_normalize_email(email),),
                )
                return cur.fetchone()
            except Exception:
                return None
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
                DO UPDATE SET email=EXCLUDED.email, tenant_id=EXCLUDED.tenant_id,
                    refresh_token=EXCLUDED.refresh_token, access_token=EXCLUDED.access_token,
                    expires_at=EXCLUDED.expires_at, connected_at=now(), status='active'
                """,
                (int(user_id), _normalize_email(email), MS_TENANT_ID,
                 token_data.get("refresh_token") or "", token_data.get("access_token") or "",
                 str(expires_in)),
            )
        conn.commit()
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
    refreshed = _http_form_post(token_url, {
        "client_id": MS_CLIENT_ID, "client_secret": MS_CLIENT_SECRET,
        "grant_type": "refresh_token", "refresh_token": refresh_token,
        "redirect_uri": MS_REDIRECT_URI, "scope": MS_OAUTH_SCOPES,
    })
    _upsert_o365_tokens(int(token_row["user_id"]), token_row.get("email") or "", refreshed)
    return refreshed.get("access_token") or ""


def _send_graph_mail(sender_email: str, to_emails, cc_emails, subject: str, html_body: str):
    token_row = _get_o365_token_row(sender_email)
    access_token = _refresh_graph_token_if_needed(token_row)
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": e}} for e in to_emails],
            "ccRecipients": [{"emailAddress": {"address": e}} for e in cc_emails],
        },
        "saveToSentItems": True,
    }
    return _http_json_request(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        method="POST", data=payload,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
    )


def _format_bullets(text: str):
    lines = [ln.strip(" -\t") for ln in str(text or "").splitlines() if ln.strip()]
    return "".join([f"<li>{ln}</li>" for ln in lines]) if lines else "<li>NA</li>"


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
    <p>Regards,<br>{am_name}<br>{am_email}<br>DNISPL</p>
    """


def send_email_smtp(to_emails, subject: str, body: str, cc_emails=None) -> bool:
    to_list = [e.strip().lower() for e in (to_emails or []) if (e or "").strip() and "@" in e]
    cc_list = [e.strip().lower() for e in (cc_emails or []) if (e or "").strip() and "@" in e]
    if not to_list and not cc_list:
        return False
    if not (SMTP_HOST and SMTP_USER and SMTP_PASSWORD):
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
        return True
    except Exception as exc:
        print(f"[SMTP] Email send failed: {exc}")
        return False


def send_presales_assignment_email(opportunity_name, opp_id, presales_email, presales_due_iso):
    target = (presales_email or "").strip().lower()
    if "@" not in target:
        return
    subject = f"[CRM] New Opportunity Assigned: {opportunity_name or opp_id}"
    body = (f"Opportunity: {opportunity_name or ''}\nID: {opp_id}\n"
            f"Assigned To: {target}\nDue: {presales_due_iso}\n\n"
            "Please review and submit solution/proposal within SLA.")
    send_email_smtp([target], subject, body)


def send_opportunity_assignment_email(opportunity_name, opp_id, presales_email, sales_email, presales_due_iso, account_manager_email=""):
    presales_target = (presales_email or "").strip().lower()
    if "@" not in presales_target:
        return
    cc_list = []
    for e in [SUPERVISOR_EMAIL, sales_email, account_manager_email]:
        e = (e or "").strip().lower()
        if e and "@" in e and e != presales_target and e not in cc_list:
            cc_list.append(e)
    subject = f"[CRM] Opportunity Assigned to Presales: {opportunity_name or opp_id}"
    body = (f"Opportunity: {opportunity_name or ''}\nID: {opp_id}\n"
            f"Sales Owner: {sales_email or 'NA'}\nAssigned Presales: {presales_target}\n"
            f"Due: {presales_due_iso}\n\nPlease review requirements within SLA.")
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
                updates["presales_assigned_at"] = assignment_due.isoformat().replace("+00:00", "Z")
                send_presales_assignment_email(row.get("name") or "", row.get("id") or "",
                    updates["assigned_presales"], presales_due.isoformat().replace("+00:00", "Z"))
            has_proposal = bool((row.get("final_pricing_proposal") or "").strip())
            if has_proposal and workflow_stage != "Final Proposal Shared":
                updates["workflow_stage"] = "Final Proposal Shared"
                updates["final_proposal_at"] = (parse_iso_dt(row.get("final_proposal_at")) or now).isoformat().replace("+00:00", "Z")
            elif (not has_proposal
                  and workflow_stage in ("Assigned to Presales", "Awaiting Purchase Costing", "Costing Returned")
                  and now > presales_due
                  and not (row.get("presales_escalated_at") or "").strip()):
                updates["workflow_stage"] = "Presales Overdue"
                updates["presales_escalated_at"] = now.isoformat().replace("+00:00", "Z")
            if updates:
                sets = ", ".join([f"{k}=%s" for k in updates.keys()] + ["updated_at=now()"])
                cur.execute(f"UPDATE opportunities SET {sets} WHERE id=%s", list(updates.values()) + [row["id"]])
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
                cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (value,))
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
            cur.execute("SELECT id FROM users WHERE lower(name)=lower(%s)", (value,))
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
    suspect_answers = {f"suspect_q{i}": (data.get(f"suspect_q{i}") or "").strip() for i in range(1, 11)}
    suspect_score = compute_suspect_score(suspect_answers)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if account_id.isdigit():
                cur.execute("SELECT id FROM accounts WHERE id=%s", (int(account_id),))
            else:
                cur.execute("SELECT id FROM accounts WHERE lower(account_name)=lower(%s)", (name,))
            row = cur.fetchone()
            if row:
                cur.execute("""
                    UPDATE accounts SET account_manager_id=%s, industry=%s, tier=%s, location=%s,
                    company_size=%s, annual_spend=%s, mode=%s,
                    suspect_q1=%s, suspect_q2=%s, suspect_q3=%s, suspect_q4=%s, suspect_q5=%s,
                    suspect_q6=%s, suspect_q7=%s, suspect_q8=%s, suspect_q9=%s, suspect_q10=%s,
                    suspect_score=%s, updated_at=now() WHERE id=%s
                """, (manager_id,
                    (data.get("industry") or "").strip(), (data.get("tier") or "").strip(),
                    (data.get("location") or "").strip(),
                    (data.get("company_size") or data.get("companySize") or "").strip(),
                    (data.get("annual_spend") or data.get("annualSpend") or "").strip(),
                    (data.get("mode") or "").strip(),
                    suspect_answers["suspect_q1"], suspect_answers["suspect_q2"],
                    suspect_answers["suspect_q3"], suspect_answers["suspect_q4"],
                    suspect_answers["suspect_q5"], suspect_answers["suspect_q6"],
                    suspect_answers["suspect_q7"], suspect_answers["suspect_q8"],
                    suspect_answers["suspect_q9"], suspect_answers["suspect_q10"],
                    suspect_score, int(row["id"])))
                conn.commit()
                return "updated"
            cur.execute("""
                INSERT INTO accounts (account_name, account_manager_id, industry, tier, location,
                company_size, annual_spend, mode, suspect_q1, suspect_q2, suspect_q3, suspect_q4,
                suspect_q5, suspect_q6, suspect_q7, suspect_q8, suspect_q9, suspect_q10,
                suspect_score, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
            """, (name, manager_id,
                (data.get("industry") or "").strip(), (data.get("tier") or "").strip(),
                (data.get("location") or "").strip(),
                (data.get("company_size") or data.get("companySize") or "").strip(),
                (data.get("annual_spend") or data.get("annualSpend") or "").strip(),
                (data.get("mode") or "").strip(),
                suspect_answers["suspect_q1"], suspect_answers["suspect_q2"],
                suspect_answers["suspect_q3"], suspect_answers["suspect_q4"],
                suspect_answers["suspect_q5"], suspect_answers["suspect_q6"],
                suspect_answers["suspect_q7"], suspect_answers["suspect_q8"],
                suspect_answers["suspect_q9"], suspect_answers["suspect_q10"],
                suspect_score))
            conn.commit()
            return "created"
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# ROUTES — Health, Users, Accounts, Activities, Leads, Contacts,
#          Opportunities (unchanged from original)
# ═══════════════════════════════════════════════════════════════

@app.route("/api/health", methods=["GET"])
def health():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM users")
            users = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM accounts")
            accounts = cur.fetchone()["c"]
        return jsonify({"status": "ok", "users": users, "accounts": accounts})
    finally:
        conn.close()


@app.route("/api/users", methods=["GET"])
def list_users():
    conn = get_conn()
    try:
        role = (request.args.get("role") or "").strip().lower()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if role:
                cur.execute("SELECT id, email, name, role FROM users WHERE lower(role)=lower(%s) ORDER BY name", (role,))
            else:
                cur.execute("SELECT id, email, name, role FROM users ORDER BY name")
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/accounts", methods=["GET"])
def list_accounts():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base_q = """
                SELECT a.id, a.account_name, a.created_at, a.updated_at,
                       a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                       a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                       a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10,
                       a.suspect_score,
                       u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                FROM accounts a LEFT JOIN users u ON u.id = a.account_manager_id
            """
            if _is_supervisor(viewer_role):
                cur.execute(base_q + " ORDER BY a.account_name")
                return jsonify(cur.fetchall())
            if not viewer_email:
                return jsonify({"error": "viewer_email required"}), 400
            cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (viewer_email,))
            mgr = cur.fetchone()
            if not mgr:
                return jsonify([])
            cur.execute(base_q + " WHERE a.account_manager_id=%s ORDER BY a.account_name", (int(mgr["id"]),))
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/accounts", methods=["POST"])
def create_or_update_account():
    allowed, ip = _rate_limit_write("accounts_post", per_60s=35)
    if not allowed:
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
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
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
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (viewer_email,))
                mgr = cur.fetchone()
                if not mgr or int(mgr["id"]) != int(row.get("account_manager_id") or -1):
                    return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM accounts WHERE id=%s", (int(account_id),))
        conn.commit()
        return jsonify({"status": "deleted", "id": int(account_id)})
    finally:
        conn.close()


@app.route("/api/accounts/import", methods=["POST"])
def import_accounts():
    if "file" not in request.files:
        return jsonify({"error": "Missing file"}), 400
    file_obj = request.files["file"]
    try:
        content = file_obj.read().decode("utf-8", errors="replace")
        reader = csv.DictReader(StringIO(content))
    except Exception as exc:
        return jsonify({"error": f"Could not read CSV: {exc}"}), 400
    created = updated = 0
    failed = []
    for idx, row in enumerate(reader, start=2):
        name = (row.get("account_name") or "").strip()
        manager = (row.get("account_manager") or "").strip()
        if not name or not manager:
            failed.append({"row": idx, "error": "Missing account_name/account_manager"})
            continue
        try:
            manager_id = ensure_user(manager)
            result = upsert_account({"account_name": name, "account_manager": manager}, manager_id)
            if result == "created":
                created += 1
            else:
                updated += 1
        except Exception as exc:
            failed.append({"row": idx, "error": str(exc), "account_name": name})
    return jsonify({"created": created, "updated": updated, "failed_count": len(failed), "failed_rows": failed[:50]})


@app.route("/api/bootstrap", methods=["GET"])
def bootstrap_data():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    payload = {"accounts": [], "activities": [], "leads": [], "contacts": [], "opportunities": []}
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            acct_q = """SELECT a.id, a.account_name, a.created_at, a.updated_at,
                       a.industry, a.tier, a.location, a.company_size, a.annual_spend, a.mode,
                       a.suspect_q1, a.suspect_q2, a.suspect_q3, a.suspect_q4, a.suspect_q5,
                       a.suspect_q6, a.suspect_q7, a.suspect_q8, a.suspect_q9, a.suspect_q10, a.suspect_score,
                       u.id AS account_manager_id, u.name AS account_manager, u.email AS account_manager_email
                FROM accounts a LEFT JOIN users u ON u.id = a.account_manager_id"""
            opp_cols = """id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at"""
            if _is_supervisor(viewer_role):
                cur.execute(acct_q + " ORDER BY a.account_name")
                payload["accounts"] = cur.fetchall()
                cur.execute("SELECT id,type,subject,notes,date,owner,account_id,account_name,created_at,updated_at FROM activities ORDER BY date DESC")
                payload["activities"] = cur.fetchall()
                cur.execute("SELECT id,name,company,email,phone,source,status,notes,owner,created_at,updated_at FROM leads ORDER BY updated_at DESC")
                payload["leads"] = cur.fetchall()
                cur.execute("SELECT id,name,title,email,phone,role_type,influence_level,emotion,account_id,owner,created_at,updated_at FROM contacts ORDER BY updated_at DESC")
                payload["contacts"] = cur.fetchall()
                cur.execute(f"SELECT {opp_cols} FROM opportunities ORDER BY updated_at DESC")
                rows = cur.fetchall()
                enforce_opportunity_sla(conn, rows)
                cur.execute(f"SELECT {opp_cols} FROM opportunities ORDER BY updated_at DESC")
                payload["opportunities"] = cur.fetchall()
            elif viewer_email:
                cur.execute("SELECT id FROM users WHERE lower(email)=lower(%s)", (viewer_email,))
                mgr = cur.fetchone()
                if mgr:
                    cur.execute(acct_q + " WHERE a.account_manager_id=%s ORDER BY a.account_name", (int(mgr["id"]),))
                    payload["accounts"] = cur.fetchall()
                cur.execute("SELECT id,type,subject,notes,date,owner,account_id,account_name,created_at,updated_at FROM activities WHERE lower(owner)=lower(%s) ORDER BY date DESC", (viewer_email,))
                payload["activities"] = cur.fetchall()
                cur.execute("SELECT id,name,company,email,phone,source,status,notes,owner,created_at,updated_at FROM leads WHERE lower(owner)=lower(%s) ORDER BY updated_at DESC", (viewer_email,))
                payload["leads"] = cur.fetchall()
                cur.execute("SELECT id,name,title,email,phone,role_type,influence_level,emotion,account_id,owner,created_at,updated_at FROM contacts WHERE lower(owner)=lower(%s) ORDER BY updated_at DESC", (viewer_email,))
                payload["contacts"] = cur.fetchall()
                cur.execute(f"SELECT {opp_cols} FROM opportunities WHERE lower(owner)=lower(%s) OR lower(sales_owner)=lower(%s) OR lower(assigned_presales)=lower(%s) OR lower(assigned_purchase)=lower(%s) ORDER BY updated_at DESC",
                    (viewer_email, viewer_email, viewer_email, viewer_email))
                rows = cur.fetchall()
                enforce_opportunity_sla(conn, rows)
                cur.execute(f"SELECT {opp_cols} FROM opportunities WHERE lower(owner)=lower(%s) OR lower(sales_owner)=lower(%s) OR lower(assigned_presales)=lower(%s) OR lower(assigned_purchase)=lower(%s) ORDER BY updated_at DESC",
                    (viewer_email, viewer_email, viewer_email, viewer_email))
                payload["opportunities"] = cur.fetchall()
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"error": f"bootstrap failed: {exc}", **payload}), 200
    finally:
        conn.close()


@app.route("/api/activities", methods=["GET"])
def list_activities():
    viewer_email = (request.args.get("viewer_email") or "").strip()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute("SELECT id,type,subject,notes,date,owner,account_id,account_name,created_at,updated_at FROM activities ORDER BY date DESC")
            else:
                if not viewer_email:
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute("SELECT id,type,subject,notes,date,owner,account_id,account_name,created_at,updated_at FROM activities WHERE lower(owner)=lower(%s) ORDER BY date DESC", (viewer_email,))
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/activities", methods=["POST"])
def upsert_activity():
    data = request.get_json(silent=True) or {}
    activity_id = (data.get("id") or "").strip() or f"act_{int(datetime.utcnow().timestamp() * 1000)}"
    activity_type = (data.get("type") or "").strip()
    subject = (data.get("subject") or "").strip()
    date = (data.get("date") or "").strip()
    owner = (data.get("owner") or "").strip()
    if not activity_type or not subject or not date or not owner:
        return jsonify({"error": "type, subject, date, owner are required"}), 400
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM activities WHERE id=%s", (activity_id,))
            if cur.fetchone():
                cur.execute("UPDATE activities SET type=%s,subject=%s,notes=%s,date=%s,owner=%s,account_id=%s,account_name=%s,updated_at=now() WHERE id=%s",
                    (activity_type, subject, (data.get("notes") or ""), date, owner,
                     (data.get("account_id") or ""), (data.get("account_name") or ""), activity_id))
                status = "updated"
            else:
                cur.execute("INSERT INTO activities (id,type,subject,notes,date,owner,account_id,account_name,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now(),now())",
                    (activity_id, activity_type, subject, (data.get("notes") or ""), date, owner,
                     (data.get("account_id") or ""), (data.get("account_name") or "")))
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
            cur.execute("SELECT id,owner FROM activities WHERE id=%s", (activity_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            if not _is_supervisor(viewer_role) and (row["owner"] or "").lower() != viewer_email.lower():
                return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM activities WHERE id=%s", (activity_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": activity_id})
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
                cur.execute("SELECT * FROM leads ORDER BY updated_at DESC")
            else:
                if not viewer_email:
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute("SELECT * FROM leads WHERE lower(owner)=lower(%s) ORDER BY updated_at DESC", (viewer_email,))
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
            if cur.fetchone():
                cur.execute("UPDATE leads SET name=%s,company=%s,email=%s,phone=%s,source=%s,status=%s,notes=%s,owner=%s,updated_at=now() WHERE id=%s",
                    ((data.get("name") or ""), (data.get("company") or ""), (data.get("email") or ""),
                     (data.get("phone") or ""), (data.get("source") or ""), (data.get("status") or ""),
                     (data.get("notes") or ""), owner, lead_id))
                status = "updated"
            else:
                cur.execute("INSERT INTO leads (id,name,company,email,phone,source,status,notes,owner,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())",
                    (lead_id, (data.get("name") or ""), (data.get("company") or ""), (data.get("email") or ""),
                     (data.get("phone") or ""), (data.get("source") or ""), (data.get("status") or ""),
                     (data.get("notes") or ""), owner))
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
            cur.execute("SELECT id,owner FROM leads WHERE id=%s", (lead_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            if not _is_supervisor(viewer_role) and (row["owner"] or "").lower() != viewer_email.lower():
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
                cur.execute("SELECT * FROM contacts ORDER BY updated_at DESC")
            else:
                if not viewer_email:
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute("SELECT * FROM contacts WHERE lower(owner)=lower(%s) ORDER BY updated_at DESC", (viewer_email,))
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
            if cur.fetchone():
                cur.execute("UPDATE contacts SET name=%s,title=%s,email=%s,phone=%s,role_type=%s,influence_level=%s,emotion=%s,account_id=%s,owner=%s,updated_at=now() WHERE id=%s",
                    ((data.get("name") or ""), (data.get("title") or ""), (data.get("email") or ""),
                     (data.get("phone") or ""), (data.get("role_type") or data.get("roleType") or ""),
                     (data.get("influence_level") or data.get("influenceLevel") or ""),
                     (data.get("emotion") or ""), (data.get("account_id") or data.get("accountId") or ""),
                     owner, contact_id))
                status = "updated"
            else:
                cur.execute("INSERT INTO contacts (id,name,title,email,phone,role_type,influence_level,emotion,account_id,owner,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())",
                    (contact_id, (data.get("name") or ""), (data.get("title") or ""),
                     (data.get("email") or ""), (data.get("phone") or ""),
                     (data.get("role_type") or data.get("roleType") or ""),
                     (data.get("influence_level") or data.get("influenceLevel") or ""),
                     (data.get("emotion") or ""),
                     (data.get("account_id") or data.get("accountId") or ""), owner))
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
            cur.execute("SELECT id,owner FROM contacts WHERE id=%s", (contact_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            if not _is_supervisor(viewer_role) and (row["owner"] or "").lower() != viewer_email.lower():
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
            opp_cols = """id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, sales_comments, requirements,
                       presales_architecture, presales_questions, boq, purchase_costing,
                       costing_tat, final_pricing_proposal, presales_assigned_at, presales_due_at,
                       purchase_assigned_at, purchase_due_at, costing_returned_at, final_proposal_at,
                       assignment_due_at, sales_submitted_at, presales_escalated_at,
                       intake_problem_statement, intake_why_now, intake_business_impact, intake_current_state,
                       intake_budget_range, intake_decision_timeline, intake_risk_if_not_solved,
                       intake_key_stakeholders, intake_in_scope, intake_out_of_scope, intake_current_environment,
                       intake_pain_points, intake_compliance_requirements, intake_integration_requirements,
                       intake_competitors, intake_win_strategy, created_at, updated_at"""
            if _is_supervisor(viewer_role):
                cur.execute(f"SELECT {opp_cols} FROM opportunities ORDER BY updated_at DESC")
                rows = cur.fetchall()
                enforce_opportunity_sla(conn, rows)
                cur.execute(f"SELECT {opp_cols} FROM opportunities ORDER BY updated_at DESC")
                return jsonify(cur.fetchall())
            if not viewer_email:
                return jsonify({"error": "viewer_email required"}), 400
            cur.execute(f"SELECT {opp_cols} FROM opportunities WHERE lower(owner)=lower(%s) OR lower(sales_owner)=lower(%s) OR lower(assigned_presales)=lower(%s) OR lower(assigned_purchase)=lower(%s) ORDER BY updated_at DESC",
                (viewer_email, viewer_email, viewer_email, viewer_email))
            rows = cur.fetchall()
            enforce_opportunity_sla(conn, rows)
            cur.execute(f"SELECT {opp_cols} FROM opportunities WHERE lower(owner)=lower(%s) OR lower(sales_owner)=lower(%s) OR lower(assigned_presales)=lower(%s) OR lower(assigned_purchase)=lower(%s) ORDER BY updated_at DESC",
                (viewer_email, viewer_email, viewer_email, viewer_email))
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/opportunities", methods=["POST"])
def upsert_opportunity():
    data = request.get_json(silent=True) or {}
    opp_id = (data.get("id") or "").strip() or f"opp_{int(datetime.utcnow().timestamp() * 1000)}"
    owner = (data.get("owner") or "").strip()
    if not owner:
        return jsonify({"error": "owner is required"}), 400

    def g(key, alt=None):
        return (data.get(key) or data.get(alt) or "").strip() if alt else (data.get(key) or "").strip()

    required_intake = [("intake_problem_statement","intakeProblemStatement"),
        ("intake_why_now","intakeWhyNow"),("intake_business_impact","intakeBusinessImpact"),
        ("intake_current_state","intakeCurrentState"),("intake_budget_range","intakeBudgetRange"),
        ("intake_decision_timeline","intakeDecisionTimeline")]
    missing = [k for k, alt in required_intake if not (data.get(k) or data.get(alt) or "").strip()]
    if missing:
        return jsonify({"error": "Mandatory intake fields missing", "missing_fields": missing}), 400

    payload = {
        "name": g("name"), "account_id": g("account_id","accountId"),
        "value": float(data.get("value") or 0), "stage": g("stage"),
        "owner": owner, "sales_owner": g("sales_owner","salesOwner") or owner,
        "workflow_stage": g("workflow_stage","workflowStage"),
        "assigned_presales": g("assigned_presales","assignedPresales"),
        "assigned_purchase": g("assigned_purchase","assignedPurchase"),
        "sales_comments": g("sales_comments","salesComments"),
        "requirements": g("requirements"),
        "presales_architecture": g("presales_architecture","presalesArchitecture"),
        "presales_questions": g("presales_questions","presalesQuestions"),
        "boq": g("boq"), "purchase_costing": g("purchase_costing","purchaseCosting"),
        "costing_tat": g("costing_tat","costingTat"),
        "final_pricing_proposal": g("final_pricing_proposal","finalPricingProposal"),
        "presales_assigned_at": g("presales_assigned_at","presalesAssignedAt"),
        "presales_due_at": g("presales_due_at","presalesDueAt"),
        "purchase_assigned_at": g("purchase_assigned_at","purchaseAssignedAt"),
        "purchase_due_at": g("purchase_due_at","purchaseDueAt"),
        "costing_returned_at": g("costing_returned_at","costingReturnedAt"),
        "final_proposal_at": g("final_proposal_at","finalProposalAt"),
        "assignment_due_at": g("assignment_due_at","assignmentDueAt"),
        "sales_submitted_at": g("sales_submitted_at","salesSubmittedAt"),
        "presales_escalated_at": g("presales_escalated_at","presalesEscalatedAt"),
        "intake_problem_statement": g("intake_problem_statement","intakeProblemStatement"),
        "intake_why_now": g("intake_why_now","intakeWhyNow"),
        "intake_business_impact": g("intake_business_impact","intakeBusinessImpact"),
        "intake_current_state": g("intake_current_state","intakeCurrentState"),
        "intake_budget_range": g("intake_budget_range","intakeBudgetRange"),
        "intake_decision_timeline": g("intake_decision_timeline","intakeDecisionTimeline"),
        "intake_risk_if_not_solved": g("intake_risk_if_not_solved","intakeRiskIfNotSolved"),
        "intake_key_stakeholders": g("intake_key_stakeholders","intakeKeyStakeholders"),
        "intake_in_scope": g("intake_in_scope","intakeInScope"),
        "intake_out_of_scope": g("intake_out_of_scope","intakeOutOfScope"),
        "intake_current_environment": g("intake_current_environment","intakeCurrentEnvironment"),
        "intake_pain_points": g("intake_pain_points","intakePainPoints"),
        "intake_compliance_requirements": g("intake_compliance_requirements","intakeComplianceRequirements"),
        "intake_integration_requirements": g("intake_integration_requirements","intakeIntegrationRequirements"),
        "intake_competitors": g("intake_competitors","intakeCompetitors"),
        "intake_win_strategy": g("intake_win_strategy","intakeWinStrategy"),
    }

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id,assigned_presales,workflow_stage,sales_owner,owner FROM opportunities WHERE id=%s", (opp_id,))
            exists = cur.fetchone()
            prev_ap = ((exists or {}).get("assigned_presales") or "").strip().lower()
            prev_ws = ((exists or {}).get("workflow_stage") or "").strip()

            cols = list(payload.keys())
            vals = [payload[c] for c in cols]
            if exists:
                sets = ", ".join([f"{c}=%s" for c in cols] + ["updated_at=now()"])
                cur.execute(f"UPDATE opportunities SET {sets} WHERE id=%s", vals + [opp_id])
                status = "updated"
            else:
                placeholders = ", ".join(["%s"] * len(cols))
                cur.execute(f"INSERT INTO opportunities (id,{','.join(cols)},created_at,updated_at) VALUES (%s,{placeholders},now(),now())",
                    [opp_id] + vals)
                status = "created"
        conn.commit()

        cur_ap = (payload.get("assigned_presales") or "").strip().lower()
        ws_now = (payload.get("workflow_stage") or "").strip()
        if (ws_now == "Assigned to Presales" and cur_ap and
            (prev_ws != "Assigned to Presales" or cur_ap != prev_ap or status == "created")):
            due_iso = payload.get("presales_due_at") or ""
            if not due_iso:
                due_iso = (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat().replace("+00:00","Z")
            send_opportunity_assignment_email(payload.get("name") or "", opp_id, cur_ap,
                payload.get("sales_owner") or owner, due_iso)
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
            cur.execute("SELECT id,owner,sales_owner FROM opportunities WHERE id=%s", (opp_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            if not _is_supervisor(viewer_role):
                allowed = {(row.get("owner") or "").lower(), (row.get("sales_owner") or "").lower()}
                if viewer_email.lower() not in allowed:
                    return jsonify({"error": "not allowed"}), 403
            cur.execute("DELETE FROM opportunities WHERE id=%s", (opp_id,))
        conn.commit()
        return jsonify({"status": "deleted", "id": opp_id})
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# AOP — Flat column schema (matches your actual Supabase table)
# ═══════════════════════════════════════════════════════════════

@app.route("/api/aop", methods=["GET"])
def list_aop_plans():
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    fy_year = (request.args.get("fy_year") or "2025-26").strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if _is_supervisor(viewer_role):
                cur.execute("SELECT * FROM aop_plans WHERE fy_year=%s", (fy_year,))
            else:
                if not viewer_email:
                    return jsonify([])
                # Join to get only accounts managed by this user
                cur.execute("""
                    SELECT p.* FROM aop_plans p
                    JOIN accounts a ON CAST(a.id AS TEXT) = p.account_id
                    JOIN users u ON u.id = a.account_manager_id
                    WHERE p.fy_year=%s AND lower(u.email)=lower(%s)
                """, (fy_year, viewer_email))
            rows = cur.fetchall()
        return jsonify([_aop_row_to_frontend(r) for r in rows])
    except Exception as exc:
        print(f"[AOP GET] error: {exc}")
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

    # Get flat month/category values
    flat = _frontend_to_aop_flat(data)

    # Get account name & manager from DB
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT a.account_name, u.email AS mgr_email
                FROM accounts a LEFT JOIN users u ON u.id=a.account_manager_id
                WHERE CAST(a.id AS TEXT)=%s""", (account_id,))
            acc_row = cur.fetchone() or {}
            account_name = acc_row.get("account_name") or ""
            account_manager = acc_row.get("mgr_email") or owner

            # Build dynamic SET clause for flat columns
            flat_cols = list(flat.keys())
            flat_vals = [flat[c] for c in flat_cols]

            meta_cols = ["account_name", "account_manager", "current_revenue", "target_growth",
                         "key_solutions", "oem", "updated_by"]
            meta_vals = [
                account_name, account_manager,
                float(data.get("current_revenue") or 0),
                float(data.get("target_growth") or 0),
                str(data.get("key_solutions") or ""),
                str(data.get("oem") or ""),
                owner,
            ]

            all_cols = meta_cols + flat_cols
            all_vals = meta_vals + flat_vals

            set_clause = ", ".join([f"{c}=%s" for c in all_cols] + ["updated_at=now()"])
            insert_cols = ["account_id", "fy_year"] + all_cols
            insert_placeholders = ", ".join(["%s"] * len(insert_cols))

            cur.execute(f"""
                INSERT INTO aop_plans ({', '.join(insert_cols)}, created_at, updated_at)
                VALUES ({insert_placeholders}, now(), now())
                ON CONFLICT (account_id, fy_year)
                DO UPDATE SET {set_clause}
            """, [account_id, fy_year] + all_vals + all_vals)
        conn.commit()
        return jsonify({"status": "ok", "account_id": account_id, "fy_year": fy_year})
    except Exception as exc:
        print(f"[AOP POST] error: {exc}")
        return jsonify({"error": f"AOP save failed: {exc}"}), 500
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
                cur.execute("SELECT * FROM aop_actuals WHERE fy_year=%s", (fy_year,))
            else:
                if not viewer_email:
                    return jsonify([])
                cur.execute("""
                    SELECT x.* FROM aop_actuals x
                    JOIN accounts a ON CAST(a.id AS TEXT)=x.account_id
                    JOIN users u ON u.id=a.account_manager_id
                    WHERE x.fy_year=%s AND lower(u.email)=lower(%s)
                """, (fy_year, viewer_email))
            rows = cur.fetchall()
        # Normalize numeric fields
        out = []
        for r in rows:
            out.append({
                "account_id": str(r.get("account_id") or ""),
                "account_name": r.get("account_name") or "",
                "account_manager": r.get("account_manager") or "",
                "fy_year": r.get("fy_year") or "",
                "month": r.get("month") or "",
                "hardware": float(r.get("hardware") or 0),
                "software": float(r.get("software") or 0),
                "managed_services": float(r.get("managed_services") or 0),
                "notes": r.get("notes") or "",
                "updated_at": str(r.get("updated_at") or ""),
            })
        return jsonify(out)
    except Exception as exc:
        print(f"[AOP ACTUALS GET] error: {exc}")
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
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT account_name FROM accounts WHERE CAST(id AS TEXT)=%s", (account_id,))
            acc = cur.fetchone() or {}
            cur.execute("""
                INSERT INTO aop_actuals (account_id, account_name, account_manager, fy_year, month,
                    hardware, software, managed_services, notes, updated_by, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
                ON CONFLICT (account_id, fy_year, month)
                DO UPDATE SET hardware=EXCLUDED.hardware, software=EXCLUDED.software,
                    managed_services=EXCLUDED.managed_services, notes=EXCLUDED.notes,
                    updated_by=EXCLUDED.updated_by, updated_at=now()
            """, (account_id, acc.get("account_name") or "", owner, fy_year, month,
                  float(data.get("hardware") or 0), float(data.get("software") or 0),
                  float(data.get("managed_services") or 0), str(data.get("notes") or ""), owner))
        conn.commit()
        return jsonify({"status": "ok", "account_id": account_id, "fy_year": fy_year, "month": month})
    except Exception as exc:
        return jsonify({"error": f"Actual save failed: {exc}"}), 500
    finally:
        conn.close()


# ── AOP Bulk Import (fixed for flat schema) ────────────────────────────────
AOP_RAW_DATA = []  # kept empty here; populate from your original file if needed

@app.route("/api/aop/bulk-import", methods=["POST"])
def aop_bulk_import():
    data = request.get_json(silent=True) or {}
    viewer_role = (data.get("viewer_role") or "").strip().lower()
    if not _is_supervisor(viewer_role):
        return jsonify({"error": "supervisor only"}), 403
    rows = data.get("rows") or AOP_RAW_DATA
    if not rows:
        return jsonify({"error": "No data to import. Pass 'rows' array or configure AOP_RAW_DATA."}), 400

    month_map = {
        "april":"Apr","may":"May","june":"Jun","july":"Jul","august":"Aug","september":"Sep",
        "october":"Oct","november":"Nov","december":"Dec","january":"Jan","february":"Feb","march":"Mar"
    }
    conn = get_conn()
    try:
        ok = skipped = 0
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for row in rows:
                account_name = (row.get("account_name") or "").strip()
                am_email = (row.get("am_email") or "").strip().lower()
                win_pct = float(row.get("win_pct") or 0)
                aop_cr = float(row.get("aop_cr") or 0)
                months_raw = row.get("months_raw") or {}
                if not account_name or not am_email:
                    skipped += 1; continue
                cur.execute("SELECT a.id FROM accounts a JOIN users u ON u.id=a.account_manager_id WHERE lower(a.account_name)=lower(%s) AND lower(u.email)=lower(%s) LIMIT 1", (account_name, am_email))
                acc = cur.fetchone()
                if not acc:
                    cur.execute("SELECT id FROM accounts WHERE lower(account_name)=lower(%s) LIMIT 1", (account_name,))
                    acc = cur.fetchone()
                if not acc:
                    skipped += 1; continue

                # Build flat values from months_raw
                flat = {}
                for raw_month, val in months_raw.items():
                    fe_month = month_map.get(raw_month.lower())
                    if not fe_month:
                        continue
                    mp = _aop_month_col_prefix(fe_month)
                    # Split evenly across hardware/software/managed_services by win_pct
                    v = float(val or 0) * win_pct
                    # Put everything in hardware for bulk import (can be edited later)
                    flat[f"{mp}_hardware"] = round(v, 4)
                    flat[f"{mp}_software"] = 0.0
                    flat[f"{mp}_managed_services"] = 0.0

                all_flat_cols = list(flat.keys())
                all_flat_vals = [flat[c] for c in all_flat_cols]
                if not all_flat_cols:
                    skipped += 1; continue

                set_clause = ", ".join([f"{c}=%s" for c in all_flat_cols] + ["updated_at=now()","account_manager=%s","updated_by=%s"])
                insert_cols = ["account_id","fy_year","account_name","account_manager","current_revenue","target_growth","updated_by"] + all_flat_cols
                insert_vals = [str(acc["id"]), "2025-26", account_name, am_email, aop_cr, 0.0, am_email] + all_flat_vals
                insert_ph = ", ".join(["%s"] * len(insert_cols))

                cur.execute(f"""
                    INSERT INTO aop_plans ({', '.join(insert_cols)}, created_at, updated_at)
                    VALUES ({insert_ph}, now(), now())
                    ON CONFLICT (account_id, fy_year)
                    DO UPDATE SET {set_clause}
                """, insert_vals + all_flat_vals + [am_email, am_email])
                ok += 1
        conn.commit()
        return jsonify({"status": "ok", "imported": ok, "skipped": skipped})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# Passwords, OAuth, MoM, AI Extract, PO Notify
# ═══════════════════════════════════════════════════════════════

@app.route("/api/passwords/<email>", methods=["GET"])
def get_password(email: str):
    viewer_role = (request.args.get("viewer_role") or "").strip().lower()
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    if not _is_supervisor(viewer_role) and viewer_email != email.strip().lower():
        return jsonify({"error": "not allowed"}), 403
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT email,password,updated_at FROM user_passwords WHERE lower(email)=lower(%s)", (email,))
            row = cur.fetchone()
            return jsonify(row if row else {"email": email, "password": None})
    finally:
        conn.close()


@app.route("/api/passwords", methods=["POST"])
def set_password():
    data = request.get_json(silent=True) or {}
    viewer_role = (data.get("viewer_role") or "").strip().lower()
    viewer_email = (data.get("viewer_email") or "").strip().lower()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"error": "email and password required"}), 400
    if not _is_supervisor(viewer_role) and viewer_email != email:
        return jsonify({"error": "not allowed"}), 403
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO user_passwords (email,password,updated_at) VALUES (%s,%s,now()) ON CONFLICT(email) DO UPDATE SET password=EXCLUDED.password, updated_at=now()", (email, password))
        conn.commit()
        return jsonify({"status": "ok", "email": email})
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
    params = {"client_id": MS_CLIENT_ID, "response_type": "code", "redirect_uri": MS_REDIRECT_URI,
               "response_mode": "query", "scope": MS_OAUTH_SCOPES, "state": state}
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
        return "Invalid state", 400
    try:
        token_data = _http_form_post(
            f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token",
            {"client_id": MS_CLIENT_ID, "client_secret": MS_CLIENT_SECRET,
             "grant_type": "authorization_code", "code": code,
             "redirect_uri": MS_REDIRECT_URI, "scope": MS_OAUTH_SCOPES})
        user = _get_user_row_by_email(email)
        _upsert_o365_tokens(int(user["id"]), email, token_data)
        return """<html><body><h3>Connected.</h3>
        <script>if(window.opener){window.opener.postMessage({type:'ms_o365_connected'},'*');window.close();}</script>
        </body></html>"""
    except Exception as exc:
        return f"OAuth failed: {exc}", 500


@app.route("/api/oauth/microsoft/status", methods=["GET"])
def microsoft_oauth_status():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    if not viewer_email:
        return jsonify({"connected": False}), 400
    row = _get_o365_token_row(viewer_email)
    return jsonify({"connected": bool(row and (row.get("status") or "") == "active")})


@app.route("/api/mom/send", methods=["POST"])
def send_mom_mail_endpoint():
    data = request.get_json(silent=True) or {}
    viewer_email = _normalize_email(data.get("viewer_email") or "")
    to_emails = _split_emails(data.get("to_emails") or "")
    cc_emails = _split_emails(data.get("cc_emails") or "")
    if not to_emails:
        return jsonify({"error": "to_emails required"}), 400
    account_id = str(data.get("account_id") or "").strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            am_email = viewer_email
            am_name = viewer_email.split("@")[0]
            account_name = data.get("account_name") or ""
            if account_id:
                cur.execute("SELECT a.account_name, u.email, u.name FROM accounts a LEFT JOIN users u ON u.id=a.account_manager_id WHERE CAST(a.id AS TEXT)=%s", (account_id,))
                row = cur.fetchone()
                if row:
                    account_name = row.get("account_name") or account_name
                    am_email = _normalize_email(row.get("email") or am_email)
                    am_name = row.get("name") or am_name
            meeting_date = (data.get("meeting_date") or "").strip() or datetime.now().strftime("%d-%b-%Y")
            subject = (data.get("subject") or f"Minutes of Meeting | {account_name} | {meeting_date}").strip()
            payload = {"account_name": account_name, "meeting_date": meeting_date,
                       "client_name": data.get("client_name") or "Team",
                       "mom_intro": data.get("mom_intro") or "",
                       "mom_discussion": data.get("mom_discussion") or "",
                       "mom_actions": data.get("mom_actions") or "",
                       "mom_next_steps": data.get("mom_next_steps") or "",
                       "account_manager_name": am_name, "account_manager_email": am_email}
            html = _build_mom_html(payload)
            _send_graph_mail(am_email, to_emails, cc_emails, subject, html)
            activity_id = str(data.get("activity_id") or "").strip()
            if activity_id:
                cur.execute("UPDATE activities SET mom_sent_at=now(),mom_sent_to=%s,mom_send_status='sent',updated_at=now() WHERE id=%s",
                    (", ".join(to_emails), activity_id))
                conn.commit()
        return jsonify({"status": "sent", "from": am_email, "to": to_emails, "cc": cc_emails})
    except Exception as exc:
        return jsonify({"error": f"MoM send failed: {exc}"}), 500
    finally:
        conn.close()


GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_VISION_MODEL = os.environ.get("GROQ_VISION_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct").strip()

# Fallback models to try if primary fails
GROQ_FALLBACK_MODELS = [
    "meta-llama/llama-4-scout-17b-16e-instruct",
    "llama-3.2-90b-vision-preview",
    "llama-3.2-11b-vision-preview",
]


@app.route("/api/ai-extract", methods=["POST"])
def ai_extract():
    """Proxy AI extraction to Groq with model fallback."""
    if not GROQ_API_KEY:
        return jsonify({"error": "AI extraction not configured — GROQ_API_KEY missing on server. Please set it in your environment variables."}), 503

    data = request.get_json(silent=True) or {}
    prompt = (data.get("prompt") or "").strip()
    image_b64 = (data.get("image_b64") or "").strip()
    media_type = (data.get("media_type") or "image/jpeg").strip()

    if not image_b64 or not prompt:
        return jsonify({"error": "image_b64 and prompt are required"}), 400

    # Strip data-URL prefix if present
    if "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]

    data_url = f"data:{media_type};base64,{image_b64}"

    # Try primary model, then fallbacks
    models_to_try = [GROQ_VISION_MODEL] + [m for m in GROQ_FALLBACK_MODELS if m != GROQ_VISION_MODEL]
    last_error = None

    for model in models_to_try:
        payload = {
            "model": model,
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": data_url}},
                {"type": "text", "text": prompt},
            ]}],
        }
        try:
            result = _http_json_request(
                "https://api.groq.com/openai/v1/chat/completions",
                method="POST", data=payload,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            )
            text = "".join((c.get("message") or {}).get("content") or "" for c in (result.get("choices") or []))
            if text:
                return jsonify({"text": text, "model_used": model})
            last_error = "Empty response from model"
        except Exception as exc:
            last_error = str(exc)
            print(f"[AI-EXTRACT] model={model} failed: {exc}")
            # If 403/401, stop trying — key is the problem
            if "403" in str(exc) or "401" in str(exc):
                return jsonify({"error": f"Groq API authentication failed (HTTP {exc}). Check that GROQ_API_KEY is valid and not expired."}), 503
            continue

    return jsonify({"error": f"AI extraction failed after trying all models. Last error: {last_error}"}), 500


@app.route("/api/po-notify", methods=["POST"])
def po_notify():
    data = request.get_json(silent=True) or {}
    event = (data.get("event") or "").strip()
    po_number = (data.get("po_number") or "Draft PO").strip()
    account_name = (data.get("account_name") or "").strip()
    stage = (data.get("stage") or "").strip()
    creator_email = _normalize_email(data.get("creator_email") or "")
    approved_by = _normalize_email(data.get("approved_by") or "")
    value = float(data.get("value") or 0)
    val_str = f"₹{value/100000:.1f}L" if value >= 100000 else f"₹{int(value):,}"

    if event == "submitted":
        to = ["vinod.v@dnispl.com", "rakesh.uniyal@dnispl.com"]
        cc = [SUPERVISOR_EMAIL]
        if creator_email and creator_email not in to:
            cc.append(creator_email)
        send_email_smtp(to, f"[PO Approval Required] {po_number} | {account_name} | {val_str}",
            f"PO {po_number} submitted for approval.\nAccount: {account_name}\nValue: {val_str}\nStage: {stage}\nBy: {creator_email}", cc_emails=cc)
    elif event in ("approved", "ceo_approved"):
        to = [e for e in [creator_email, SUPERVISOR_EMAIL] if e and "@" in e]
        if stage == "Both Approved - Pending Implementation":
            to.append("pokhraj.yadav@dnispl.com")
        elif stage == "Pending CEO Approval":
            to.append("ashish.mehra@dnispl.com")
        elif event == "ceo_approved":
            to += ["vinod.v@dnispl.com", "rakesh.uniyal@dnispl.com"]
        send_email_smtp(list(dict.fromkeys(to)), f"[PO {event.upper()}] {po_number} — {stage}",
            f"PO {po_number} update.\nAccount: {account_name}\nValue: {val_str}\nStage: {stage}\nBy: {approved_by}")
    else:
        return jsonify({"error": f"unknown event: {event}"}), 400
    return jsonify({"status": "ok", "event": event})


# ═══════════════════════════════════════════════════════════════
# Salary & Reports
# ═══════════════════════════════════════════════════════════════

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
            cur.execute("INSERT INTO user_salary (email,salary_data,updated_at) VALUES (%s,%s::jsonb,now()) ON CONFLICT (email) DO UPDATE SET salary_data=EXCLUDED.salary_data, updated_at=now()",
                (viewer_email, json.dumps(salary_data)))
        conn.commit()
        return jsonify({"status": "ok"})
    finally:
        conn.close()


@app.route("/api/reports/team", methods=["GET"])
def reports_team():
    if not _is_supervisor((request.args.get("viewer_role") or "").strip().lower()):
        return jsonify({"error": "supervisor only"}), 403
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id,email,name,role FROM users WHERE role IN ('account_manager','presales') ORDER BY name")
            return jsonify(cur.fetchall())
    finally:
        conn.close()


@app.route("/api/reports/kra", methods=["GET"])
def kra_report():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    target_email = _normalize_email(request.args.get("target_email") or viewer_email)
    fy_year = (request.args.get("fy_year") or "2025-26").strip()
    quarter = (request.args.get("quarter") or "Q1").strip().upper()
    if not _is_supervisor(viewer_role) and viewer_email != target_email:
        return jsonify({"error": "not authorized"}), 403

    qmonths = {"Q1":["Apr","May","Jun"],"Q2":["Jul","Aug","Sep"],"Q3":["Oct","Nov","Dec"],"Q4":["Jan","Feb","Mar"]}
    months = qmonths.get(quarter, ["Apr","May","Jun"])

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Revenue target from AOP flat columns
            q_target = 0.0
            for m in months:
                mp = _aop_month_col_prefix(m)
                hw_col = f"{mp}_hardware"
                sw_col = f"{mp}_software"
                ms_col = f"{mp}_managed_services"
                try:
                    cur.execute(f"""
                        SELECT COALESCE(SUM(p.{hw_col} + p.{sw_col} + p.{ms_col}), 0) AS total
                        FROM aop_plans p
                        JOIN accounts a ON CAST(a.id AS TEXT)=p.account_id
                        JOIN users u ON u.id=a.account_manager_id
                        WHERE p.fy_year=%s AND lower(u.email)=lower(%s)
                    """, (fy_year, target_email))
                    q_target += float((cur.fetchone() or {}).get("total") or 0)
                except Exception:
                    pass

            cur.execute("SELECT COALESCE(SUM(value),0) AS won FROM opportunities WHERE lower(owner)=lower(%s) AND lower(stage) IN ('closed won')", (target_email,))
            won_cr = float((cur.fetchone() or {}).get("won") or 0) / 10000000

            cur.execute("SELECT COUNT(DISTINCT a.id) AS total FROM accounts a JOIN users u ON u.id=a.account_manager_id WHERE lower(u.email)=lower(%s)", (target_email,))
            total_accts = int((cur.fetchone() or {}).get("total") or 0)

            cur.execute("SELECT COUNT(DISTINCT c.account_id) AS covered FROM contacts c JOIN accounts a ON CAST(a.id AS TEXT)=c.account_id JOIN users u ON u.id=a.account_manager_id WHERE lower(u.email)=lower(%s)", (target_email,))
            covered = int((cur.fetchone() or {}).get("covered") or 0)
            cov_pct = round(covered/total_accts*100,1) if total_accts > 0 else 0

            cur.execute("SELECT COALESCE(SUM(value),0) AS pipe FROM opportunities WHERE lower(owner)=lower(%s) AND lower(stage) NOT IN ('closed won','closed lost')", (target_email,))
            pipe_cr = float((cur.fetchone() or {}).get("pipe") or 0) / 10000000
            pipe_target = q_target * 3
            pipe_ach = min(round(pipe_cr/pipe_target*100,1) if pipe_target > 0 else 0, 150)

            rev_ach = min(round(won_cr/q_target*100,1) if q_target > 0 else 0, 150)

            return jsonify({"role":"account_manager","target_email":target_email,"quarter":quarter,"fy_year":fy_year,
                "q_target_cr":round(q_target,2),"won_value_cr":round(won_cr,2),
                "kras":[
                    {"id":"revenue","name":"Revenue & Sales Target","weight":25,"target":f"₹{q_target:.2f}Cr ({quarter})","actual_label":f"₹{won_cr:.2f}Cr won","actual_value":won_cr,"target_value":q_target,"unit":"Cr","achievement_pct":rev_ach},
                    {"id":"account_coverage","name":"Account Coverage","weight":40,"target":"100% accounts with contacts","actual_label":f"{covered}/{total_accts} ({cov_pct}%)","actual_value":cov_pct,"target_value":100,"unit":"%","achievement_pct":min(cov_pct,150)},
                    {"id":"pipeline","name":"Pipeline Management","weight":20,"target":f"3x quota ₹{pipe_target:.1f}Cr","actual_label":f"₹{pipe_cr:.2f}Cr","actual_value":pipe_cr,"target_value":pipe_target,"unit":"Cr","achievement_pct":pipe_ach},
                    {"id":"activities","name":"Activity & Engagement","weight":15,"target":"Manual input","actual_label":"Manual input required","actual_value":None,"target_value":None,"unit":None,"achievement_pct":None,"manual":True},
                ]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@app.route("/api/reports/incentive", methods=["GET"])
def incentive_report():
    viewer_email = _normalize_email(request.args.get("viewer_email") or "")
    viewer_role = (request.args.get("viewer_role") or "account_manager").strip().lower()
    target_email = _normalize_email(request.args.get("target_email") or viewer_email)
    quarter = (request.args.get("quarter") or "Q1").strip().upper()
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
        var = float(sal.get("variable_annual_lakhs") or 0)
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


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"CRM backend running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=True)
