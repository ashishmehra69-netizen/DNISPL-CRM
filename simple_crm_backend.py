import csv
import os
import smtplib
import threading
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from io import StringIO
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request


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


@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return resp


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


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
            cur.execute('''
                CREATE TABLE IF NOT EXISTS aop_plans (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    account_name TEXT,
                    account_manager TEXT,
                    fy_year TEXT NOT NULL DEFAULT '2025-26',
                    current_revenue NUMERIC DEFAULT 0,
                    target_growth NUMERIC DEFAULT 0,
                    key_solutions TEXT DEFAULT '',
                    oem TEXT DEFAULT '',
                    updated_by TEXT,
                    apr_hardware NUMERIC DEFAULT 0,
                    apr_software NUMERIC DEFAULT 0,
                    apr_managed_services NUMERIC DEFAULT 0,
                    may_hardware NUMERIC DEFAULT 0,
                    may_software NUMERIC DEFAULT 0,
                    may_managed_services NUMERIC DEFAULT 0,
                    jun_hardware NUMERIC DEFAULT 0,
                    jun_software NUMERIC DEFAULT 0,
                    jun_managed_services NUMERIC DEFAULT 0,
                    jul_hardware NUMERIC DEFAULT 0,
                    jul_software NUMERIC DEFAULT 0,
                    jul_managed_services NUMERIC DEFAULT 0,
                    aug_hardware NUMERIC DEFAULT 0,
                    aug_software NUMERIC DEFAULT 0,
                    aug_managed_services NUMERIC DEFAULT 0,
                    sep_hardware NUMERIC DEFAULT 0,
                    sep_software NUMERIC DEFAULT 0,
                    sep_managed_services NUMERIC DEFAULT 0,
                    oct_hardware NUMERIC DEFAULT 0,
                    oct_software NUMERIC DEFAULT 0,
                    oct_managed_services NUMERIC DEFAULT 0,
                    nov_hardware NUMERIC DEFAULT 0,
                    nov_software NUMERIC DEFAULT 0,
                    nov_managed_services NUMERIC DEFAULT 0,
                    dec_hardware NUMERIC DEFAULT 0,
                    dec_software NUMERIC DEFAULT 0,
                    dec_managed_services NUMERIC DEFAULT 0,
                    jan_hardware NUMERIC DEFAULT 0,
                    jan_software NUMERIC DEFAULT 0,
                    jan_managed_services NUMERIC DEFAULT 0,
                    feb_hardware NUMERIC DEFAULT 0,
                    feb_software NUMERIC DEFAULT 0,
                    feb_managed_services NUMERIC DEFAULT 0,
                    mar_hardware NUMERIC DEFAULT 0,
                    mar_software NUMERIC DEFAULT 0,
                    mar_managed_services NUMERIC DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(account_id, fy_year)
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS aop_actuals (
                    id SERIAL PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    account_name TEXT,
                    account_manager TEXT,
                    fy_year TEXT NOT NULL DEFAULT '2025-26',
                    month TEXT NOT NULL,
                    hardware NUMERIC DEFAULT 0,
                    software NUMERIC DEFAULT 0,
                    managed_services NUMERIC DEFAULT 0,
                    notes TEXT DEFAULT '',
                    updated_by TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(account_id, fy_year, month)
                )
            ''')
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_passwords (
                    email TEXT PRIMARY KEY,
                    password TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
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


@app.before_request
def _ensure_init_once():
    ensure_db_initialized()


def compute_suspect_score(data: dict) -> int:
    score = 0
    for idx in range(1, 11):
        if str(data.get(f"suspect_q{idx}") or "").strip():
            score += 1
    return score


def _is_supervisor(viewer_role: str) -> bool:
    return (viewer_role or "").strip().lower() in ("supervisor", "admin")


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


@app.route("/api/accounts", methods=["POST"])
def create_or_update_account():
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
                           assigned_presales, assigned_purchase, sales_comments, requirements,
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
            query_scoped = """
                SELECT id, name, account_id, value, stage, owner, sales_owner, workflow_stage,
                       assigned_presales, assigned_purchase, sales_comments, requirements,
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
                   OR lower(assigned_presales)=lower(%s)
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

            cur.execute(query_scoped, (viewer_email, viewer_email, viewer_email, viewer_email))
            rows = cur.fetchall()
            if enforce_opportunity_sla(conn, rows):
                cur.execute(query_scoped, (viewer_email, viewer_email, viewer_email, viewer_email))
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
                        assigned_presales=%s, assigned_purchase=%s, sales_comments=%s, requirements=%s,
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
                        payload["assigned_purchase"], payload["sales_comments"], payload["requirements"],
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
                        assigned_presales, assigned_purchase, sales_comments, requirements,
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
                        payload["assigned_purchase"], payload["sales_comments"], payload["requirements"],
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
# ─── AOP helpers ────────────────────────────────────────────────────────────
 
_AOP_MONTHS_LOWER   = ['apr','may','jun','jul','aug','sep','oct','nov','dec','jan','feb','mar']
_AOP_MONTHS_DISPLAY = ['Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar']
_AOP_CATS_DB        = ['hardware','software','managed_services']
_AOP_CATS_FE        = ['Hardware','Software','Managed_Services']
 
def _all_aop_cols():
    return [f"{m}_{c}" for m in _AOP_MONTHS_LOWER for c in _AOP_CATS_DB]
 
def _aop_row_to_dict(row):
    if not row:
        return None
    d = dict(row)
    for m_lo, m_di in zip(_AOP_MONTHS_LOWER, _AOP_MONTHS_DISPLAY):
        for c_db, c_fe in zip(_AOP_CATS_DB, _AOP_CATS_FE):
            d[f"{m_di}_{c_fe}"] = float(d.get(f"{m_lo}_{c_db}") or 0)
    d['current_revenue'] = float(d.get('current_revenue') or 0)
    d['target_growth']   = float(d.get('target_growth')   or 0)
    return d
 
def _fe_to_db_aop(data):
    out = {}
    for m_lo, m_di in zip(_AOP_MONTHS_LOWER, _AOP_MONTHS_DISPLAY):
        for c_db, c_fe in zip(_AOP_CATS_DB, _AOP_CATS_FE):
            val = data.get(f"{m_di}_{c_fe}") or data.get(f"{m_di}_{c_fe.replace('_',' ')}") or 0
            out[f"{m_lo}_{c_db}"] = float(val)
    return out
 
 
# ─── GET /api/aop ────────────────────────────────────────────────────────────
 
@app.route("/api/aop", methods=["GET"])
def list_aop():
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role  = (request.args.get("viewer_role")  or "account_manager").strip().lower()
    account_id   = (request.args.get("account_id")   or "").strip()
    fy_year      = (request.args.get("fy_year")       or "2025-26").strip()
 
    all_cols = ", ".join(_all_aop_cols())
    base_sel = f"""
        SELECT id, account_id, account_name, account_manager, fy_year,
               current_revenue, target_growth, key_solutions, oem,
               updated_by, created_at, updated_at, {all_cols}
        FROM aop_plans
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if account_id:
                cur.execute(base_sel + " WHERE account_id=%s AND fy_year=%s", (account_id, fy_year))
                row = cur.fetchone()
                return jsonify(_aop_row_to_dict(row) or {})
            if _is_supervisor(viewer_role):
                cur.execute(base_sel + " WHERE fy_year=%s ORDER BY account_name", (fy_year,))
            else:
                if not viewer_email:
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute(
                    base_sel + " WHERE fy_year=%s AND lower(account_manager)=lower(%s) ORDER BY account_name",
                    (fy_year, viewer_email)
                )
            rows = cur.fetchall()
        return jsonify([_aop_row_to_dict(r) for r in rows])
    finally:
        conn.close()
 
 
# ─── POST /api/aop ───────────────────────────────────────────────────────────
 
@app.route("/api/aop", methods=["POST"])
def upsert_aop():
    data       = request.get_json(silent=True) or {}
    account_id = (data.get("account_id") or "").strip()
    fy_year    = (data.get("fy_year")    or "2025-26").strip()
    if not account_id:
        return jsonify({"error": "account_id is required"}), 400
 
    month_data      = _fe_to_db_aop(data)
    account_manager = (data.get("account_manager") or "").strip().lower()
    updated_by      = (data.get("updated_by")      or account_manager).strip().lower()
    all_cols        = _all_aop_cols()
 
    base_vals = [
        (data.get("account_name")    or "").strip(),
        account_manager,
        float(data.get("current_revenue") or 0),
        float(data.get("target_growth")   or 0),
        (data.get("key_solutions") or "").strip(),
        (data.get("oem")           or "").strip(),
        updated_by,
    ]
    month_vals = [month_data.get(c, 0.0) for c in all_cols]
 
    set_parts = [
        "account_name=%s","account_manager=%s","current_revenue=%s",
        "target_growth=%s","key_solutions=%s","oem=%s",
        "updated_by=%s","updated_at=now()"
    ] + [f"{c}=%s" for c in all_cols]
 
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM aop_plans WHERE account_id=%s AND fy_year=%s",
                (account_id, fy_year)
            )
            exists = cur.fetchone()
            if exists:
                cur.execute(
                    f"UPDATE aop_plans SET {', '.join(set_parts)} WHERE account_id=%s AND fy_year=%s",
                    base_vals + month_vals + [account_id, fy_year]
                )
                status = "updated"
            else:
                ins_cols = (["account_id","fy_year","account_name","account_manager",
                             "current_revenue","target_growth","key_solutions","oem","updated_by"]
                            + all_cols)
                placeholders = ", ".join(["%s"] * len(ins_cols))
                cur.execute(
                    f"INSERT INTO aop_plans ({', '.join(ins_cols)}) VALUES ({placeholders})",
                    [account_id, fy_year] + base_vals + month_vals
                )
                status = "created"
        conn.commit()
        return jsonify({"status": status, "account_id": account_id, "fy_year": fy_year})
    finally:
        conn.close()
 
 
# ─── GET /api/aop/actuals ────────────────────────────────────────────────────
 
@app.route("/api/aop/actuals", methods=["GET"])
def list_aop_actuals():
    viewer_email = (request.args.get("viewer_email") or "").strip().lower()
    viewer_role  = (request.args.get("viewer_role")  or "account_manager").strip().lower()
    account_id   = (request.args.get("account_id")   or "").strip()
    fy_year      = (request.args.get("fy_year")       or "2025-26").strip()
 
    base_sel = """
        SELECT id, account_id, account_name, account_manager, fy_year,
               month, hardware, software, managed_services, notes,
               updated_by, created_at, updated_at
        FROM aop_actuals
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if account_id:
                cur.execute(base_sel + " WHERE account_id=%s AND fy_year=%s ORDER BY month",
                            (account_id, fy_year))
                return jsonify([dict(r) for r in cur.fetchall()])
            if _is_supervisor(viewer_role):
                cur.execute(base_sel + " WHERE fy_year=%s ORDER BY account_name, month", (fy_year,))
            else:
                if not viewer_email:
                    return jsonify({"error": "viewer_email required"}), 400
                cur.execute(
                    base_sel + " WHERE fy_year=%s AND lower(account_manager)=lower(%s) ORDER BY account_name, month",
                    (fy_year, viewer_email)
                )
            return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()
 
 
# ─── POST /api/aop/actuals ───────────────────────────────────────────────────
 
@app.route("/api/aop/actuals", methods=["POST"])
def upsert_aop_actual():
    data       = request.get_json(silent=True) or {}
    account_id = (data.get("account_id") or "").strip()
    month      = (data.get("month")      or "").strip()
    fy_year    = (data.get("fy_year")    or "2025-26").strip()
    if not account_id or not month:
        return jsonify({"error": "account_id and month are required"}), 400
 
    account_manager  = (data.get("account_manager") or "").strip().lower()
    updated_by       = (data.get("updated_by")      or account_manager).strip().lower()
    hardware         = float(data.get("hardware")         or 0)
    software         = float(data.get("software")         or 0)
    managed_services = float(data.get("managed_services") or 0)
    notes            = (data.get("notes") or "").strip()
 
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM aop_actuals WHERE account_id=%s AND fy_year=%s AND month=%s",
                (account_id, fy_year, month)
            )
            exists = cur.fetchone()
            if exists:
                cur.execute(
                    """UPDATE aop_actuals
                       SET account_name=%s, account_manager=%s, hardware=%s,
                           software=%s, managed_services=%s, notes=%s,
                           updated_by=%s, updated_at=now()
                       WHERE account_id=%s AND fy_year=%s AND month=%s""",
                    ((data.get("account_name") or "").strip(), account_manager,
                     hardware, software, managed_services, notes, updated_by,
                     account_id, fy_year, month)
                )
                status = "updated"
            else:
                cur.execute(
                    """INSERT INTO aop_actuals
                       (account_id, account_name, account_manager, fy_year, month,
                        hardware, software, managed_services, notes, updated_by)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (account_id, (data.get("account_name") or "").strip(),
                     account_manager, fy_year, month,
                     hardware, software, managed_services, notes, updated_by)
                )
                status = "created"
        conn.commit()
        return jsonify({"status": status, "account_id": account_id, "month": month})
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"Simple CRM backend running on port {port}")
    print("DB host:", urlparse(DATABASE_URL).hostname)
    app.run(host="0.0.0.0", port=port, debug=True)
