"""
render_autoflow_tab()
=====================
Drop this file next to app.py and add one import at the top of app.py:

    from autoflow_tab import render_autoflow_tab

Then follow the 3-point change plan in app.py (see diagram).

REQUIRES:  database.py already in project (uses DatabaseManager)
NO CHANGES to database.py or file_processor.py are needed.

DB setup (run once — paste into MySQL):
────────────────────────────────────────
CREATE TABLE IF NOT EXISTS auto_flows (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(120) NOT NULL,
    description VARCHAR(255),
    trigger_type ENUM('manual','schedule','after_import') NOT NULL DEFAULT 'manual',
    cron_expr   VARCHAR(60),                  -- e.g. "0 8 * * 1-5"
    target_script VARCHAR(200) NOT NULL,      -- path to .py file, e.g. "scripts/daily_report.py"
    script_args  VARCHAR(500),               -- optional CLI args string
    is_active   TINYINT(1) NOT NULL DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flow_runs (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    flow_id     INT NOT NULL,
    triggered_by VARCHAR(100),               -- username or 'scheduler'
    status      ENUM('running','success','failed') NOT NULL DEFAULT 'running',
    started_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME,
    duration_sec FLOAT,
    exit_code   INT,
    log_output  TEXT,
    error_msg   VARCHAR(500),
    FOREIGN KEY (flow_id) REFERENCES auto_flows(id) ON DELETE CASCADE
);
"""

import streamlit as st
import pandas as pd
import subprocess
import threading
import time
import os
from datetime import datetime
from database import DatabaseManager


# ─────────────────────────────────────────────
# Internal helpers (private to this module)
# ─────────────────────────────────────────────

def _get_db() -> DatabaseManager:
    """Return the shared DatabaseManager from session_state."""
    return st.session_state.get("db_manager") or DatabaseManager()


def _load_flows() -> pd.DataFrame:
    """SELECT all flows ordered by name."""
    db = _get_db()
    return db.execute_query(
        "SELECT id, name, description, trigger_type, cron_expr, "
        "target_script, script_args, is_active, created_at "
        "FROM auto_flows ORDER BY name"
    )


def _load_recent_runs(limit: int = 30) -> pd.DataFrame:
    """SELECT recent run history with flow name joined."""
    db = _get_db()
    return db.execute_query(
        """
        SELECT r.id, f.name AS flow_name, r.triggered_by, r.status,
               r.started_at, r.finished_at, r.duration_sec,
               r.exit_code, r.error_msg
        FROM flow_runs r
        JOIN auto_flows f ON f.id = r.flow_id
        ORDER BY r.started_at DESC
        LIMIT %s
        """,
        (str(limit),)          # execute_query wraps params as strings internally
    )


def _save_flow(name: str, description: str, trigger_type: str,
               cron_expr: str, target_script: str, script_args: str,
               is_active: bool) -> bool:
    """INSERT a new flow row. Returns True on success."""
    try:
        db = _get_db()
        db.execute_nonquery(
            """
            INSERT INTO auto_flows
                (name, description, trigger_type, cron_expr,
                 target_script, script_args, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (name, description, trigger_type,
             cron_expr or None, target_script,
             script_args or None, int(is_active))
        )
        return True
    except Exception as e:
        st.error(f"❌ Could not save flow: {e}")
        return False


def _toggle_flow(flow_id: int, new_state: bool) -> None:
    """Flip is_active for a flow."""
    try:
        _get_db().execute_nonquery(
            "UPDATE auto_flows SET is_active = %s WHERE id = %s",
            (int(new_state), flow_id)
        )
    except Exception as e:
        st.error(f"❌ Toggle failed: {e}")


def _delete_flow(flow_id: int) -> None:
    """Hard-delete a flow (cascade removes its run history)."""
    try:
        _get_db().execute_nonquery(
            "DELETE FROM auto_flows WHERE id = %s", (flow_id,)
        )
    except Exception as e:
        st.error(f"❌ Delete failed: {e}")


def _insert_run_record(flow_id: int, triggered_by: str) -> int:
    """
    Insert a 'running' record and return its new id.
    Uses a raw connection because execute_nonquery doesn't return lastrowid.
    """
    from database import DatabaseManager as _DM
    import mysql.connector
    db = _DM()
    cfg = dict(db.connection_config)
    cfg["autocommit"] = True
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO flow_runs (flow_id, triggered_by, status) VALUES (%s, %s, 'running')",
        (flow_id, triggered_by)
    )
    run_id = cur.lastrowid
    cur.close()
    conn.close()
    return run_id


def _update_run_record(run_id: int, status: str, exit_code: int,
                       log_output: str, error_msg: str,
                       duration_sec: float) -> None:
    """Finish a run record."""
    try:
        _get_db().execute_nonquery(
            """
            UPDATE flow_runs
            SET status=%s, exit_code=%s, log_output=%s,
                error_msg=%s, duration_sec=%s, finished_at=NOW()
            WHERE id=%s
            """,
            (status, exit_code, log_output[:65000] if log_output else "",
             error_msg[:490] if error_msg else "", round(duration_sec, 2), run_id)
        )
    except Exception as e:
        # Non-fatal — run completed even if we can't log it
        st.warning(f"⚠️ Could not update run log: {e}")


def _run_script_thread(flow_id: int, run_id: int, script_path: str,
                       script_args: str) -> None:
    """
    Spawns the .py file in a subprocess.
    Runs in a background thread so Streamlit doesn't block.
    Result written back to flow_runs via _update_run_record.
    """
    start = time.time()
    cmd = ["python", script_path]
    if script_args and script_args.strip():
        cmd += script_args.strip().split()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600          # 1-hour hard cap
        )
        duration = time.time() - start
        status   = "success" if proc.returncode == 0 else "failed"
        log_out  = (proc.stdout or "") + ("\n--- STDERR ---\n" + proc.stderr if proc.stderr else "")
        _update_run_record(run_id, status, proc.returncode, log_out, proc.stderr, duration)
    except subprocess.TimeoutExpired:
        duration = time.time() - start
        _update_run_record(run_id, "failed", -1, "", "Timeout after 3600 s", duration)
    except Exception as e:
        duration = time.time() - start
        _update_run_record(run_id, "failed", -1, "", str(e), duration)


def _execute_flow(flow_id: int, script_path: str,
                  script_args: str, triggered_by: str) -> int:
    """
    Public entry point: logs + fires background thread.
    Returns run_id so callers can surface it in the UI.
    """
    run_id = _insert_run_record(flow_id, triggered_by)
    t = threading.Thread(
        target=_run_script_thread,
        args=(flow_id, run_id, script_path, script_args),
        daemon=True
    )
    t.start()
    return run_id


# ─────────────────────────────────────────────
# Status pill helper
# ─────────────────────────────────────────────

_STATUS_STYLE = {
    "success": "background:#e6f4ea;color:#137333;",
    "failed":  "background:#fce8e6;color:#c5221f;",
    "running": "background:#e8f0fe;color:#1a56db;",
}
_STATUS_ICON = {"success": "✅", "failed": "❌", "running": "⏳"}

def _pill(status: str) -> str:
    style = _STATUS_STYLE.get(status, "background:#f1f3f4;color:#444;")
    icon  = _STATUS_ICON.get(status, "•")
    return (f'<span style="{style}padding:2px 10px;'
            f'border-radius:12px;font-size:12px;font-weight:600;">'
            f'{icon} {status}</span>')


# ─────────────────────────────────────────────
# Sub-panels
# ─────────────────────────────────────────────

def _render_stats_row(flows_df: pd.DataFrame, runs_df: pd.DataFrame) -> None:
    """3-card stats strip at the top of the tab."""
    total_flows   = len(flows_df) if flows_df is not None else 0
    active_flows  = (flows_df["is_active"].astype(str) == "1").sum() if total_flows else 0
    today_str     = datetime.now().strftime("%Y-%m-%d")
    runs_today    = 0
    success_rate  = "—"

    if runs_df is not None and not runs_df.empty:
        runs_today = runs_df["started_at"].astype(str).str.startswith(today_str).sum()
        done = runs_df[runs_df["status"].isin(["success", "failed"])]
        if len(done):
            pct = (done["status"] == "success").sum() / len(done) * 100
            success_rate = f"{pct:.0f}%"

    c1, c2, c3 = st.columns(3)
    c1.metric("⚡ Active Flows",  f"{active_flows} / {total_flows}")
    c2.metric("▶️ Runs today",    runs_today)
    c3.metric("✅ Success rate",  success_rate)


def _render_flow_cards(flows_df: pd.DataFrame) -> None:
    """
    One card per flow.
    Shows: name · trigger pill · step pills pipeline · active toggle · Run Now button.
    """
    if flows_df is None or flows_df.empty:
        st.info("ยังไม่มี flow — กด **＋ สร้าง flow ใหม่** ด้านล่าง")
        return

    for _, row in flows_df.iterrows():
        fid       = int(row["id"])
        is_active = str(row["is_active"]) == "1"

        with st.container():
            # ── header row ──────────────────────────────────────────────
            col_name, col_toggle, col_run, col_del = st.columns([5, 1.2, 1.2, 0.8])

            with col_name:
                trigger_color = {
                    "schedule":     "#534AB7",
                    "after_import": "#0b8043",
                    "manual":       "#888",
                }.get(str(row["trigger_type"]), "#888")
                st.markdown(
                    f'<span style="font-size:15px;font-weight:600;">{row["name"]}</span> '
                    f'<span style="background:{trigger_color}20;color:{trigger_color};'
                    f'padding:1px 8px;border-radius:10px;font-size:11px;">'
                    f'{row["trigger_type"]}</span>',
                    unsafe_allow_html=True
                )
                if row.get("description"):
                    st.caption(str(row["description"]))

                # ── step pills pipeline ──────────────────────────────
                # Pills derived from the script path basename for now.
                # Replace with a proper steps column once Flow Builder is done.
                script_name = os.path.basename(str(row["target_script"]))
                cron_label  = str(row["cron_expr"]) if row.get("cron_expr") else ""
                pills_html  = (
                    f'<span style="background:#f3f2ff;color:#534AB7;'
                    f'padding:2px 9px;border-radius:10px;font-size:11px;">'
                    f'🐍 {script_name}</span>'
                )
                if cron_label:
                    pills_html += (
                        f' &rarr; <span style="background:#e8f0fe;color:#1a56db;'
                        f'padding:2px 9px;border-radius:10px;font-size:11px;">'
                        f'🕐 {cron_label}</span>'
                    )
                st.markdown(pills_html, unsafe_allow_html=True)

            with col_toggle:
                new_state = st.toggle(
                    "เปิด",
                    value=is_active,
                    key=f"toggle_{fid}",
                    help="เปิด/ปิด flow นี้"
                )
                if new_state != is_active:
                    _toggle_flow(fid, new_state)
                    st.rerun()

            with col_run:
                if st.button("▶ Run", key=f"run_{fid}",
                             type="primary", use_container_width=True,
                             disabled=not is_active):
                    username = st.session_state.get("current_import_user", "manual")
                    run_id = _execute_flow(
                        fid,
                        str(row["target_script"]),
                        str(row.get("script_args") or ""),
                        username,
                    )
                    st.success(f"🚀 Flow started — run #{run_id}")
                    time.sleep(0.5)
                    st.rerun()

            with col_del:
                if st.button("🗑", key=f"del_{fid}",
                             help="ลบ flow นี้", use_container_width=True):
                    st.session_state[f"confirm_del_{fid}"] = True

            # ── delete confirm ───────────────────────────────────────
            if st.session_state.get(f"confirm_del_{fid}"):
                st.warning(f"ยืนยันลบ **{row['name']}**?")
                y, n = st.columns(2)
                if y.button("ใช่ ลบเลย", key=f"yes_del_{fid}", type="primary"):
                    _delete_flow(fid)
                    st.session_state.pop(f"confirm_del_{fid}", None)
                    st.rerun()
                if n.button("ยกเลิก", key=f"no_del_{fid}"):
                    st.session_state.pop(f"confirm_del_{fid}", None)
                    st.rerun()

            st.divider()


def _render_create_flow_form() -> None:
    """Expander with form to add a new flow."""
    with st.expander("＋ สร้าง flow ใหม่", expanded=False):
        with st.form("create_flow_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                name = st.text_input(
                    "ชื่อ flow *",
                    placeholder="เช่น Daily Broadband Report"
                )
                trigger_type = st.selectbox(
                    "Trigger",
                    ["manual", "schedule", "after_import"],
                    help=(
                        "manual — กด Run เอง\n"
                        "schedule — ตามเวลา (cron)\n"
                        "after_import — หลัง Import Data สำเร็จ"
                    )
                )
                cron_expr = st.text_input(
                    "Cron expression",
                    placeholder="0 8 * * 1-5  (จันทร์-ศุกร์ 08:00)",
                    help="ใช้ได้เมื่อ trigger = schedule เท่านั้น"
                )
            with c2:
                target_script = st.text_input(
                    "Path ของ .py file *",
                    placeholder="scripts/daily_report.py"
                )
                script_args = st.text_input(
                    "Arguments (optional)",
                    placeholder="--env prod --date today"
                )
                description = st.text_input(
                    "คำอธิบาย",
                    placeholder="สร้าง report ประจำวัน"
                )
                is_active = st.checkbox("เปิดใช้งานทันที", value=True)

            submitted = st.form_submit_button(
                "💾 บันทึก Flow", type="primary", use_container_width=True
            )
            if submitted:
                if not name.strip() or not target_script.strip():
                    st.error("กรุณากรอก ชื่อ flow และ Path ของ .py file")
                elif not os.path.isfile(target_script.strip()):
                    st.warning(
                        f"⚠️ ไม่พบไฟล์ `{target_script.strip()}` "
                        f"— บันทึกไว้ก่อน แต่ Run อาจล้มเหลวถ้า path ผิด"
                    )
                    if _save_flow(name, description, trigger_type, cron_expr,
                                  target_script, script_args, is_active):
                        st.success("✅ บันทึก flow แล้ว (กรุณาตรวจ path)")
                        st.rerun()
                else:
                    if _save_flow(name, description, trigger_type, cron_expr,
                                  target_script, script_args, is_active):
                        st.success("✅ สร้าง flow เรียบร้อย")
                        st.rerun()


def _render_run_history(runs_df: pd.DataFrame) -> None:
    """Collapsible recent-runs table + log viewer."""
    with st.expander("📋 Recent runs (30 ล่าสุด)", expanded=False):
        if runs_df is None or runs_df.empty:
            st.info("ยังไม่มีประวัติการรัน")
            return

        # Build display table
        display = runs_df[
            ["id", "flow_name", "triggered_by", "status",
             "started_at", "duration_sec", "exit_code"]
        ].copy()
        display["status"] = display["status"].apply(
            lambda s: f"{_STATUS_ICON.get(s,'•')} {s}"
        )
        display.columns = [
            "Run ID", "Flow", "ทำโดย", "สถานะ",
            "เริ่ม", "ใช้เวลา (วิ)", "Exit code"
        ]
        display.index = range(1, len(display) + 1)
        st.dataframe(display, use_container_width=True)

        # ── Log viewer ────────────────────────────────────────────────
        st.markdown("#### 🔍 ดู Log ของ run")
        run_ids = runs_df["id"].tolist()
        chosen_id = st.selectbox(
            "เลือก Run ID",
            options=run_ids,
            format_func=lambda x: (
                f"#{x}  {runs_df.loc[runs_df['id']==x, 'flow_name'].values[0]}"
                f"  ({runs_df.loc[runs_df['id']==x, 'started_at'].values[0]})"
            ),
            key="log_viewer_select"
        )
        if st.button("แสดง Log", key="btn_show_log"):
            db = _get_db()
            log_df = db.execute_query(
                "SELECT log_output, error_msg FROM flow_runs WHERE id = %s",
                (str(chosen_id),)
            )
            if log_df is not None and not log_df.empty:
                log_text = str(log_df.iloc[0]["log_output"] or "(ไม่มี stdout)")
                err_text = str(log_df.iloc[0]["error_msg"] or "")
                st.code(log_text, language="text")
                if err_text:
                    st.error(f"stderr: {err_text}")


# ─────────────────────────────────────────────
# Main entry point (called by app.py)
# ─────────────────────────────────────────────

def render_autoflow_tab() -> None:
    """
    Self-contained Auto Flow tab.
    Safe to call from app.py — touches no other tab's state.
    """
    st.header("⚡ Auto Flow")
    st.caption("จัดการและรัน .py scripts แบบตั้งเวลาหรือ manual")

    # ── Guard: DB must be available ──────────────────────────────────
    if "db_manager" not in st.session_state:
        st.error("❌ ยังไม่ได้เชื่อมต่อฐานข้อมูล")
        return

    # ── Load data ────────────────────────────────────────────────────
    flows_df = _load_flows()
    runs_df  = _load_recent_runs()

    # ── Stats strip ──────────────────────────────────────────────────
    _render_stats_row(flows_df, runs_df)
    st.divider()

    # ── Flow cards ───────────────────────────────────────────────────
    st.subheader("📦 Flows")

    col_refresh, col_spacer = st.columns([1, 7])
    with col_refresh:
        if st.button("🔄 Refresh", key="autoflow_refresh", use_container_width=True):
            st.rerun()

    _render_flow_cards(flows_df)

    # ── Create form ──────────────────────────────────────────────────
    _render_create_flow_form()

    st.divider()

    # ── Run history ──────────────────────────────────────────────────
    _render_run_history(runs_df)

    # ── Footer note ──────────────────────────────────────────────────
    st.caption(
        "💡 trigger=schedule ต้องการ scheduler ภายนอก (เช่น APScheduler / cron) "
        "ที่เรียก _execute_flow() ตามเวลา — ส่วนนี้พร้อมเสียบใน Phase 2"
    )
