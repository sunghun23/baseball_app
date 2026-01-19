#깃허브 업로드 및 푸쉬
#git add .
#git commit -m "update: all project files (templates, static, app.py etc)"
#git push

# app.py (FINAL STABLE VERSION)
import os
from functools import wraps
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, jsonify
)
import psycopg2
from psycopg2.extras import RealDictCursor

# -------------------- Config --------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret_key")
ADMIN_CODE = os.getenv("ADMIN_CODE", "@apfhsk12")
DB_URL = os.getenv("DATABASE_URL")  # Render env var ONLY

# -------------------- DB Helpers --------------------
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")

    db_url = DB_URL
    if "sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"

    return psycopg2.connect(
        db_url,
        cursor_factory=RealDictCursor,
        connect_timeout=5  # 🔥 Render + Supabase 필수
    )

# -------------------- Auth --------------------
def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            flash("관리자 권한이 필요합니다.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper

# -------------------- Stat Helpers --------------------
def calc_player_avg(conn, player_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(ab),0) ab, COALESCE(SUM(hits),0) hits FROM batting WHERE player_id=%s",
            (player_id,)
        )
        r = cur.fetchone()
    return round(r["hits"] / r["ab"], 3) if r["ab"] > 0 else 0.0


def calc_player_era(conn, player_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(innings),0) inn, COALESCE(SUM(er),0) er FROM pitching WHERE player_id=%s",
            (player_id,)
        )
        r = cur.fetchone()
    return round((r["er"] * 9) / r["inn"], 2) if r["inn"] > 0 else 0.0

# -------------------- Login --------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("code") == ADMIN_CODE:
            session["is_admin"] = True
            flash("관리자 로그인 성공", "success")
            return redirect(url_for("index"))
        flash("관리자 코드가 올바르지 않습니다.", "danger")
    return render_template("login.html", is_admin=session.get("is_admin", False))


@app.route("/logout")
def logout():
    session.clear()
    flash("로그아웃 되었습니다.", "info")
    return redirect(url_for("index"))

# -------------------- Home --------------------
@app.route("/")
def index():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(b.ab),0) ab,
                   COALESCE(SUM(b.hits),0) hits,
                   COALESCE(SUM(b.hr),0) hr,
                   COALESCE(SUM(b.rbi),0) rbi,
                   ROUND(
                     CASE WHEN SUM(b.ab)>0
                     THEN SUM(b.hits)::numeric/SUM(b.ab)
                     ELSE 0 END, 3
                   ) avg
            FROM players p
            LEFT JOIN batting b ON p.id=b.player_id
            GROUP BY p.id
            ORDER BY avg DESC
        """)
        bat = cur.fetchall()

        cur.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(pi.innings),0) inn,
                   COALESCE(SUM(pi.er),0) er,
                   COALESCE(SUM(pi.so),0) so,
                   COALESCE(SUM(pi.bb),0) bb,
                   ROUND(
                     CASE WHEN SUM(pi.innings)>0
                     THEN (SUM(pi.er)*9)::numeric/SUM(pi.innings)
                     ELSE 0 END, 2
                   ) era
            FROM players p
            LEFT JOIN pitching pi ON p.id=pi.player_id
            GROUP BY p.id
            ORDER BY era ASC
        """)
        pit = cur.fetchall()
    conn.close()

    return render_template(
        "index.html",
        bat_stats=bat,
        pit_stats=pit,
        is_admin=session.get("is_admin", False)
    )

# -------------------- Player Detail --------------------
@app.route("/player/<int:player_id>")
def player_detail(player_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM players WHERE id=%s", (player_id,))
        player = cur.fetchone()
        if not player:
            conn.close()
            flash("선수를 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))

        cur.execute("""
            SELECT * FROM batting
            WHERE player_id=%s
            ORDER BY created_at
        """, (player_id,))
        batting = cur.fetchall()

        cur.execute("""
            SELECT * FROM pitching
            WHERE player_id=%s
            ORDER BY created_at
        """, (player_id,))
        pitching = cur.fetchall()

        avg = calc_player_avg(conn, player_id)
        era = calc_player_era(conn, player_id)

    conn.close()

    return render_template(
        "player_detail.html",
        player=player,
        batting=batting,
        pitching=pitching,
        agg_avg=avg,
        agg_era=era,
        is_admin=session.get("is_admin", False)
    )

# -------------------- Health Check --------------------
@app.route("/healthz")
def healthz():
    return "ok", 200

# -------------------- Local Run --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
