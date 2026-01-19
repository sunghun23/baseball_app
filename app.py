#깃허브 업로드 및 푸쉬
#git add .
#git commit -m "update: all project files (templates, static, app.py etc)"
#git push

import os
from functools import wraps
from flask import (
    Flask, jsonify, request,
    render_template, redirect, url_for,
    session, flash
)
import psycopg2
from psycopg2.extras import RealDictCursor

# =========================================================
# App Config
# =========================================================

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key")
ADMIN_CODE = os.getenv("ADMIN_CODE", "admin123")
DATABASE_URL = os.getenv("DATABASE_URL")

# =========================================================
# DB Connection (Render-safe)
# =========================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    db_url = DATABASE_URL
    if "sslmode=" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"

    return psycopg2.connect(
        db_url,
        cursor_factory=RealDictCursor,
        connect_timeout=5
    )

# =========================================================
# Health Check (Render)
# =========================================================

@app.route("/healthz")
def healthz():
    return "ok", 200

# =========================================================
# Auth Guard
# =========================================================

def admin_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if not session.get("is_admin"):
            flash("관리자 권한이 필요합니다.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrap

# =========================================================
# Login
# =========================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("code") == ADMIN_CODE:
            session["is_admin"] = True
            flash("관리자 로그인 성공", "success")
            return redirect(url_for("index"))
        flash("관리자 코드가 올바르지 않습니다.", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("로그아웃 되었습니다.", "info")
    return redirect(url_for("index"))

# =========================================================
# Home
# =========================================================

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
                       CASE WHEN SUM(b.ab)>0 THEN SUM(b.hits)*1.0/SUM(b.ab) ELSE 0 END
                   ,3) avg
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
                       CASE WHEN SUM(pi.innings)>0 THEN (SUM(pi.er)*9)/SUM(pi.innings) ELSE 0 END
                   ,2) era
            FROM players p
            LEFT JOIN pitching pi ON p.id=pi.player_id
            GROUP BY p.id
            ORDER BY era ASC
        """)
        pit = cur.fetchall()

    conn.close()
    return render_template("index.html", bat_stats=bat, pit_stats=pit)

# =========================================================
# Player CRUD
# =========================================================

@app.route("/players/new", methods=["POST"])
@admin_required
def add_player():
    name = request.form["name"]
    team = request.form["team"]

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO players (name, team) VALUES (%s, %s)",
            (name, team)
        )
    conn.commit()
    conn.close()
    flash("선수 추가 완료", "success")
    return redirect(url_for("index"))

@app.route("/players/<int:player_id>/delete")
@admin_required
def delete_player(player_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM players WHERE id=%s", (player_id,))
    conn.commit()
    conn.close()
    flash("선수 삭제 완료", "info")
    return redirect(url_for("index"))

# =========================================================
# Player Detail
# =========================================================

@app.route("/players/<int:player_id>")
def player_detail(player_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM players WHERE id=%s", (player_id,))
        player = cur.fetchone()

        cur.execute(
            "SELECT * FROM batting WHERE player_id=%s ORDER BY created_at DESC",
            (player_id,)
        )
        batting = cur.fetchall()

        cur.execute(
            "SELECT * FROM pitching WHERE player_id=%s ORDER BY created_at DESC",
            (player_id,)
        )
        pitching = cur.fetchall()

    conn.close()
    return render_template(
        "player_detail.html",
        player=player,
        batting=batting,
        pitching=pitching
    )

# =========================================================
# Batting CRUD
# =========================================================

@app.route("/batting/add/<int:player_id>", methods=["POST"])
@admin_required
def add_batting(player_id):
    ab = int(request.form["ab"])
    hits = int(request.form["hits"])
    hr = int(request.form.get("hr", 0))
    rbi = int(request.form.get("rbi", 0))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO batting (player_id, ab, hits, hr, rbi)
            VALUES (%s, %s, %s, %s, %s)
        """, (player_id, ab, hits, hr, rbi))
    conn.commit()
    conn.close()

    flash("타격 기록 추가", "success")
    return redirect(url_for("player_detail", player_id=player_id))

# =========================================================
# Pitching CRUD
# =========================================================

@app.route("/pitching/add/<int:player_id>", methods=["POST"])
@admin_required
def add_pitching(player_id):
    innings = float(request.form["innings"])
    er = int(request.form["er"])
    so = int(request.form.get("so", 0))
    bb = int(request.form.get("bb", 0))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pitching (player_id, innings, er, so, bb)
            VALUES (%s, %s, %s, %s, %s)
        """, (player_id, innings, er, so, bb))
    conn.commit()
    conn.close()

    flash("투수 기록 추가", "success")
    return redirect(url_for("player_detail", player_id=player_id))
