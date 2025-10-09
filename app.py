# app.py
import os
import sqlite3
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify

# -------------------- App / Config --------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret_key")
ADMIN_CODE = os.getenv("ADMIN_CODE", "admin123")
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "baseball.db"))

# -------------------- DB Helpers --------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def safe_add_column(conn, table, col_def):
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # already exists

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            position TEXT,
            team TEXT
        );

        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            date TEXT
        );

        CREATE TABLE IF NOT EXISTS batting (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_id INTEGER,
            ab INTEGER DEFAULT 0,
            hits INTEGER DEFAULT 0,
            hr INTEGER DEFAULT 0,
            rbi INTEGER DEFAULT 0,
            avg REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS pitching (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_id INTEGER,
            innings REAL DEFAULT 0,
            er INTEGER DEFAULT 0,
            so INTEGER DEFAULT 0,
            bb INTEGER DEFAULT 0,
            era REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_batting_player ON batting(player_id);
        CREATE INDEX IF NOT EXISTS idx_batting_game ON batting(game_id);
        CREATE INDEX IF NOT EXISTS idx_pitching_player ON pitching(player_id);
        CREATE INDEX IF NOT EXISTS idx_pitching_game ON pitching(game_id);
    """)
    # add missing columns if legacy DB
    safe_add_column(conn, "games", "location TEXT")
    safe_add_column(conn, "batting", "created_at TEXT DEFAULT CURRENT_TIMESTAMP")
    safe_add_column(conn, "pitching", "created_at TEXT DEFAULT CURRENT_TIMESTAMP")
    safe_add_column(conn, "batting", "avg REAL")
    safe_add_column(conn, "pitching", "era REAL")
    conn.close()

init_db()

# -------------------- Auth / Guards --------------------
def admin_required(f):
    @wraps(f)
    def _wrap(*args, **kwargs):
        if not session.get("is_admin"):
            flash("관리자 권한이 필요합니다.", "danger")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return _wrap

# -------------------- Stats Helpers --------------------
def calc_player_avg(conn, player_id):
    row = conn.execute(
        "SELECT COALESCE(SUM(ab),0) ab, COALESCE(SUM(hits),0) hits FROM batting WHERE player_id=?",
        (player_id,)
    ).fetchone()
    ab, hits = row["ab"] or 0, row["hits"] or 0
    return round(hits / ab, 3) if ab > 0 else 0.0

def calc_player_era(conn, player_id):
    row = conn.execute(
        "SELECT COALESCE(SUM(innings),0.0) inn, COALESCE(SUM(er),0) er FROM pitching WHERE player_id=?",
        (player_id,)
    ).fetchone()
    inn, er = float(row["inn"] or 0.0), row["er"] or 0
    return round((er * 9.0) / inn, 2) if inn > 0 else 0.0

def recalc_batting_snapshots(conn, player_id):
    rows = conn.execute(
        "SELECT id, ab, hits FROM batting WHERE player_id=? ORDER BY COALESCE((SELECT date FROM games g WHERE g.id=batting.game_id), created_at), id",
        (player_id,)
    ).fetchall()
    cum_ab = 0
    cum_hits = 0
    for r in rows:
        cum_ab += r["ab"] or 0
        cum_hits += r["hits"] or 0
        snap = round(cum_hits / cum_ab, 3) if cum_ab > 0 else 0.0
        conn.execute("UPDATE batting SET avg=? WHERE id=?", (snap, r["id"]))
    conn.commit()

def recalc_pitching_snapshots(conn, player_id):
    rows = conn.execute(
        "SELECT id, innings, er FROM pitching WHERE player_id=? ORDER BY COALESCE((SELECT date FROM games g WHERE g.id=pitching.game_id), created_at), id",
        (player_id,)
    ).fetchall()
    cum_inn = 0.0
    cum_er = 0
    for r in rows:
        cum_inn += float(r["innings"] or 0.0)
        cum_er += r["er"] or 0
        snap = round((cum_er * 9.0) / cum_inn, 2) if cum_inn > 0 else 0.0
        conn.execute("UPDATE pitching SET era=? WHERE id=?", (snap, r["id"]))
    conn.commit()

# -------------------- Login --------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        code = request.form.get("code", "").strip()
        if code == ADMIN_CODE:
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

# -------------------- Home / Search / API --------------------
@app.route("/")
def index():
    conn = get_db()
    bat_stats = conn.execute("""
        SELECT p.id, p.name, p.team,
               COALESCE(SUM(b.ab),0) ab,
               COALESCE(SUM(b.hits),0) hits,
               COALESCE(SUM(b.hr),0) hr,
               COALESCE(SUM(b.rbi),0) rbi,
               ROUND(CASE WHEN SUM(b.ab)>0 THEN SUM(b.hits)*1.0/SUM(b.ab) ELSE 0 END, 3) avg
        FROM players p
        LEFT JOIN batting b ON p.id=b.player_id
        GROUP BY p.id
        ORDER BY avg DESC, hits DESC, hr DESC
    """).fetchall()

    pit_stats = conn.execute("""
        SELECT p.id, p.name, p.team,
               COALESCE(SUM(pi.innings),0) inn,
               COALESCE(SUM(pi.er),0) er,
               COALESCE(SUM(pi.so),0) so,
               COALESCE(SUM(pi.bb),0) bb,
               ROUND(CASE WHEN SUM(pi.innings)>0 THEN SUM(pi.er)*9.0/SUM(pi.innings) ELSE 0 END, 2) era
        FROM players p
        LEFT JOIN pitching pi ON p.id=pi.player_id
        GROUP BY p.id
        ORDER BY era ASC, so DESC
    """).fetchall()
    conn.close()
    return render_template("index.html",
                           bat_stats=bat_stats, pit_stats=pit_stats,
                           is_admin=session.get("is_admin", False))

@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    players = []
    if q:
        conn = get_db()
        players = conn.execute("SELECT * FROM players WHERE name LIKE ? ORDER BY name", (f"%{q}%",)).fetchall()
        conn.close()
    return render_template("search.html", results=players, is_admin=session.get("is_admin", False))

@app.route("/api/players")
def api_players():
    q = request.args.get("q", "").strip()
    conn = get_db()
    if q:
        rows = conn.execute("SELECT id, name, position, team FROM players WHERE name LIKE ? ORDER BY name LIMIT 20",
                            (f"%{q}%",)).fetchall()
    else:
        rows = conn.execute("SELECT id, name, position, team FROM players ORDER BY name LIMIT 20").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# -------------------- Players CRUD --------------------
@app.route("/add_player", methods=["GET", "POST"])
@admin_required
def add_player():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        position = request.form.get("position", "").strip()
        team = request.form.get("team", "").strip()
        if not name:
            flash("이름을 입력하세요.", "warning")
            return redirect(url_for("add_player"))
        conn = get_db()
        dup = conn.execute("SELECT 1 FROM players WHERE name=?", (name,)).fetchone()
        if dup:
            flash("이미 존재하는 선수입니다. (이름 중복 방지)", "warning")
        else:
            conn.execute("INSERT INTO players (name, position, team) VALUES (?,?,?)", (name, position, team))
            conn.commit()
            flash("선수 등록 완료", "success")
        conn.close()
        return redirect(url_for("index"))
    return render_template("add_player.html", is_admin=True)

@app.route("/edit_player/<int:player_id>", methods=["GET", "POST"])
@admin_required
def edit_player(player_id):
    conn = get_db()
    player = conn.execute("SELECT * FROM players WHERE id=?", (player_id,)).fetchone()
    if not player:
        conn.close()
        flash("선수를 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        position = request.form.get("position", "").strip()
        team = request.form.get("team", "").strip()
        dup = conn.execute("SELECT 1 FROM players WHERE name=? AND id<>?", (name, player_id)).fetchone()
        if dup:
            flash("동일 이름의 선수가 이미 존재합니다.", "warning")
            conn.close()
            return redirect(url_for("edit_player", player_id=player_id))
        conn.execute("UPDATE players SET name=?, position=?, team=? WHERE id=?", (name, position, team, player_id))
        conn.commit()
        conn.close()
        flash("선수 정보가 수정되었습니다.", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    conn.close()
    return render_template("edit_player.html", player=player, is_admin=True)

@app.route("/delete_player/<int:player_id>", methods=["POST"])
@admin_required
def delete_player(player_id):
    conn = get_db()
    conn.execute("DELETE FROM players WHERE id=?", (player_id,))
    conn.commit()
    conn.close()
    flash("선수를 삭제했습니다.", "info")
    return redirect(url_for("index"))

# -------------------- Games CRUD --------------------
@app.route("/add_game", methods=["GET", "POST"])
@admin_required
def add_game():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        date = request.form.get("date", "").strip() or None
        location = request.form.get("location", "").strip() or None
        if not name:
            flash("경기명을 입력하세요.", "warning")
            return redirect(url_for("add_game"))
        conn = get_db()
        conn.execute("INSERT INTO games (name, date, location) VALUES (?,?,?)", (name, date, location))
        conn.commit()
        conn.close()
        flash("경기 등록 완료", "success")
        return redirect(url_for("leaderboard"))
    return render_template("add_game.html", is_admin=True)

@app.route("/edit_game/<int:game_id>", methods=["GET", "POST"])
@admin_required
def edit_game(game_id):
    conn = get_db()
    game = conn.execute("SELECT * FROM games WHERE id=?", (game_id,)).fetchone()
    if not game:
        conn.close()
        flash("경기를 찾을 수 없습니다.", "danger")
        return redirect(url_for("leaderboard"))
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        date = request.form.get("date", "").strip() or None
        location = request.form.get("location", "").strip() or None
        if not name:
            flash("경기명을 입력하세요.", "warning")
            conn.close()
            return redirect(url_for("edit_game", game_id=game_id))
        conn.execute("UPDATE games SET name=?, date=?, location=? WHERE id=?", (name, date, location, game_id))
        conn.commit()
        conn.close()
        flash("경기 정보가 수정되었습니다.", "success")
        return redirect(url_for("leaderboard", game_id=game_id))
    conn.close()
    return render_template("edit_game.html", game=game, is_admin=True)

@app.route("/delete_game/<int:game_id>", methods=["POST"])
@admin_required
def delete_game(game_id):
    conn = get_db()
    conn.execute("DELETE FROM games WHERE id=?", (game_id,))
    conn.commit()
    conn.close()
    flash("경기를 삭제했습니다.", "info")
    return redirect(url_for("leaderboard"))

# -------------------- Batting CRUD (+ auto AVG) --------------------
@app.route("/add_batting", methods=["GET", "POST"])
@admin_required
def add_batting():
    conn = get_db()
    games = conn.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC").fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        ab = int(request.form.get("ab") or 0)
        hits = int(request.form.get("hits") or 0)
        hr = int(request.form.get("hr") or 0)
        rbi = int(request.form.get("rbi") or 0)

        pre = conn.execute("SELECT COALESCE(SUM(ab),0) ab, COALESCE(SUM(hits),0) hits FROM batting WHERE player_id=?", (player_id,)).fetchone()
        cum_ab = (pre["ab"] or 0) + ab
        cum_hits = (pre["hits"] or 0) + hits
        snap = round(cum_hits / cum_ab, 3) if cum_ab > 0 else 0.0

        conn.execute("INSERT INTO batting (player_id, game_id, ab, hits, hr, rbi, avg) VALUES (?,?,?,?,?,?,?)",
                     (player_id, game_id, ab, hits, hr, rbi, snap))
        conn.commit()
        recalc_batting_snapshots(conn, player_id)
        conn.close()
        flash("타격 기록 추가 완료", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    players = conn.execute("SELECT id, name, position FROM players ORDER BY name").fetchall()
    conn.close()
    return render_template("add_batting.html", games=games, players=players, is_admin=True)

@app.route("/batting/<int:record_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_batting(record_id):
    conn = get_db()
    rec = conn.execute("SELECT * FROM batting WHERE id=?", (record_id,)).fetchone()
    if not rec:
        conn.close()
        flash("기록을 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))
    games = conn.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC").fetchall()
    players = conn.execute("SELECT id, name FROM players ORDER BY name").fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        ab = int(request.form.get("ab") or 0)
        hits = int(request.form.get("hits") or 0)
        hr = int(request.form.get("hr") or 0)
        rbi = int(request.form.get("rbi") or 0)

        conn.execute("UPDATE batting SET player_id=?, game_id=?, ab=?, hits=?, hr=?, rbi=? WHERE id=?",
                     (player_id, game_id, ab, hits, hr, rbi, record_id))
        conn.commit()
        recalc_batting_snapshots(conn, player_id)
        conn.close()
        flash("타격 기록이 수정되었습니다.", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    conn.close()
    return render_template("edit_batting.html", rec=rec, games=games, players=players, is_admin=True)

@app.route("/batting/<int:record_id>/delete", methods=["POST"])
@admin_required
def delete_batting(record_id):
    conn = get_db()
    rec = conn.execute("SELECT player_id FROM batting WHERE id=?", (record_id,)).fetchone()
    if not rec:
        conn.close()
        flash("기록을 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))
    player_id = rec["player_id"]
    conn.execute("DELETE FROM batting WHERE id=?", (record_id,))
    conn.commit()
    recalc_batting_snapshots(conn, player_id)
    conn.close()
    flash("타격 기록이 삭제되었습니다.", "info")
    return redirect(url_for("player_detail", player_id=player_id))

# -------------------- Pitching CRUD (+ auto ERA) --------------------
@app.route("/add_pitching", methods=["GET", "POST"])
@admin_required
def add_pitching():
    conn = get_db()
    games = conn.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC").fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        innings = float(request.form.get("innings") or 0)
        er = int(request.form.get("er") or 0)
        so = int(request.form.get("so") or 0)
        bb = int(request.form.get("bb") or 0)

        pre = conn.execute("SELECT COALESCE(SUM(innings),0.0) inn, COALESCE(SUM(er),0) er FROM pitching WHERE player_id=?",
                           (player_id,)).fetchone()
        cum_inn = float(pre["inn"] or 0.0) + float(innings)
        cum_er = (pre["er"] or 0) + er
        snap = round((cum_er * 9.0) / cum_inn, 2) if cum_inn > 0 else 0.0

        conn.execute("INSERT INTO pitching (player_id, game_id, innings, er, so, bb, era) VALUES (?,?,?,?,?,?,?)",
                     (player_id, game_id, innings, er, so, bb, snap))
        conn.commit()
        recalc_pitching_snapshots(conn, player_id)
        conn.close()
        flash("투수 기록 추가 완료", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    players = conn.execute("SELECT id, name, position FROM players ORDER BY name").fetchall()
    conn.close()
    return render_template("add_pitching.html", games=games, players=players, is_admin=True)

@app.route("/pitching/<int:record_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_pitching(record_id):
    conn = get_db()
    rec = conn.execute("SELECT * FROM pitching WHERE id=?", (record_id,)).fetchone()
    if not rec:
        conn.close()
        flash("기록을 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))
    games = conn.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC").fetchall()
    players = conn.execute("SELECT id, name FROM players ORDER BY name").fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        innings = float(request.form.get("innings") or 0)
        er = int(request.form.get("er") or 0)
        so = int(request.form.get("so") or 0)
        bb = int(request.form.get("bb") or 0)

        conn.execute("UPDATE pitching SET player_id=?, game_id=?, innings=?, er=?, so=?, bb=? WHERE id=?",
                     (player_id, game_id, innings, er, so, bb, record_id))
        conn.commit()
        recalc_pitching_snapshots(conn, player_id)
        conn.close()
        flash("투수 기록이 수정되었습니다.", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    conn.close()
    return render_template("edit_pitching.html", rec=rec, games=games, players=players, is_admin=True)

@app.route("/pitching/<int:record_id>/delete", methods=["POST"])
@admin_required
def delete_pitching(record_id):
    conn = get_db()
    rec = conn.execute("SELECT player_id FROM pitching WHERE id=?", (record_id,)).fetchone()
    if not rec:
        conn.close()
        flash("기록을 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))
    player_id = rec["player_id"]
    conn.execute("DELETE FROM pitching WHERE id=?", (record_id,))
    conn.commit()
    recalc_pitching_snapshots(conn, player_id)
    conn.close()
    flash("투수 기록이 삭제되었습니다.", "info")
    return redirect(url_for("player_detail", player_id=player_id))

# -------------------- Player Detail (graphs + history) --------------------
@app.route("/player/<int:player_id>")
def player_detail(player_id):
    conn = get_db()
    player = conn.execute("SELECT * FROM players WHERE id=?", (player_id,)).fetchone()
    if not player:
        conn.close()
        flash("선수를 찾을 수 없습니다.", "danger")
        return redirect(url_for("index"))

    batting = conn.execute("""
        SELECT b.id, g.name AS game_name, g.date AS game_date,
               b.ab, b.hits, b.hr, b.rbi,
               ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) AS per_game_avg,
               b.avg AS snapshot_avg, b.created_at
        FROM batting b
        LEFT JOIN games g ON g.id=b.game_id
        WHERE b.player_id=?
        ORDER BY COALESCE(g.date, b.created_at), b.id
    """, (player_id,)).fetchall()

    pitching = conn.execute("""
        SELECT p.id, g.name AS game_name, g.date AS game_date,
               p.innings, p.er, p.so, p.bb,
               ROUND(CASE WHEN p.innings>0 THEN p.er*9.0/p.innings ELSE 0 END, 2) AS per_game_era,
               p.era AS snapshot_era, p.created_at
        FROM pitching p
        LEFT JOIN games g ON g.id=p.game_id
        WHERE p.player_id=?
        ORDER BY COALESCE(g.date, p.created_at), p.id
    """, (player_id,)).fetchall()

    totals_b = conn.execute(
        "SELECT COALESCE(SUM(ab),0) ab, COALESCE(SUM(hits),0) hits, COALESCE(SUM(hr),0) hr, COALESCE(SUM(rbi),0) rbi FROM batting WHERE player_id=?",
        (player_id,)
    ).fetchone()
    totals_p = conn.execute(
        "SELECT COALESCE(SUM(innings),0) inn, COALESCE(SUM(er),0) er, COALESCE(SUM(so),0) so, COALESCE(SUM(bb),0) bb FROM pitching WHERE player_id=?",
        (player_id,)
    ).fetchone()

    agg_avg = calc_player_avg(conn, player_id)
    agg_era = calc_player_era(conn, player_id)
    conn.close()

    b_labels, b_avg_series, b_hr_cum, b_rbi_cum = [], [], [], []
    hrc, rbic = 0, 0
    for r in batting:
        label = r["game_date"] or r["game_name"] or r["created_at"][:10]
        b_labels.append(label)
        b_avg_series.append(r["per_game_avg"])
        hrc += r["hr"] or 0
        rbic += r["rbi"] or 0
        b_hr_cum.append(hrc)
        b_rbi_cum.append(rbic)

    p_labels, p_era_series, p_k_cum, p_ip_cum = [], [], [], []
    kc, ipc = 0, 0.0
    for r in pitching:
        label = r["game_date"] or r["game_name"] or r["created_at"][:10]
        p_labels.append(label)
        p_era_series.append(r["per_game_era"])
        kc += r["so"] or 0
        ipc += float(r["innings"] or 0.0)
        p_k_cum.append(kc)
        p_ip_cum.append(round(ipc, 1))

    return render_template(
        "player_detail.html",
        player=player,
        batting=batting,
        pitching=pitching,
        totals_b=totals_b,
        totals_p=totals_p,
        agg_avg=agg_avg,
        agg_era=agg_era,
        b_labels=b_labels,
        b_avg_series=b_avg_series,
        b_hr_series=b_hr_cum,
        b_rbi_series=b_rbi_cum,
        p_labels=p_labels,
        p_era_series=p_era_series,
        p_k_series=p_k_cum,
        p_ip_series=p_ip_cum,
        is_admin=session.get("is_admin", False)
    )

# -------------------- Leaderboard (overall or by game) --------------------
@app.route("/leaderboard")
def leaderboard():
    game_id = request.args.get("game_id", type=int)
    conn = get_db()
    games = conn.execute("SELECT * FROM games ORDER BY (date IS NULL), date DESC, id DESC").fetchall()

    if game_id:
        bat = conn.execute("""
            SELECT p.id, p.name, p.team, b.ab, b.hits, b.hr, b.rbi,
                   ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) avg
            FROM batting b JOIN players p ON p.id=b.player_id
            WHERE b.game_id=? ORDER BY hits DESC, hr DESC, rbi DESC, avg DESC LIMIT 50
        """, (game_id,)).fetchall()
        pit = conn.execute("""
            SELECT p.id, p.name, p.team, pg.innings, pg.er, pg.so, pg.bb,
                   ROUND(CASE WHEN pg.innings>0 THEN pg.er*9.0/pg.innings ELSE 0 END, 2) era
            FROM pitching pg JOIN players p ON p.id=pg.player_id
            WHERE pg.game_id=? ORDER BY era ASC, so DESC LIMIT 50
        """, (game_id,)).fetchall()
    else:
        bat = conn.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(b.ab),0) ab,
                   COALESCE(SUM(b.hits),0) hits,
                   COALESCE(SUM(b.hr),0) hr,
                   COALESCE(SUM(b.rbi),0) rbi,
                   ROUND(CASE WHEN SUM(b.ab)>0 THEN SUM(b.hits)*1.0/SUM(b.ab) ELSE 0 END, 3) avg
            FROM players p LEFT JOIN batting b ON b.player_id=p.id
            GROUP BY p.id HAVING ab>0
            ORDER BY avg DESC, hits DESC, hr DESC, rbi DESC LIMIT 50
        """).fetchall()
        pit = conn.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(pg.innings),0) inn,
                   COALESCE(SUM(pg.er),0) er,
                   COALESCE(SUM(pg.so),0) so,
                   COALESCE(SUM(pg.bb),0) bb,
                   ROUND(CASE WHEN SUM(pg.innings)>0 THEN SUM(pg.er)*9.0/SUM(pg.innings) ELSE 0 END, 2) era
            FROM players p LEFT JOIN pitching pg ON pg.player_id=p.id
            GROUP BY p.id HAVING inn>0
            ORDER BY era ASC, so DESC LIMIT 50
        """).fetchall()
    conn.close()

    return render_template("leaderboard.html", games=games, game_id=game_id, bat=bat, pit=pit, is_admin=session.get("is_admin", False))

# -------------------- Game detail (optional deep-dive) --------------------
@app.route("/game/<int:game_id>")
def game_detail(game_id):
    conn = get_db()
    game = conn.execute("SELECT * FROM games WHERE id=?", (game_id,)).fetchone()
    if not game:
        conn.close()
        flash("경기를 찾을 수 없습니다.", "danger")
        return redirect(url_for("leaderboard"))
    batting = conn.execute("""
        SELECT b.player_id, p.name AS player_name, b.ab, b.hits, b.hr, b.rbi,
               ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) avg
        FROM batting b JOIN players p ON p.id=b.player_id
        WHERE b.game_id=? ORDER BY avg DESC, hits DESC
    """, (game_id,)).fetchall()
    pitching = conn.execute("""
        SELECT pg.player_id, p.name AS player_name, pg.innings, pg.er, pg.so, pg.bb,
               ROUND(CASE WHEN pg.innings>0 THEN pg.er*9.0/pg.innings ELSE 0 END, 2) era
        FROM pitching pg JOIN players p ON p.id=pg.player_id
        WHERE pg.game_id=? ORDER BY era ASC, so DESC
    """, (game_id,)).fetchall()
    conn.close()
    return render_template("game_detail.html", game=game, batting=batting, pitching=pitching, is_admin=session.get("is_admin", False))

# -------------------- Run --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
