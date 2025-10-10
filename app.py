# app.py
import os
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

# -------------------- Config --------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret_key")
ADMIN_CODE = os.getenv("ADMIN_CODE", "admin123")
DB_URL = os.getenv("DATABASE_URL")  # e.g. postgresql://postgres:xxxx@host:5432/postgres


# -------------------- DB Helpers --------------------
def get_db():
    # DB_URL should include sslmode=require for hosted Postgres (e.g., Supabase/Render)
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            position TEXT,
            team TEXT
        );

        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            date TEXT,
            location TEXT
        );

        CREATE TABLE IF NOT EXISTS batting (
            id SERIAL PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
            game_id INTEGER REFERENCES games(id) ON DELETE SET NULL,
            ab INTEGER DEFAULT 0,
            hits INTEGER DEFAULT 0,
            hr INTEGER DEFAULT 0,
            rbi INTEGER DEFAULT 0,
            avg REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pitching (
            id SERIAL PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
            game_id INTEGER REFERENCES games(id) ON DELETE SET NULL,
            innings REAL DEFAULT 0,
            er INTEGER DEFAULT 0,
            so INTEGER DEFAULT 0,
            bb INTEGER DEFAULT 0,
            era REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_batting_player ON batting(player_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_batting_game ON batting(game_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pitching_player ON pitching(player_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pitching_game ON pitching(game_id);")
    conn.commit()
    cur.close()
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
def calc_player_avg(conn, player_id: int) -> float:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(ab),0), COALESCE(SUM(hits),0) FROM batting WHERE player_id=%s",
            (player_id,)
        )
        ab, hits = cur.fetchone().values()
    return round(hits / ab, 3) if ab > 0 else 0.0


def calc_player_era(conn, player_id: int) -> float:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(innings),0.0), COALESCE(SUM(er),0) FROM pitching WHERE player_id=%s",
            (player_id,)
        )
        inn, er = cur.fetchone().values()
    return round((er * 9.0) / inn, 2) if inn > 0 else 0.0


def recalc_batting_snapshots(conn, player_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.id, b.ab, b.hits
            FROM batting b
            LEFT JOIN games g ON g.id=b.game_id
            WHERE b.player_id=%s
            ORDER BY COALESCE(g.date, to_char(b.created_at,'YYYY-MM-DD')), b.id
        """, (player_id,))
        rows = cur.fetchall()
        cum_ab = 0
        cum_hits = 0
        for r in rows:
            cum_ab += r["ab"] or 0
            cum_hits += r["hits"] or 0
            snap = round(cum_hits / cum_ab, 3) if cum_ab > 0 else 0.0
            cur.execute("UPDATE batting SET avg=%s WHERE id=%s", (snap, r["id"]))
    conn.commit()


def recalc_pitching_snapshots(conn, player_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.innings, p.er
            FROM pitching p
            LEFT JOIN games g ON g.id=p.game_id
            WHERE p.player_id=%s
            ORDER BY COALESCE(g.date, to_char(p.created_at,'YYYY-MM-DD')), p.id
        """, (player_id,))
        rows = cur.fetchall()
        cum_inn = 0.0
        cum_er = 0
        for r in rows:
            cum_inn += float(r["innings"] or 0.0)
            cum_er += r["er"] or 0
            snap = round((cum_er * 9.0) / cum_inn, 2) if cum_inn > 0 else 0.0
            cur.execute("UPDATE pitching SET era=%s WHERE id=%s", (snap, r["id"]))
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
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(b.ab),0) AS ab,
                   COALESCE(SUM(b.hits),0) AS hits,
                   COALESCE(SUM(b.hr),0) AS hr,
                   COALESCE(SUM(b.rbi),0) AS rbi,
                   ROUND(CASE WHEN SUM(b.ab)>0 THEN SUM(b.hits)*1.0/SUM(b.ab) ELSE 0 END, 3) AS avg
            FROM players p
            LEFT JOIN batting b ON p.id=b.player_id
            GROUP BY p.id
            ORDER BY avg DESC, hits DESC, hr DESC
        """)
        bat_stats = cur.fetchall()

        cur.execute("""
            SELECT p.id, p.name, p.team,
                   COALESCE(SUM(pi.innings),0) AS inn,
                   COALESCE(SUM(pi.er),0) AS er,
                   COALESCE(SUM(pi.so),0) AS so,
                   COALESCE(SUM(pi.bb),0) AS bb,
                   ROUND(CASE WHEN SUM(pi.innings)>0 THEN SUM(pi.er)*9.0/SUM(pi.innings) ELSE 0 END, 2) AS era
            FROM players p
            LEFT JOIN pitching pi ON p.id=pi.player_id
            GROUP BY p.id
            ORDER BY era ASC, so DESC
        """)
        pit_stats = cur.fetchall()
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
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM players WHERE name ILIKE %s ORDER BY name", (f"%{q}%",))
            players = cur.fetchall()
        conn.close()
    return render_template("search.html", results=players, is_admin=session.get("is_admin", False))


@app.route("/api/players")
def api_players():
    q = request.args.get("q", "").strip()
    conn = get_db()
    with conn.cursor() as cur:
        if q:
            cur.execute(
                "SELECT id, name, position, team FROM players WHERE name ILIKE %s ORDER BY name LIMIT 20",
                (f"%{q}%",)
            )
        else:
            cur.execute("SELECT id, name, position, team FROM players ORDER BY name LIMIT 20")
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)


# -------------------- Players CRUD --------------------
@app.route("/add_player", methods=["GET", "POST"])
@admin_required
def add_player():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        position = request.form.get("position", "").strip() or None
        team = request.form.get("team", "").strip() or None
        if not name:
            flash("이름을 입력하세요.", "warning")
            return redirect(url_for("add_player"))
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM players WHERE name=%s", (name,))
            dup = cur.fetchone()
            if dup:
                flash("이미 존재하는 선수입니다. (이름 중복 방지)", "warning")
            else:
                cur.execute(
                    "INSERT INTO players (name, position, team) VALUES (%s, %s, %s)",
                    (name, position, team)
                )
                conn.commit()
                flash("선수 등록 완료", "success")
        conn.close()
        return redirect(url_for("index"))
    return render_template("add_player.html", is_admin=True)


@app.route("/edit_player/<int:player_id>", methods=["GET", "POST"])
@admin_required
def edit_player(player_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM players WHERE id=%s", (player_id,))
        player = cur.fetchone()
        if not player:
            conn.close()
            flash("선수를 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            position = request.form.get("position", "").strip() or None
            team = request.form.get("team", "").strip() or None
            cur.execute("SELECT 1 FROM players WHERE name=%s AND id<>%s", (name, player_id))
            dup = cur.fetchone()
            if dup:
                flash("동일 이름의 선수가 이미 존재합니다.", "warning")
                conn.close()
                return redirect(url_for("edit_player", player_id=player_id))
            cur.execute("UPDATE players SET name=%s, position=%s, team=%s WHERE id=%s",
                        (name, position, team, player_id))
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
    with conn.cursor() as cur:
        cur.execute("DELETE FROM players WHERE id=%s", (player_id,))
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
        with conn.cursor() as cur:
            cur.execute("INSERT INTO games (name, date, location) VALUES (%s, %s, %s)",
                        (name, date, location))
        conn.commit()
        conn.close()
        flash("경기 등록 완료", "success")
        return redirect(url_for("leaderboard"))
    return render_template("add_game.html", is_admin=True)


@app.route("/edit_game/<int:game_id>", methods=["GET", "POST"])
@admin_required
def edit_game(game_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM games WHERE id=%s", (game_id,))
        game = cur.fetchone()
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
            cur.execute("UPDATE games SET name=%s, date=%s, location=%s WHERE id=%s",
                        (name, date, location, game_id))
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
    with conn.cursor() as cur:
        cur.execute("DELETE FROM games WHERE id=%s", (game_id,))
    conn.commit()
    conn.close()
    flash("경기를 삭제했습니다.", "info")
    return redirect(url_for("leaderboard"))


# -------------------- Batting CRUD (+ auto AVG) --------------------
@app.route("/add_batting", methods=["GET", "POST"])
@admin_required
def add_batting():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC")
        games = cur.fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        ab = int(request.form.get("ab") or 0)
        hits = int(request.form.get("hits") or 0)
        hr = int(request.form.get("hr") or 0)
        rbi = int(request.form.get("rbi") or 0)

        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(ab),0), COALESCE(SUM(hits),0) FROM batting WHERE player_id=%s",
                        (player_id,))
            pre_ab, pre_hits = cur.fetchone().values()
            cum_ab = (pre_ab or 0) + ab
            cum_hits = (pre_hits or 0) + hits
            snap = round(cum_hits / cum_ab, 3) if cum_ab > 0 else 0.0

            cur.execute("""
                INSERT INTO batting (player_id, game_id, ab, hits, hr, rbi, avg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_id, game_id, ab, hits, hr, rbi, snap))
            conn.commit()
            recalc_batting_snapshots(conn, player_id)
        conn.close()
        flash("타격 기록 추가 완료", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, position FROM players ORDER BY name")
        players = cur.fetchall()
    conn.close()
    return render_template("add_batting.html", games=games, players=players, is_admin=True)


@app.route("/batting/<int:record_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_batting(record_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM batting WHERE id=%s", (record_id,))
        rec = cur.fetchone()
        if not rec:
            conn.close()
            flash("기록을 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))
        cur.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC")
        games = cur.fetchall()
        cur.execute("SELECT id, name FROM players ORDER BY name")
        players = cur.fetchall()

    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        ab = int(request.form.get("ab") or 0)
        hits = int(request.form.get("hits") or 0)
        hr = int(request.form.get("hr") or 0)
        rbi = int(request.form.get("rbi") or 0)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE batting SET player_id=%s, game_id=%s, ab=%s, hits=%s, hr=%s, rbi=%s
                WHERE id=%s
            """, (player_id, game_id, ab, hits, hr, rbi, record_id))
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
    with conn.cursor() as cur:
        cur.execute("SELECT player_id FROM batting WHERE id=%s", (record_id,))
        rec = cur.fetchone()
        if not rec:
            conn.close()
            flash("기록을 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))
        player_id = rec["player_id"]
        cur.execute("DELETE FROM batting WHERE id=%s", (record_id,))
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
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC")
        games = cur.fetchall()
    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        innings = float(request.form.get("innings") or 0)
        er = int(request.form.get("er") or 0)
        so = int(request.form.get("so") or 0)
        bb = int(request.form.get("bb") or 0)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(innings),0.0), COALESCE(SUM(er),0)
                FROM pitching WHERE player_id=%s
            """, (player_id,))
            pre_inn, pre_er = cur.fetchone().values()
            cum_inn = float(pre_inn or 0.0) + float(innings)
            cum_er = (pre_er or 0) + er
            snap = round((cum_er * 9.0) / cum_inn, 2) if cum_inn > 0 else 0.0

            cur.execute("""
                INSERT INTO pitching (player_id, game_id, innings, er, so, bb, era)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_id, game_id, innings, er, so, bb, snap))
            conn.commit()
            recalc_pitching_snapshots(conn, player_id)
        conn.close()
        flash("투수 기록 추가 완료", "success")
        return redirect(url_for("player_detail", player_id=player_id))
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, position FROM players ORDER BY name")
        players = cur.fetchall()
    conn.close()
    return render_template("add_pitching.html", games=games, players=players, is_admin=True)


@app.route("/pitching/<int:record_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_pitching(record_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pitching WHERE id=%s", (record_id,))
        rec = cur.fetchone()
        if not rec:
            conn.close()
            flash("기록을 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))
        cur.execute("SELECT id, name, date FROM games ORDER BY (date IS NULL), date DESC, id DESC")
        games = cur.fetchall()
        cur.execute("SELECT id, name FROM players ORDER BY name")
        players = cur.fetchall()

    if request.method == "POST":
        player_id = int(request.form.get("player_id"))
        game_id_raw = request.form.get("game_id")
        game_id = int(game_id_raw) if game_id_raw else None
        innings = float(request.form.get("innings") or 0)
        er = int(request.form.get("er") or 0)
        so = int(request.form.get("so") or 0)
        bb = int(request.form.get("bb") or 0)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pitching SET player_id=%s, game_id=%s, innings=%s, er=%s, so=%s, bb=%s
                WHERE id=%s
            """, (player_id, game_id, innings, er, so, bb, record_id))
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
    with conn.cursor() as cur:
        cur.execute("SELECT player_id FROM pitching WHERE id=%s", (record_id,))
        rec = cur.fetchone()
        if not rec:
            conn.close()
            flash("기록을 찾을 수 없습니다.", "danger")
            return redirect(url_for("index"))
        player_id = rec["player_id"]
        cur.execute("DELETE FROM pitching WHERE id=%s", (record_id,))
        conn.commit()
        recalc_pitching_snapshots(conn, player_id)
    conn.close()
    flash("투수 기록이 삭제되었습니다.", "info")
    return redirect(url_for("player_detail", player_id=player_id))


# -------------------- Player Detail (graphs + history) --------------------
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
            SELECT b.id,
                   g.name AS game_name,
                   g.date AS game_date,
                   b.ab, b.hits, b.hr, b.rbi,
                   ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) AS per_game_avg,
                   b.avg AS snapshot_avg,
                   b.created_at
            FROM batting b
            LEFT JOIN games g ON g.id=b.game_id
            WHERE b.player_id=%s
            ORDER BY COALESCE(g.date, to_char(b.created_at,'YYYY-MM-DD')), b.id
        """, (player_id,))
        batting = cur.fetchall()

        cur.execute("""
            SELECT p.id,
                   g.name AS game_name,
                   g.date AS game_date,
                   p.innings, p.er, p.so, p.bb,
                   ROUND(CASE WHEN p.innings>0 THEN p.er*9.0/p.innings ELSE 0 END, 2) AS per_game_era,
                   p.era AS snapshot_era,
                   p.created_at
            FROM pitching p
            LEFT JOIN games g ON g.id=p.game_id
            WHERE p.player_id=%s
            ORDER BY COALESCE(g.date, to_char(p.created_at,'YYYY-MM-DD')), p.id
        """, (player_id,))
        pitching = cur.fetchall()

        cur.execute("SELECT COALESCE(SUM(ab),0) AS ab, COALESCE(SUM(hits),0) AS hits, COALESCE(SUM(hr),0) AS hr, COALESCE(SUM(rbi),0) AS rbi FROM batting WHERE player_id=%s",
                    (player_id,))
        totals_b = cur.fetchone()

        cur.execute("SELECT COALESCE(SUM(innings),0) AS inn, COALESCE(SUM(er),0) AS er, COALESCE(SUM(so),0) AS so, COALESCE(SUM(bb),0) AS bb FROM pitching WHERE player_id=%s",
                    (player_id,))
        totals_p = cur.fetchone()

        agg_avg = calc_player_avg(conn, player_id)
        agg_era = calc_player_era(conn, player_id)

    conn.close()

    # chart series
    b_labels, b_avg_series, b_hr_cum, b_rbi_cum = [], [], [], []
    hrc, rbic = 0, 0
    for r in batting:
        label = r["game_date"] or r["game_name"] or str(r["created_at"])[:10]
        b_labels.append(label)
        b_avg_series.append(r["per_game_avg"])
        hrc += r["hr"] or 0
        rbic += r["rbi"] or 0
        b_hr_cum.append(hrc)
        b_rbi_cum.append(rbic)

    p_labels, p_era_series, p_k_cum, p_ip_cum = [], [], [], []
    kc, ipc = 0, 0.0
    for r in pitching:
        label = r["game_date"] or r["game_name"] or str(r["created_at"])[:10]
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
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM games ORDER BY (date IS NULL), date DESC, id DESC")
        games = cur.fetchall()

        if game_id:
            cur.execute("""
                SELECT p.id, p.name, p.team, b.ab, b.hits, b.hr, b.rbi,
                       ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) AS avg
                FROM batting b
                JOIN players p ON p.id=b.player_id
                WHERE b.game_id=%s
                ORDER BY hits DESC, hr DESC, rbi DESC, avg DESC
                LIMIT 50
            """, (game_id,))
            bat = cur.fetchall()

            cur.execute("""
                SELECT p.id, p.name, p.team, pg.innings, pg.er, pg.so, pg.bb,
                       ROUND(CASE WHEN pg.innings>0 THEN pg.er*9.0/pg.innings ELSE 0 END, 2) AS era
                FROM pitching pg
                JOIN players p ON p.id=pg.player_id
                WHERE pg.game_id=%s
                ORDER BY era ASC, so DESC
                LIMIT 50
            """, (game_id,))
            pit = cur.fetchall()
        else:
            cur.execute("""
                SELECT p.id, p.name, p.team,
                       COALESCE(SUM(b.ab),0) AS ab,
                       COALESCE(SUM(b.hits),0) AS hits,
                       COALESCE(SUM(b.hr),0) AS hr,
                       COALESCE(SUM(b.rbi),0) AS rbi,
                       ROUND(CASE WHEN SUM(b.ab)>0 THEN SUM(b.hits)*1.0/SUM(b.ab) ELSE 0 END, 3) AS avg
                FROM players p LEFT JOIN batting b ON b.player_id=p.id
                GROUP BY p.id
                HAVING COALESCE(SUM(b.ab),0) > 0
                ORDER BY avg DESC, hits DESC, hr DESC, rbi DESC
                LIMIT 50
            """)
            bat = cur.fetchall()

            cur.execute("""
                SELECT p.id, p.name, p.team,
                       COALESCE(SUM(pg.innings),0) AS inn,
                       COALESCE(SUM(pg.er),0) AS er,
                       COALESCE(SUM(pg.so),0) AS so,
                       COALESCE(SUM(pg.bb),0) AS bb,
                       ROUND(CASE WHEN SUM(pg.innings)>0 THEN SUM(pg.er)*9.0/SUM(pg.innings) ELSE 0 END, 2) AS era
                FROM players p LEFT JOIN pitching pg ON pg.player_id=p.id
                GROUP BY p.id
                HAVING COALESCE(SUM(pg.innings),0) > 0
                ORDER BY era ASC, so DESC
                LIMIT 50
            """)
            pit = cur.fetchall()
    conn.close()

    return render_template("leaderboard.html",
                           games=games, game_id=game_id, bat=bat, pit=pit,
                           is_admin=session.get("is_admin", False))


# -------------------- Game detail --------------------
@app.route("/game/<int:game_id>")
def game_detail(game_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM games WHERE id=%s", (game_id,))
        game = cur.fetchone()
        if not game:
            conn.close()
            flash("경기를 찾을 수 없습니다.", "danger")
            return redirect(url_for("leaderboard"))

        cur.execute("""
            SELECT b.player_id, p.name AS player_name, b.ab, b.hits, b.hr, b.rbi,
                   ROUND(CASE WHEN b.ab>0 THEN b.hits*1.0/b.ab ELSE 0 END, 3) AS avg
            FROM batting b JOIN players p ON p.id=b.player_id
            WHERE b.game_id=%s
            ORDER BY avg DESC, hits DESC
        """, (game_id,))
        batting = cur.fetchall()

        cur.execute("""
            SELECT pg.player_id, p.name AS player_name, pg.innings, pg.er, pg.so, pg.bb,
                   ROUND(CASE WHEN pg.innings>0 THEN pg.er*9.0/pg.innings ELSE 0 END, 2) AS era
            FROM pitching pg JOIN players p ON p.id=pg.player_id
            WHERE pg.game_id=%s
            ORDER BY era ASC, so DESC
        """, (game_id,))
        pitching = cur.fetchall()
    conn.close()
    return render_template("game_detail.html", game=game, batting=batting, pitching=pitching, is_admin=session.get("is_admin", False))


# -------------------- Health / Optional --------------------
@app.route("/healthz")
def healthz():
    return "ok", 200


# -------------------- Run --------------------
if __name__ == "__main__":
    # For local dev; on Render use gunicorn via Procfile
    app.run(host="0.0.0.0", port=5000, debug=False)
