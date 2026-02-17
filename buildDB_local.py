import os
import re
import sqlite3
import datetime as dt
import pandas as pd
import tempfile
from contextlib import closing

# =========================================================
# KONFIG
# =========================================================

CATALOG = "I:/6_EQUITIES/Database/Eiere/"
F_PREFIX = "TopChanges_Nordea_Invest_DAG_"

# Remote DBs på nettverksdisk (LESING kan være OK, men vi skal IKKE skrive tilbake)
DB_PATH_REMOTE_FULL = "I:/6_EQUITIES/Database/Eiere-Database/topchanges.db"
DB_PATH_REMOTE_RECENT = "I:/6_EQUITIES/Database/Eiere-Database/topchanges_recent_60d.db"  # kun info, brukes ikke

# Lokal arbeidsmappe (temp)
LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL_FULL = os.path.join(LOCAL_WORKDIR, "topchanges.db")
DB_PATH_LOCAL_RECENT = os.path.join(LOCAL_WORKDIR, "topchanges_recent_60d.db")

DATO_START = dt.date(2021, 1, 1)
DATO_END = dt.date(2035, 12, 31)

RECENT_DAYS = 60
ENCODING = "latin-1"
SEP = ";"

SQLITE_TIMEOUT_SEC = 60
SQLITE_BUSY_TIMEOUT_MS = 60000

# NY: styrer om vi i det hele tatt skal hente snapshot fra remote ved første oppstart
ALLOW_REMOTE_SNAPSHOT = True  # sett til False hvis du vil tvinge "lokal-only" uten nedlasting


# =========================================================
# SCHEMA
# =========================================================

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS ingested_files (
    filename TEXT PRIMARY KEY,
    mtime REAL NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS investor (
    investor_id TEXT PRIMARY KEY,
    investor_type TEXT,
    first_name TEXT,
    last_name TEXT,
    country_code TEXT,
    raw_id TEXT
);

CREATE TABLE IF NOT EXISTS security (
    isin TEXT PRIMARY KEY,
    ticker TEXT,
    isin_name TEXT,
    paper_group TEXT,
    issuer_orgnr TEXT,
    issuer_name TEXT,
    registered_country TEXT,
    market TEXT,
    sector TEXT,
    gics_sector TEXT,
    ask_paper TEXT,
    issued_shares REAL,
    last_price REAL
);

CREATE TABLE IF NOT EXISTS position_change (
    isin TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    date_today TEXT NOT NULL,
    date_yesterday TEXT,
    holding_today REAL,
    holding_yesterday REAL,
    price_today REAL,
    price_yesterday REAL,
    change_qty REAL,
    abs_change_qty REAL,
    change_percent REAL,
    flag_new_source INTEGER,
    flag_exit_source INTEGER,
    rank INTEGER,
    source_file TEXT NOT NULL,

    PRIMARY KEY (isin, investor_id, date_today),
    FOREIGN KEY (isin) REFERENCES security(isin),
    FOREIGN KEY (investor_id) REFERENCES investor(investor_id)
);

CREATE INDEX IF NOT EXISTS idx_position_date_today ON position_change(date_today);
CREATE INDEX IF NOT EXISTS idx_position_investor ON position_change(investor_id);
CREATE INDEX IF NOT EXISTS idx_position_isin ON position_change(isin);
"""


# =========================================================
# HJELPERE
# =========================================================

def nuke_sqlite_files(db_path: str):
    for ext in ["", "-wal", "-shm", "-journal"]:
        p = db_path + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except PermissionError:
                raise PermissionError(f"Får ikke slettet (låst?): {p}")


def parse_file_date_from_name(filename: str) -> dt.date | None:
    if not filename.startswith(F_PREFIX):
        return None
    rest = filename[len(F_PREFIX):]
    if len(rest) < 6:
        return None
    try:
        yy = int(rest[0:2])
        mm = int(rest[2:4])
        dd = int(rest[4:6])
        return dt.date(2000 + yy, mm, dd)
    except Exception:
        return None


def normalize_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None

    if re.fullmatch(r"\d+", s):
        n = int(s)

        if len(s) == 8:
            try:
                d = dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                return d.isoformat()
            except Exception:
                return None

        if len(s) == 6:
            try:
                d = dt.date(2000 + int(s[0:2]), int(s[2:4]), int(s[4:6]))
                return d.isoformat()
            except Exception:
                return None

        if 20000 <= n <= 80000:
            try:
                d = pd.to_datetime(n, unit="D", origin="1899-12-30").date()
                return d.isoformat()
            except Exception:
                return None

    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isna(d):
            return None
        return d.date().isoformat()
    except Exception:
        return None


def clean_num(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def ensure_security_last_price_column(conn: sqlite3.Connection):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(security)").fetchall()]
    if "last_price" not in cols:
        conn.execute("ALTER TABLE security ADD COLUMN last_price REAL")
        conn.commit()


def ensure_position_change_price_today_column(conn: sqlite3.Connection):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(position_change)").fetchall()]
    if "price_today" not in cols:
        conn.execute("ALTER TABLE position_change ADD COLUMN price_today REAL")
        conn.commit()


def ensure_perf_indexes(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE INDEX IF NOT EXISTS idx_pc_isin_date_today ON position_change(isin, date_today);
    CREATE INDEX IF NOT EXISTS idx_pc_isin_price_yest ON position_change(isin, price_yesterday);
    CREATE INDEX IF NOT EXISTS idx_pc_isin_price_today ON position_change(isin, price_today);
    CREATE INDEX IF NOT EXISTS idx_pc_date_isin ON position_change(date_today, isin);
    CREATE INDEX IF NOT EXISTS idx_pc_date_investor ON position_change(date_today, investor_id);
    """)
    conn.commit()


def refresh_security_last_price_from_position_change(conn: sqlite3.Connection):
    conn.execute("""
    WITH candidates AS (
        SELECT
            isin,
            date_today,
            CASE
                WHEN COALESCE(price_today, 0) > 0 THEN price_today
                WHEN COALESCE(price_yesterday, 0) > 0 THEN price_yesterday
                ELSE NULL
            END AS eff_price
        FROM position_change
    ),
    last_dates AS (
        SELECT isin, MAX(date_today) AS last_date
        FROM candidates
        WHERE COALESCE(eff_price, 0) > 0
        GROUP BY isin
    ),
    last_prices AS (
        SELECT c.isin, MAX(c.eff_price) AS last_price
        FROM candidates c
        JOIN last_dates ld
          ON ld.isin = c.isin
         AND ld.last_date = c.date_today
        WHERE COALESCE(c.eff_price, 0) > 0
        GROUP BY c.isin
    )
    UPDATE security
    SET last_price = (
        SELECT lp.last_price
        FROM last_prices lp
        WHERE lp.isin = security.isin
    )
    WHERE isin IN (SELECT isin FROM last_prices);
    """)
    conn.commit()


def integrity_ok(db_path: str) -> bool:
    if not os.path.exists(db_path):
        return False
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=SQLITE_TIMEOUT_SEC)) as conn:
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        return bool(row and row[0] == "ok")


# =========================================================
# COPY HELPERS (KUN INN TIL LOKAL, ALDRI UT)
# =========================================================

def sqlite_backup_copy(src_path: str, dst_path: str, src_readonly: bool = True):
    """Konsistent kopi via SQLite Backup API (kun inn til lokal)."""
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    nuke_sqlite_files(dst_path)

    if not os.path.exists(src_path):
        raise FileNotFoundError(src_path)

    if src_readonly:
        src_uri = f"file:{src_path}?mode=ro"
        src_conn = sqlite3.connect(src_uri, uri=True, timeout=SQLITE_TIMEOUT_SEC)
    else:
        src_conn = sqlite3.connect(src_path, timeout=SQLITE_TIMEOUT_SEC)

    with closing(src_conn) as src:
        src.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        with closing(sqlite3.connect(f"file:{dst_path}?mode=rwc", uri=True, timeout=SQLITE_TIMEOUT_SEC)) as dst:
            dst.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
            src.backup(dst)
            dst.commit()


def ensure_local_db_or_create_empty():
    """
    NO-PUSH flyt:
    - Hvis lokal FULL finnes og er OK: bruk den
    - Ellers:
        - hvis ALLOW_REMOTE_SNAPSHOT og remote finnes: hent snapshot én gang
        - ellers: lag ny tom lokal DB
    """
    os.makedirs(LOCAL_WORKDIR, exist_ok=True)

    if os.path.exists(DB_PATH_LOCAL_FULL) and integrity_ok(DB_PATH_LOCAL_FULL):
        print("Bruker eksisterende lokal FULL DB.")
        return

    if ALLOW_REMOTE_SNAPSHOT and os.path.exists(DB_PATH_REMOTE_FULL):
        print("Lokal FULL mangler/korrupt. Henter snapshot fra remote (én gang).")
        sqlite_backup_copy(DB_PATH_REMOTE_FULL, DB_PATH_LOCAL_FULL, src_readonly=True)
        if not integrity_ok(DB_PATH_LOCAL_FULL):
            raise sqlite3.DatabaseError("Lokal kopi feilet integrity_check. Remote kan være korrupt.")
        return

    print("Starter ny tom lokal DB:", DB_PATH_LOCAL_FULL)
    nuke_sqlite_files(DB_PATH_LOCAL_FULL)
    conn = sqlite3.connect(f"file:{DB_PATH_LOCAL_FULL}?mode=rwc", uri=True, timeout=SQLITE_TIMEOUT_SEC)
    try:
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


# =========================================================
# INGEST-SELEKSJON (kun nye/endrede filer)
# =========================================================

def get_files_to_ingest(conn: sqlite3.Connection) -> list[str]:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            filename TEXT PRIMARY KEY,
            mtime REAL NOT NULL,
            ingested_at TEXT NOT NULL
        );
    """)
    conn.commit()

    ing = dict(conn.execute("SELECT filename, mtime FROM ingested_files").fetchall())

    out = []
    for fn in os.listdir(CATALOG):
        d = parse_file_date_from_name(fn)
        if d is None:
            continue
        if d < DATO_START or d > DATO_END:
            continue

        path = os.path.join(CATALOG, fn)
        mtime = os.path.getmtime(path)

        if fn not in ing or float(ing[fn]) != float(mtime):
            out.append(fn)

    return sorted(out)


def mark_ingested(conn: sqlite3.Connection, filename: str, mtime: float):
    conn.execute(
        "INSERT OR REPLACE INTO ingested_files(filename, mtime, ingested_at) VALUES (?, ?, ?)",
        (filename, mtime, dt.datetime.now().isoformat(timespec="seconds"))
    )


# =========================================================
# INGEST (uendret)
# =========================================================

def ingest_one_file(conn: sqlite3.Connection, filename: str) -> bool:
    path = os.path.join(CATALOG, filename)
    mtime = os.path.getmtime(path)

    print(f"LESER: {filename}")

    try:
        df = pd.read_csv(path, sep=SEP, encoding=ENCODING, dtype=str)
    except Exception:
        df = pd.read_csv(path, sep=SEP, encoding=ENCODING, dtype=str, engine="python")

    def pick_col(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col_investor_id = pick_col("New_ID", "investor_ID", "Investor_ID", "InvestorID")
    col_investor_type = pick_col("Investortype", "InvestorType")
    col_first = pick_col("Fornavn", "FirstName")
    col_last = pick_col("Etternavn", "LastName")
    col_country = pick_col("Country code", "Country_code", "CountryCode")
    col_raw_id = pick_col("Date of Birth", "DOB", "Raw_ID")

    col_isin = pick_col("ISIN")
    col_ticker = pick_col("Ticker")
    col_isin_name = pick_col("ISINNAVN", "ISINNAVN ", "ISINName")
    col_paper_group = pick_col("PAPIRGRUPPE", "Papirgruppe")
    col_issuer_orgnr = pick_col("Orgnr", "Org.nr", "IssuerOrgnr")
    col_issuer_name = pick_col("Utsteder navn", "Utsteder_navn", "IssuerName")
    col_reg_country = pick_col("Registrert land", "Registered country")
    col_market = pick_col("Markedsplass", "Market")
    col_sector = pick_col("Sektor", "Sector")
    col_gics = pick_col("GICS_SECTOR", "GICS Sector")
    col_ask = pick_col("ASK-papir", "ASK_papir")
    col_issued = pick_col("Utstedt antall", "Issued_shares")

    col_date_today = pick_col("DatoIdag", "Dato idag", "DateToday")
    col_date_yest = pick_col("DatoIgaar", "Dato igaar", "DateYesterday")
    col_h_today = pick_col("Beh. idag", "Beh idag", "Holding today")
    col_h_yest = pick_col("Beh. igaar", "Beh igaar", "Holding yesterday")

    col_price_today = pick_col("Kurs idag", "Kurs idag ", "Price today")
    col_price_yest = pick_col("Kurs igaar", "Kurs igaar ", "Price yesterday")

    col_change = pick_col("Change", "ChangeQty")
    col_abs_change = pick_col("AbsChange", "Abs change")
    col_change_pct = pick_col("ChangePercent", "Change %")
    col_flag_exit = pick_col("Forlatt", "Exit")
    col_flag_new = pick_col("Ny", "New")
    col_rank = pick_col("Rank")

    if col_isin is None or col_investor_id is None or col_date_today is None:
        raise ValueError(
            f"Mangler nødvendige kolonner i {filename}. Trenger minst ISIN, investor_id og DatoIdag."
        )

    inv = pd.DataFrame({
        "investor_id": df[col_investor_id].astype(str).str.strip(),
        "investor_type": df[col_investor_type].astype(str).str.strip() if col_investor_type else None,
        "first_name": df[col_first].astype(str).str.strip() if col_first else None,
        "last_name": df[col_last].astype(str).str.strip() if col_last else None,
        "country_code": df[col_country].astype(str).str.strip() if col_country else None,
        "raw_id": df[col_raw_id].astype(str).str.strip() if col_raw_id else None,
    })
    inv = inv.dropna(subset=["investor_id"])
    inv["investor_id"] = inv["investor_id"].replace({"nan": None, "": None})
    inv = inv.dropna(subset=["investor_id"]).drop_duplicates(subset=["investor_id"])

    conn.executemany(
        """
        INSERT INTO investor(investor_id, investor_type, first_name, last_name, country_code, raw_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(investor_id) DO UPDATE SET
            investor_type=COALESCE(excluded.investor_type, investor.investor_type),
            first_name=COALESCE(excluded.first_name, investor.first_name),
            last_name=COALESCE(excluded.last_name, investor.last_name),
            country_code=COALESCE(excluded.country_code, investor.country_code),
            raw_id=COALESCE(excluded.raw_id, investor.raw_id)
        """,
        inv[["investor_id", "investor_type", "first_name", "last_name", "country_code", "raw_id"]]
        .itertuples(index=False, name=None)
    )

    sec = pd.DataFrame({
        "isin": df[col_isin].astype(str).str.strip(),
        "ticker": df[col_ticker].astype(str).str.strip() if col_ticker else None,
        "isin_name": df[col_isin_name].astype(str).str.strip() if col_isin_name else None,
        "paper_group": df[col_paper_group].astype(str).str.strip() if col_paper_group else None,
        "issuer_orgnr": df[col_issuer_orgnr].astype(str).str.strip() if col_issuer_orgnr else None,
        "issuer_name": df[col_issuer_name].astype(str).str.strip() if col_issuer_name else None,
        "registered_country": df[col_reg_country].astype(str).str.strip() if col_reg_country else None,
        "market": df[col_market].astype(str).str.strip() if col_market else None,
        "sector": df[col_sector].astype(str).str.strip() if col_sector else None,
        "gics_sector": df[col_gics].astype(str).str.strip() if col_gics else None,
        "ask_paper": df[col_ask].astype(str).str.strip() if col_ask else None,
        "issued_shares": df[col_issued].map(clean_num) if col_issued else None,
    })
    sec = sec.dropna(subset=["isin"])
    sec["isin"] = sec["isin"].replace({"nan": None, "": None})
    sec = sec.dropna(subset=["isin"]).drop_duplicates(subset=["isin"])

    conn.executemany(
        """
        INSERT INTO security(isin, ticker, isin_name, paper_group, issuer_orgnr, issuer_name,
                             registered_country, market, sector, gics_sector, ask_paper, issued_shares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(isin) DO UPDATE SET
            ticker=COALESCE(excluded.ticker, security.ticker),
            isin_name=COALESCE(excluded.isin_name, security.isin_name),
            paper_group=COALESCE(excluded.paper_group, security.paper_group),
            issuer_orgnr=COALESCE(excluded.issuer_orgnr, security.issuer_orgnr),
            issuer_name=COALESCE(excluded.issuer_name, security.issuer_name),
            registered_country=COALESCE(excluded.registered_country, security.registered_country),
            market=COALESCE(excluded.market, security.market),
            sector=COALESCE(excluded.sector, security.sector),
            gics_sector=COALESCE(excluded.gics_sector, security.gics_sector),
            ask_paper=COALESCE(excluded.ask_paper, security.ask_paper),
            issued_shares=COALESCE(excluded.issued_shares, security.issued_shares)
        """,
        sec[["isin", "ticker", "isin_name", "paper_group", "issuer_orgnr", "issuer_name",
             "registered_country", "market", "sector", "gics_sector", "ask_paper", "issued_shares"]]
        .itertuples(index=False, name=None)
    )

    facts = pd.DataFrame({
        "isin": df[col_isin].astype(str).str.strip(),
        "investor_id": df[col_investor_id].astype(str).str.strip(),
        "date_today": df[col_date_today].map(normalize_date),
        "date_yesterday": df[col_date_yest].map(normalize_date) if col_date_yest else None,
        "holding_today": df[col_h_today].map(clean_num) if col_h_today else None,
        "holding_yesterday": df[col_h_yest].map(clean_num) if col_h_yest else None,
        "price_today": df[col_price_today].map(clean_num) if col_price_today else None,
        "price_yesterday": df[col_price_yest].map(clean_num) if col_price_yest else None,
        "change_qty": df[col_change].map(clean_num) if col_change else None,
        "abs_change_qty": df[col_abs_change].map(clean_num) if col_abs_change else None,
        "change_percent": df[col_change_pct].map(clean_num) if col_change_pct else None,
        "flag_new_source": df[col_flag_new].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_flag_new else None,
        "flag_exit_source": df[col_flag_exit].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_flag_exit else None,
        "rank": df[col_rank].map(lambda x: int(float(x)) if str(x).strip() not in ["", "nan"] else None) if col_rank else None,
        "source_file": filename
    })

    facts = facts.dropna(subset=["isin", "investor_id", "date_today"])
    facts = facts.drop_duplicates(subset=["isin", "investor_id", "date_today"])

    conn.executemany(
        """
        INSERT OR REPLACE INTO position_change(
            isin, investor_id, date_today, date_yesterday,
            holding_today, holding_yesterday, price_today, price_yesterday,
            change_qty, abs_change_qty, change_percent,
            flag_new_source, flag_exit_source, rank,
            source_file
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        facts[[
            "isin", "investor_id", "date_today", "date_yesterday",
            "holding_today", "holding_yesterday", "price_today", "price_yesterday",
            "change_qty", "abs_change_qty", "change_percent",
            "flag_new_source", "flag_exit_source", "rank",
            "source_file"
        ]].itertuples(index=False, name=None)
    )

    # Best-effort last_price fra fil:
    tmp = pd.DataFrame({
        "isin": df[col_isin].astype(str).str.strip(),
        "pt": df[col_price_today].map(clean_num) if col_price_today else None,
        "py": df[col_price_yest].map(clean_num) if col_price_yest else None,
    }).dropna(subset=["isin"])

    max_today = (
        tmp.dropna(subset=["pt"])
           .loc[tmp["pt"] > 0]
           .groupby("isin", as_index=False)["pt"].max()
           .rename(columns={"pt": "max_pt"})
    )
    max_yest = (
        tmp.dropna(subset=["py"])
           .loc[tmp["py"] > 0]
           .groupby("isin", as_index=False)["py"].max()
           .rename(columns={"py": "max_py"})
    )

    lastp = max_today.merge(max_yest, on="isin", how="outer")
    lastp["last_price"] = lastp["max_pt"].fillna(lastp["max_py"])
    lastp = lastp.dropna(subset=["last_price"])

    if not lastp.empty:
        conn.executemany(
            "UPDATE security SET last_price = ? WHERE isin = ?",
            lastp[["last_price", "isin"]].itertuples(index=False, name=None)
        )

    mark_ingested(conn, filename, mtime)
    return True


# =========================================================
# BUILD RECENT (ROLLING 60D)
# =========================================================

def build_recent_db(source_db_path: str, out_db_path: str, date_from: dt.date):
    nuke_sqlite_files(out_db_path)

    conn = sqlite3.connect(f"file:{out_db_path}?mode=rwc", uri=True, timeout=SQLITE_TIMEOUT_SEC)
    try:
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        conn.executescript(SCHEMA_SQL)
        conn.commit()

        ensure_security_last_price_column(conn)
        ensure_position_change_price_today_column(conn)
        ensure_perf_indexes(conn)

        conn.execute("ATTACH DATABASE ? AS src", (source_db_path,))

        conn.execute("BEGIN;")
        conn.execute("INSERT INTO investor SELECT * FROM src.investor;")
        conn.execute("INSERT INTO security SELECT * FROM src.security;")
        conn.execute("""
            INSERT INTO position_change
            SELECT *
            FROM src.position_change
            WHERE date_today >= ?
        """, (date_from.isoformat(),))
        conn.execute("INSERT INTO ingested_files SELECT * FROM src.ingested_files;")
        conn.execute("COMMIT;")
        conn.commit()

        try:
            conn.execute("DETACH DATABASE src;")
        except sqlite3.OperationalError as e:
            print("WARN: DETACH DATABASE src feilet (ufarlig):", e)

        refresh_security_last_price_from_position_change(conn)

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.commit()

    finally:
        conn.close()


# =========================================================
# MAIN (NO PUSH)
# =========================================================

def main():
    # 1) Bruk lokal FULL hvis OK, ellers snapshot->lokal (valgfritt) eller tom DB
    ensure_local_db_or_create_empty()

    # 2) Oppdater lokal FULL kun med nye/endrede filer
    conn = sqlite3.connect(f"file:{DB_PATH_LOCAL_FULL}?mode=rwc", uri=True, timeout=SQLITE_TIMEOUT_SEC)
    try:
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        conn.executescript(SCHEMA_SQL)
        conn.commit()

        ensure_security_last_price_column(conn)
        ensure_position_change_price_today_column(conn)
        ensure_perf_indexes(conn)

        files = get_files_to_ingest(conn)
        print(f"Fant {len(files)} nye/endrede filer.")

        if files:
            for i, fn in enumerate(files, start=1):
                ingest_one_file(conn, fn)
                if i % 25 == 0:
                    print("Starter commit ...")
                    conn.commit()
                    print(f"Commit ferdig ved fil {i}/{len(files)}")

        print("Starter final commit ...")
        conn.commit()
        print("Final commit ferdig.")

        print("Refresh security.last_price fra position_change ...")
        refresh_security_last_price_from_position_change(conn)
        print("Ferdig refresh av last_price.")

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.commit()

    finally:
        conn.close()

    if not integrity_ok(DB_PATH_LOCAL_FULL):
        raise sqlite3.DatabaseError("Lokal FULL DB feilet integrity_check etter bygg.")

    # 3) Bygg recent 60D lokalt
    today = dt.date.today()
    recent_from = today - dt.timedelta(days=RECENT_DAYS)
    print(f"Bygger RECENT DB ({RECENT_DAYS} dager) fra og med {recent_from} ...")
    build_recent_db(DB_PATH_LOCAL_FULL, DB_PATH_LOCAL_RECENT, recent_from)
    print("RECENT DB ferdig (LOKAL):", DB_PATH_LOCAL_RECENT)

    if not integrity_ok(DB_PATH_LOCAL_RECENT):
        raise sqlite3.DatabaseError("Lokal RECENT DB feilet integrity_check etter bygg.")

    # 4) STOPP: Ingen opplasting / ingen kopiering til nett
    print("FERDIG (NO-PUSH). Ingen filer ble kopiert tilbake til nettverksdisk.")
    print("Lokale DB-er:")
    print(" - FULL  :", DB_PATH_LOCAL_FULL)
    print(" - RECENT:", DB_PATH_LOCAL_RECENT)


if __name__ == "__main__":
    main()
