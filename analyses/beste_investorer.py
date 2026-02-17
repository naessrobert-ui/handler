# analyses/beste_investorer.py
from __future__ import annotations

import os
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import pandas as pd
import streamlit as st


# -----------------------------
# Konfig
# -----------------------------
DEFAULT_FROM = dt.date(2022, 1, 1)

INVTYPE_BEGGE = "Begge"
INVTYPE_PRIVAT = "Privat"
INVTYPE_ORG = "Organisasjon"


@dataclass
class DbCols:
    # position_change
    date_col: str
    isin_col: str
    investor_col: str
    qty_col: str
    price_trade_col: str  # kildekolonne i position_change (typisk price_yesterday)

    # security
    sec_isin_col: str
    sec_ticker_col: Optional[str]
    sec_name_col: Optional[str]
    sec_last_price_col: Optional[str]  # NY: siste aksjekurs i security (last_price)

    # investor
    inv_type_col: Optional[str]
    inv_first_col: Optional[str]
    inv_last_col: Optional[str]
    inv_name_col: Optional[str]
    inv_country_col: Optional[str]


# =========================================================
# DB helpers
# =========================================================
def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return [r[0] if isinstance(r, tuple) else r["name"] for r in rows]


def _table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    out = []
    for r in rows:
        out.append(r[1] if isinstance(r, tuple) else r["name"])
    return out


def _table_cols_set(conn: sqlite3.Connection, table: str) -> set[str]:
    return set(_table_cols(conn, table))


def ensure_indexes(conn: sqlite3.Connection, cols: DbCols) -> None:
    """
    Oppretter indekser som gjør self-join og filtrering rask.
    Kjøres trygt hver gang (IF NOT EXISTS).
    """
    try:
        conn.executescript(f"""
        CREATE INDEX IF NOT EXISTS idx_pc_isin_date ON position_change({cols.isin_col}, {cols.date_col});
        CREATE INDEX IF NOT EXISTS idx_pc_investor_date ON position_change({cols.investor_col}, {cols.date_col});
        CREATE INDEX IF NOT EXISTS idx_pc_date ON position_change({cols.date_col});
        """)
        conn.commit()
    except Exception:
        # Ikke stopp appen hvis DB er read-only el.
        pass


def detect_cols(conn: sqlite3.Connection) -> DbCols:
    tables = set(_list_tables(conn))
    if "position_change" not in tables:
        raise RuntimeError(f"Fant ikke tabell 'position_change'. Tabeller: {sorted(tables)}")
    if "security" not in tables:
        raise RuntimeError(f"Fant ikke tabell 'security'. Tabeller: {sorted(tables)}")

    pc_cols = set(_table_cols(conn, "position_change"))
    sec_cols = set(_table_cols(conn, "security"))
    inv_cols = set(_table_cols(conn, "investor")) if "investor" in tables else set()

    # position_change
    date_col = "date_today" if "date_today" in pc_cols else ("date" if "date" in pc_cols else None)
    isin_col = "isin" if "isin" in pc_cols else None
    investor_col = "investor_id" if "investor_id" in pc_cols else None
    qty_col = "change_qty" if "change_qty" in pc_cols else ("qty" if "qty" in pc_cols else None)

    # pris-kolonne (kilde) i position_change
    if "price_yesterday" in pc_cols:
        price_trade_col = "price_yesterday"
    elif "price_today" in pc_cols:
        price_trade_col = "price_today"
    elif "price" in pc_cols:
        price_trade_col = "price"
    else:
        price_trade_col = None

    if not all([date_col, isin_col, investor_col, qty_col, price_trade_col]):
        raise RuntimeError(
            "Fant ikke forventede kolonner i position_change.\n"
            f"Kolonner funnet: {sorted(pc_cols)}\n"
            "Forventer minst: date_today/date, isin, investor_id, change_qty/qty, price_yesterday/price_today/price."
        )

    # security
    sec_isin_col = "isin" if "isin" in sec_cols else None
    if not sec_isin_col:
        raise RuntimeError(f"Fant ikke 'isin' i security. Kolonner: {sorted(sec_cols)}")
    sec_ticker_col = "ticker" if "ticker" in sec_cols else None
    sec_name_col = "isin_name" if "isin_name" in sec_cols else ("name" if "name" in sec_cols else None)

    # NY: siste kurs i security
    sec_last_price_col = "last_price" if "last_price" in sec_cols else None

    # investor (robust)
    inv_type_col = None
    for c in ["investor_type", "type", "investor_category"]:
        if c in inv_cols:
            inv_type_col = c
            break
    inv_first_col = "first_name" if "first_name" in inv_cols else None
    inv_last_col = "last_name" if "last_name" in inv_cols else None
    inv_name_col = "name" if "name" in inv_cols else None

    # landkode
    inv_country_col = None
    for c in ["country_code", "countryCode", "country", "iso_country", "country_iso"]:
        if c in inv_cols:
            inv_country_col = c
            break

    return DbCols(
        date_col=date_col,
        isin_col=isin_col,
        investor_col=investor_col,
        qty_col=qty_col,
        price_trade_col=price_trade_col,
        sec_isin_col=sec_isin_col,
        sec_ticker_col=sec_ticker_col,
        sec_name_col=sec_name_col,
        sec_last_price_col=sec_last_price_col,
        inv_type_col=inv_type_col,
        inv_first_col=inv_first_col,
        inv_last_col=inv_last_col,
        inv_name_col=inv_name_col,
        inv_country_col=inv_country_col,
    )


# =========================================================
# CSV helpers
# =========================================================
def read_semicolon_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=";", encoding="latin-1")
    except Exception:
        return pd.read_csv(path, sep=",", encoding="latin-1")


def load_first_column_values(csv_path: str) -> List[str]:
    df = read_semicolon_csv(csv_path)
    col = df.columns[0]
    vals = df[col].astype(str).str.strip().tolist()
    vals = [x for x in vals if x and x.lower() not in ("selskap", "investor_id", "id", "ticker", "isin", "aksje")]

    out = []
    seen = set()
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# =========================================================
# Investor search
# =========================================================
def _clean_nan(s: str) -> str:
    s2 = (s or "").strip()
    return "" if s2.lower() == "nan" else s2


def fetch_investors(conn: sqlite3.Connection, cols: DbCols, query: str, limit: int = 50):
    if len((query or "").strip()) < 3:
        return []

    q = query.upper().strip()
    like = f"%{q}%"

    where = ["UPPER(COALESCE(investor_id,'')) LIKE ?"]
    params = [like]

    if cols.inv_first_col:
        where.append(f"UPPER(COALESCE({cols.inv_first_col},'')) LIKE ?")
        params.append(like)
    if cols.inv_last_col:
        where.append(f"UPPER(COALESCE({cols.inv_last_col},'')) LIKE ?")
        params.append(like)
    if cols.inv_first_col and cols.inv_last_col:
        where.append(f"UPPER(COALESCE({cols.inv_first_col},'') || ' ' || COALESCE({cols.inv_last_col},'')) LIKE ?")
        params.append(like)
    if cols.inv_name_col:
        where.append(f"UPPER(COALESCE({cols.inv_name_col},'')) LIKE ?")
        params.append(like)

    select_cols = ["investor_id"]
    if cols.inv_country_col:
        select_cols.append(f"{cols.inv_country_col} AS country_code")
    else:
        select_cols.append("'' AS country_code")
    if cols.inv_type_col:
        select_cols.append(f"{cols.inv_type_col} AS investor_type")
    else:
        select_cols.append("'' AS investor_type")

    if cols.inv_first_col:
        select_cols.append(f"{cols.inv_first_col} AS first_name")
    else:
        select_cols.append("'' AS first_name")

    if cols.inv_last_col:
        select_cols.append(f"{cols.inv_last_col} AS last_name")
    else:
        select_cols.append("'' AS last_name")

    if cols.inv_name_col:
        select_cols.append(f"{cols.inv_name_col} AS name")
    else:
        select_cols.append("'' AS name")

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM investor
    WHERE {" OR ".join(where)}
    ORDER BY COALESCE(last_name,''), COALESCE(first_name,''), COALESCE(name,'')
    LIMIT ?
    """
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def investor_search_multiselect(conn: sqlite3.Connection, cols: DbCols, country_filter: Optional[List[str]] = None) -> List[str]:
    query = st.text_input("Søk investor (min 3 tegn)", key="best_inv_search")
    rows = fetch_investors(conn, cols, query)
    if country_filter:
        cf = {c.strip().upper() for c in country_filter if c and c.strip()}
        rows = [r for r in rows if (str(r.get('country_code','')) or '').strip().upper() in cf]

    options = []
    label_to_id = {}

    for r in rows:
        first = _clean_nan(str(r["first_name"]))
        last = _clean_nan(str(r["last_name"]))
        name = _clean_nan(str(r["name"]))

        disp = " ".join(x for x in [first, last] if x).strip()
        if not disp:
            disp = name
        if not disp:
            disp = "(Ukjent)"

        cc = _clean_nan(str(r.get('country_code','')))
        label = f"{disp} [{cc}]" if cc else disp
        options.append(label)
        label_to_id[label] = str(r["investor_id"]).strip()

    picked_labels = st.multiselect(
        "Velg investorer",
        options,
        default=[],
        help="Søk først (min 4 tegn), velg deretter én eller flere investorer.",
        key="best_inv_pick",
    )
    return [label_to_id[x] for x in picked_labels]


# =========================================================
# Country code filter
# =========================================================
def fetch_distinct_country_codes(conn: sqlite3.Connection, cols: DbCols) -> List[str]:
    if not cols.inv_country_col or "investor" not in _list_tables(conn):
        return []
    rows = conn.execute(
        f"SELECT DISTINCT {cols.inv_country_col} AS cc FROM investor WHERE COALESCE({cols.inv_country_col},'') <> '' ORDER BY cc"
    ).fetchall()
    out: List[str] = []
    for r in rows:
        cc = ("" if r["cc"] is None else str(r["cc"])).strip()
        if cc:
            out.append(cc)
    return out


def fetch_investor_country_map(conn: sqlite3.Connection, cols: DbCols) -> Dict[str, str]:
    if not cols.inv_country_col or "investor" not in _list_tables(conn):
        return {}
    rows = conn.execute(f"SELECT investor_id, {cols.inv_country_col} AS cc FROM investor").fetchall()
    return {str(r["investor_id"]).strip(): ("" if r["cc"] is None else str(r["cc"]).strip()) for r in rows}


def normalize_country_filter(selected: Optional[List[str]], raw_cc: str) -> bool:
    if not selected:
        return True
    s = {c.strip().upper() for c in selected if c and c.strip()}
    return (raw_cc or "").strip().upper() in s

# =========================================================
# Investor type filter
# =========================================================
def fetch_investor_type_map(conn: sqlite3.Connection, cols: DbCols) -> Dict[str, str]:
    if not cols.inv_type_col:
        return {}
    rows = conn.execute(f"SELECT investor_id, {cols.inv_type_col} AS t FROM investor").fetchall()
    return {str(r["investor_id"]).strip(): ("" if r["t"] is None else str(r["t"])) for r in rows}


def normalize_invtype_filter(selected: str, raw_type: str) -> bool:
    """
    Robust mapping av investortype.
    Støtter typiske verdier som 'Privat', 'Organisasjon', 'P'/'O', m.m.
    """
    sel = (selected or "").strip().lower()
    t = (raw_type or "").strip().lower()

    if sel in ("", "alle", "begge"):
        return True

    # Normaliser korte koder
    if t in ("p", "priv", "privat", "private", "person", "individual"):
        t_norm = "privat"
    elif t in ("o", "org", "organisasjon", "organization", "company", "firm", "corporate"):
        t_norm = "org"
    else:
        t_norm = t

    if sel == INVTYPE_PRIVAT.lower():
        return any(x in t_norm for x in ["priv", "person", "individual"])
    if sel == INVTYPE_ORG.lower():
        return any(x in t_norm for x in ["org", "company", "firm", "corpor", "as", "asa", "ltd", "plc"])

    return True


def normalize_invtype_filter_multi(selected_types: Optional[List[str]], raw_type: str) -> bool:
    """Returnerer True hvis investorens type matcher valgte typer.
    selected_types kommer fra multiselect og kan være tom/None (da ingen filtrering).
    """
    if not selected_types:
        return True

    s = set(selected_types)
    # Hvis begge er valgt -> ingen filtrering
    if INVTYPE_PRIVAT in s and INVTYPE_ORG in s:
        return True

    if INVTYPE_PRIVAT in s:
        return bool(normalize_invtype_filter(INVTYPE_PRIVAT, raw_type))

    if INVTYPE_ORG in s:
        return bool(normalize_invtype_filter(INVTYPE_ORG, raw_type))

    # Ukjent valg -> ingen filtrering
    return True


# =========================================================
# Security resolving for ticker/name/isin inputs
# =========================================================
def search_securities_by_prefix(conn: sqlite3.Connection, cols: DbCols, query: str, limit: int = 50) -> List[sqlite3.Row]:
    """Finn verdipapirer der ticker eller navn STARTER med query (case-insensitivt)."""
    q = (query or "").strip()
    if not q:
        return []
    q_up = q.upper()
    prefix = f"{q_up}%"

    if not (cols.sec_ticker_col or cols.sec_name_col):
        return []

    where: List[str] = []
    params: List[str] = []

    if cols.sec_ticker_col:
        where.append(f"UPPER(COALESCE({cols.sec_ticker_col},'')) LIKE ?")
        params.append(prefix)
    if cols.sec_name_col:
        where.append(f"UPPER(COALESCE({cols.sec_name_col},'')) LIKE ?")
        params.append(prefix)

    ticker_sel = f"COALESCE({cols.sec_ticker_col}, '') AS ticker" if cols.sec_ticker_col else "'' AS ticker"
    name_sel = f"COALESCE({cols.sec_name_col}, '') AS navn" if cols.sec_name_col else "'' AS navn"

    ticker_sort_col = cols.sec_ticker_col if cols.sec_ticker_col else "''"
    name_sort_col = cols.sec_name_col if cols.sec_name_col else "''"

    sql = f"""
    SELECT
        {cols.sec_isin_col} AS isin,
        {ticker_sel},
        {name_sel}
    FROM security
    WHERE {" OR ".join(where)}
    ORDER BY
        CASE WHEN UPPER(COALESCE({ticker_sort_col},'')) = ? THEN 0 ELSE 1 END,
        UPPER(COALESCE({ticker_sort_col},'')),
        UPPER(COALESCE({name_sort_col},''))
    LIMIT ?
    """

    params2 = params + [q_up, limit]
    return conn.execute(sql, params2).fetchall()


def resolve_isin_list_by_ticker_name_prefix(conn: sqlite3.Connection, cols: DbCols, tokens: List[str]) -> List[str]:
    """Mapper en liste med ticker/navn-prefix til ISIN-liste (kun prefix-match, ikke ISIN-input)."""
    tokens = [t.strip() for t in (tokens or []) if t and t.strip()]
    if not tokens:
        return []
    isins: set[str] = set()
    for t in tokens:
        rows = search_securities_by_prefix(conn, cols, t, limit=500)
        for r in rows:
            isins.add(str(r["isin"]).strip())
    return sorted(isins)
def _detect_price_table(conn: sqlite3.Connection) -> Optional[Tuple[str, str, str]]:
    tables = _list_tables(conn)

    preferred = [
        "price_history", "security_price", "security_prices", "daily_price", "prices",
        "kurs", "kursdata", "price", "price_data",
    ]
    candidates = [t for t in preferred if t in tables]

    for t in tables:
        if t in candidates:
            continue
        tl = t.lower()
        if "price" in tl or "kurs" in tl:
            candidates.append(t)

    for t in candidates:
        cols = _table_cols_set(conn, t)
        if "isin" not in cols:
            continue

        date_col = None
        for dc in ["date", "dato", "day", "trade_date", "date_today"]:
            if dc in cols:
                date_col = dc
                break
        if not date_col:
            continue

        price_col = None
        for pc in ["close", "close_price", "price", "kurs", "last", "adj_close"]:
            if pc in cols:
                price_col = pc
                break
        if not price_col:
            continue

        return (t, date_col, price_col)

    return None


def build_last_price_cache(conn: sqlite3.Connection, cols: DbCols) -> Tuple[Dict[str, float], str]:
    # 0) PRIORITET: security.last_price
    if cols.sec_last_price_col:
        df = pd.read_sql(
            f"""
            SELECT {cols.sec_isin_col} AS isin,
                   {cols.sec_last_price_col} AS last_price
            FROM security
            WHERE COALESCE({cols.sec_last_price_col}, 0) > 0
            """,
            conn,
        )
        if not df.empty:
            df["isin"] = df["isin"].astype(str).str.strip()
            df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
            return dict(zip(df["isin"], df["last_price"])), f"Pris-kilde: security.{cols.sec_last_price_col}"

    # 1) pris-tabell fallback
    price_info = _detect_price_table(conn)
    if price_info:
        table, date_col, price_col = price_info
        sql = f"""
        WITH last_dates AS (
            SELECT isin, MAX({date_col}) AS last_date
            FROM {table}
            WHERE COALESCE({price_col},0) > 0
            GROUP BY isin
        )
        SELECT p.isin AS isin, p.{price_col} AS last_price
        FROM {table} p
        JOIN last_dates ld
          ON ld.isin = p.isin
         AND ld.last_date = p.{date_col}
        """
        df = pd.read_sql(sql, conn)
        if not df.empty:
            df["isin"] = df["isin"].astype(str).str.strip()
            df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
            mp = dict(zip(df["isin"], df["last_price"]))
            return mp, f"Pris-kilde: {table}.{price_col} (siste dato med pris > 0)"

    # 2) fallback: position_change siste pris > 0
    sql = f"""
    WITH last_dates AS (
        SELECT {cols.isin_col} AS isin, MAX({cols.date_col}) AS last_date
        FROM position_change
        WHERE COALESCE({cols.price_trade_col},0) > 0
        GROUP BY {cols.isin_col}
    )
    SELECT pc.{cols.isin_col} AS isin,
           pc.{cols.price_trade_col} AS last_price
    FROM position_change pc
    JOIN last_dates ld
      ON ld.isin = pc.{cols.isin_col}
     AND ld.last_date = pc.{cols.date_col}
    """
    df = pd.read_sql(sql, conn)
    if df.empty:
        return {}, "Pris-kilde: (ingen) – fant ingen pris > 0"
    df["isin"] = df["isin"].astype(str).str.strip()
    df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce").fillna(0.0)
    return dict(zip(df["isin"], df["last_price"])), f"Pris-kilde: position_change.{cols.price_trade_col} (siste dato med pris > 0)"


# =========================================================
# Core calculation
# =========================================================
def compute_best_investors(
    conn: sqlite3.Connection,
    cols: DbCols,
    date_from: dt.date,
    date_to: dt.date,
    investor_ids: Optional[List[str]],
    invtype_choice: str,
    country_codes: Optional[List[str]],
    isin_filter: Optional[List[str]],
    min_trades: int,
    min_brutto_mnok: float,
    last_price_map: Dict[str, float],
) -> pd.DataFrame:
    # Bygg WHERE dynamisk og filtrer TIDLIG i SQL (viktig for ytelse)
    where = [f"date(pc.{cols.date_col}) BETWEEN date(?) AND date(?)"]
    params: List[str] = [date_from.isoformat(), date_to.isoformat()]

    if investor_ids:
        wanted = [str(x).strip() for x in investor_ids]
        placeholders = ",".join(["?"] * len(wanted))
        where.append(f"pc.{cols.investor_col} IN ({placeholders})")
        params.extend(wanted)

    if isin_filter:
        wanted_isin = [x.strip() for x in isin_filter]
        placeholders = ",".join(["?"] * len(wanted_isin))
        where.append(f"pc.{cols.isin_col} IN ({placeholders})")
        params.extend(wanted_isin)

    # VIKTIG: Ikke join direkte til position_change for "neste dag" pris.
    # Det finnes typisk mange rader per (isin, dato) (én per investor), og da får du dupliserte handler.
    # Vi bygger derfor en liten pris-CTE som aggregerer til ÉN pris per (isin, dato).
    sql_trades = f"""
    WITH prices AS (
        SELECT
            {cols.isin_col} AS isin,
            date({cols.date_col}) AS d,
            MAX({cols.price_trade_col}) AS p
        FROM position_change
        WHERE COALESCE({cols.price_trade_col}, 0) > 0
        GROUP BY {cols.isin_col}, date({cols.date_col})
    )
    SELECT
        date(pc.{cols.date_col}) AS dato,
        pc.{cols.investor_col} AS investor_id,
        pc.{cols.isin_col} AS isin,
        pc.{cols.qty_col} AS qty,
        pc.{cols.price_trade_col} AS price_main,
        p2.p AS price_nextday
    FROM position_change pc
    LEFT JOIN prices p2
      ON p2.isin = pc.{cols.isin_col}
     AND p2.d = date(pc.{cols.date_col}, '+1 day')
    WHERE {" AND ".join(where)}
    """
    df = pd.read_sql(sql_trades, conn, params=params)
    if df.empty:
        return pd.DataFrame()

    df["investor_id"] = df["investor_id"].astype(str).str.strip()
    df["isin"] = df["isin"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)

    df["price_main"] = pd.to_numeric(df["price_main"], errors="coerce").fillna(0.0)
    df["price_nextday"] = pd.to_numeric(df["price_nextday"], errors="coerce").fillna(0.0)

    # Velg trade_price: primær hvis >0, ellers neste dag hvis >0, ellers dropp
    df["trade_price"] = df["price_main"]
    mask0 = df["trade_price"] <= 0
    df.loc[mask0, "trade_price"] = df.loc[mask0, "price_nextday"]

    df = df[df["trade_price"] > 0]
    if df.empty:
        return pd.DataFrame()

    # investor type filter (gjøres i pandas)
    type_map = fetch_investor_type_map(conn, cols)
    if type_map and invtype_choice and str(invtype_choice).strip() and str(invtype_choice).strip() != "Alle":
        mask = df["investor_id"].map(type_map).apply(lambda t: bool(normalize_invtype_filter(str(invtype_choice), t)))
        mask = pd.Series(mask).fillna(False).astype(bool)
        df = df[mask]
        if df.empty:
            return pd.DataFrame()

    # country filter (gjøres i pandas)
    cc_map = fetch_investor_country_map(conn, cols)
    if cc_map and country_codes:
        mask_cc = df["investor_id"].map(cc_map).apply(lambda cc: bool(normalize_country_filter(country_codes, cc)))
        mask_cc = pd.Series(mask_cc).fillna(False).astype(bool)
        df = df[mask_cc]
        if df.empty:
            return pd.DataFrame()

    # last price (hele DB) – nå primært security.last_price
    df["last_price"] = df["isin"].map(last_price_map).fillna(0.0)

    # gevinst per rad: qty*(last - trade) (qty<0 gir salg-fortegn automatisk)
    df["profit_kr"] = df["qty"] * (df["last_price"] - df["trade_price"])

    # bruttohandel: |qty|*trade_price
    df["gross_kr"] = (df["qty"].abs() * df["trade_price"]).abs()

    agg = df.groupby("investor_id").agg(
        trades=("qty", "count"),
        gross_kr=("gross_kr", "sum"),
        profit_kr=("profit_kr", "sum"),
    ).reset_index()

    agg["Brutto MNOK"] = agg["gross_kr"] / 1_000_000
    agg["Gevinst MNOK"] = agg["profit_kr"] / 1_000_000
    agg["Gevinst %"] = agg.apply(
        lambda r: (r["profit_kr"] / r["gross_kr"]) * 100.0 if r["gross_kr"] else 0.0, axis=1
    )

    # filtre
    agg = agg[(agg["trades"] >= int(min_trades)) & (agg["Brutto MNOK"] >= float(min_brutto_mnok))]
    if agg.empty:
        return pd.DataFrame()

    # navn
    navn_map: Dict[str, str] = {}
    if "investor" in _list_tables(conn):
        select_cols = ["investor_id"]
        if cols.inv_first_col:
            select_cols.append(f"COALESCE({cols.inv_first_col},'') AS first_name")
        else:
            select_cols.append("'' AS first_name")
        if cols.inv_last_col:
            select_cols.append(f"COALESCE({cols.inv_last_col},'') AS last_name")
        else:
            select_cols.append("'' AS last_name")
        if cols.inv_name_col:
            select_cols.append(f"COALESCE({cols.inv_name_col},'') AS name")
        else:
            select_cols.append("'' AS name")

        inv = pd.read_sql(f"SELECT {', '.join(select_cols)} FROM investor", conn)
        inv["investor_id"] = inv["investor_id"].astype(str).str.strip()

        def mk_name(r):
            first = _clean_nan(str(r.get("first_name", "")))
            last = _clean_nan(str(r.get("last_name", "")))
            name = _clean_nan(str(r.get("name", "")))
            disp = " ".join(x for x in [first, last] if x).strip()
            if not disp:
                disp = name
            return disp

        inv["navn"] = inv.apply(mk_name, axis=1)
        navn_map = dict(zip(inv["investor_id"], inv["navn"]))

    agg["navn"] = agg["investor_id"].map(navn_map).fillna("")

    out = agg[["investor_id", "navn", "trades", "Brutto MNOK", "Gevinst MNOK", "Gevinst %"]].copy()
    out.rename(columns={"trades": "Antall handler"}, inplace=True)

    out = out.sort_values("Gevinst MNOK", ascending=False).reset_index(drop=True)
    return out


# =========================================================
# Transactions drill-down
# =========================================================
def fetch_transactions(
    conn: sqlite3.Connection,
    cols: DbCols,
    investor_id: str,
    date_from: dt.date,
    date_to: dt.date,
    isin_filter: Optional[List[str]],
    last_price_map: Dict[str, float],
) -> pd.DataFrame:
    ticker_expr = f"COALESCE(s.{cols.sec_ticker_col}, '') AS ticker" if cols.sec_ticker_col else "'' AS ticker"
    name_expr = f"COALESCE(s.{cols.sec_name_col}, '') AS navn" if cols.sec_name_col else "'' AS navn"

    where = [f"pc.{cols.investor_col} = ?",
             f"date(pc.{cols.date_col}) BETWEEN date(?) AND date(?)"]
    params: List[str] = [investor_id, date_from.isoformat(), date_to.isoformat()]

    if isin_filter:
        wanted_isin = [x.strip() for x in isin_filter]
        placeholders = ",".join(["?"] * len(wanted_isin))
        where.append(f"pc.{cols.isin_col} IN ({placeholders})")
        params.extend(wanted_isin)

    # Samme dupliserings-fiks som i compute_best_investors: aggreger én pris per (isin, dato)
    sql = f"""
    WITH prices AS (
        SELECT
            {cols.isin_col} AS isin,
            date({cols.date_col}) AS d,
            MAX({cols.price_trade_col}) AS p
        FROM position_change
        WHERE COALESCE({cols.price_trade_col}, 0) > 0
        GROUP BY {cols.isin_col}, date({cols.date_col})
    )
    SELECT
        date(pc.{cols.date_col}) AS dato,
        pc.{cols.isin_col} AS isin,
        {ticker_expr},
        {name_expr},
        pc.{cols.qty_col} AS qty,
        pc.{cols.price_trade_col} AS price_main,
        p2.p AS price_nextday
    FROM position_change pc
    LEFT JOIN security s ON s.{cols.sec_isin_col} = pc.{cols.isin_col}
    LEFT JOIN prices p2
      ON p2.isin = pc.{cols.isin_col}
     AND p2.d = date(pc.{cols.date_col}, '+1 day')
    WHERE {" AND ".join(where)}
    ORDER BY date(pc.{cols.date_col}) ASC
    """
    df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return df

    df["isin"] = df["isin"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["price_main"] = pd.to_numeric(df["price_main"], errors="coerce").fillna(0.0)
    df["price_nextday"] = pd.to_numeric(df["price_nextday"], errors="coerce").fillna(0.0)

    df["trade_price"] = df["price_main"]
    mask0 = df["trade_price"] <= 0
    df.loc[mask0, "trade_price"] = df.loc[mask0, "price_nextday"]

    df = df[df["trade_price"] > 0]
    if df.empty:
        return df

    df["last_price"] = df["isin"].map(last_price_map).fillna(0.0)

    df["gross_kr"] = (df["qty"].abs() * df["trade_price"]).abs()
    df["profit_kr"] = df["qty"] * (df["last_price"] - df["trade_price"])

    df["Brutto MNOK"] = df["gross_kr"] / 1_000_000
    df["Gevinst MNOK"] = df["profit_kr"] / 1_000_000
    return df


# =========================================================
# UI
# =========================================================
def run(db_path: str, list_dir: Optional[str] = None):
    # Kompakt UI (mindre høyde/padding på input-felter)
    st.markdown(
        """
        <style>
          div.block-container { padding-top: 1.1rem; padding-bottom: 1.1rem; }
          div[role="radiogroup"] { gap: 0.35rem !important; }
          label[data-baseweb="radio"] { margin: 0 !important; padding: 0 !important; }
          div[data-baseweb="select"] > div { min-height: 34px !important; }
          div[data-baseweb="input"] > div { min-height: 34px !important; }
          div[data-baseweb="input"] input { padding-top: 6px !important; padding-bottom: 6px !important; }
          div[data-baseweb="input"] input[type="number"] { padding-top: 6px !important; padding-bottom: 6px !important; }
          .stDivider { margin-top: 0.7rem; margin-bottom: 0.7rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.header("Beste investorer")
    st.caption("Ranger investorer på gevinst (kr) og gevinst% for valgt periode og utvalg.")

    conn = db_connect(db_path)
    cols = detect_cols(conn)

    # Indeksering for ytelse
    ensure_indexes(conn, cols)

    # ---------- session state ----------
    if "best_res" not in st.session_state:
        st.session_state.best_res = None
    if "best_params" not in st.session_state:
        st.session_state.best_params = None
    if "best_selected_investor" not in st.session_state:
        st.session_state.best_selected_investor = None
        st.session_state.best_selected_investor_name = ""
    if "best_selected_investor_name" not in st.session_state:
        st.session_state.best_selected_investor_name = ""
    if "best_last_price_map" not in st.session_state:
        st.session_state.best_last_price_map = None
    if "best_last_price_source" not in st.session_state:
        st.session_state.best_last_price_source = None

    # Periode
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.date_input("Fra dato", value=DEFAULT_FROM, key="best_from")
    with c2:
        date_to = st.date_input("Til dato", value=dt.date.today(), key="best_to")

    if date_to < date_from:
        st.error("Til-dato kan ikke være før fra-dato.")
        return

    st.divider()

    # Investorutvalg
    st.subheader("Investorutvalg")
    investor_mode = st.radio(
        "Velg investorer",
        ["Alle", "CSV fra Eiere-Styring", "Søk og velg"],
        horizontal=True,
        key="best_inv_mode",
    )

    investor_ids: Optional[List[str]] = None

    if investor_mode == "CSV fra Eiere-Styring":
        if not list_dir:
            st.warning("list_dir er ikke satt. Send inn list_dir fra page-filen.")
            return

        csv_files = [f for f in os.listdir(list_dir) if f.lower().endswith(".csv")]
        csv_files.sort()

        picked = st.selectbox(
            "Velg CSV med investorer",
            csv_files,
            index=None,
            placeholder="Velg fil…",
            key="best_inv_csv",
        )
        if picked:
            investor_ids = load_first_column_values(os.path.join(list_dir, picked))

    elif investor_mode == "Søk og velg":
        country_options_search = fetch_distinct_country_codes(conn, cols)
        country_codes_search = st.multiselect(
            "Country code (filter i søk)",
            options=country_options_search,
            default=[],
            key="best_country_codes_search",
        )
        investor_ids = investor_search_multiselect(conn, cols, country_filter=country_codes_search) or None

    invtype_choice = st.selectbox(
        "Investortype",
        ["Alle", INVTYPE_ORG, INVTYPE_PRIVAT],
        index=0,
        key="best_invtype",
    )

    country_options = fetch_distinct_country_codes(conn, cols)
    country_codes = st.multiselect(
        "Country code",
        options=country_options,
        default=[],
        help="Filtrer investorer på landkode (Country code).",
        key="best_country_codes",
    )

    st.divider()

    # Aksjeutvalg
    st.subheader("Aksjeutvalg")
    aksje_mode = st.radio(
        "Velg aksjer",
        ["Alle", "Én aksje (ticker/navn)", "CSV med aksjer (ticker/navn)"],
        horizontal=True,
        key="best_sec_mode",
    )

    isin_filter: Optional[List[str]] = None

    if aksje_mode == "Én aksje (ticker/navn)":
        q = st.text_input("Skriv ticker eller starten av selskapsnavn", key="best_one_sec")
        if q.strip():
            rows = search_securities_by_prefix(conn, cols, q.strip(), limit=50)

            if not rows:
                st.info("Ingen treff på ticker/navn-start. Prøv færre tegn eller et annet prefix.")
            else:
                # bygg valg-liste
                options = []
                opt_to_isin = {}
                for r in rows:
                    ticker = (r["ticker"] or "").strip()
                    navn = (r["navn"] or "").strip()
                    isin = str(r["isin"]).strip()

                    if ticker and navn:
                        label = f"{ticker} — {navn}"
                    elif navn:
                        label = navn
                    elif ticker:
                        label = ticker
                    else:
                        label = isin  # siste utvei (vises nesten aldri)

                    # unngå kollisjon
                    if label in opt_to_isin and opt_to_isin[label] != isin:
                        label = f"{label} ({isin})"

                    options.append(label)
                    opt_to_isin[label] = isin

                picked = st.selectbox(
                    "Velg selskap",
                    options=options,
                    index=None,
                    placeholder="Velg fra treff…",
                    key="best_one_sec_pick",
                )
                if picked:
                    isin_filter = [opt_to_isin[picked]]

    elif aksje_mode == "CSV med aksjer (ticker/navn)":
        if not list_dir:
            st.warning("list_dir er ikke satt. Send inn list_dir fra page-filen.")
            return

        csv_files = [f for f in os.listdir(list_dir) if f.lower().endswith(".csv")]
        csv_files.sort()

        picked = st.selectbox(
            "Velg CSV med aksjer",
            csv_files,
            index=None,
            placeholder="Velg fil…",
            key="best_sec_csv",
        )
        if picked:
            tokens = load_first_column_values(os.path.join(list_dir, picked))
            isin_filter = resolve_isin_list_by_ticker_name_prefix(conn, cols, tokens) or None
            if tokens and not isin_filter:
                st.info("Fant ingen selskaper fra CSV basert på ticker/navn-start (prefix-match).")

    st.divider()

    # Parametre
    st.subheader("Filtre")
    c1, c2 = st.columns(2)
    with c1:
        min_trades = st.number_input("Min antall handler", min_value=1, value=10, step=1, key="best_min_trades")
    with c2:
        min_brutto_mnok = st.number_input("Min bruttohandel (MNOK)", min_value=0.0, value=10.0, step=1.0, key="best_min_gross")

    run_btn = st.button("Kjør analyse", type="primary", key="best_run")

    # ---------- KJØR analyse ----------
    if run_btn:
        last_map, source_txt = build_last_price_cache(conn, cols)
        st.session_state.best_last_price_map = last_map
        st.session_state.best_last_price_source = source_txt

        res = compute_best_investors(
            conn=conn,
            cols=cols,
            date_from=date_from,
            date_to=date_to,
            investor_ids=investor_ids,
            invtype_choice=invtype_choice,
            country_codes=country_codes,
            isin_filter=isin_filter,
            min_trades=int(min_trades),
            min_brutto_mnok=float(min_brutto_mnok),
            last_price_map=last_map,
        )

        st.session_state.best_res = res
        st.session_state.best_params = {
            "date_from": date_from,
            "date_to": date_to,
            "isin_filter": isin_filter,
        }
        st.session_state.best_selected_investor = None

    # ---------- VIS resultat ----------
    res = st.session_state.best_res
    params = st.session_state.best_params
    last_map = st.session_state.best_last_price_map or {}
    source_txt = st.session_state.best_last_price_source or "Pris-kilde: (ukjent)"

    if res is None:
        return

    if res.empty:
        st.warning("Ingen treff med valgte filtre/utvalg.")
        return

    st.subheader("Resultat")
    st.caption(source_txt)

    # visning UTEN investor_id (brukes bare internt)
    res_display = res.drop(columns=["investor_id"], errors="ignore")

    chosen_investor_id = None
    chosen_investor_name = None

    # row selection hvis støttet
    try:
        event = st.dataframe(
            res_display,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            key="best_result_df",
        )
        if event and hasattr(event, "selection") and event.selection and event.selection.rows:
            idx = event.selection.rows[0]
            chosen_investor_id = str(res.iloc[idx]["investor_id"]).strip()
            chosen_investor_name = str(res.iloc[idx].get("navn", "")).strip()
    except Exception:
        pass

    # fallback: dropdown (vis kun navn)
    if not chosen_investor_id:
        labels: List[str] = []
        label_to_id: Dict[str, str] = {}
        label_to_name: Dict[str, str] = {}
        seen: Dict[str, int] = {}

        for _, row in res.iterrows():
            name = str(row.get("navn", "")).strip() or "(Ukjent)"
            iid = str(row.get("investor_id", "")).strip()

            n = seen.get(name, 0) + 1
            seen[name] = n
            label = name if n == 1 else f"{name} • {n}"

            labels.append(label)
            label_to_id[label] = iid
            label_to_name[label] = name

        picked_label = st.selectbox(
            "Vis transaksjoner for investor",
            options=labels,
            index=None,
            placeholder="Søk/velg navn…",
            key="best_tx_pick",
        )
        if picked_label:
            chosen_investor_id = label_to_id[picked_label]
            chosen_investor_name = label_to_name[picked_label]

    if chosen_investor_id:
        st.session_state.best_selected_investor = chosen_investor_id
        st.session_state.best_selected_investor_name = chosen_investor_name or ""

    st.download_button(
        "Last ned resultat (CSV)",
        res_display.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
        file_name=f"beste_investorer_{params['date_from']}_{params['date_to']}.csv" if params else "beste_investorer.csv",
        mime="text/csv",
        key="best_dl_res",
    )

    # ---------- TRANS ------
    chosen_investor_id = st.session_state.best_selected_investor
    if chosen_investor_id:
        st.divider()
        title_name = (st.session_state.get("best_selected_investor_name") or "").strip() or "(Ukjent)"
        st.subheader(f"Transaksjoner for {title_name}")

        d_from = params["date_from"] if params else date_from
        d_to = params["date_to"] if params else date_to
        f_isin = params["isin_filter"] if params else isin_filter

        tx = fetch_transactions(
            conn=conn,
            cols=cols,
            investor_id=chosen_investor_id,
            date_from=d_from,
            date_to=d_to,
            isin_filter=f_isin,
            last_price_map=last_map,
        )

        if tx.empty:
            st.info("Ingen transaksjoner i perioden.")
            return

        show_cols = ["dato", "ticker", "navn", "isin", "qty", "trade_price", "last_price", "Brutto MNOK", "Gevinst MNOK"]
        st.dataframe(tx[show_cols], use_container_width=True)

        gross_mnok = tx["gross_kr"].sum() / 1_000_000
        profit_mnok = tx["profit_kr"].sum() / 1_000_000
        pct = (tx["profit_kr"].sum() / tx["gross_kr"].sum() * 100.0) if tx["gross_kr"].sum() else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("Kontroll: Brutto MNOK", f"{gross_mnok:,.2f}")
        c2.metric("Kontroll: Gevinst MNOK", f"{profit_mnok:,.2f}")
        c3.metric("Kontroll: Gevinst %", f"{pct:,.2f}")

        st.download_button(
            "Last ned transaksjoner (CSV)",
            tx.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
            file_name=f"transaksjoner_{title_name}_{d_from}_{d_to}.csv".replace(" ", "_"),
            mime="text/csv",
            key="best_dl_tx",
        )
