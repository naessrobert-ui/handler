# analyses/eier_oversikt.py
from __future__ import annotations

import sqlite3
import datetime as dt
import pandas as pd
import streamlit as st


# =========================================================
# DB
# =========================================================

def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# =========================================================
# INVESTOR-SØK (kopiert fra Handler_eier-mønster)
# =========================================================

def fetch_investors(conn, query: str, limit: int = 50):
    if len((query or "").strip()) < 4:
        return []

    q = query.upper().strip()
    like = f"%{q}%"

    sql = """
    SELECT investor_id, investor_type, first_name, last_name
    FROM investor
    WHERE
        UPPER(COALESCE(investor_id,'')) LIKE ?
        OR UPPER(COALESCE(first_name,'')) LIKE ?
        OR UPPER(COALESCE(last_name,'')) LIKE ?
        OR UPPER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE ?
    ORDER BY
        COALESCE(last_name,''),
        COALESCE(first_name,'')
    LIMIT ?
    """
    return conn.execute(sql, (like, like, like, like, limit)).fetchall()


def build_investor_select(conn) -> str | None:
    """
    Returnerer investor_id (str) eller None hvis ingen valgt.
    """
    query = st.text_input("Søk investor (min 4 tegn)")
    investors = fetch_investors(conn, query)

    investor_map: dict[str, str] = {}
    options: list[str] = []

    for r in investors:
        first = (r["first_name"] or "")
        last = (r["last_name"] or "")

        # Håndter at noen kan være "nan" som tekst (samme som Handler_eier)
        first = "" if str(first).strip().lower() == "nan" else str(first).strip()
        last = "" if str(last).strip().lower() == "nan" else str(last).strip()

        name = " ".join(x for x in [first, last] if x).strip()
        if not name:
            name = str(r["investor_id"]).strip()

        label = f"{name} ({r['investor_id']})"
        options.append(label)
        investor_map[label] = str(r["investor_id"]).strip()

    selected = st.selectbox(
        "Velg investor",
        options,
        index=None,
        placeholder="Velg investor…",
    )

    if not selected:
        return None

    return investor_map[selected]


# =========================================================
# DATA
# =========================================================

def fetch_agg_by_security(conn, investor_id: str, date_from: dt.date, date_to: dt.date):
    """
    Aggregerer endringer per verdipapir for valgt investor i perioden.
    """
    sql = """
WITH prices AS (
    SELECT
        isin,
        date(date_today) AS d,
        MAX(price_yesterday) AS p
    FROM position_change
    WHERE COALESCE(price_yesterday, 0) > 0
    GROUP BY isin, date(date_today)
),
trades AS (
    SELECT
        pc.date_today AS dato,
        pc.isin AS isin,
        pc.change_qty AS change_qty,
        COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) AS trade_price
    FROM position_change pc
    LEFT JOIN prices p2
      ON p2.isin = pc.isin
     AND p2.d = date(pc.date_today, '+1 day')
    WHERE pc.investor_id = ?
      AND pc.date_today BETWEEN ? AND ?
)
SELECT
    s.ticker,
    t.isin,
    COALESCE(s.isin_name,'') AS navn,
    COUNT(*) AS antall_obs,
    SUM(COALESCE(t.change_qty,0)) AS netto_antall,
    SUM(COALESCE(t.change_qty,0) * t.trade_price) AS netto_belop,
    SUM(ABS(COALESCE(t.change_qty,0) * t.trade_price)) AS brutto_belop
FROM trades t
JOIN security s ON s.isin = t.isin
WHERE COALESCE(t.trade_price,0) > 0
GROUP BY s.ticker, t.isin, s.isin_name
ORDER BY ABS(netto_belop) DESC"""
    return conn.execute(
        sql,
        (investor_id, date_from.isoformat(), date_to.isoformat())
    ).fetchall()


def fetch_timeseries_investor(conn, investor_id: str, date_from: dt.date, date_to: dt.date):
    """
    Tidsserie: netto beløp per dag (sum over alle ISIN).
    """
    sql = """
WITH prices AS (
    SELECT
        isin,
        date(date_today) AS d,
        MAX(price_yesterday) AS p
    FROM position_change
    WHERE COALESCE(price_yesterday, 0) > 0
    GROUP BY isin, date(date_today)
),
trades AS (
    SELECT
        pc.date_today AS dato,
        pc.change_qty AS change_qty,
        COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) AS trade_price
    FROM position_change pc
    LEFT JOIN prices p2
      ON p2.isin = pc.isin
     AND p2.d = date(pc.date_today, '+1 day')
    WHERE pc.investor_id = ?
      AND pc.date_today BETWEEN ? AND ?
)
SELECT
    dato,
    SUM(COALESCE(change_qty,0) * trade_price) AS netto_belop
FROM trades
WHERE COALESCE(trade_price,0) > 0
GROUP BY dato
ORDER BY dato ASC"""
    return conn.execute(
        sql,
        (investor_id, date_from.isoformat(), date_to.isoformat())
    ).fetchall()


def fetch_security_candidates(conn, query: str, limit: int = 50):
    if len((query or "").strip()) < 2:
        return []

    q = query.upper().strip()
    like = f"%{q}%"

    sql = """
    SELECT isin, COALESCE(ticker,'') AS ticker, COALESCE(isin_name,'') AS navn
    FROM security
    WHERE
        UPPER(COALESCE(isin,'')) LIKE ?
        OR UPPER(COALESCE(ticker,'')) LIKE ?
        OR UPPER(COALESCE(isin_name,'')) LIKE ?
    ORDER BY COALESCE(ticker,''), COALESCE(isin_name,'')
    LIMIT ?
    """
    return conn.execute(sql, (like, like, like, limit)).fetchall()


def build_security_select(conn) -> str | None:
    query = st.text_input("Søk aksje/ISIN (min 2 tegn)")
    rows = fetch_security_candidates(conn, query)

    m: dict[str, str] = {}
    opts: list[str] = []

    for r in rows:
        isin = str(r["isin"]).strip()
        ticker = str(r["ticker"] or "").strip()
        navn = str(r["navn"] or "").strip()
        label = f"{ticker} | {navn} | {isin}".strip()
        opts.append(label)
        m[label] = isin

    chosen = st.selectbox("Velg verdipapir", opts, index=None, placeholder="Velg verdipapir…")
    if not chosen:
        return None
    return m[chosen]


def fetch_agg_by_investor_for_isin(conn, isin: str, date_from: dt.date, date_to: dt.date):
    """
    Aggregerer endringer per investor for valgt ISIN i perioden.
    """
    sql = """
WITH prices AS (
    SELECT
        isin,
        date(date_today) AS d,
        MAX(price_yesterday) AS p
    FROM position_change
    WHERE COALESCE(price_yesterday, 0) > 0
    GROUP BY isin, date(date_today)
),
trades AS (
    SELECT
        pc.investor_id AS investor_id,
        pc.change_qty AS change_qty,
        COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) AS trade_price
    FROM position_change pc
    LEFT JOIN prices p2
      ON p2.isin = pc.isin
     AND p2.d = date(pc.date_today, '+1 day')
    WHERE pc.isin = ?
      AND pc.date_today BETWEEN ? AND ?
)
SELECT
    t.investor_id,
    COALESCE(i.first_name,'') AS first_name,
    COALESCE(i.last_name,'') AS last_name,
    COUNT(*) AS antall_obs,
    SUM(COALESCE(t.change_qty,0)) AS netto_antall,
    SUM(COALESCE(t.change_qty,0) * t.trade_price) AS netto_belop
FROM trades t
JOIN investor i ON i.investor_id = t.investor_id
WHERE COALESCE(t.trade_price,0) > 0
GROUP BY t.investor_id, i.first_name, i.last_name
ORDER BY ABS(netto_belop) DESC"""
    return conn.execute(
        sql,
        (isin, date_from.isoformat(), date_to.isoformat())
    ).fetchall()


# =========================================================
# UI
# =========================================================

def run(db_path: str):
    st.header("Eier oversikt")
    st.caption("Oppsummering av eierskap og endringer over tid, per aksje eller per eier")

    conn = db_connect(db_path)

    tab1, tab2 = st.tabs(["Per eier", "Per aksje"])

    today = dt.date.today()

    # -------------------------
    # Per eier
    # -------------------------
    with tab1:
        st.subheader("Oversikt per eier")

        investor_id = build_investor_select(conn)

        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input("Fra dato", value=today - dt.timedelta(days=30), key="e_from")
        with col2:
            date_to = st.date_input("Til dato", value=today, key="e_to")

        if not investor_id:
            st.info("Velg investor for å vise data")
            return

        if date_to < date_from:
            st.error("Til-dato kan ikke være før fra-dato")
            return

        if st.button("Hent data", type="primary", key="btn_eier"):
            rows = fetch_agg_by_security(conn, investor_id, date_from, date_to)
            df = pd.DataFrame([dict(r) for r in rows])

            if df.empty:
                st.warning("Ingen data i perioden")
            else:
                df["Netto MNOK"] = df["netto_belop"] / 1_000_000
                df["Brutto MNOK"] = df["brutto_belop"] / 1_000_000

                st.dataframe(
                    df[["ticker", "isin", "navn", "antall_obs", "netto_antall", "Netto MNOK", "Brutto MNOK"]],
                    use_container_width=True
                )

                # tidsserie
                ts = pd.DataFrame([dict(r) for r in fetch_timeseries_investor(conn, investor_id, date_from, date_to)])
                if not ts.empty:
                    ts["Netto MNOK"] = ts["netto_belop"] / 1_000_000
                    st.line_chart(ts.set_index("dato")["Netto MNOK"])

                st.download_button(
                    "Last ned CSV",
                    df.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
                    file_name=f"eier_oversikt_{investor_id}.csv",
                    mime="text/csv",
                )

    # -------------------------
    # Per aksje
    # -------------------------
    with tab2:
        st.subheader("Oversikt per aksje")

        isin = build_security_select(conn)

        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input("Fra dato", value=today - dt.timedelta(days=30), key="a_from")
        with col2:
            date_to = st.date_input("Til dato", value=today, key="a_to")

        if not isin:
            st.info("Velg verdipapir for å vise data")
            return

        if date_to < date_from:
            st.error("Til-dato kan ikke være før fra-dato")
            return

        if st.button("Hent data", type="primary", key="btn_aksje"):
            rows = fetch_agg_by_investor_for_isin(conn, isin, date_from, date_to)
            df = pd.DataFrame([dict(r) for r in rows])

            if df.empty:
                st.warning("Ingen data i perioden")
            else:
                # pen label-kolonne
                def mk_name(r):
                    first = (r.get("first_name") or "").strip()
                    last = (r.get("last_name") or "").strip()
                    first = "" if first.lower() == "nan" else first
                    last = "" if last.lower() == "nan" else last
                    name = " ".join(x for x in [first, last] if x).strip()
                    return name if name else str(r.get("investor_id"))

                df["navn"] = df.apply(mk_name, axis=1)
                df["Netto MNOK"] = df["netto_belop"] / 1_000_000

                st.dataframe(
                    df[["navn", "investor_id", "antall_obs", "netto_antall", "Netto MNOK"]],
                    use_container_width=True
                )

                st.download_button(
                    "Last ned CSV",
                    df.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
                    file_name=f"aksje_oversikt_{isin}.csv",
                    mime="text/csv",
                )
