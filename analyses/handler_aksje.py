# analyses/handler_aksje.py
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
# DATAHENTING
# =========================================================

def fetch_security_suggestions(conn: sqlite3.Connection, query: str, limit: int = 50):
    q = (query or "").strip()
    if len(q) < 2:
        return []

    q_up = q.upper()
    like_any = f"%{q_up}%"
    like_pfx = f"{q_up}%"

    sql = """
    SELECT
        isin,
        COALESCE(ticker,'') AS ticker,
        COALESCE(isin_name,'') AS isin_name
    FROM security
    WHERE
        UPPER(COALESCE(ticker,'')) LIKE :pfx
        OR UPPER(COALESCE(isin_name,'')) LIKE :pfx
        OR UPPER(COALESCE(ticker,'')) LIKE :any
        OR UPPER(COALESCE(isin_name,'')) LIKE :any
    ORDER BY
        CASE
            WHEN UPPER(COALESCE(ticker,'')) LIKE :pfx THEN 0
            WHEN UPPER(COALESCE(isin_name,'')) LIKE :pfx THEN 1
            ELSE 2
        END,
        COALESCE(ticker,'') ASC,
        COALESCE(isin_name,'') ASC
    LIMIT :lim
    """
    return conn.execute(sql, {"pfx": like_pfx, "any": like_any, "lim": int(limit)}).fetchall()


def fetch_buy_sell_by_investor(
    conn: sqlite3.Connection,
    isin: str,
    date_from: dt.date,
    date_to: dt.date,
):
    """
    Returnerer per investor:
      - kjøp_antall / kjøp_beløp  (kun change_qty > 0)
      - salg_antall / salg_beløp  (kun change_qty < 0)  (beløp rapporteres positivt)
      - antall_obs total
      - netto_beløp (inkl fortegn)
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
    t.investor_id AS investor_id,
    COALESCE(i.first_name,'') AS first_name,
    COALESCE(i.last_name,'') AS last_name,
    COALESCE(i.investor_type,'') AS investor_type,

    COUNT(*) AS antall_obs,

    SUM(CASE WHEN COALESCE(t.change_qty,0) > 0 THEN COALESCE(t.change_qty,0) ELSE 0 END) AS kjop_antall,
    SUM(CASE WHEN COALESCE(t.change_qty,0) > 0 THEN COALESCE(t.change_qty,0) * t.trade_price ELSE 0 END) AS kjop_belop,

    SUM(CASE WHEN COALESCE(t.change_qty,0) < 0 THEN ABS(COALESCE(t.change_qty,0)) ELSE 0 END) AS salg_antall,
    SUM(CASE WHEN COALESCE(t.change_qty,0) < 0 THEN ABS(COALESCE(t.change_qty,0) * t.trade_price) ELSE 0 END) AS salg_belop,

    SUM(COALESCE(t.change_qty,0) * t.trade_price) AS netto_belop
FROM trades t
LEFT JOIN investor i ON i.investor_id = t.investor_id
WHERE COALESCE(t.trade_price,0) > 0
GROUP BY t.investor_id, i.first_name, i.last_name, i.investor_type"""
    return conn.execute(sql, (isin, date_from.isoformat(), date_to.isoformat())).fetchall()


def fetch_all_transactions_for_security(
    conn: sqlite3.Connection,
    isin: str,
    date_from: dt.date,
    date_to: dt.date,
):
    sql = """
WITH prices AS (
    SELECT
        isin,
        date(date_today) AS d,
        MAX(price_yesterday) AS p
    FROM position_change
    WHERE COALESCE(price_yesterday, 0) > 0
    GROUP BY isin, date(date_today)
)
SELECT
    pc.date_today AS dato,
    COALESCE(s.ticker,'') AS ticker,
    pc.isin AS isin,
    COALESCE(s.isin_name,'') AS navn,

    pc.investor_id AS investor_id,
    COALESCE(i.first_name,'') AS first_name,
    COALESCE(i.last_name,'') AS last_name,
    COALESCE(i.investor_type,'') AS investor_type,

    pc.change_qty AS antall,
    COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) AS kurs,
    (COALESCE(pc.change_qty,0) * COALESCE(NULLIF(pc.price_yesterday, 0), p2.p)) AS belop
FROM position_change pc
JOIN security s ON s.isin = pc.isin
LEFT JOIN investor i ON i.investor_id = pc.investor_id
LEFT JOIN prices p2
  ON p2.isin = pc.isin
 AND p2.d = date(pc.date_today, '+1 day')
WHERE pc.isin = ?
  AND pc.date_today BETWEEN ? AND ?
  AND COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) > 0
ORDER BY pc.date_today ASC, ABS(COALESCE(pc.change_qty,0) * COALESCE(NULLIF(pc.price_yesterday, 0), p2.p)) DESC"""
    return conn.execute(sql, (isin, date_from.isoformat(), date_to.isoformat())).fetchall()


# =========================================================
# HJELPERE
# =========================================================

def _clean_name(first: str, last: str, fallback: str) -> str:
    def fix(x: str) -> str:
        x = (x or "").strip()
        return "" if x.lower() == "nan" else x

    first = fix(first)
    last = fix(last)
    name = " ".join([first, last]).strip()
    return name if name else fallback


def _format_counts(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    # Vis antall som heltall (uten .0)
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(0).astype("int64")
    return df


# =========================================================
# STREAMLIT UI
# =========================================================

def run(db_path: str):
    st.header("📈 Handler per aksje")
    st.caption("To tabeller per eier: mest netto kjøp og mest netto salg. (Valgfritt: vis alle transaksjoner)")

    conn = db_connect(db_path)

    # --- Velg aksje
    q = st.text_input("Søk selskap / ticker (min 2 tegn)", placeholder="Eks: YAR, NOD, Aker...")
    suggestions = fetch_security_suggestions(conn, q)

    sec_options = []
    sec_map = {}  # label -> isin
    for r in suggestions:
        ticker = (r["ticker"] or "").strip()
        isin = (r["isin"] or "").strip()
        navn = (r["isin_name"] or "").strip()
        sec_options.append(f"{ticker} | {navn} | {isin}")
        sec_map[sec_options[-1]] = isin

    selected_sec = st.selectbox("Velg aksje", sec_options, index=None, placeholder="Velg aksje…")

    # --- Datoer
    today = dt.date.today()
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("Startdato", value=today - dt.timedelta(days=30))
    with col2:
        date_to = st.date_input("Sluttdato", value=today)

    if date_to < date_from:
        st.error("Sluttdato kan ikke være før startdato.")
        return
    if not selected_sec:
        st.info("Velg en aksje for å hente data.")
        return

    top_n = st.slider("Vis topp N eiere", min_value=10, max_value=200, value=30, step=10)
    show_all = st.checkbox("Vis alle transaksjoner (valgfritt)", value=False)

    if st.button("Hent", type="primary"):
        isin = sec_map[selected_sec]

        # --- Aggreger per eier (kjøp/salg)
        rows = fetch_buy_sell_by_investor(conn, isin, date_from, date_to)
        df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

        if df.empty:
            st.warning("Ingen data funnet i perioden.")
            return

        # Bruk navn, ellers "Ukjent eier" (ikke investor_id)
        df["eier"] = [
            _clean_name(r["first_name"], r["last_name"], "Ukjent eier") for _, r in df.iterrows()
        ]

        # MNOK-kolonner
        df["kjop_mnok"] = df["kjop_belop"].fillna(0) / 1_000_000
        df["salg_mnok"] = df["salg_belop"].fillna(0) / 1_000_000
        df["netto_mnok"] = df["netto_belop"].fillna(0) / 1_000_000

        # Ryddige heltall for antall
        df = _format_counts(df, ["antall_obs", "kjop_antall", "salg_antall"])

        # Vis 1 desimal på MNOK i tabell (Streamlit-format)
        fmt = {
            "kjop_mnok": st.column_config.NumberColumn("Kjøp (MNOK)", format="%.1f"),
            "salg_mnok": st.column_config.NumberColumn("Salg (MNOK)", format="%.1f"),
            "netto_mnok": st.column_config.NumberColumn("Netto (MNOK)", format="%.1f"),
        }

        # --- Tabell 1: Mest netto kjøp
        st.subheader("Mest netto kjøp (per eier)")
        buy_df = df[df["netto_mnok"] > 0].sort_values("netto_mnok", ascending=False).head(top_n)

        if buy_df.empty:
            st.info("Ingen netto kjøpere i perioden.")
        else:
            st.dataframe(
                buy_df[
                    [
                        "eier",
                        "investor_type",
                        "antall_obs",
                        "kjop_antall",
                        "kjop_mnok",
                        "salg_antall",
                        "salg_mnok",
                        "netto_mnok",
                    ]
                ],
                use_container_width=True,
                column_config=fmt,
            )

            st.download_button(
                "Last ned CSV (mest netto kjøp)",
                buy_df.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
                file_name=f"handler_aksje_mest_netto_kjop_{isin}_{date_from}_{date_to}.csv",
                mime="text/csv",
            )

        # --- Tabell 2: Mest netto salg
        st.subheader("Mest netto salg (per eier)")
        sell_df = df[df["netto_mnok"] < 0].sort_values("netto_mnok", ascending=True).head(top_n)

        if sell_df.empty:
            st.info("Ingen netto selgere i perioden.")
        else:
            st.dataframe(
                sell_df[
                    [
                        "eier",
                        "investor_type",
                        "antall_obs",
                        "kjop_antall",
                        "kjop_mnok",
                        "salg_antall",
                        "salg_mnok",
                        "netto_mnok",
                    ]
                ],
                use_container_width=True,
                column_config=fmt,
            )

            st.download_button(
                "Last ned CSV (mest netto salg)",
                sell_df.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
                file_name=f"handler_aksje_mest_netto_salg_{isin}_{date_from}_{date_to}.csv",
                mime="text/csv",
            )

        # --- Valgfritt: alle transaksjoner
        if show_all:
            st.divider()
            st.subheader("Alle transaksjoner/observasjoner (valgfritt)")

            trows = fetch_all_transactions_for_security(conn, isin, date_from, date_to)
            tdf = pd.DataFrame([dict(r) for r in trows]) if trows else pd.DataFrame()

            if tdf.empty:
                st.info("Ingen transaksjoner.")
            else:
                tdf["eier"] = [
                    _clean_name(r["first_name"], r["last_name"], "Ukjent eier") for _, r in tdf.iterrows()
                ]
                tdf["belop_mnok"] = tdf["belop"].fillna(0) / 1_000_000

                # 1 desimal på beløp
                tdf["belop_mnok"] = pd.to_numeric(tdf["belop_mnok"], errors="coerce").fillna(0).round(1)

                st.dataframe(
                    tdf[["dato", "ticker", "isin", "navn", "eier", "investor_type", "antall", "kurs", "belop_mnok"]],
                    use_container_width=True,
                    column_config={
                        "belop_mnok": st.column_config.NumberColumn("Beløp (MNOK)", format="%.1f"),
                    },
                )

                st.download_button(
                    "Last ned CSV (alle transaksjoner)",
                    tdf.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
                    file_name=f"handler_aksje_transaksjoner_{isin}_{date_from}_{date_to}.csv",
                    mime="text/csv",
                )
