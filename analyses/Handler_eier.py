# analyses/handler_eier.py
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


def fetch_aggregated_by_security(
    conn,
    investor_id: str,
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
),
trades AS (
    SELECT
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


def fetch_transactions_for_security(
    conn,
    investor_id: str,
    isin: str,
    date_from: dt.date,
    date_to: dt.date,
):
    """
    Returnerer enkelt-observasjoner for investor+isin i datointervall.
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
)
SELECT
    pc.date_today AS dato,
    pc.change_qty AS antall,
    COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) AS kurs,
    (COALESCE(pc.change_qty,0) * COALESCE(NULLIF(pc.price_yesterday, 0), p2.p)) AS belop
FROM position_change pc
LEFT JOIN prices p2
  ON p2.isin = pc.isin
 AND p2.d = date(pc.date_today, '+1 day')
WHERE pc.investor_id = ?
  AND pc.isin = ?
  AND pc.date_today BETWEEN ? AND ?
  AND COALESCE(NULLIF(pc.price_yesterday, 0), p2.p) > 0
ORDER BY pc.date_today ASC"""
    return conn.execute(
        sql,
        (investor_id, isin, date_from.isoformat(), date_to.isoformat())
    ).fetchall()


# =========================================================
# STREAMLIT UI
# =========================================================

def run(db_path: str):
    st.header("📌 Handler per eier")
    st.caption("Aggregerer handler per verdipapir for valgt investor, med detaljer per valgt aksje.")

    # Hold på siste resultat mellom reruns
    if "handler_eier_last_df" not in st.session_state:
        st.session_state.handler_eier_last_df = None
    if "handler_eier_last_meta" not in st.session_state:
        st.session_state.handler_eier_last_meta = None

    conn = db_connect(db_path)

    # --- Investor-søk
    query = st.text_input("Søk investor (min 4 tegn)")
    investors = fetch_investors(conn, query)

    investor_map: dict[str, str] = {}
    options: list[str] = []

    for r in investors:
        first = (r["first_name"] or "")
        last = (r["last_name"] or "")

        # Håndter at noen kan være "nan" som tekst
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

    # --- Datoer
    today = dt.date.today()
    col1, col2 = st.columns(2)

    with col1:
        date_from = st.date_input("Fra dato", value=today - dt.timedelta(days=30))
    with col2:
        date_to = st.date_input("Til dato", value=today)

    if not selected:
        st.info("Velg investor for å vise handler")
        return

    if date_to < date_from:
        st.error("Til-dato kan ikke være før fra-dato")
        return

    # --- Hent aggregat
    if st.button("Hent handler", type="primary"):
        investor_id = investor_map[selected]
        rows = fetch_aggregated_by_security(conn, investor_id, date_from, date_to)

        if not rows:
            st.warning("Ingen handler i perioden")
            st.session_state.handler_eier_last_df = None
            st.session_state.handler_eier_last_meta = None
            return

        df = pd.DataFrame([dict(r) for r in rows])
        df["Netto MNOK"] = df["netto_belop"] / 1_000_000
        df["Brutto MNOK"] = df["brutto_belop"] / 1_000_000

        st.session_state.handler_eier_last_df = df
        st.session_state.handler_eier_last_meta = {
            "investor_id": investor_id,
            "date_from": date_from,
            "date_to": date_to,
        }

    # --- Vis aggregat hvis vi har det
    df = st.session_state.handler_eier_last_df
    meta = st.session_state.handler_eier_last_meta

    if df is None or meta is None:
        return

    st.dataframe(
        df[
            [
                "ticker",
                "isin",
                "navn",
                "antall_obs",
                "netto_antall",
                "Netto MNOK",
                "Brutto MNOK",
            ]
        ],
        use_container_width=True,
    )

    st.success(f"{len(df)} verdipapir")

    # CSV
    st.download_button(
        "Last ned CSV",
        df.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
        file_name=f"handler_{meta['investor_id']}.csv",
        mime="text/csv",
    )

    # =========================================================
    # DRILL-DOWN / DETALJER
    # =========================================================

    st.divider()
    st.subheader("Detaljer")

    # Lag en pen valgliste fra aggregatet
    df2 = df.copy()
    df2["ticker"] = df2["ticker"].fillna("")
    df2["navn"] = df2["navn"].fillna("")
    df2["valg"] = df2["ticker"] + " | " + df2["navn"] + " | " + df2["isin"]

    default_idx = 0 if len(df2) > 0 else None
    choice = st.selectbox(
        "Velg verdipapir for å se enkelt-transaksjoner (observasjoner)",
        df2["valg"].tolist(),
        index=default_idx,
    )

    chosen_isin = df2.loc[df2["valg"] == choice, "isin"].iloc[0]
    chosen_ticker = df2.loc[df2["valg"] == choice, "ticker"].iloc[0]

    detail_rows = fetch_transactions_for_security(
        conn,
        meta["investor_id"],
        chosen_isin,
        meta["date_from"],
        meta["date_to"],
    )
    detail_df = pd.DataFrame([dict(r) for r in detail_rows])

    if detail_df.empty:
        st.info("Ingen detaljer funnet.")
        return

    # Beregn beløp i MNOK og rydd litt
    detail_df["belop_mnok"] = detail_df["belop"] / 1_000_000

    st.caption(f"Viser {len(detail_df)} observasjoner for {chosen_ticker} ({chosen_isin})")

    st.dataframe(
        detail_df[["dato", "antall", "kurs", "belop_mnok"]],
        use_container_width=True,
    )

    total = float(detail_df["belop"].fillna(0).sum())
    st.write(f"Sum beløp: **{total/1_000_000:,.2f} MNOK**".replace(",", " "))

