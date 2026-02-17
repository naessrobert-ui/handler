# analyses/handler_best_viktige.py
from __future__ import annotations

import os
import sqlite3
import datetime as dt
from typing import Iterable

import pandas as pd
import streamlit as st


# =========================================================
# DB
# =========================================================

def db_connect(db_path: str) -> sqlite3.Connection:
    """
    NB: TEMP-tabeller i SQLite lever kun i den connectionen de opprettes i.
    Streamlit rerunner ofte, og du kan ende med ny connection -> temp-tabell borte.
    Derfor gjenoppretter vi temp-tabellen fra session_state (pack) ved behov.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# =========================================================
# CSV-lesing
# =========================================================

def read_csv_guess(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", encoding="latin-1", dtype=str).fillna("")


def extract_owner_patterns(list_name: str, df: pd.DataFrame) -> list[str]:
    """
    Beste.csv: første kolonne (header "Selskap") inneholder eier-navn.
    Viktige.csv: har kolonnen "Eier" (eier-navn).
    """
    patterns: list[str] = []

    if list_name.lower() == "beste":
        first_col = df.columns[0]
        patterns = df[first_col].astype(str).str.strip().tolist()
    else:
        eier_col = None
        for c in df.columns:
            if str(c).strip().lower() == "eier":
                eier_col = c
                break
        if eier_col is None:
            eier_col = df.columns[1] if df.shape[1] >= 2 else df.columns[0]
        patterns = df[eier_col].astype(str).str.strip().tolist()

    cleaned: list[str] = []
    for p in patterns:
        p2 = (p or "").strip()
        if not p2:
            continue
        low = p2.lower()
        if low in {"selskap", "eier"}:
            continue
        cleaned.append(p2)

    # dedup
    out: list[str] = []
    seen = set()
    for x in cleaned:
        key = x.lower()
        if key not in seen:
            seen.add(key)
            out.append(x)
    return out


# =========================================================
# Pattern -> investor_id
# =========================================================

def resolve_investor_ids(conn: sqlite3.Connection, patterns: list[str], max_hits_per_pattern: int = 50) -> pd.DataFrame:
    rows = []
    sql = """
    SELECT investor_id, investor_type, COALESCE(first_name,'') AS first_name, COALESCE(last_name,'') AS last_name
    FROM investor
    WHERE
      UPPER(COALESCE(investor_id,'')) LIKE :q
      OR UPPER(COALESCE(first_name,'')) LIKE :q
      OR UPPER(COALESCE(last_name,'')) LIKE :q
      OR UPPER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE :q
      OR UPPER(COALESCE(last_name,'') || ' ' || COALESCE(first_name,'')) LIKE :q
    LIMIT :lim
    """
    for pat in patterns:
        q = f"%{pat.upper()}%"
        hits = conn.execute(sql, {"q": q, "lim": int(max_hits_per_pattern)}).fetchall()
        for h in hits:
            d = dict(h)
            d["matched_pattern"] = pat
            rows.append(d)

    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["investor_id", "investor_type", "first_name", "last_name", "matched_pattern"]
    )
    if not df.empty:
        df["investor_id"] = df["investor_id"].astype(str).str.strip()
        df = df[df["investor_id"] != ""].drop_duplicates(subset=["investor_id"])
    return df


def ensure_temp_investor_table(conn: sqlite3.Connection, investor_ids: Iterable[str]) -> None:
    """
    Oppretter TEMP-tabellen i *denne* connectionen.
    Kalles:
      - når bruker trykker "Hent"
      - og på hver rerun når pack finnes (for å re-etablere temp-tabell)
    """
    conn.execute("DROP TABLE IF EXISTS temp_selected_investors;")
    conn.execute("CREATE TEMP TABLE temp_selected_investors (investor_id TEXT PRIMARY KEY);")
    drafting = [(str(x).strip(),) for x in investor_ids if str(x).strip()]
    conn.executemany("INSERT OR IGNORE INTO temp_selected_investors(investor_id) VALUES (?)", drafting)
    conn.commit()


# =========================================================
# Queries
# =========================================================

def fetch_top_by_security(conn: sqlite3.Connection, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
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
    JOIN temp_selected_investors t ON t.investor_id = pc.investor_id
    LEFT JOIN prices p2
      ON p2.isin = pc.isin
     AND p2.d = date(pc.date_today, '+1 day')
    WHERE pc.date_today BETWEEN ? AND ?
)
SELECT
  COALESCE(s.ticker,'') AS ticker,
  t.isin AS isin,
  COALESCE(s.isin_name,'') AS navn,
  COUNT(*) AS antall_obs,

  SUM(CASE WHEN COALESCE(t.change_qty,0) > 0
           THEN COALESCE(t.change_qty,0) * t.trade_price
           ELSE 0 END) AS kjop_belop,

  SUM(CASE WHEN COALESCE(t.change_qty,0) < 0
           THEN ABS(COALESCE(t.change_qty,0) * t.trade_price)
           ELSE 0 END) AS salg_belop,

  SUM(COALESCE(t.change_qty,0) * t.trade_price) AS netto_belop,

  SUM(ABS(COALESCE(t.change_qty,0) * t.trade_price)) AS brutto_belop
FROM trades t
JOIN security s ON s.isin = t.isin
WHERE COALESCE(t.trade_price,0) > 0
GROUP BY s.ticker, t.isin, s.isin_name"""
    rows = conn.execute(sql, (date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if df.empty:
        return df

    for c in ["kjop_belop", "salg_belop", "netto_belop", "brutto_belop"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
    df["salg_mnok"] = df["salg_belop"] / 1_000_000
    df["netto_mnok"] = df["netto_belop"] / 1_000_000
    df["brutto_mnok"] = df["brutto_belop"] / 1_000_000
    return df


def fetch_by_investor_for_security(conn: sqlite3.Connection, isin: str, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
    """
    Samlet per eier for valgt aksje (ingen transaksjonsliste).
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
    JOIN temp_selected_investors t ON t.investor_id = pc.investor_id
    LEFT JOIN prices p2
      ON p2.isin = pc.isin
     AND p2.d = date(pc.date_today, '+1 day')
    WHERE pc.isin = ?
      AND pc.date_today BETWEEN ? AND ?
)
SELECT
  tr.investor_id AS investor_id,
  COALESCE(i.first_name,'') AS first_name,
  COALESCE(i.last_name,'') AS last_name,
  COALESCE(i.investor_type,'') AS investor_type,
  COUNT(*) AS antall_obs,

  SUM(CASE WHEN COALESCE(tr.change_qty,0) > 0 THEN COALESCE(tr.change_qty,0) ELSE 0 END) AS kjop_antall,
  SUM(CASE WHEN COALESCE(tr.change_qty,0) > 0 THEN COALESCE(tr.change_qty,0) * tr.trade_price ELSE 0 END) AS kjop_belop,

  SUM(CASE WHEN COALESCE(tr.change_qty,0) < 0 THEN ABS(COALESCE(tr.change_qty,0)) ELSE 0 END) AS salg_antall,
  SUM(CASE WHEN COALESCE(tr.change_qty,0) < 0 THEN ABS(COALESCE(tr.change_qty,0) * tr.trade_price) ELSE 0 END) AS salg_belop,

  SUM(COALESCE(tr.change_qty,0) * tr.trade_price) AS netto_belop
FROM trades tr
LEFT JOIN investor i ON i.investor_id = tr.investor_id
WHERE COALESCE(tr.trade_price,0) > 0
GROUP BY tr.investor_id, i.first_name, i.last_name, i.investor_type"""
    rows = conn.execute(sql, (isin, date_from.isoformat(), date_to.isoformat())).fetchall()
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    if df.empty:
        return df

    for c in ["kjop_belop", "salg_belop", "netto_belop"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["kjop_mnok"] = df["kjop_belop"] / 1_000_000
    df["salg_mnok"] = df["salg_belop"] / 1_000_000
    df["netto_mnok"] = df["netto_belop"] / 1_000_000
    return df


# =========================================================
# Helpers
# =========================================================

def display_owner_name(row: pd.Series) -> str:
    first = str(row.get("first_name", "") or "").strip()
    last = str(row.get("last_name", "") or "").strip()
    fallback = str(row.get("investor_id", "") or "").strip()
    if first.lower() == "nan":
        first = ""
    if last.lower() == "nan":
        last = ""
    name = " ".join([x for x in [first, last] if x]).strip()
    return name if name else fallback


def round_cols(df: pd.DataFrame, cols: list[str], ndigits: int = 1) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).round(ndigits)
    return out


# =========================================================
# UI
# =========================================================

def run(db_path: str, list_dir: str, beste_path: str | None = None, viktige_path: str | None = None):
    st.header("⭐ Handler de beste / viktige")
    st.caption("Sorter kjøp etter netto kjøp, og salg etter største salg. Detaljer per aksje vises samlet per eier.")

    conn = db_connect(db_path)

    if beste_path is None:
        beste_path = os.path.join(list_dir, "Beste.csv")
    if viktige_path is None:
        viktige_path = os.path.join(list_dir, "Viktige.csv")

    colA, colB = st.columns([1, 2])
    with colA:
        list_name = st.selectbox("Liste", ["Beste", "Viktige"], index=0)
    with colB:
        st.text_input("Listekatalog", value=list_dir, disabled=True)

    today = dt.date.today()
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.date_input("Startdato", value=today - dt.timedelta(days=30))
    with c2:
        date_to = st.date_input("Sluttdato", value=today)

    if date_to < date_from:
        st.error("Sluttdato kan ikke være før startdato.")
        return

    top_n = st.slider("Vis topp N aksjer", 10, 200, 30, 10)

    list_path = beste_path if list_name == "Beste" else viktige_path
    if not os.path.exists(list_path):
        st.error(f"Fant ikke listefil: {list_path}")
        return

    df_list = read_csv_guess(list_path)
    patterns = extract_owner_patterns(list_name, df_list)

    if "bestvikt_pack" not in st.session_state:
        st.session_state.bestvikt_pack = None

    # -----------------------------
    # HENT (bygger pack + temp-tabell)
    # -----------------------------
    if st.button("Hent", type="primary"):
        match_df = resolve_investor_ids(conn, patterns, max_hits_per_pattern=50)
        if match_df.empty:
            st.warning("Fant ingen investorer i databasen som matcher listen.")
            st.session_state.bestvikt_pack = None
            return

        investor_ids = sorted(set(match_df["investor_id"].astype(str).tolist()))
        ensure_temp_investor_table(conn, investor_ids)

        summary_df = fetch_top_by_security(conn, date_from, date_to)
        if summary_df.empty:
            st.warning("Ingen handler funnet i perioden.")
            st.session_state.bestvikt_pack = None
            return

        st.session_state.bestvikt_pack = {
            "list_name": list_name,
            "date_from": date_from,
            "date_to": date_to,
            "summary_df": summary_df,
            "investor_ids": investor_ids,  # <- VIKTIG: brukes for å re-etablere TEMP-tabell på reruns
        }

    pack = st.session_state.bestvikt_pack
    if not pack:
        return

    # -----------------------------
    # VIKTIG: Re-etabler TEMP-tabellen i *denne* connectionen ved hver rerun
    # -----------------------------
    investor_ids = pack.get("investor_ids", [])
    if investor_ids:
        ensure_temp_investor_table(conn, investor_ids)
    else:
        st.warning("Mangler investor_ids i session_state. Trykk 'Hent' på nytt.")
        return

    summary_df = pack["summary_df"]

    # =========================================================
    # TABELL 1: "Mest kjøp" sortert etter NETTO KJØP (netto_mnok)
    # =========================================================
    st.subheader("Mest kjøp (per aksje) – sortert etter netto kjøp")

    buy_df = summary_df[summary_df["netto_belop"] > 0].copy()
    buy_df = buy_df.sort_values("netto_belop", ascending=False).head(top_n)

    buy_show = round_cols(buy_df, ["kjop_mnok", "salg_mnok", "netto_mnok", "brutto_mnok"], ndigits=1)
    st.dataframe(
        buy_show[["ticker", "isin", "navn", "antall_obs", "kjop_mnok", "netto_mnok"]],
        use_container_width=True,
    )
    st.download_button(
        "Last ned CSV (kjøp)",
        buy_show.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
        file_name=f"{pack['list_name']}_mest_kjop_sortert_netto_{pack['date_from']}_{pack['date_to']}.csv",
        mime="text/csv",
    )

    # =========================================================
    # TABELL 2: "Mest salg" sortert etter STØRSTE SALG (salg_mnok)
    # =========================================================
    st.subheader("Mest salg (per aksje) – sortert etter største salg")

    sell_df = summary_df[summary_df["salg_belop"] > 0].copy()
    sell_df = sell_df.sort_values("salg_belop", ascending=False).head(top_n)

    sell_show = round_cols(sell_df, ["kjop_mnok", "salg_mnok", "netto_mnok", "brutto_mnok"], ndigits=1)
    st.dataframe(
        sell_show[["ticker", "isin", "navn", "antall_obs", "salg_mnok", "netto_mnok"]],
        use_container_width=True,
    )
    st.download_button(
        "Last ned CSV (salg)",
        sell_show.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
        file_name=f"{pack['list_name']}_mest_salg_{pack['date_from']}_{pack['date_to']}.csv",
        mime="text/csv",
    )

    # =========================================================
    # DETALJER PER AKSJE: samlet per eier (IKKE transaksjoner)
    # Sortert etter største kjøp
    # =========================================================
    st.divider()
    st.subheader("Detaljer for valgt aksje – samlet per eier (sortert etter største kjøp)")

    dd = pd.concat(
        [buy_df[["ticker", "isin", "navn"]], sell_df[["ticker", "isin", "navn"]]],
        ignore_index=True,
    ).drop_duplicates()

    dd["valg"] = dd["ticker"].fillna("") + " | " + dd["navn"].fillna("") + " | " + dd["isin"]
    choice = st.selectbox("Velg aksje", dd["valg"].tolist(), index=0 if len(dd) else None)
    if not choice:
        return

    chosen_isin = dd.loc[dd["valg"] == choice, "isin"].iloc[0]

    by_owner_df = fetch_by_investor_for_security(conn, chosen_isin, pack["date_from"], pack["date_to"])
    if by_owner_df.empty:
        st.info("Ingen data for valgt aksje i perioden.")
        return

    by_owner_df["eier"] = by_owner_df.apply(display_owner_name, axis=1)

    by_owner_df = by_owner_df.sort_values("kjop_belop", ascending=False)

    owner_show = round_cols(by_owner_df, ["kjop_mnok", "salg_mnok", "netto_mnok"], ndigits=1)

    st.dataframe(
        owner_show[
            ["eier", "investor_id", "investor_type", "antall_obs", "kjop_mnok", "salg_mnok", "netto_mnok"]
        ],
        use_container_width=True,
    )

    st.download_button(
        "Last ned CSV (samlet per eier)",
        owner_show.to_csv(index=False, sep=";", decimal=",").encode("latin-1"),
        file_name=f"{pack['list_name']}_samlet_per_eier_{chosen_isin}_{pack['date_from']}_{pack['date_to']}.csv",
        mime="text/csv",
    )
