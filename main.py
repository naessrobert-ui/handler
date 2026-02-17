import os
import tempfile
import streamlit as st

from db_sync import ensure_local_db

# --- KONFIG ---
DB_PATH_REMOTE_FULL = r"I:\6_EQUITIES\Database\Eiere-Database\topchanges.db"
DB_PATH_REMOTE_RECENT = r"I:\6_EQUITIES\Database\Eiere-Database\topchanges_recent_60d.db"

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Handler norske aksjer; hvem gjør hva?",
    page_icon="📊",
    layout="wide",
)

# -----------------------------
# Nordea-ish styling
# -----------------------------
st.markdown(
    """
    <style>
      :root{
        --nordea-blue: #0b167a;
        --nordea-blue-2:#0a1260;
        --nordea-mint:#27d3b6;
        --bg:#f5f7fb;
        --card:#ffffff;
        --text:#0b1020;
        --muted:#5b647a;
      }
      .stApp { background: var(--bg); }
      header[data-testid="stHeader"] { background: transparent; }

      .hero {
        background: radial-gradient(1200px 400px at 20% 20%, #1a2aa8 0%, var(--nordea-blue) 40%, var(--nordea-blue-2) 100%);
        border-radius: 22px;
        padding: 42px 44px;
        color: #fff;
        box-shadow: 0 10px 30px rgba(11,22,122,0.25);
        position: relative;
        overflow: hidden;
      }
      .hero::after{
        content:"";
        position:absolute;
        right:-120px;
        top:-120px;
        width:360px;
        height:360px;
        background: rgba(39,211,182,0.18);
        border-radius: 999px;
        filter: blur(1px);
      }
      .hero h1{
        margin:0 0 10px 0;
        font-size: 40px;
        line-height: 1.1;
        letter-spacing: -0.5px;
      }
      .hero p{
        margin:0;
        max-width: 720px;
        color: rgba(255,255,255,0.88);
        font-size: 16px;
        line-height: 1.6;
      }
      .pill {
        display:inline-block;
        margin-top: 18px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255,255,255,0.14);
        border: 1px solid rgba(255,255,255,0.18);
        font-size: 13px;
        color: rgba(255,255,255,0.92);
      }

      .section-title{
        margin: 22px 0 8px 0;
        font-size: 18px;
        color: var(--text);
        font-weight: 700;
      }
      .section-subtitle{
        margin: 0 0 14px 0;
        color: var(--muted);
        font-size: 14px;
      }

      .card {
        background: var(--card);
        border-radius: 18px;
        padding: 18px 18px;
        border: 1px solid rgba(15, 23, 42, 0.08);
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
        height: 100%;
      }
      .card h3{
        margin: 0 0 6px 0;
        font-size: 16px;
        color: var(--text);
      }
      .card p{
        margin: 0 0 14px 0;
        font-size: 13px;
        color: var(--muted);
        line-height: 1.5;
      }

      div.stButton > button {
        border-radius: 999px;
        padding: 10px 14px;
        border: 1px solid rgba(11,22,122,0.25);
        background: white;
        color: var(--nordea-blue);
        font-weight: 700;
      }
      div.stButton > button:hover {
        border: 1px solid rgba(11,22,122,0.35);
        background: rgba(11,22,122,0.04);
      }
      .primary div.stButton > button{
        background: var(--nordea-mint);
        border: 1px solid rgba(39,211,182,0.6);
        color: #062a25;
      }
      .primary div.stButton > button:hover{
        background: #21c9ad;
      }

      section[data-testid="stSidebar"]{
        background: #ffffff;
        border-right: 1px solid rgba(15, 23, 42, 0.08);
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Session state
# -----------------------------
if "selected_analysis" not in st.session_state:
    st.session_state.selected_analysis = None

# Viktig: default DB-valg (radio binder direkte til denne via key)
if "db_choice" not in st.session_state:
    st.session_state.db_choice = "Rask (siste 60 dager)"

if "db_ready" not in st.session_state:
    st.session_state.db_ready = False

# Hvilket DB-valg ble faktisk lastet ned/klargjort sist?
if "db_ready_for" not in st.session_state:
    st.session_state.db_ready_for = None

# -----------------------------
# Sidebar menu
# -----------------------------
with st.sidebar:
    st.markdown("### Meny")
    st.caption("Velg analyse fra oversikten under.")
    options = [
        "Handler per eier",
        "Handler per aksje",
        "Handler de beste",
        "Eier oversikt",
        "Alle eiere i et selskap",
        "Topp 20 viktige",
    ]
    pick = st.radio("Analyser", options, index=0)
    st.session_state.selected_analysis = pick

    st.divider()
    st.caption("Database")
    st.info("Velg database-type øverst på forsiden og trykk ‘Last ned database’.")

# -----------------------------
# Hero section
# -----------------------------
st.markdown(
    """
    <div class="hero">
      <h1>Handler norske aksjer; hvem gjør hva?</h1>
      <p>
        Velg en analyse for å utforske handler og eierskap. Før du starter kan du velge om du vil bruke en
        rask database (siste 60 dager) eller komplett historikk.
      </p>
      <div class="pill">Rask modus = mindre DB • Komplett modus = full historikk</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# DB-VALG ØVERST + NEDLASTING (oppdatert)
# - Radio binder direkte til st.session_state["db_choice"]
# - Hvis bruker endrer valg etter at DB er klar -> db_ready settes False
# =========================================================
st.markdown('<div class="section-title">Databasevalg</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-subtitle">Velg hvilken database du vil jobbe med i denne sesjonen. Begge lastes lokalt som <b>topchanges.db</b>.</div>',
    unsafe_allow_html=True,
)

st.radio(
    "Hvilken database vil du laste ned lokalt?",
    ["Rask (siste 60 dager)", "Komplett (hele historikken)"],
    key="db_choice",
    horizontal=True,
)

# Hvis DB allerede er klargjort, men brukeren bytter valg -> krev ny nedlasting
if st.session_state.db_ready and st.session_state.db_ready_for != st.session_state.db_choice:
    st.session_state.db_ready = False

choice = st.session_state.db_choice
remote_path = DB_PATH_REMOTE_RECENT if choice.startswith("Rask") else DB_PATH_REMOTE_FULL

colA, colB, colC = st.columns([1, 1, 2], gap="large")
with colA:
    st.markdown('<div class="primary">', unsafe_allow_html=True)
    download_clicked = st.button("Last ned database", key="download_db")
    st.markdown("</div>", unsafe_allow_html=True)

with colB:
    force_copy = st.checkbox("Tving ny kopi", value=False, help="Kopierer på nytt selv om lokal DB ser oppdatert ut.")

with colC:
    st.info(f"Valgt remote: {remote_path}\n\nLokal fil: {DB_PATH_LOCAL}")

with st.expander("Database-status", expanded=True):
    status = st.empty()
    prog = st.progress(0)

    def ui_progress(p: float, msg: str):
        prog.progress(int(p * 100))
        status.info(msg)

    if download_clicked:
        try:
            info = ensure_local_db(
                remote_db_path=remote_path,
                local_db_path=DB_PATH_LOCAL,   # alltid samme lokale navn
                on_progress=ui_progress,
                force=force_copy,
                copy_wal_shm=True,
            )
            st.session_state.db_ready = True
            st.session_state.db_ready_for = st.session_state.db_choice

            if info.get("copied"):
                status.success(f"Kopiert til lokal: {info['local_path']}")
            else:
                status.success(f"Lokal DB er oppdatert: {info['local_path']}")

            st.caption(f"Remote: {info['remote_path']}")
        except Exception as e:
            st.session_state.db_ready = False
            status.error(f"Kunne ikke klargjøre lokal DB: {e}")
    else:
        if st.session_state.db_ready:
            status.success(f"Database er klargjort lokalt ({st.session_state.db_ready_for}).")
        else:
            status.info("Velg database-type og trykk ‘Last ned database’.")

# -----------------------------
# Cards menu (main)
# -----------------------------
st.markdown('<div class="section-title">Velg analyse</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-subtitle">Klikk på en modul for å gå videre.</div>',
    unsafe_allow_html=True,
)

cards = [
    ("Handler per eier", "Se hvilke eiere som handler mest, netto kjøp/salg og trender per eier.", "📌"),
    ("Handler per aksje", "Analyser volum og aktivitet per aksje, med filtrering på periode og segment.", "📈"),
    ("Handler de beste", "Fremhev de mest interessante handlene basert på regler (kommer senere).", "⭐"),
    ("Eier oversikt", "Oppsummering av eierskap og endringer over tid, per aksje eller per eier.", "🧭"),
    ("Alle eiere i et selskap", "Vis full eierliste for valgt selskap og endringer i beholdning.", "🏢"),
    ("Topp 20 viktige", "Kurert liste over topp 20 (defineres senere) med raske innsikter.", "🏆"),
]

ROUTES = {
    "Handler per eier": "pages/1_Handler_per_eier.py",
    "Handler per aksje": "pages/2_Handler_per_aksje.py",
    "Handler de beste": "pages/3_Handler_de_beste.py",
    "Eier oversikt": "pages/4_Eier_oversikt.py",
    "Beste investorer": "pages/5_Beste_investorer.py",
}

cols = st.columns(3, gap="large")
for i, (title, desc, icon) in enumerate(cards):
    with cols[i % 3]:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown(f"<h3>{icon} {title}</h3>", unsafe_allow_html=True)
        st.markdown(f"<p>{desc}</p>", unsafe_allow_html=True)

        is_selected = (st.session_state.selected_analysis == title)
        button_container_class = "primary" if is_selected else ""
        st.markdown(f'<div class="{button_container_class}">', unsafe_allow_html=True)

        if st.button("Velg", key=f"pick_{title}"):
            route = ROUTES.get(title)
            if route:
                # Sperre hvis DB ikke er lastet, eller hvis valget er endret etter siste nedlasting:
                if (not st.session_state.db_ready) or (st.session_state.db_ready_for != st.session_state.db_choice):
                    st.warning("Last ned valgt database først (øverst på siden).")
                else:
                    st.switch_page(route)
            else:
                st.session_state.selected_analysis = title

        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# Selected module preview
# -----------------------------
st.divider()
selected = st.session_state.selected_analysis
route = ROUTES.get(selected)

if route:
    if st.button(f"Åpne {selected}", type="primary", disabled=not st.session_state.db_ready):
        # Ekstra trygghet:
        if (not st.session_state.db_ready) or (st.session_state.db_ready_for != st.session_state.db_choice):
            st.warning("Last ned valgt database først (øverst på siden).")
        else:
            st.switch_page(route)
else:
    st.info(f"{selected} – kommer")

left, right = st.columns([2, 1], gap="large")
with left:
    st.subheader("Valgt analyse")
    st.write(f"**{selected}**")
    st.caption("DB lastes lokalt som topchanges.db, så alle moduler kan bruke samme path som før.")

with right:
    st.subheader("Klar til oppstart")
    if st.session_state.db_ready and st.session_state.db_ready_for == st.session_state.db_choice:
        st.success("Database klargjort lokalt.")
    else:
        st.warning("Last ned database øverst først.")
    st.button("Start (kommer senere)", disabled=True)

