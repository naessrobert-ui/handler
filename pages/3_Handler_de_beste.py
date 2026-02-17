import os
import tempfile
import streamlit as st

from analyses import handler_best_viktige

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

# Dette er mappen du viste i screenshot:
LIST_DIR = r"I:\6_EQUITIES\Database\Eiere-Styring"

st.set_page_config(page_title="Handler de beste", page_icon="⭐", layout="wide")
handler_best_viktige.run(DB_PATH_LOCAL, list_dir=LIST_DIR)
