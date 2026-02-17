import os
import tempfile
import streamlit as st
from analyses import handler_aksje

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

st.set_page_config(page_title="Handler per aksje", page_icon="📈", layout="wide")
handler_aksje.run(DB_PATH_LOCAL)

