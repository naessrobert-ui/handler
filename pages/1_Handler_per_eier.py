import os
import tempfile
import streamlit as st

from analyses import Handler_eier  # <-- viktig: dette må matche filnavnet

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

st.set_page_config(page_title="Handler per eier", page_icon="📌", layout="wide")



Handler_eier.run(DB_PATH_LOCAL)
