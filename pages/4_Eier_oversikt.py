import os
import tempfile
import streamlit as st

from analyses import eier_oversikt

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

st.set_page_config(
    page_title="Eier oversikt",
    page_icon="👥",
    layout="wide"
)

eier_oversikt.run(DB_PATH_LOCAL)

