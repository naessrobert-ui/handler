# pages/5_Beste_investorer.py
import os
import tempfile
import streamlit as st

from analyses import beste_investorer

LOCAL_WORKDIR = os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work")
DB_PATH_LOCAL = os.path.join(LOCAL_WORKDIR, "topchanges.db")

LIST_DIR = r"I:\6_EQUITIES\Database\Eiere-Styring"

st.set_page_config(page_title="Beste investorer", page_icon="🏆", layout="wide")
beste_investorer.run(DB_PATH_LOCAL, list_dir=LIST_DIR)
