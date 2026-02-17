# pages/5_Beste_investorer.py
import streamlit as st

from app_config import LIST_DIR, LOCAL_DB_PATH
from analyses import beste_investorer

st.set_page_config(page_title="Beste investorer", page_icon="🏆", layout="wide")
beste_investorer.run(LOCAL_DB_PATH, list_dir=LIST_DIR)
