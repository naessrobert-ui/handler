import streamlit as st

from app_config import LIST_DIR, LOCAL_DB_PATH
from analyses import handler_best_viktige

# Dette er mappen du viste i screenshot:
st.set_page_config(page_title="Handler de beste", page_icon="⭐", layout="wide")
handler_best_viktige.run(LOCAL_DB_PATH, list_dir=LIST_DIR)
