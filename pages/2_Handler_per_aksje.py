import streamlit as st
from app_config import LOCAL_DB_PATH
from analyses import handler_aksje

st.set_page_config(page_title="Handler per aksje", page_icon="📈", layout="wide")
handler_aksje.run(LOCAL_DB_PATH)

