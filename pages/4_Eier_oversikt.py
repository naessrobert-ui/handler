import streamlit as st

from app_config import LOCAL_DB_PATH
from analyses import eier_oversikt

st.set_page_config(
    page_title="Eier oversikt",
    page_icon="👥",
    layout="wide"
)

eier_oversikt.run(LOCAL_DB_PATH)

