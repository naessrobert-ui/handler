import streamlit as st

from app_config import LOCAL_DB_PATH
from analyses import Handler_eier  # <-- viktig: dette må matche filnavnet

st.set_page_config(page_title="Handler per eier", page_icon="📌", layout="wide")



Handler_eier.run(LOCAL_DB_PATH)
