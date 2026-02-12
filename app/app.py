import os
import time
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Dashboard SDS", layout="wide")

# Cargar variables desde "Secrets" Streamlit
MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
logs_collection = db["ACCESS_LOGS"]

# En Streamlit Cloud, st.experimental_user.email 
if hasattr(st, "experimental_user") and st.experimental_user:
    user_email = st.experimental_user.email or "unknown"
else:
    user_email = "anonymous"

# —————— Insertar registro de acceso ——————
logs_collection.insert_one({
    "user_email": user_email,
    "timestamp":  time.strftime("%Y-%m-%d %H:%M:%S"),
    "page":       st.query_params.get("page", ["app"])[0]  # opcional, si usas query param 'page'
})

st.title("📊 Dashboard SDS")
st.write("Selecciona una sección desde el menú lateral.")
st.info("Usa el menú lateral para navegar entre dispositivos, suministros y contadores")
