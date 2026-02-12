import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import altair as alt
import plotly.express as px

# Configurar la página
st.set_page_config(page_title="Dashboard - Monitores", layout="wide")

# Cargar variables desde "Secrets" Streamlit
#load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")
MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Definir campos para cada colección

# DEVICE
device_fields = {
    "deviceId": 1,
    "customerId": 1,
    "serialNumber": 1,  # Serial del dispositivo
    "ipAddress": 1,
    "monitorStatus": 1,
    "extendedFields": 1,  # (model, zone, location, firmware, etc.)
    "discoveryDate": 1,
    "lastContact": 1,
    "_id": 0
}

# CUSTOMER
customer_fields = {
    "customerId": 1,
    "name": 1,
    "status": 1,
    "city": 1,
    "_id": 0
}

# MONITOR
monitor_fields = {
    "monitorId": 1,
    "createdDate": 1,
    "customerId": 1,
    "lastContact": 1,
    "licenceDeviceLimit": 1,
    "licenceExpiryDate": 1,
    "licenceKey": 1,
    "licenceProviderCode": 1,
    "name": 1,
    "online": 1,
    "remoteApplication": 1,
    "status": 1,
    "_id": 0
}

# Funciones de carga con caché (TTL de 300 segundos)
@st.cache_data(ttl=300, show_spinner=False)
def get_device_data():
    devices = list(db["DEVICE"].find({}, device_fields))
    df = pd.DataFrame(devices)
    # Convertir fechas en devices
    if not df.empty:
        df["discoveryDate"] = pd.to_datetime(df["discoveryDate"], errors="coerce")
        df["lastContact"] = pd.to_datetime(df["lastContact"], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def get_customer_data():
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, customer_fields))
    return pd.DataFrame(customers)

@st.cache_data(ttl=300, show_spinner=False)
def get_monitor_data():
    monitors = list(db["MONITOR"].find({}, monitor_fields))
    df = pd.DataFrame(monitors)
    if not df.empty:
        df["lastContact"] = pd.to_datetime(df["lastContact"], errors="coerce")
        df["createdDate"] = pd.to_datetime(df["createdDate"], errors="coerce")
    return df

# Función para unir los datos de DEVICE, CUSTOMER y MONITOR
def unir_datos_meters():
    df_devices = get_device_data()
    df_customers = get_customer_data()
    df_monitors = get_monitor_data()
    
    if df_devices.empty or df_customers.empty or df_monitors.empty:
        return pd.DataFrame()
    
    # Agrupar los monitores: tomar, por cada customerId, el monitor con el lastContact más reciente
    #df_monitors = df_monitors.sort_values("lastContact", ascending=False).drop_duplicates(subset=["customerId"])
    

     # Unir con CUSTOMER usando "customerId" del DEVICE
    df = pd.merge(df_customers, df_devices, on="customerId", how="inner")
    # Unir MONITORS con CUSTOMER
    df = pd.merge(df, df_monitors, on="customerId", how="left")
    
    # Eliminar duplicados asegurando que un deviceId solo aparezca una vez por readingDateTime
    #df = df.drop_duplicates(subset=["deviceId", "readingDateTime"])

    # Renombrar columnas para visualización
    df = df.rename(columns={
        "name_x": "Cliente",
        "name_y": "Nombre monitor",
        "status_x": "Estado cliente",
        "status_y": "Estado monitor",
        "lastContact_x": "lastContact",
        "lastContact_y": "lastContact_y",
        "city": "Ciudad",
        "serialNumber": "Serial Dispositivo",
        "remoteApplication": "Version agente"
    })

     # Filtrar para excluir monitores con estado "DISCONTINUED"
    df = df[df["Estado monitor"] != "DISCONTINUED"]
    return df

# Cargar y unir los datos
df = unir_datos_meters()
if df.empty:
    st.error("No se encontraron datos en la colección METERS o en los JOINs.")
else:
       
    #Filtros
    st.sidebar.header("Filtros de Monitores")
    clientes_unicos = sorted(df["Cliente"].dropna().unique())
    filtro_cliente = st.sidebar.multiselect("Seleccionar Cliente", clientes_unicos)
    
    if filtro_cliente:
        dispositivos_unicos = sorted(df[df["Cliente"].isin(filtro_cliente)]["Serial Dispositivo"].dropna().unique())
    else:
        dispositivos_unicos = sorted(df["Serial Dispositivo"].dropna().unique())

    filtro_device = st.sidebar.multiselect("Seleccionar Dispositivo", dispositivos_unicos)

    if filtro_device:
        clientes_unicos = sorted(df[df["Serial Dispositivo"].isin(filtro_device)]["Cliente"].dropna().unique())
    else:
        clientes_unicos = sorted(df["Cliente"].dropna().unique())

    estados_unicos = sorted(df["Estado monitor"].dropna().unique())
    filtro_estado = st.sidebar.multiselect("Seleccionar estado monitor", estados_unicos)

    df_filtered = df.copy()
    if filtro_cliente:
        df_filtered = df_filtered[df_filtered["Cliente"].isin(filtro_cliente)]
    if filtro_device:
        df_filtered = df_filtered[df_filtered["Serial Dispositivo"].isin(filtro_device)]
    if filtro_estado:
        df_filtered = df_filtered[df_filtered["Estado monitor"].isin(filtro_estado)]

    # Título de la página
    st.title("📊 Dashboard de monitores")
    st.subheader("Indicadores Clave")
    # Indicador: Total de monitores
    total_monitores = df_filtered["monitorId"].nunique() if "monitorId" in df_filtered.columns else df_filtered.shape[0]
    monitores_online = df_filtered[df_filtered["online"] == True]["monitorId"].nunique()
    monitores_offline = total_monitores - monitores_online


    # Indicador: Promedio de días sin reporte
    df_mon_unique = df_filtered.drop_duplicates(subset=["monitorId"])
    df_mon_unique["days_without_reporting"] = (pd.Timestamp.now(tz='UTC') - df_mon_unique["lastContact_y"]).dt.days
    avg_days_without = df_mon_unique["days_without_reporting"].mean()

    # Indicador: Monitores con licencia próxima a expirar (por ejemplo, menos de 30 días)
    df_mon_unique["licenceExpiryDate"] = pd.to_datetime(df_mon_unique["licenceExpiryDate"], errors="coerce")

    # Eliminar monitores cuyo nombre comienza con "sda_" o "hpc_"
    df_mon_unique = df_mon_unique[~df_mon_unique["Nombre monitor"].str.startswith(("sda_", "hpc_"))]

    # Agregar zona horaria UTC para evitar el error de tz-naive vs tz-aware
    df_mon_unique["licenceExpiryDate"] = df_mon_unique["licenceExpiryDate"].dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
    df_mon_unique["days_to_expiry"] = (df_mon_unique["licenceExpiryDate"] - pd.Timestamp.now(tz='UTC')).dt.days

    monitores_licencia_prox = df_mon_unique[df_mon_unique["days_to_expiry"] < 60].shape[0] 

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Monitores", total_monitores)
    col2.metric("Monitores Online", monitores_online)
    col3.metric("Promedio Días sin Reporte", f"{avg_days_without:.1f} días")
    col4.metric("Licencia Próx a vencer (<60 días)", monitores_licencia_prox)

    st.subheader("Estado de monitores (agentes)")

    # Agrupar por "Cliente" y tomar el último reading (según readingDateTime)
    df_monitors = df_filtered.sort_values("lastContact_y").groupby(["Cliente", "Nombre monitor"], as_index=False).tail(1)

    # Formatear las columnas de fecha antes de mostrarlas
    
    df_monitors["lastContact_y"] = df_monitors["lastContact_y"].dt.strftime("%Y-%m-%d %H:%M:%S")

    st.dataframe(df_monitors[[
        "Cliente", "Nombre monitor", "Estado monitor", "Version agente", "lastContact_y","licenceExpiryDate","online"
    ]])

    # 1. Gráfico de barras: Distribución de monitores por estado (online/offline)
    df_estado = pd.DataFrame({
        "Estado": ["Online", "Offline"],
        "Cantidad": [monitores_online, monitores_offline]
    })
    chart_estado = alt.Chart(df_estado).mark_bar().encode(
        x=alt.X("Estado:N", title="Estado"),
        y=alt.Y("Cantidad:Q", title="Número de Monitores"),
        color=alt.Color("Estado:N", scale=alt.Scale(domain=["Online", "Offline"], range=["green", "red"]))
    ).properties(
        width=600,
        height=400,
        title="Distribución de Monitores por Estado"
    )
    st.altair_chart(chart_estado, use_container_width=True)

    # Barra horizontal para mostrar, por cada monitor (único), los días hasta la expiración de la licencia.
    # Primero, extraemos registros únicos de monitores basándonos en MonitorID
    df_mon_unique = df_filtered[[
        "monitorId", "Cliente", "Nombre monitor", "lastContact_y", "licenceExpiryDate", "online", "Estado monitor"
    ]].drop_duplicates(subset=["monitorId"]).copy()
    # Convertir las fechas a datetime si no lo están
    df_mon_unique["lastContact_y"] = pd.to_datetime(df_mon_unique["lastContact_y"], errors="coerce")
    df_mon_unique["licenceExpiryDate"] = pd.to_datetime(df_mon_unique["licenceExpiryDate"], errors="coerce")
    # Calcular días sin reportar
    df_mon_unique["days_without_reporting"] = (df_mon_unique["lastContact_y"] - pd.Timestamp.now(tz='UTC')).dt.days
    
    # Crear etiqueta combinada para identificar el monitor
    df_mon_unique["MonitorLabel"] = df_mon_unique["Cliente"] + " - " + df_mon_unique["Nombre monitor"]
    
    chart_bar_mon = alt.Chart(df_mon_unique).mark_bar().encode(
        x=alt.X("days_without_reporting:Q", title="Días sin reportar"),
        y=alt.Y("MonitorLabel:N", sort="-x", title="Monitor (Cliente - Nombre)"),
        color=alt.Color("Estado monitor:N", title="Estado Monitor",
                        scale=alt.Scale(domain=["ACTIVE", "DISCONTINUED"], range=["green", "red"])),
        tooltip=["Cliente", "Nombre monitor", alt.Tooltip("lastContact_y:T", title="Último Reporte"),
                 alt.Tooltip("licenceExpiryDate:T", title="Licencia Expira"), "days_without_reporting", "online"]
    ).properties(
        width=700,
        height=400,
        title="Días sin reportar"
    )
    st.altair_chart(chart_bar_mon, use_container_width=True) 
