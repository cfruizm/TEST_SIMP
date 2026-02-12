import streamlit as st
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import io
import altair as alt
import plotly.express as px

# Configurar la página
st.set_page_config(page_title="Dashboard - Dispositivos", layout="wide")

# Cargar variables desde "Secrets" Streamlit
#load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")
MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Definir los campos necesarios para cada colección
customer_fields = {"customerId": 1, "name": 1, "status": 1, "_id": 0}
device_fields = {
    "customerId": 1,
    "serialNumber": 1,
    "ipAddress": 1,
    "monitorStatus": 1,
    "extendedFields": 1,
    "discoveryDate": 1,
    "lastContact": 1,
    "_id": 0
}

# Función para cargar datos desde MongoDB (se ejecuta una sola vez y se cachea)
@st.cache_resource(ttl=300, show_spinner=False)
def get_data():
    # Cargar clientes activos
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, customer_fields))
    # Cargar dispositivos (sin filtro de status en este lado)
    devices = list(db["DEVICE"].find({}, device_fields))
    return customers, devices

# Función para unir los datos de CUSTOMER y DEVICE
def unir_datos(customers, devices):
    df_customers = pd.DataFrame(customers)
    df_devices = pd.DataFrame(devices)
    
    # Expandir la columna "extendedFields" en df_devices para extraer campos relevantes
    if "extendedFields" in df_devices.columns:
        extended_df = df_devices["extendedFields"].apply(pd.Series)
        # Queremos todos los campos excepto "sku" e "internalId"
        campos_extra = ["model", "zone", "location", "firmware", "hostName", "monitorName", "manufacturer", "mibDescription"]
        extended_df = extended_df[campos_extra].fillna("Desconocido")
        df_devices = pd.concat([df_devices.drop(columns=["extendedFields"]), extended_df], axis=1)
    else:
        for campo in ["model", "zone", "location", "firmware", "hostName", "monitorName", "manufacturer", "mibDescription"]:
            df_devices[campo] = "Desconocido"
    
    # Convertir las fechas a datetime
    df_devices["discoveryDate"] = pd.to_datetime(df_devices["discoveryDate"], errors="coerce")
    df_devices["lastContact"] = pd.to_datetime(df_devices["lastContact"], errors="coerce")
    
    # Fusionar los DataFrames por "customerId"
    if not df_customers.empty and not df_devices.empty:
        df_merged = pd.merge(df_customers, df_devices, on="customerId", how="inner")
        # Eliminar columnas innecesarias
        df_merged = df_merged.drop(columns=["customerId", "status"], errors="ignore")
        # Definir el orden deseado de columnas (ajusta según tus necesidades)
        orden_columnas = ["name", "serialNumber", "ipAddress", "monitorStatus", "model", "zone", "location", "firmware", "discoveryDate", "lastContact"]
        df_merged = df_merged[[col for col in orden_columnas if col in df_merged.columns]]
        return df_merged
    return pd.DataFrame()

# Título de la página
st.title("📊 Dashboard de Dispositivos")

# Cargar y unir datos (se cachea en get_data)
customers, devices = get_data()
df = unir_datos(customers, devices)

# Barra lateral: Filtros interactivos
st.sidebar.header("📌 Filtros")
clientes_unicos = sorted(df["name"].dropna().unique())
filtro_cliente = st.sidebar.multiselect("Seleccionar Cliente", clientes_unicos)

# Si se selecciona al menos un cliente, actualizar los demás filtros.
if filtro_cliente:
    df_filtrado_clientes = df[df["name"].isin(filtro_cliente)]
    modelos_unicos       = sorted(df_filtrado_clientes["model"].dropna().unique())
    monitor_status_unicos = sorted(df_filtrado_clientes["monitorStatus"].dropna().unique())
    zonas_unicas         = sorted(df_filtrado_clientes["zone"].dropna().unique())
    locations_unicas     = sorted(df_filtrado_clientes["location"].dropna().unique())
else:
    # Si no se filtra por cliente, usar los valores de todo el DataFrame
    modelos_unicos       = sorted(df["model"].dropna().unique())
    monitor_status_unicos = sorted(df["monitorStatus"].dropna().unique())
    zonas_unicas         = sorted(df["zone"].dropna().unique())
    locations_unicas     = sorted(df["location"].dropna().unique())
    
filtro_modelo = st.sidebar.multiselect("Seleccionar Modelo", modelos_unicos)
filtro_monitor = st.sidebar.multiselect("Seleccionar Monitor Status", monitor_status_unicos)
filtro_zona = st.sidebar.multiselect("Seleccionar Zona", zonas_unicas)
filtro_location = st.sidebar.multiselect("Seleccionar Location", locations_unicas)

if not df["lastContact"].isnull().all():
    fecha_inicio = st.sidebar.date_input("Fecha inicio (Last Contact)", df["lastContact"].min().date())
    fecha_fin = st.sidebar.date_input("Fecha fin (Last Contact)", df["lastContact"].max().date())
else:
    fecha_inicio, fecha_fin = None, None

if not df["discoveryDate"].isnull().all():
    disc_inicio = st.sidebar.date_input("Fecha inicio (Discovery Date)", df["discoveryDate"].min().date(), key="disc_inicio")
    disc_fin = st.sidebar.date_input("Fecha fin (Discovery Date)", df["discoveryDate"].max().date(), key="disc_fin")
else:
    disc_inicio, disc_fin = None, None

# Aplicar filtros a la copia de df
df_filtered = df.copy()
if filtro_cliente:
    df_filtered = df_filtered[df_filtered["name"].isin(filtro_cliente)]
if filtro_modelo:
    df_filtered = df_filtered[df_filtered["model"].isin(filtro_modelo)]
if filtro_monitor:
    df_filtered = df_filtered[df_filtered["monitorStatus"].isin(filtro_monitor)]
if filtro_zona:
    df_filtered = df_filtered[df_filtered["zone"].isin(filtro_zona)]
if filtro_location:
    df_filtered = df_filtered[df_filtered["location"].isin(filtro_location)]
if fecha_inicio and fecha_fin:
    df_filtered = df_filtered[(df_filtered["lastContact"].dt.date >= fecha_inicio) & (df_filtered["lastContact"].dt.date <= fecha_fin)]
if disc_inicio and disc_fin:
    df_filtered = df_filtered[(df_filtered["discoveryDate"].dt.date >= disc_inicio) & (df_filtered["discoveryDate"].dt.date <= disc_fin)]

# Asignar alias a las columnas
df_filtered = df_filtered.rename(columns={
    "name": "Cliente",
    "serialNumber": "Serial",
    "ipAddress": "IP",
    "monitorStatus": "Estado de Monitoreo",
    "model": "Modelo",
    "zone": "Zona",
    "location": "Ubicación",
    "firmware": "Firmware",
    "discoveryDate": "Fecha de Descubrimiento",
    "lastContact": "Último Contacto"
})

# Indicadores clave
st.subheader("Indicadores Clave")
total_dispositivos = len(df_filtered)
dispositivos_monitoreados = len(df_filtered[df_filtered["Estado de Monitoreo"] == "Y"])
porcentaje_monitoreados = (dispositivos_monitoreados / total_dispositivos * 100) if total_dispositivos > 0 else 0

col1, col2, col3 = st.columns(3)
col1.metric("Total Dispositivos", total_dispositivos)
col2.metric("Monitoreados", dispositivos_monitoreados)
col3.metric("Porcentaje Monitoreados", f"{porcentaje_monitoreados:.1f}%")

# Mostrar la tabla filtrada
st.dataframe(df_filtered)

# Gráficos

st.subheader("Gráficos")

# Gráfico de barras: Distribución por Modelo
#st.markdown("**Distribución por Modelo**")
#model_counts = df_filtered["model"].sort_values(ascending=False).value_counts()
#st.bar_chart(model_counts)

# Calcula la distribución por modelo, ordenada de mayor a menor:
model_counts = df_filtered["Modelo"].value_counts().sort_values(ascending=False).reset_index()
model_counts.columns = ["Modelo", "count"]

# Crea un gráfico de barras con Altair, definiendo explícitamente el orden de los modelos
chart = alt.Chart(model_counts).mark_bar().encode(
    x=alt.X("Modelo:N", sort=model_counts["Modelo"].tolist(), title="Modelo"),
    y=alt.Y("count:Q", title="Cantidad de Dispositivos"),
    tooltip=["Modelo", "count"]
).properties(
    width=600,
    height=400,
    title="Distribución de Dispositivos por Modelo"
)

# Mostrar el gráfico en Streamlit
st.altair_chart(chart, use_container_width=True)

#Gráfico circular: Proporción de Dispositivos Monitoreados vs No Monitoreados
st.markdown("**Proporción de Monitoreados vs No Monitoreados**")

# Función auxiliar para graficar un pie chart (usando Matplotlib)

def create_pie_chart(data):
    # Crear una figura con tamaño fijo
    fig, ax = plt.subplots(figsize=(2,2))
    ax.pie(data, labels=data.index, autopct="%1.1f%%", startangle=90)
    ax.axis("equal")
    fig.tight_layout(pad=0)
    return fig

# 'df_filtered' es tu DataFrame ya filtrado
monitor_counts = df_filtered["Estado de Monitoreo"].value_counts()

# Generar la figura usando los datos filtrados
fig_pie = create_pie_chart(monitor_counts)

# Mostrar la figura con st.pyplot, sin que se expanda al ancho del contenedor
st.pyplot(fig_pie, use_container_width=False)

# Gráfico de líneas: Tendencia de Último Contacto

#st.markdown("**Tendencia de Último Contacto**")
#if not df_filtered["lastContact"].isnull().all():
#   df_line = df_filtered.groupby(df_filtered["lastContact"].dt.date).size().reset_index(name="count")
#    df_line.columns = ["Fecha", "Número de Reportes"]
#    st.line_chart(df_line.set_index("Fecha"))
#else:
#    st.write("No hay datos de Last Contact para graficar tendencias.")

if not df_filtered["Último Contacto"].isnull().all():
    # Calcular días transcurridos desde el último contacto
    current_time = pd.Timestamp.now(tz='UTC')
    df_filtered["days_since_last_contact"] = (current_time - df_filtered["Último Contacto"]).dt.days

    # Crear un histograma con Altair para visualizar la distribución de días desde el último contacto
    chart = alt.Chart(df_filtered).mark_bar().encode(
        x=alt.X("days_since_last_contact:Q", bin=alt.Bin(maxbins=20), title="Días desde el último contacto"),
        y=alt.Y("count()", title="Cantidad de Dispositivos"),
        tooltip=["days_since_last_contact", "count()"]
    ).properties(
        width=600,
        height=400,
        title="Distribución de Días desde el Último Contacto"
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.write("No hay datos de Last Contact para graficar tendencias.")

# Gráfico de barras: Consolidado de versiones de firmware
#st.markdown("**Consolidado de Versiones de Firmware**")
#st.bar_chart(df_filtered["Firmware"].value_counts())
import altair as alt
import pandas as pd

# Suponiendo que df_filtered es tu DataFrame filtrado y ya renombrado,
# y que la columna de firmware ya se llama "Firmware"

def consolidar_firmware(df, top_n=10):
    # Calcular la cantidad de dispositivos por cada versión de firmware
    firmware_counts = df["Firmware"].value_counts()
    
    # Si hay más de top_n versiones, agrupar el resto en "Otros"
    if len(firmware_counts) > top_n:
        top_firmwares = firmware_counts.iloc[:top_n]
        otros = firmware_counts.iloc[top_n:].sum()
        top_firmwares["Otros"] = otros
    else:
        top_firmwares = firmware_counts

    # Convertir a DataFrame
    firmware_df = top_firmwares.reset_index()
    firmware_df.columns = ["Firmware", "Cantidad"]
    return firmware_df

# Usar la función para consolidar los datos
firmware_df = consolidar_firmware(df_filtered, top_n=10)

# Crear el treemap con Plotly Express
treemap_fig = px.treemap(
    firmware_df,
    path=['Firmware'], 
    values='Cantidad',  
    title="Distribución de Firmware"
)
st.plotly_chart(treemap_fig, use_container_width=True)

# Tabla resumen: Agrupación por Zona y Location
st.subheader("Agrupación por Zona y Ubicación")
resumen = df_filtered.groupby(["Zona", "Ubicación"]).size().reset_index(name="Cantidad de Dispositivos")
st.dataframe(resumen)
