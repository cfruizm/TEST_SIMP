import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import altair as alt
import plotly.express as px

# Configurar la página
st.set_page_config(page_title="Dashboard - Contadores", layout="wide")

# Cargar variables desde "Secrets" Streamlit
#load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")
MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Definir campos para cada colección

# METERS (Contadores)
meters_fields = {
    "billingDate": 1,
    "readingDate": 1,
    "readingDateTime": 1,
    "a4Mono": 1,
    "a4Colour": 1,
    "engineCycles": 1,
    "scans": 1,
    "nonCopyScans": 1,
    "monoSmall": 1,
    "monoLarge": 1,
    "colourSmall": 1,
    "colourLarge": 1,
    "monoTier": 1,
    "colourTier1": 1,
    "colourTier2": 1,
    "colourTier3": 1,
    "monoPages": 1,
    "colourPages": 1,
    "duplex": 1,
    "deviceId": 1,
    "_id": 0
}

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


# Funciones de carga con caché (TTL de 300 segundos)
@st.cache_data(ttl=300, show_spinner=False)
def get_meters_data():
    meters = list(db["METERS"].find({}, meters_fields))
    df = pd.DataFrame(meters)
    # Convertir readingDateTime y billingDate a datetime
    df["readingDateTime"] = pd.to_datetime(df["readingDateTime"], errors="coerce")
    df["billingDate"] = pd.to_datetime(df["billingDate"], errors="coerce")
    return df

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

# Función para unir los datos de METERS, DEVICE, CUSTOMER y MONITOR
def unir_datos_meters():
    df_meters = get_meters_data()
    df_devices = get_device_data()
    df_customers = get_customer_data()

    if df_meters.empty or df_devices.empty or df_customers.empty:
        return pd.DataFrame()
    
    # Unir METERS con DEVICE usando "deviceId"
    df = pd.merge(df_meters, df_devices, on="deviceId", how="inner")
    # Unir con CUSTOMER usando "customerId" del DEVICE
    df = pd.merge(df, df_customers, on="customerId", how="inner")

    # Renombrar columnas para visualización
    df = df.rename(columns={
        "name": "Cliente",
        "status": "Estado cliente",
        "lastContact": "lastContact",
        "city": "Ciudad",
        "serialNumber": "Serial Dispositivo",
        "engineCycles": "Ciclos de motor",
        "monoSmall": "Paginas mono",
        "colourPages": "paginas color"
    })
    return df

# Función para calcular consumos diarios (diferencias) para contadores relevantes
def calcular_consumo_diario(df):
    # Ordenar por deviceId y readingDateTime
    df = df.sort_values(["deviceId", "readingDateTime"])
    # Calcular diferencias por dispositivo para engineCycles y monoPages
    df["engineCycles_daily"] = df.groupby("deviceId")["Ciclos de motor"].diff()
    df["monoPages_daily"] = df.groupby("deviceId")["Paginas mono"].diff()
    df["colourPages_daily"] = df.groupby("deviceId")["paginas color"].diff()
    df["totalPages_daily"] = df["monoPages_daily"] + df["colourPages_daily"]
    # Otras métricas se pueden calcular de forma similar
    return df

# Cargar y unir los datos
df = unir_datos_meters()
if df.empty:
    st.error("No se encontraron datos en la colección METERS o en los JOINs.")
else:
    # Calcular consumos diarios
    df = calcular_consumo_diario(df)
    
    #Filtros
    st.sidebar.header("Filtros de Contadores")
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

    # Filtro por rango de fecha (por ejemplo, readingDateTime)
    min_date = df["billingDate"].min().date()
    max_date = df["billingDate"].max().date()
    filtro_fecha = st.sidebar.date_input("Rango de Fecha", value=(min_date, max_date))
    
    df_filtered = df.copy()
    if filtro_cliente:
        df_filtered = df_filtered[df_filtered["Cliente"].isin(filtro_cliente)]
    if filtro_device:
        df_filtered = df_filtered[df_filtered["Serial Dispositivo"].isin(filtro_device)]
    if isinstance(filtro_fecha, (list, tuple)) and len(filtro_fecha)==2:
        df_filtered = df_filtered[(df_filtered["billingDate"].dt.date >= filtro_fecha[0]) & 
                                  (df_filtered["billingDate"].dt.date <= filtro_fecha[1])]

    # Título de la página
    st.title("📊 Dashboard de contadores")
    st.subheader("Indicadores Clave")
    total_registros = df_filtered.shape[0]
    # Promedio diario de engineCycles para todos los dispositivos filtrados
    avg_engineCycles = df_filtered[df_filtered["engineCycles_daily"] != 0]["engineCycles_daily"].mean()
    # Promedio diario de monoPages
    avg_monoPages = df_filtered[df_filtered["monoPages_daily"] != 0]["monoPages_daily"].mean()
    # Promedio diario de colourPages
    avg_colourPages = df_filtered[df_filtered["colourPages_daily"] != 0]["colourPages_daily"].mean()

    col1, col2, col3 = st.columns(3)
    #col1.metric("Total Registros (Contadores)", total_registros)
    col1.metric("Promedio Diario (ciclos de motor)", f"{avg_engineCycles:.0f}")
    col2.metric("Promedio Diario (Paginas mono)", f"{avg_monoPages:.0f}")
    col3.metric("Promedio Diario (Paginas color)",f"{avg_colourPages:.0f}")

    st.subheader("Datos de Contadores")
    # Formatear las columnas de fecha antes de mostrarlas
    df_filtered["billingDate"] = df_filtered["billingDate"].dt.strftime("%Y-%m-%d")
    df_filtered["readingDateTime"] = df_filtered["readingDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Crear la columna "simplex" (impresiones en modo simplex)
    df_filtered["simplex"] = df_filtered["Ciclos de motor"] - df_filtered["duplex"]

    st.dataframe(df_filtered[[
        "Cliente", "Serial Dispositivo", "billingDate", "readingDateTime", "Ciclos de motor", "engineCycles_daily",
        "Paginas mono", "monoPages_daily", "paginas color", "colourPages_daily", "scans", "duplex", "simplex"
    ]])

    st.subheader("Gráficos de Tendencia")
    # 1. 
    # Agrupar por "Serial Dispositivo" y tomar el último reading (según readingDateTime)
    df_latest = df_filtered.sort_values("readingDateTime").groupby("Serial Dispositivo", as_index=False).tail(1)

    # Top 20 Impresoras con MAYOR Engine Cycles
    df_top20 = df_latest.sort_values("Ciclos de motor", ascending=False).head(20)
    chart_top20 = alt.Chart(df_top20).mark_bar().encode(
        x=alt.X("Ciclos de motor:Q", title="Ciclos de motor"),
        y=alt.Y("Serial Dispositivo:N", sort="-x", title="Impresoras (Mayor Ciclo de motor)"),
        tooltip=["Cliente", "Serial Dispositivo", "Ciclos de motor", "readingDateTime"]
    ).properties(
        width=600,
        height=400,
        title="Top 20 Impresoras con Mayor Engine Cycles"
    )
    st.altair_chart(chart_top20, use_container_width=True)

    # Top 20 Impresoras con MENOR Engine Cycles (según el último reading)
    df_bottom20 = df_latest.sort_values("Ciclos de motor", ascending=True).head(20)
    chart_bottom20 = alt.Chart(df_bottom20).mark_bar().encode(
        x=alt.X("Ciclos de motor:Q", title="Ciclos de motor"),
        y=alt.Y("Serial Dispositivo:N", sort="-x", title="Impresoras (Menor Ciclo de motor)"),
        tooltip=["Cliente", "Serial Dispositivo", "Ciclos de motor", "readingDateTime"]
    ).properties(
        width=600,
        height=400,
        title="Top 20 Impresoras con Menor Engine Cycles"
    )
    st.altair_chart(chart_bottom20, use_container_width=True)

    # 2. Serie Temporal: Consumo Diario (Ciclos de motor)
    chart_line_diff = alt.Chart(df_filtered.dropna(subset=["engineCycles_daily"])).mark_line().encode(
        x=alt.X("billingDate:O", title="Fecha"),
        y=alt.Y("engineCycles_daily:Q", title="Consumo Diario"),
        color=alt.Color("Serial Dispositivo:N", legend=alt.Legend(title="Dispositivo")),
        tooltip=["Cliente", "Serial Dispositivo", "readingDateTime", "engineCycles_daily"]
    ).properties(
        width=700,
        height=400,
        title="Consumo Diario (ciclos de motor)"
    )
    st.altair_chart(chart_line_diff, use_container_width=True)

    # 3. Estadisticas impresoras a color
    # Filtrar impresoras a color (donde colourSmall > 0)
    df_color = df_filtered[df_filtered["colourSmall"] > 0].copy()

    # Agrupar por "Serial Dispositivo" y tomar el registro más reciente basado en billingDate
    df_latest = df_color.sort_values("billingDate").groupby("Serial Dispositivo", as_index=False).tail(1)

    # Convertir el DataFrame a formato largo para las columnas "monoSmall" y "colourSmall"
    df_melt = df_latest.melt(
        id_vars=["Serial Dispositivo", "Cliente"],
        value_vars=["Paginas mono", "colourSmall"],
        var_name="PrintType",
        value_name="Paginas"
    )

    # Crear el gráfico de barras agrupadas utilizando xOffset para separar las barras por PrintType
    chart_color = alt.Chart(df_melt).mark_bar().encode(
        x=alt.X("Serial Dispositivo:N", title="Impresora"),
        xOffset=alt.XOffset("PrintType:N"),  # Separa las barras por PrintType
        y=alt.Y("Paginas:Q", title="Páginas Impresas"),
        color=alt.Color("PrintType:N", title="Tipo", scale=alt.Scale(range=["steelblue", "tomato"])),
        tooltip=["Cliente", "Serial Dispositivo", "PrintType", "Paginas"]
    ).properties(
        width=700,
        height=400,
        title="Estadísticas de Impresoras a Color: mono vs. colour"
    )

    st.altair_chart(chart_color, use_container_width=True)

    # 4. # Scatter Plot: Relación entre Total de Páginas Impresas y EngineCycles diarios
    df_efficiency = df_filtered.dropna(subset=["totalPages_daily", "engineCycles_daily"])

    chart_efficiency = alt.Chart(df_efficiency).mark_circle(size=60).encode(
        x=alt.X("totalPages_daily:Q", title="Total de Páginas Impresas Diarias"),
        y=alt.Y("engineCycles_daily:Q", title="Ciclos de Motor Diarios"),
        color=alt.Color("Cliente:N", legend=alt.Legend(title="Cliente")),
        tooltip=["Cliente", "Serial Dispositivo", "readingDateTime", "totalPages_daily", "engineCycles_daily"]
    ).properties(
        width=700,
        height=400,
        title="Relación: Total de Páginas Impresas vs. Ciclos de Motor Diarios"
    )

    st.altair_chart(chart_efficiency, use_container_width=True)


    # 5. comparativa duplex y simplex
    
    # Agrupar por "Serial Dispositivo" y tomar el último registro (según readingDateTime)
    df_latest = df_filtered.sort_values("readingDateTime").groupby("Serial Dispositivo", as_index=False).tail(1)

    # Seleccionar las top 20 impresoras con mayor impresión en modo duplex
    df_top_duplex = df_latest.sort_values("duplex", ascending=False).head(20)

    # Transformar el DataFrame a formato largo para comparar "duplex" y "simplex"
    df_top_duplex_melt = df_top_duplex.melt(
        id_vars=["Serial Dispositivo", "Cliente"],
        value_vars=["duplex", "simplex"],
        var_name="Modo",
        value_name="Impresiones"
    )

    # Crear gráfico de barras lado a lado
    chart_duplex = alt.Chart(df_top_duplex_melt).mark_bar().encode(
        x=alt.X("Serial Dispositivo:N", title="Impresora", sort="-y"),
        y=alt.Y("Impresiones:Q", title="Cantidad de Impresiones"),
        color=alt.Color("Modo:N", title="Modo", scale=alt.Scale(domain=["duplex", "simplex"], range=["steelblue", "orange"])),
        tooltip=["Cliente", "Serial Dispositivo", "Modo", "Impresiones"]
    ).properties(
        width=700,
        height=400,
        title="Comparativa: Impresiones Duplex vs. Simplex (Top Impresoras por Duplex)"
    )

    st.altair_chart(chart_duplex, use_container_width=True)

    #6. ScatterPlot Scans Vs Impresiones

    # Filtrar dispositivos que tienen scans > 0 (multifuncionales)
    df_scans = df_filtered[df_filtered["scans"] > 0].copy()

    # Agrupar por "Serial Dispositivo" y tomar el último registro (según readingDateTime)
    df_latest_scans = df_scans.sort_values("readingDateTime").groupby("Serial Dispositivo", as_index=False).tail(1)

    # Crear el gráfico de dispersión: Engine Cycles vs. Scans
    chart_scans = alt.Chart(df_latest_scans).mark_circle(size=60).encode(
        x=alt.X("Ciclos de motor:Q", title="Ciclos de motor (Acumulado)"),
        y=alt.Y("scans:Q", title="Scans"),
        color=alt.Color("Cliente:N", legend=alt.Legend(title="Cliente")),
        tooltip=["Cliente", "Serial Dispositivo", "Ciclos de motor", "scans", "readingDateTime"]
    ).properties(
        width=700,
        height=400,
        title="Relación: Engine Cycles vs. Scans (Multifuncionales)"
    )

    st.altair_chart(chart_scans, use_container_width=True)
