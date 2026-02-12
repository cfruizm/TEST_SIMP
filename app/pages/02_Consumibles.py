import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import altair as alt
import plotly.express as px

# Configurar la página
st.set_page_config(page_title="Dashboard - Consumibles", layout="wide")

# Verificar que st.secrets tenga los valores correctos
#st.write("st.secrets:", st.secrets)
#st.write("MONGO_URI:", st.secrets["MONGO_URI"])
#st.write("DATABASE_NAME:", st.secrets["DATABASE_NAME"])

# Cargar variables desde "Secrets" Streamlit
#load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")
MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = st.secrets["DATABASE_NAME"]

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Definir campos para cada colección
consumable_fields = {
    "deviceId": 1,
    "consumableId": 1,
    "colour": 1,
    "daysLeft": 1,
    "daysMonitored": 1,
    "description": 1,  # Descripción del consumible
    "engineCyclesMonitored": 1,
    "lastRead": 1,
    "pagesLeft": 1,
    "percentLeft": 1,
    "serialNumber": 1,  # Serial del consumible
    "sku": 1,
    "type": 1,
    "yield": 1,
    "_id": 0
}

device_fields = {
    "deviceId": 1,
    "customerId": 1,
    "serialNumber": 1,  # Serial del dispositivo
    "ipAddress": 1,
    "monitorStatus": 1,
    "extendedFields": 1,    # Contendrá "model", "zone", "location", "firmware", etc.
    "discoveryDate": 1,
    "lastContact": 1,
    "_id": 0
}

customer_fields = {
    "customerId": 1,
    "name": 1,
    "status": 1,
    "city": 1,
    "_id": 0
}


# Funciones de carga (usando caché con TTL para actualizar cada 5 minutos)
@st.cache_data(ttl=300, show_spinner=False)
def get_consumable_data():
    consumables = list(db["CONSUMABLE"].find({}, consumable_fields))
    return pd.DataFrame(consumables)

@st.cache_data(ttl=300, show_spinner=False)
def get_device_data():
    devices = list(db["DEVICE"].find({}, device_fields))
    return pd.DataFrame(devices)

@st.cache_data(ttl=300, show_spinner=False)
def get_customer_data():
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, customer_fields))
    return pd.DataFrame(customers)

# Función para unir datos de CONSUMABLE, DEVICE y CUSTOMER (INNER JOIN)
def unir_datos_consumibles():
    df_consumables = get_consumable_data()
    df_devices = get_device_data()
    df_customers = get_customer_data()
    
    if df_consumables.empty or df_devices.empty or df_customers.empty:
        return pd.DataFrame()

    # Expandir "extendedFields" en df_devices para extraer datos útiles
    if "extendedFields" in df_devices.columns:
        extended_df = df_devices["extendedFields"].apply(pd.Series)
        campos_extra = ["model", "zone", "location", "firmware", "hostName", "monitorName", "manufacturer", "mibDescription"]
        extended_df = extended_df[campos_extra].fillna("Desconocido")
        df_devices = pd.concat([df_devices.drop(columns=["extendedFields"]), extended_df], axis=1)
    else:
        for campo in ["model", "zone", "location", "firmware"]:
            df_devices[campo] = "Desconocido"

    # Convertir fechas a datetime en df_devices
    df_devices["discoveryDate"] = pd.to_datetime(df_devices["discoveryDate"], errors="coerce")
    df_devices["lastContact"] = pd.to_datetime(df_devices["lastContact"], errors="coerce")

    # Unir consumibles con dispositivos usando "deviceId"
    df_join = pd.merge(df_consumables, df_devices, on="deviceId", how="inner")
    # Unir con clientes usando "customerId" del DEVICE
    df_join = pd.merge(df_join, df_customers, on="customerId", how="inner")

    # Renombrar columnas para diferenciar serial de dispositivo y consumible
    df_join = df_join.rename(columns={
        "serialNumber_x": "Serial Consumible",
        "serialNumber_y": "Serial Dispositivo"
    })

    # Orden deseado de columnas
    orden_columnas = [
        "name", "city",
        "deviceId",
        "Serial Dispositivo",
        "ipAddress",
        "monitorStatus",
        "model", "zone", "location", "firmware",
        "consumableId",
        "type", "colour",
        "daysLeft", "percentLeft", "daysMonitored", "engineCyclesMonitored",
        "lastRead", "pagesLeft", "sku", "yield",
        "Serial Consumible",
        "description"
    ]
    df_join = df_join[[col for col in orden_columnas if col in df_join.columns]]

    # Renombrar para visualización
    df_join = df_join.rename(columns={
        "name": "Cliente",
        "city": "Ciudad",
        "ipAddress": "Direccion IP",
        "monitorStatus": "Estado de Monitoreo",
        "model": "Modelo",
        "zone": "Zona",
        "location": "Ubicación",
        "firmware": "Firmware",
        "consumableId": "ID Consumible",
        "type": "Tipo",
        "colour": "Color",
        "daysLeft": "Días Restantes",
        "percentLeft": "Porcentaje Restante",
        "daysMonitored": "Días Monitoreados",
        "engineCyclesMonitored": "Impresiones",
        "lastRead": "Última Lectura",
        "pagesLeft": "Páginas Restantes",
        "sku": "SKU",
        "yield": "durac. (teo)",
        "description": "Descripción"
    })
    return df_join

# Función para marcar el estado de cada consumible
def marcar_estado_suministros(df):
    # Convertir "Última Lectura" a datetime (si no lo está)
    df["Última Lectura"] = pd.to_datetime(df["Última Lectura"], errors="coerce")
    # Crear la clave de agrupación:
    # Si el Tipo es "UNKNOWN", se incluye la Descripción para distinguirlos
    df["group_key"] = df.apply(lambda row: f"{row['deviceId']}_{row['Tipo']}_{row['Color']}_{row['Descripción']}"
                                 if row["Tipo"].upper() == "UNKNOWN" 
                                 else f"{row['deviceId']}_{row['Tipo']}_{row['Color']}", axis=1)
    def marcar_grupo(grp):
        if len(grp) > 1:
            max_date = grp["Última Lectura"].max()
            # Marcar como "Actual" solo los registros con la fecha máxima (si hay empates, se marcan todos como "Actual")
            grp["Estado Suministro"] = np.where(grp["Última Lectura"] == max_date, "Actual", "Reemplazado")
        else:
            grp["Estado Suministro"] = "Actual"
        return grp
    df = df.groupby("group_key", group_keys=False).apply(marcar_grupo)
    df.drop(columns=["group_key"], inplace=True)
    return df

# Cargar y unir los datos
df = unir_datos_consumibles()

if df.empty:
    st.error("No se encontraron datos al unir las colecciones.")
else:
    # Asegurarse de que "Impresiones" y "Días Monitoreados" existen
    if "Impresiones" not in df.columns:
        df["Impresiones"] = 0
    if "Días Monitoreados" not in df.columns:
        df["Días Monitoreados"] = 0

    # Marcar el estado de los consumibles
    df = marcar_estado_suministros(df)

    # Calcular la tasa de consumo
    df["consumption_rate"] = np.where(
        df["Días Monitoreados"] == 0,
        0,
        df["Impresiones"] / df["Días Monitoreados"]
    )

    # Parámetros para el forecast
    target_days = 90     # Días de suministro deseados
    threshold_days = 30  # Umbral para reordenar
    adjustment_factor = 0.1

    # Calcular la recomendación de compra solo para consumibles "Actual"
    df["reorder_recommendation"] = np.where(
        (df["Días Restantes"] <= threshold_days) & (df["Estado Suministro"] == "Actual"),
        df["consumption_rate"] * (target_days - df["Días Restantes"]) * adjustment_factor,
        0
    )
    df["reorder_recommendation"] = (
        df["reorder_recommendation"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .round()
        .astype(int)
    )



    # Leer los parámetros de consulta desde la URL
    query_params = st.query_params
    default_cliente = query_params.get("cliente", query_params.get("Cliente", []))

    # Barra lateral: Filtros
    st.sidebar.header("📌 Filtros de Consumibles")
    clientes_unicos = sorted(df["Cliente"].dropna().unique())
    filtro_cliente = st.sidebar.multiselect("Seleccionar Cliente", clientes_unicos, default=default_cliente)
    dispositivos_unicos = sorted(df["Serial Dispositivo"].dropna().unique())
    filtro_device = st.sidebar.multiselect("Seleccionar Dispositivo (Serial)", dispositivos_unicos)
    tipos_unicos = sorted(df["Tipo"].dropna().unique())
    filtro_tipo = st.sidebar.multiselect("Seleccionar Tipo", tipos_unicos)
    colores_unicos = sorted(df["Color"].dropna().unique())
    filtro_color = st.sidebar.multiselect("Seleccionar Color", colores_unicos)
    estado_suministro = sorted(df["Estado Suministro"].dropna().unique())
    filtro_estado_suministro = st.sidebar.multiselect("Seleccionar estado suministro", estado_suministro)
    

    # Ajuste del slider para "Días Restantes"
    max_days_val = int(df["Días Restantes"].max())
    slider_max = 1000 if max_days_val > 1000 else max_days_val
    st.sidebar.info("El rango de Días Restantes se limita a 0 - " + str(slider_max) + " para mayor precisión.")
    filtro_dias = st.sidebar.slider("Rango de Días Restantes", min_value=0, max_value=slider_max, value=(0, slider_max))

    df_filtered = df.copy()
    if filtro_tipo:
        df_filtered = df_filtered[df_filtered["Tipo"].isin(filtro_tipo)]
    if filtro_color:
        df_filtered = df_filtered[df_filtered["Color"].isin(filtro_color)]
    if filtro_device:
        df_filtered = df_filtered[df_filtered["Serial Dispositivo"].isin(filtro_device)]
    if filtro_cliente:
        df_filtered = df_filtered[df_filtered["Cliente"].isin(filtro_cliente)]
    if filtro_estado_suministro:
        df_filtered = df_filtered[df_filtered["Estado Suministro"].isin(filtro_estado_suministro)]
    df_filtered = df_filtered[(df_filtered["Días Restantes"] >= filtro_dias[0]) & (df_filtered["Días Restantes"] <= filtro_dias[1])]

    # Cobertura del suministro = (5% * durac_teo / Impresiones) * 100 => (%) y Rendimiento del consumible = (Impresiones / durac_teo) * 100
    
    df_filtered["Cobertura Suministro"] = np.where(
    df_filtered["Estado Suministro"] == "Actual",
    (0.05 * df_filtered["durac. (teo)"]) / (df_filtered["Impresiones"] + df_filtered["Páginas Restantes"]) * 100,
    (0.05 * df_filtered["durac. (teo)"]) / df_filtered["Impresiones"] * 100
    )
    df_filtered["Rendimiento Consumible"] = np.where(
    df_filtered["Estado Suministro"] == "Actual",
    ((df_filtered["Impresiones"] +df_filtered["Páginas Restantes"]) / df_filtered["durac. (teo)"]) * 100,
    (df_filtered["Impresiones"] / df_filtered["durac. (teo)"]) * 100
    )
    
    #df_filtered["Rendimiento Consumible"] = (df_filtered["Impresiones"] / df_filtered["durac. (teo)"]) * 100

    df_filtered["Cobertura Suministro"] = df_filtered["Cobertura Suministro"].round(2)
    df_filtered["Rendimiento Consumible"] = df_filtered["Rendimiento Consumible"].round(2)


    # Título de la página
    st.title("📊 Dashboard de consumibles")

    st.subheader("Indicadores Clave")
    # Sólo considerar consumibles "Actual" y en Monitoreo
    df_actual = df_filtered[(df_filtered["Estado Suministro"] == "Actual") & (df_filtered["Estado de Monitoreo"] == "Y")] 
    total_consumibles = len(df_actual)
    consumibles_30d = len(df_actual[df_actual["Días Restantes"] <= threshold_days])
    porcentaje_30d = (consumibles_30d / total_consumibles * 100) if total_consumibles > 0 else 0
    consumibles_criticos = len(df_actual[df_actual["Días Restantes"] <= 10])
    porcentaje_criticos = (consumibles_criticos / total_consumibles * 100) if total_consumibles > 0 else 0
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Consumibles", total_consumibles)
    col2.metric("Consumibles (<30 días)", consumibles_30d)
    col3.metric("Porcentaje <30", f"{porcentaje_30d:.1f}%")
    col4.metric("Consumibles Críticos (<10 días)", consumibles_criticos)
    col5.metric("Porcentaje Crítico", f"{porcentaje_criticos:.1f}%")

    st.subheader("Gráficos")
    # 1. Distribución por Rangos de Días Restantes
    bins = [0, 30, 60, 90, slider_max + 1]
    labels = ["<30", "30-60", "60-90", ">=90"]
    df_filtered["dias_range"] = pd.cut(df_filtered["Días Restantes"], bins=bins, labels=labels, include_lowest=True)
    df_filtered["dias_range"] = pd.Categorical(df_filtered["dias_range"], categories=labels, ordered=True)
    range_counts = df_filtered["dias_range"].value_counts().sort_index()
    range_counts_df = range_counts.reset_index()
    range_counts_df.columns = ["dias_range", "count"]
    chart_range = alt.Chart(range_counts_df).mark_bar().encode(
        x=alt.X("dias_range:N", sort=labels, title="Rango de Días Restantes"),
        y=alt.Y("count:Q", title="Cantidad de Consumibles")
    ).properties(
        width=600,
        height=500,
        title="Distribución de Consumibles por Rango de Días Restantes"
    )
    st.altair_chart(chart_range, use_container_width=True)

    # 2. Gráfico Conteo de Consumibles a Reordenar por Tipo (para los consumibles Actual)

    df_tipo = df_actual[df_actual["reorder_recommendation"] > 0].groupby("Tipo", as_index=False).size()
    chart_tipo = alt.Chart(df_tipo).mark_bar().encode(
        x=alt.X("Tipo:N", sort=alt.SortField(field="size", order="descending"), title="Tipo"),
        y=alt.Y("size:Q", title="Cantidad de Suministros a Reordenar"),
        tooltip=["Tipo", "size"]
    ).properties(
        width=600,
        height=500,
        title="Suministros a Reordenar por Tipo"
    )
    st.altair_chart(chart_tipo, use_container_width=True)

    # 3. Gráfico de barras: Suministros a Reordenar (Agrupados por SKU y Descripción) 
    
    # Agrupar todos los consumibles a reordenar (reorder_recommendation > 0) por SKU y Descripción, usando la cuenta de registros.
    df_reorder = df_filtered[df_filtered["reorder_recommendation"] > 0].groupby(["SKU", "Descripción"], as_index=False).size()
    st.markdown("**Top 10 Suministros a Reordenar (Por SKU y Descripción)**")
    # Filtrar solo registros con reorder_recommendation > 0 y agrupar por SKU y Descripción
    top_reorders = df_filtered[df_filtered["reorder_recommendation"] > 0].groupby(["SKU", "Descripción"], as_index=False).size()
    top_reorders = top_reorders.sort_values("size", ascending=False).head(11)
    top_reorders["Etiqueta"] = top_reorders["SKU"].astype(str) # + " - " + top_reorders["Descripción"].astype(str)
    
    chart_top = alt.Chart(top_reorders).mark_bar().encode(
        x=alt.X("size:Q", title="Cantidad de Consumibles a Pedir"),
        y=alt.Y("Etiqueta:N", sort="-x", title="SKU - Descripción"),
        tooltip=["SKU", "Descripción", "size"]
    ).properties(
        width=600,
        height=570,
    )
    st.altair_chart(chart_top, use_container_width=True)

    # 4. tabla reorder
    st.markdown("**Tabla de Suministros a Reordenar**")
    st.dataframe(df_reorder.sort_values("size", ascending=False),height=1070)

    # 5. Scatter Plot de Días Restantes vs. consumption_rate
    # Asegurarse de que "Tipo" es de tipo cadena
    df_filtered["Tipo"] = df_filtered["Tipo"].astype(str)
    
    st.markdown("**Días Restantes vs. Tasa de Consumo (Actual)**")
    df_scatter = df_actual.copy()
    chart_scatter = alt.Chart(df_scatter).mark_circle(size=60).encode(
        x=alt.X("Días Restantes:Q", title="Días Restantes"),
        y=alt.Y("consumption_rate:Q", title="Tasa de Consumo (Impresiones/Día)"),
        #color=alt.Color("Estado Suministro:N"),
        tooltip=["Cliente", "Serial Dispositivo", "Tipo", "Color", "Porcentaje Restante", "Días Restantes", "Impresiones", "consumption_rate", "reorder_recommendation"]
    ).properties(
        width=600,
        height=500,
    )
    st.altair_chart(chart_scatter, use_container_width=True)

    # 7  Scatter Plot para evaluar rendimiento del suministro
    st.markdown("**Rendimiento Teórico vs. Impresiones (actuales)**")
    # Renombrar yield a "durac. (teo)" en df_filtered para el gráfico
    df_filtered = df_filtered.rename(columns={"durac. (teo)": "durac_teo"})
    # Si no se ha renombrado previamente, reemplazar "Rendimiento" por "durac_teo"
    if "durac_teo" not in df_filtered.columns and "Rendimiento" in df_filtered.columns:
        df_filtered = df_filtered.rename(columns={"Rendimiento": "durac_teo"})
    chart_scatter = alt.Chart(df_filtered).mark_circle(size=60).encode(
        x=alt.X("Impresiones:Q", title="Impresiones toner Actual"),
        y=alt.Y("durac_teo:Q", title="Durac. (teo)"),
        size=alt.Size("Páginas Restantes:Q", title="Impresiones Restantes"),
        color=alt.Color("Porcentaje Restante:Q", title="Toner Restante", scale=alt.Scale(scheme="redyellowgreen")),
        tooltip=["Cliente", "Serial Dispositivo", "Tipo", "Color", "Impresiones", "durac_teo", "Porcentaje Restante", "Páginas Restantes"]
    ).properties(
        width=700,
        height=500,
        #title="Scatter Plot: Rendac. (teo) vs. Impresiones, con Toner y Páginas Restantes"
    )
    st.altair_chart(chart_scatter, use_container_width=True)

    #8. # --- Crear el gráfico por rangos de Cobertura Suministro ---
    # Definimos los bins para la cobertura:
    bins = [0, 5, 8, 12, 20, np.inf]
    labels = ["<=5%", "5% - 8%", ">8% - 12%", "12% - 20%", ">20%"]
    df_filtered["Rango Cobertura"] = pd.cut(df_filtered["Cobertura Suministro"], bins=bins, labels=labels, right=True)

    # Convertir la columna a categoría ordenada según el orden deseado
    df_filtered["Rango Cobertura"] = pd.Categorical(df_filtered["Rango Cobertura"], categories=labels, ordered=True)

    # Agrupar por estos rangos y contar la cantidad de suministros
    df_cobertura = df_filtered.groupby("Rango Cobertura").size().reset_index(name="Cantidad")

    # Crear un gráfico de barras con Altair, especificando el orden en el eje X
    chart_cobertura = alt.Chart(df_cobertura).mark_bar().encode(
        x=alt.X("Rango Cobertura:N", sort=labels, title="Rango de Cobertura"),
        y=alt.Y("Cantidad:Q", title="Cantidad de Suministros"),
        tooltip=["Rango Cobertura", "Cantidad"]
    ).properties(
        width=600,
        height=500,
        title="Cantidad de Suministros por Rango de Cobertura"
    )

    st.altair_chart(chart_cobertura, use_container_width=True)

    # 9. tabla con el detalle por consumibles

    st.dataframe(df_filtered[[
    "Cliente", "Serial Dispositivo", "Direccion IP", "Serial Consumible", "Tipo", "Color", "SKU", "Descripción",
    "Días Restantes", "Porcentaje Restante", "Impresiones", "durac_teo", "reorder_recommendation", "Estado Suministro",
    "Cobertura Suministro", "Rendimiento Consumible"
]], height=450)

