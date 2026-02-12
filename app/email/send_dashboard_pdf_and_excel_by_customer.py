import os
import asyncio
from pyppeteer import launch
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from dotenv import load_dotenv
from urllib.parse import urlencode
import pandas as pd
from pymongo import MongoClient
import numpy as np
import warnings
warnings.filterwarnings(
    "ignore", 
    message="DataFrameGroupBy.apply operated on the grouping columns", 
    category=DeprecationWarning
)

# Cargar variables de entorno
load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")

# Configuración SMTP y URL base del dashboard
SMTP_SERVER = os.getenv("SMTP_SERVER")      
SMTP_PORT = int(os.getenv("SMTP_PORT", 25))
FROM_ADDR = os.getenv("FROM_ADDR")
DEFAULT_TO_ADDRS = os.getenv("TO_ADDRS").split(',')
BASE_DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://10.0.1.58:8501/Consumibles")

# Ruta al ejecutable de Edge (basado en Chromium)
CHROME_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

# Conexión a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# --- Funciones para extraer datos desde MongoDB ---

# Campos para la colección CONSUMABLE
consumable_fields = {
    "deviceId": 1,
    "consumableId": 1,
    "colour": 1,
    "daysLeft": 1,
    "daysMonitored": 1,
    "description": 1,
    "engineCyclesMonitored": 1,
    "lastRead": 1,
    "pagesLeft": 1,
    "percentLeft": 1,
    "serialNumber": 1,
    "sku": 1,
    "type": 1,
    "yield": 1,
    "_id": 0
}

# Campos para la colección DEVICE
device_fields = {
    "deviceId": 1,
    "customerId": 1,
    "serialNumber": 1,
    "ipAddress": 1,
    "monitorStatus": 1,
    "extendedFields": 1,
    "discoveryDate": 1,
    "lastContact": 1,
    "_id": 0
}

# Campos para la colección CUSTOMER
customer_fields = {
    "customerId": 1,
    "name": 1,
    "status": 1,
    "city": 1,
    "contactEmail": 1,   # Se asume que aquí están los correos
    "_id": 0
}

# Campos para la colección MONITOR
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

def get_consumable_data():
    consumables = list(db["CONSUMABLE"].find({}, consumable_fields))
    return pd.DataFrame(consumables)

def get_device_data():
    devices = list(db["DEVICE"].find({"monitorStatus": "Y"}, device_fields))
    df = pd.DataFrame(devices)
    if not df.empty:
        df["discoveryDate"] = pd.to_datetime(df["discoveryDate"], errors="coerce")
        df["lastContact"] = pd.to_datetime(df["lastContact"], errors="coerce")
    return df

def get_customer_data():
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, customer_fields))
    return pd.DataFrame(customers)

def get_monitor_data():
    monitors = list(db["MONITOR"].find({}, monitor_fields))
    df = pd.DataFrame(monitors)
    if not df.empty:
        df["lastContact"] = pd.to_datetime(df["lastContact"], errors="coerce")
        df["createdDate"] = pd.to_datetime(df["createdDate"], errors="coerce")
    return df

def unir_datos_consumibles():
    df_consumables = get_consumable_data()
    df_devices = get_device_data()
    df_customers = get_customer_data()
    df_monitors = get_monitor_data()
    
    if df_consumables.empty or df_devices.empty or df_customers.empty or df_monitors.empty:
        return pd.DataFrame()
    
    # Unir CONSUMABLE con DEVICE usando "deviceId"
    df = pd.merge(df_consumables, df_devices, on="deviceId", how="inner")
    # Unir con CUSTOMER usando "customerId" del DEVICE
    df = pd.merge(df, df_customers, on="customerId", how="inner")
    # Unir con MONITOR usando "customerId" del DEVICE
    df = pd.merge(df, df_monitors, on="customerId", how="inner")
    
    # Renombrar columnas para diferenciarlas
    df = df.rename(columns={
        "serialNumber_x": "Serial Consumible",
        "serialNumber_y": "Serial Dispositivo",
        "name_x": "Nombre cliente",
        "name_y": "Nombre monitor",
    })
    
    # Incluir la columna deviceId para usarla en marcar_estado_suministros
    # Seleccionar y ordenar columnas (incluyendo deviceId)
    orden_columnas = [
        "Nombre cliente", "city", "deviceId", "Serial Dispositivo", "ipAddress", "monitorStatus",
        "extendedFields", "Serial Consumible", "type", "description", "sku", "colour", "daysLeft", "percentLeft",
        "daysMonitored", "engineCyclesMonitored", "lastRead", "pagesLeft", "yield", "Estado Suministro", "Cobertura Suministro",
        "Rendimiento Consumible"
    ]
    df = df[[col for col in orden_columnas if col in df.columns]]
    
    # Renombrar para visualización
    df = df.rename(columns={
        "Nombre cliente": "Cliente",
        "city": "Ciudad",
        "ipAddress": "Direccion IP",
        "monitorStatus": "Estado de Monitoreo",
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

    df["Cobertura Suministro"] = ((0.05 * df["durac. (teo)"]) / df["Impresiones"]) * 100
    df["Rendimiento Consumible"] = (df["Impresiones"] / df["durac. (teo)"]) * 100

    df["Cobertura Suministro"] = df["Cobertura Suministro"].round(2)
    df["Rendimiento Consumible"] = df["Rendimiento Consumible"].round(2)

    return df

# Función para marcar el estado de cada consumible
def marcar_estado_suministros(df):
    df["Última Lectura"] = pd.to_datetime(df["Última Lectura"], errors="coerce")
    # Crear la clave de agrupación
    df["group_key"] = df.apply(
        lambda row: f"{row['deviceId']}_{row['Tipo']}_{row['Color']}_{row['Descripción']}" 
            if row["Tipo"].upper() == "UNKNOWN" 
            else f"{row['deviceId']}_{row['Tipo']}_{row['Color']}",
        axis=1
    )
    def marcar_grupo(grp):
        if len(grp) > 1:
            max_date = grp["Última Lectura"].max()
            grp["Estado Suministro"] = np.where(grp["Última Lectura"] == max_date, "Actual", "Reemplazado")
        else:
            grp["Estado Suministro"] = "Actual"
        return grp
    # Utilizamos groupby sin el parámetro include_groups y luego eliminamos la columna de grupo
    df = df.groupby("group_key", group_keys=False).apply(marcar_grupo).reset_index(drop=True)
    df.drop(columns=["group_key"], inplace=True)
    return df

def get_emails_for_customer(customer_name):
    # Extraer solo los clientes activos
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, {"name": 1, "contactEmail": 1, "_id": 0}))
    df = pd.DataFrame(customers)
    df_selected = df[df["name"] == customer_name]
    if not df_selected.empty:
        emails = []
        for email in df_selected["contactEmail"].dropna().tolist():
            emails.extend([e.strip() for e in email.split(",") if e.strip() != ""])
        return list(set(emails))
    return []

def generate_excel_report(df, filename="informe_consumibles.xlsx"):
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    # Convertir columnas datetime a timezone-naive
    df = df.apply(lambda x: x.dt.tz_localize(None) if x.dtype == 'datetime64[ns, UTC]' else x)
    # Excluir la columna 'deviceId'
    df_sin_deviceId = df.drop(columns=['deviceId'], errors='ignore')
    df_sin_deviceId.to_excel(writer, index=False, sheet_name='Detalle Consumibles')
    writer.close()
    with open(filename, "rb") as f:
        excel_data = f.read()
    return excel_data

# Función para generar PDF con pyppeteer
async def generate_pdf(url):
    browser = await launch(headless=True, args=['--no-sandbox'], executablePath=CHROME_PATH)
    page = await browser.newPage()
    await page.setViewport({'width': 1700, 'height': 3500})
    await page.goto(url, {"waitUntil": "networkidle2", "timeout": 40000})
    
    await page.evaluate('''() => {
        const sidebar = document.querySelector('section[data-testid="stSidebar"]');
        if (sidebar) {
            sidebar.style.display = "none";
        }
        const mainContainer = document.querySelector("main");
        if (mainContainer) {
            mainContainer.style.marginLeft = "0px";
            mainContainer.style.width = "100%";
            mainContainer.style.maxWidth = "none";
        }
        const dfContainer = document.querySelector('div[data-testid="stDataFrameContainer"]');
        if (dfContainer) {
            dfContainer.style.width = "2000px";
            dfContainer.style.overflowX = "visible";
        }
        const glideEditors = document.querySelectorAll('.stDataFrameGlideDataEditor');
        glideEditors.forEach(el => {
            el.style.width = "2000px";
            el.style.overflowX = "visible";
        });
    }''')
    
    await asyncio.sleep(20)
    
    options = {
        'format': 'A4',
        'printBackground': True,
        'landscape': True,
        'margin-top': '10mm',
        'margin-right': '20mm',
        'margin-bottom': '10mm',
        'margin-left': '10mm',
        'viewport-size': '2480x3500'
    }
    pdf_bytes = await page.pdf(options=options)
    await browser.close()
    return pdf_bytes

def send_email_with_pdf_and_excel(pdf_data, excel_data, to_addrs, customer_name):
    msg = MIMEMultipart()
    msg["Subject"] = f"Dashboard de Consumibles - Informe Automático para {customer_name}"
    msg["From"] = FROM_ADDR
    msg["To"] = ", ".join(to_addrs)
    
    body = MIMEText(f"Adjunto se encuentra el informe en PDF y Excel del dashboard de consumibles filtrado para el cliente {customer_name}.", "plain")
    msg.attach(body)
    
    attachment_pdf = MIMEApplication(pdf_data, _subtype="pdf")
    attachment_pdf.add_header("Content-Disposition", "attachment", filename=f"dashboard_consumibles_{customer_name}.pdf")
    msg.attach(attachment_pdf)
    
    attachment_excel = MIMEApplication(excel_data, _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    attachment_excel.add_header("Content-Disposition", "attachment", filename=f"detalle_consumibles_{customer_name}.xlsx")
    msg.attach(attachment_excel)
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.sendmail(FROM_ADDR, to_addrs, msg.as_string())
        print(f"Email enviado exitosamente para {customer_name} a: {to_addrs}")
    except Exception as e:
        print("Error al enviar email:", e)

def main():
    """
    Función principal que genera y envía informes de PDF y Excel para todos los clientes.
    """
    try:
        # Obtener lista de clientes
        df_customers = get_customer_data()
        if df_customers.empty:
            print("No hay clientes disponibles para procesar.")
            return

        print(f"Procesando {len(df_customers)} clientes...")

        for idx, customer in df_customers.iterrows():
            try:
                customer_name = customer["name"]
                print(f"\nProcesando cliente: {customer_name}")

                # Construir URL filtrada para este cliente (asumiendo que el dashboard usa el query param "Cliente")
                params = {"Cliente": customer_name}
                url = BASE_DASHBOARD_URL + "?" + urlencode(params)
                print("Generando PDF para:", customer_name, "con URL:", url)

                # Generar PDF para la vista filtrada
                pdf_data = asyncio.run(generate_pdf(url))
                if not pdf_data:
                    print(f"No se pudo generar PDF para {customer_name}")
                    continue

                # Obtener el DataFrame de detalle y filtrar por cliente
                df_detail = unir_datos_consumibles()
                if not df_detail.empty:
                    df_detail = df_detail[df_detail["Cliente"] == customer_name]
                    df_detail = marcar_estado_suministros(df_detail)
                else:
                    df_detail = pd.DataFrame()

                # Generar el archivo Excel a partir de la tabla de detalle
                excel_data = generate_excel_report(df_detail, filename=f"detalle_consumibles_{customer_name}.xlsx")
                
                # Obtener correos asociados al cliente
                emails = get_emails_for_customer(customer_name)
                if not emails:
                    print(f"No se encontraron correos para {customer_name}, usando correos por defecto.")
                    emails = DEFAULT_TO_ADDRS

                # Enviar el PDF y el Excel por correo
                send_email_with_pdf_and_excel(pdf_data, excel_data, emails, customer_name)
                print(f"Informes enviados a {customer_name} exitosamente.")
            except Exception as e:
                print(f"Error procesando cliente {customer.get('name', 'Desconocido')}: {e}")

        print("Proceso completado.")
    except Exception as e:
        print("Error general en la ejecución:", e)

def main_test():
    # Cliente de prueba
    customer_name = "Esenttia"
    
    # Obtener correos asociados al cliente
    emails = get_emails_for_customer(customer_name)
    if not emails:
        print(f"No se encontraron correos para {customer_name}, usando correos por defecto.")
        emails = DEFAULT_TO_ADDRS

    # Construir URL filtrada para este cliente
    params = {"Cliente": customer_name}
    url = BASE_DASHBOARD_URL + "?" + urlencode(params)
    print("Generando PDF para:", customer_name, "con URL:", url)
    
    # Generar el PDF de la vista filtrada
    pdf_data = asyncio.run(generate_pdf(url))
    if not pdf_data:
        print(f"No se pudo generar PDF para {customer_name}")
        return
    
    # Guardar opcionalmente el PDF localmente
    filename = f"dashboard_consumibles_{customer_name}.pdf"
    with open(filename, "wb") as f:
        f.write(pdf_data)
    
    # Obtener el DataFrame de detalle y filtrar por cliente
    df_detail = unir_datos_consumibles()
    if not df_detail.empty:
        df_detail = df_detail[df_detail["Cliente"] == customer_name]
        df_detail = marcar_estado_suministros(df_detail)
    else:
        df_detail = pd.DataFrame()
    
    # Generar el archivo Excel a partir de la tabla de detalle
    excel_data = generate_excel_report(df_detail, filename=f"detalle_consumibles_{customer_name}.xlsx")
    
    # Enviar el PDF y el Excel por correo
    send_email_with_pdf_and_excel(pdf_data, excel_data, emails, customer_name)

if __name__ == "__main__":
    main_test()
    #main()