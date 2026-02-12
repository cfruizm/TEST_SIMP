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

# Cargar variables de entorno
load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")

# Configuración SMTP y URL base del dashboard
SMTP_SERVER = os.getenv("SMTP_SERVER")      # Ej: "smtp.gmail.com"
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
FROM_ADDR = os.getenv("FROM_ADDR")
# Correo por defecto (en caso de que no se encuentre email para un cliente)
DEFAULT_TO_ADDRS = os.getenv("TO_ADDRS").split(',')
BASE_DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://10.0.1.58:8501/Consumibles")

# Ruta al ejecutable de Edge (basado en Chromium)
CHROME_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

# Conexión a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Función para obtener clientes con sus correos
def get_customers():
    # Extraemos solo los clientes activos
    customers = list(db["CUSTOMER"].find({"status": "ACTIVE"}, {"name": 1, "contactEmail": 1, "address": 1, "_id": 0}))
    df = pd.DataFrame(customers)
    return df

def get_emails_for_customer(customer_name):
    df = get_customers()
    df_selected = df[df["name"] == customer_name]
    if not df_selected.empty:
        # Si el campo contactEmail es una cadena que puede contener múltiples emails separados por coma:
        emails = []
        for email in df_selected["contactEmail"].dropna().tolist():
            emails.extend([e.strip() for e in email.split(",") if e.strip() != ""])
        return list(set(emails))
    return []

# Función para generar PDF con pyppeteer a partir de una URL
async def generate_pdf(url):
    browser = await launch(headless=True, args=['--no-sandbox'], executablePath=CHROME_PATH)
    page = await browser.newPage()
    # Establecer un viewport amplio (puedes ajustarlo)
    await page.setViewport({'width': 1700, 'height': 3500})
    await page.goto(url, {"waitUntil": "networkidle2", "timeout": 40000})
    
    # Inyectar JS/CSS para ocultar el sidebar y expandir el contenido (ajusta según tu dashboard)
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
        // Seleccionar el contenedor de la tabla y forzarlo a un ancho grande
        const dfContainer = document.querySelector('div[data-testid="stDataFrameContainer"]');
        if (dfContainer) {
            dfContainer.style.width = "2000px"; // Puedes ajustar este valor si es necesario
            dfContainer.style.overflowX = "visible";
        }
        // También forzar que los contenedores internos (stDataFrameGlideDataEditor) se expandan
        const glideEditors = document.querySelectorAll('.stDataFrameGlideDataEditor');
        glideEditors.forEach(el => {
            el.style.width = "2000px";
            el.style.overflowX = "visible";
        });
    }''')
    
    # Esperar tiempo suficiente para que se apliquen los filtros y se renderice todo
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

def send_email_with_pdf(pdf_data, to_addrs, customer_name):
    msg = MIMEMultipart()
    msg["Subject"] = f"Dashboard de Consumibles - Informe Automático para {customer_name}"
    msg["From"] = FROM_ADDR
    msg["To"] = ", ".join(to_addrs)
    
    body = MIMEText(f"Adjunto se encuentra el informe en PDF del dashboard de consumibles filtrado para el cliente {customer_name}.", "plain")
    msg.attach(body)
    
    attachment = MIMEApplication(pdf_data, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=f"dashboard_consumibles_{customer_name}.pdf")
    msg.attach(attachment)
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.sendmail(FROM_ADDR, to_addrs, msg.as_string())
        print(f"Email enviado exitosamente para {customer_name} a: {to_addrs}")
    except Exception as e:
        print("Error al enviar email:", e)

# def main():
#     df_customers = get_customers()
#     if df_customers.empty:
#         print("No se encontraron clientes.")
#         return

#     # Iterar sobre cada cliente
#     for idx, row in df_customers.iterrows():
#         customer_name = row["name"]
#         # Construir URL filtrada para este cliente
#         params = {"Cliente": customer_name}
#         url = BASE_DASHBOARD_URL + "?" + urlencode(params)
#         print("Generando PDF para:", customer_name, "URL:", url)
        
#         # Generar PDF para la vista filtrada
#         pdf_data = asyncio.run(generate_pdf(url))
#         if not pdf_data:
#             print(f"No se pudo generar PDF para {customer_name}")
#             continue
        
#         # Guardar el PDF localmente (opcional)
#         filename = f"dashboard_consumibles_{customer_name}.pdf"
#         with open(filename, "wb") as f:
#             f.write(pdf_data)
        
#         # Obtener correos asociados al cliente
#         emails = get_emails_for_customer(customer_name)
#         if not emails:
#             print(f"No se encontraron correos para {customer_name}, usando correos por defecto.")
#             emails = DEFAULT_TO_ADDRS
        
#         # Enviar el PDF por correo
#         send_email_with_pdf(pdf_data, emails, customer_name)

# if __name__ == "__main__":
#     main()

def main_test():
    # Definir el cliente de prueba
    customer_name = "Esenttia"
    
    # Obtener correos asociados a ese cliente
    emails = get_emails_for_customer(customer_name)
    if not emails:
        print(f"No se encontraron correos para {customer_name}, usando correos por defecto.")
        emails = DEFAULT_TO_ADDRS

    # Construir la URL filtrada para ese cliente
    # Nota: Aquí se asume que tu dashboard (02_Consumibles.py) ha sido modificado
    # para leer el parámetro "Cliente" de la URL y aplicar ese filtro automáticamente.
    params = {"Cliente": customer_name}
    url = BASE_DASHBOARD_URL + "?" + urlencode(params)
    print("Generando PDF para:", customer_name, "con URL:", url)

    # Generar el PDF para la vista filtrada
    pdf_data = asyncio.run(generate_pdf(url))
    if not pdf_data:
        print(f"No se pudo generar PDF para {customer_name}")
        return

    # Opcional: guardar el PDF localmente
    filename = f"dashboard_consumibles_{customer_name}.pdf"
    with open(filename, "wb") as f:
        f.write(pdf_data)
    
    # Enviar el PDF por correo solo al cliente de prueba
    send_email_with_pdf(pdf_data, emails, customer_name)
    
if __name__ == "__main__":
    main_test()

