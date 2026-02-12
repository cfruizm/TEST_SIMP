import os
import asyncio
from pyppeteer import launch
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")

SMTP_SERVER = os.getenv("SMTP_SERVER")  # Ej: "smtp.gmail.com"
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
#SMTP_USERNAME = os.getenv("SMTP_USERNAME")
#SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_ADDR = os.getenv("FROM_ADDR")
TO_ADDRS = os.getenv("TO_ADDRS").split(',')
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://10.0.1.58:8501/Consumibles")

# Ruta a Chromium del sistema.

CHROME_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

async def generate_pdf(url):
    # Inicia el navegador usando la ruta especificada
    browser = await launch(headless=True, args=['--no-sandbox'], executablePath=CHROME_PATH)
    page = await browser.newPage()
    await page.setViewport({'width': 1770, 'height': 1380})
    # Navega a la URL y espera a que la red esté inactiva
    await page.goto(url, {"waitUntil": "networkidle2", "timeout": 40000})
    # Inyectar JavaScript para ocultar el sidebar de Streamlit
    await page.evaluate('''() => {
        // Ocultar el sidebar
        const sidebar = document.querySelector('section[data-testid="stSidebar"]');
        if (sidebar) {
            sidebar.style.display = "none";
        }
        // Asegurar que el contenedor principal use todo el ancho
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

    # Espera adicional para asegurarse de que se renderice todo
    await asyncio.sleep(20)
    # Generar el PDF con opciones: formato A4 y sin márgenes
    options = {
        'format': 'A4',
        'printBackground': True,
        'landscape': True,
        'margin-top': '10mm',
        'margin-right': '10mm',
        'margin-bottom': '10mm',
        'margin-left': '10mm',
        'viewport-size': '1000x800'
    }
    pdf_bytes = await page.pdf(options=options)
    await browser.close()
    return pdf_bytes

def send_email_with_pdf(pdf_data):
    msg = MIMEMultipart()
    msg["Subject"] = "Dashboard de Consumibles - Informe Automático"
    msg["From"] = FROM_ADDR
    msg["To"] = ", ".join(TO_ADDRS)
    
    body = MIMEText("Adjunto se encuentra el informe en PDF del dashboard de consumibles.", "plain")
    msg.attach(body)
    
    attachment = MIMEApplication(pdf_data, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename="dashboard_consumibles.pdf")
    msg.attach(attachment)
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.sendmail(FROM_ADDR, TO_ADDRS, msg.as_string())
        print("Email enviado exitosamente.")
    except Exception as e:
        print("Error al enviar email:", e)

def main():
    # Usar asyncio.run para generar el PDF
    pdf_data = asyncio.run(generate_pdf(DASHBOARD_URL))
    if pdf_data:
        # Opcional: guardar localmente el PDF
        with open("dashboard_consumibles.pdf", "wb") as f:
            f.write(pdf_data)
        send_email_with_pdf(pdf_data)
    else:
        print("No se pudo generar el PDF.")

if __name__ == "__main__":
    main()
