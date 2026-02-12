import os
import pdfkit
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")

# Parámetros de conexión a MongoDB y otros (si es necesario)
# ...

# Configuración SMTP (asegúrate de tener estos valores en tu .env)
SMTP_SERVER = os.getenv("SMTP_SERVER")  
SMTP_PORT = int(os.getenv("SMTP_PORT", 25))
#SMTP_USERNAME = os.getenv("SMTP_USERNAME")
#SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_ADDR = os.getenv("FROM_ADDR")
# TO_ADDRS debe ser una cadena separada por comas, por ejemplo: "user1@example.com,user2@example.com"
TO_ADDRS = os.getenv("TO_ADDRS").split(',')

# URL donde está corriendo el dashboard de consumibles (ej: localhost)
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://10.0.1.58:8501/Contadores")

# Configurar la ruta de wkhtmltopdf (ajusta según la instalación en tu sistema)
path_wkhtmltopdf = r"D:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
config_pdf = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)

def generate_pdf_from_dashboard(url):
    """
    Genera un PDF a partir de la URL del dashboard.
    Retorna un objeto BytesIO con el contenido del PDF.
    """
    options = {
        'javascript-delay': '60000',  # Espera 15s para que cargue JavaScript
        'no-stop-slow-scripts': '',
        'enable-local-file-access': '',
        'viewport-size': '1280x1024',  # Establece un tamaño de ventana para la captura
        'quiet': ''  # Opcional, para reducir la salida en consola
    }
    try:
        pdf_data = pdfkit.from_url(url, False, configuration=config_pdf, options=options)
        return pdf_data
    except Exception as e:
        print("Error al generar el PDF:", e)
        return None

def send_email_with_pdf(pdf_data):
    """
    Envía el PDF adjunto por correo utilizando SMTP.
    """
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
            #server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(FROM_ADDR, TO_ADDRS, msg.as_string())
        print("Email enviado exitosamente.")
    except Exception as e:
        print("Error al enviar email:", e)

def main():
    pdf_data = generate_pdf_from_dashboard(DASHBOARD_URL)
    if pdf_data is None:
        print("No se pudo generar el PDF.")
    else:
        # Opcional: guardar el PDF localmente
        with open("dashboard_consumibles.pdf", "wb") as f:
            f.write(pdf_data)
        send_email_with_pdf(pdf_data)

if __name__ == "__main__":
    main()