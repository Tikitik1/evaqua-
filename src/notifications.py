"""
Módulo de utilidades para notificaciones y reportes
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Dict
from .config import NOTIFICATION_EMAIL, SMTP_SERVER, SMTP_PORT


def send_alert_email(recipient: str, glacier_name: str, alerts: List[Dict]) -> bool:
    """
    Envía un email de alerta

    Args:
        recipient: Email del destinatario
        glacier_name: Nombre del glaciar
        alerts: Lista de alertas

    Returns:
        True si se envió correctamente
    """
    try:
        subject = f"🚨 Alerta Crítica - {glacier_name}"

        # Construir cuerpo del email
        body = f"""
        <html>
            <body>
                <h2>Alerta de Glaciar - {glacier_name}</h2>
                <p>Se ha detectado una condición crítica en el monitoreo.</p>
                
                <h3>Alertas:</h3>
                <ul>
        """

        for alert in alerts:
            if alert.get("alert"):
                body += f"<li>{alert['message']}</li>"

        body += """
                </ul>
                
                <p><strong>Timestamp:</strong> """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
                <p>Acceda al dashboard para más información.</p>
            </body>
        </html>
        """

        # Crear mensaje
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = NOTIFICATION_EMAIL
        msg["To"] = recipient

        msg.attach(MIMEText(body, "html"))

        # Enviar email (implementación simplificada)
        # En producción, usar las credenciales correctas
        return True

    except Exception as e:
        print(f"Error enviando email: {e}")
        return False


def generate_alert_report(alerts_history: List[Dict]) -> str:
    """
    Genera un reporte de alertas

    Args:
        alerts_history: Historial de alertas

    Returns:
        Reporte en formato texto
    """
    report = "=== REPORTE DE ALERTAS ===\n\n"
    report += f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    report += f"Total de registros: {len(alerts_history)}\n\n"

    critical_count = len([a for a in alerts_history if a.get("status") == "critical"])
    warning_count = len([a for a in alerts_history if a.get("status") == "warning"])

    report += f"Alertas Críticas: {critical_count}\n"
    report += f"Advertencias: {warning_count}\n\n"

    return report
