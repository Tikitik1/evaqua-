"""
Configuración de umbrales y parámetros del sistema de alertas
"""

# Umbrales de temperatura (°C)
TEMP_THRESHOLD_WARNING = 3.0  # Alerta amarilla
TEMP_THRESHOLD_CRITICAL = 5.0  # Alerta roja

# Umbrales de velocidad de deshielo (m/año)
MELT_RATE_WARNING = 10.0
MELT_RATE_CRITICAL = 25.0

# Umbrales de volumen perdido (%)
VOLUME_LOSS_WARNING = 10.0  # % de volumen perdido
VOLUME_LOSS_CRITICAL = 25.0

# Umbrales de velocidad del glaciar (m/año)
VELOCITY_WARNING = 100.0
VELOCITY_CRITICAL = 200.0

# Niveles de alerta
ALERT_LEVELS = {
    "info": {"color": "#0099cc", "emoji": "ℹ️", "label": "Información"},
    "warning": {"color": "#ffaa00", "emoji": "⚠️", "label": "Advertencia"},
    "critical": {"color": "#ff3333", "emoji": "🚨", "label": "Crítica"},
}

# Configuración de monitoreo
MONITORING_INTERVAL = 3600  # segundos (1 hora)
DATA_RETENTION_DAYS = 30  # días

# Regiones principales a monitorear
MONITORED_REGIONS = [
    "Aysén",
    "Magallanes",
    "Los Lagos",
]

# Parámetros de correo (para alertas futuras)
NOTIFICATION_EMAIL = "alertas@glaciares.cl"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# API Keys & URLs
# Open-Elevation API (Gratuita, sin key, pero con límites de request)
OPEN_ELEVATION_URL = "https://api.open-elevation.com/api/v1/lookup"
OPENTOPOGRAPHY_API_KEY = None # Ya no se usa, pero mantenemos referencia por si acaso

# Colores para Mapa
RISK_COLORS = {
    'bajo': '#2ecc71',      # Verde
    'medio': '#f39c12',     # Amarillo
    'alto': '#e67e22',      # Naranja
    'critico': '#e74c3c'    # Rojo
}
