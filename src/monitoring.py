"""
Módulo de monitoreo para detectar condiciones críticas en glaciares
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from .config import (
    TEMP_THRESHOLD_WARNING,
    TEMP_THRESHOLD_CRITICAL,
    MELT_RATE_WARNING,
    MELT_RATE_CRITICAL,
    VOLUME_LOSS_WARNING,
    VOLUME_LOSS_CRITICAL,
    VELOCITY_WARNING,
    VELOCITY_CRITICAL,
    ALERT_LEVELS,
)


class GlacierMonitor:
    """Sistema de monitoreo de glaciares"""

    def __init__(self):
        self.alerts_history = []
        self.last_monitoring = None

    def analyze_glacier(self, glacier_data: Dict) -> Dict:
        """
        Analiza un glaciar y detecta condiciones críticas

        Args:
            glacier_data: Diccionario con datos del glaciar

        Returns:
            Diccionario con análisis y alertas detectadas
        """
        alerts = []
        status = "normal"

        # Análisis de temperatura
        temp_analysis = self._analyze_temperature(glacier_data)
        if temp_analysis["alert"]:
            alerts.append(temp_analysis)
            if temp_analysis["level"] == "critical":
                status = "critical"
            elif status != "critical":
                status = "warning"

        # Análisis de velocidad de deshielo
        melt_analysis = self._analyze_melt_rate(glacier_data)
        if melt_analysis["alert"]:
            alerts.append(melt_analysis)
            if melt_analysis["level"] == "critical":
                status = "critical"
            elif status != "critical":
                status = "warning"

        # Análisis de pérdida de volumen
        volume_analysis = self._analyze_volume_loss(glacier_data)
        if volume_analysis["alert"]:
            alerts.append(volume_analysis)
            if volume_analysis["level"] == "critical":
                status = "critical"
            elif status != "critical":
                status = "warning"

        # Análisis de velocidad del glaciar
        velocity_analysis = self._analyze_velocity(glacier_data)
        if velocity_analysis["alert"]:
            alerts.append(velocity_analysis)
            if velocity_analysis["level"] == "critical":
                status = "critical"
            elif status != "critical":
                status = "warning"

        result = {
            "glacier_name": glacier_data.get("name", "Desconocido"),
            "region": glacier_data.get("region", ""),
            "timestamp": datetime.now(),
            "status": status,
            "alerts": alerts,
            "temperature": glacier_data.get("temperature", 0),
            "melt_rate": glacier_data.get("melt_rate", 0),
            "volume_loss_percent": glacier_data.get("volume_loss_percent", 0),
            "velocity": glacier_data.get("velocity", 0),
        }

        self.alerts_history.append(result)
        return result

    def _analyze_temperature(self, glacier_data: Dict) -> Dict:
        """Análisis de temperatura"""
        temp = glacier_data.get("temperature", 0)
        alert = False
        level = "info"

        if temp >= TEMP_THRESHOLD_CRITICAL:
            alert = True
            level = "critical"
        elif temp >= TEMP_THRESHOLD_WARNING:
            alert = True
            level = "warning"

        return {
            "type": "temperature",
            "alert": alert,
            "level": level,
            "value": temp,
            "threshold": TEMP_THRESHOLD_CRITICAL if level == "critical" else TEMP_THRESHOLD_WARNING,
            "message": f"Temperatura: {temp}°C - Umbral: {TEMP_THRESHOLD_CRITICAL if level == 'critical' else TEMP_THRESHOLD_WARNING}°C",
        }

    def _analyze_melt_rate(self, glacier_data: Dict) -> Dict:
        """Análisis de velocidad de deshielo"""
        melt_rate = glacier_data.get("melt_rate", 0)
        alert = False
        level = "info"

        if melt_rate >= MELT_RATE_CRITICAL:
            alert = True
            level = "critical"
        elif melt_rate >= MELT_RATE_WARNING:
            alert = True
            level = "warning"

        return {
            "type": "melt_rate",
            "alert": alert,
            "level": level,
            "value": melt_rate,
            "threshold": MELT_RATE_CRITICAL if level == "critical" else MELT_RATE_WARNING,
            "message": f"Velocidad de deshielo: {melt_rate} m/año - Umbral: {MELT_RATE_CRITICAL if level == 'critical' else MELT_RATE_WARNING} m/año",
        }

    def _analyze_volume_loss(self, glacier_data: Dict) -> Dict:
        """Análisis de pérdida de volumen"""
        volume_loss = glacier_data.get("volume_loss_percent", 0)
        alert = False
        level = "info"

        if volume_loss >= VOLUME_LOSS_CRITICAL:
            alert = True
            level = "critical"
        elif volume_loss >= VOLUME_LOSS_WARNING:
            alert = True
            level = "warning"

        return {
            "type": "volume_loss",
            "alert": alert,
            "level": level,
            "value": volume_loss,
            "threshold": VOLUME_LOSS_CRITICAL if level == "critical" else VOLUME_LOSS_WARNING,
            "message": f"Pérdida de volumen: {volume_loss}% - Umbral: {VOLUME_LOSS_CRITICAL if level == 'critical' else VOLUME_LOSS_WARNING}%",
        }

    def _analyze_velocity(self, glacier_data: Dict) -> Dict:
        """Análisis de velocidad del glaciar"""
        velocity = glacier_data.get("velocity", 0)
        alert = False
        level = "info"

        if velocity >= VELOCITY_CRITICAL:
            alert = True
            level = "critical"
        elif velocity >= VELOCITY_WARNING:
            alert = True
            level = "warning"

        return {
            "type": "velocity",
            "alert": alert,
            "level": level,
            "value": velocity,
            "threshold": VELOCITY_CRITICAL if level == "critical" else VELOCITY_WARNING,
            "message": f"Velocidad: {velocity} m/año - Umbral: {VELOCITY_CRITICAL if level == 'critical' else VELOCITY_WARNING} m/año",
        }

    def get_alerts_by_level(self, level: str = None) -> List[Dict]:
        """Obtiene alertas filtradas por nivel"""
        if not level:
            return self.alerts_history

        return [alert for alert in self.alerts_history if alert.get("status") == level]

    def get_critical_glaciers(self) -> List[Dict]:
        """Obtiene glaciares con estado crítico"""
        return [alert for alert in self.alerts_history if alert.get("status") == "critical"]

    def get_alerts_summary(self) -> Dict:
        """Resumen de alertas por nivel"""
        summary = {"info": 0, "warning": 0, "critical": 0}

        for alert in self.alerts_history:
            status = alert.get("status", "info")
            if status in summary:
                summary[status] += 1

        return summary
