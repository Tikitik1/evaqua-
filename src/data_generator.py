"""
Generador de datos simulados para demostración del sistema de alertas
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict


class DataGenerator:
    """Genera datos simulados de glaciares"""

    GLACIARES = [
        {
            "id": 1,
            "name": "San Rafael",
            "region": "Aysén",
            "lat": -46.3892,
            "lon": -73.7822,
            "area_km2": 1350,
            "initial_volume": 45000,
        },
        {
            "id": 2,
            "name": "Perito Moreno",
            "region": "Santa Cruz",
            "lat": -50.4926,
            "lon": -73.1895,
            "area_km2": 1079,
            "initial_volume": 38000,
        },
        {
            "id": 3,
            "name": "Upsala",
            "region": "Santa Cruz",
            "lat": -50.4500,
            "lon": -73.3500,
            "area_km2": 870,
            "initial_volume": 30000,
        },
        {
            "id": 4,
            "name": "Pio XI",
            "region": "Aysén",
            "lat": -49.3333,
            "lon": -73.6667,
            "area_km2": 1265,
            "initial_volume": 44000,
        },
        {
            "id": 5,
            "name": "Bernardo",
            "region": "Magallanes",
            "lat": -51.3621,
            "lon": -72.3263,
            "area_km2": 456,
            "initial_volume": 16000,
        },
    ]

    @classmethod
    def get_glacier_list(cls) -> List[Dict]:
        """Retorna lista de glaciares disponibles"""
        return cls.GLACIARES

    @classmethod
    def generate_glacier_data(cls, glacier_id: int, variability: float = 0.3) -> Dict:
        """
        Genera datos simulados para un glaciar

        Args:
            glacier_id: ID del glaciar
            variability: Factor de variabilidad (0-1)

        Returns:
            Diccionario con datos del glaciar
        """
        glacier = next((g for g in cls.GLACIARES if g["id"] == glacier_id), None)
        if not glacier:
            raise ValueError(f"Glaciar con ID {glacier_id} no encontrado")

        # Simular datos con variabilidad
        base_temp = np.random.uniform(0, 6)
        base_melt = np.random.uniform(5, 30)
        base_volume_loss = np.random.uniform(5, 35)
        base_velocity = np.random.uniform(50, 250)

        # Agregar variabilidad aleatoria
        temperature = base_temp + np.random.normal(0, variability * 2)
        melt_rate = base_melt + np.random.normal(0, variability * 5)
        volume_loss_percent = base_volume_loss + np.random.normal(0, variability * 10)
        velocity = base_velocity + np.random.normal(0, variability * 30)

        # Asegurar valores positivos
        temperature = max(0, temperature)
        melt_rate = max(0, melt_rate)
        volume_loss_percent = max(0, min(100, volume_loss_percent))
        velocity = max(0, velocity)

        return {
            **glacier,
            "timestamp": datetime.now(),
            "temperature": round(temperature, 2),
            "melt_rate": round(melt_rate, 2),
            "volume_loss_percent": round(volume_loss_percent, 2),
            "velocity": round(velocity, 2),
            "current_volume": glacier["initial_volume"] * (1 - volume_loss_percent / 100),
        }

    @classmethod
    def generate_time_series(
        cls, glacier_id: int, days: int = 30, interval_hours: int = 6
    ) -> pd.DataFrame:
        """
        Genera una serie de tiempo de datos para un glaciar

        Args:
            glacier_id: ID del glaciar
            days: Número de días para simular
            interval_hours: Intervalo en horas entre mediciones

        Returns:
            DataFrame con serie de tiempo
        """
        data = []
        start_date = datetime.now() - timedelta(days=days)

        for i in range(0, days * 24, interval_hours):
            timestamp = start_date + timedelta(hours=i)
            glacier_data = cls.generate_glacier_data(glacier_id, variability=0.4)
            glacier_data["timestamp"] = timestamp

            # Agregar tendencia de degradación
            trend_factor = 1 + (i / (days * 24 * 2)) * 0.3
            glacier_data["temperature"] *= trend_factor
            glacier_data["melt_rate"] *= trend_factor
            glacier_data["volume_loss_percent"] *= trend_factor

            data.append(glacier_data)

        df = pd.DataFrame(data)
        return df.sort_values("timestamp").reset_index(drop=True)

    @classmethod
    def generate_all_glaciers_snapshot(cls) -> pd.DataFrame:
        """Genera datos actuales para todos los glaciares"""
        data = []
        for glacier in cls.GLACIARES:
            glacier_data = cls.generate_glacier_data(glacier["id"])
            data.append(glacier_data)

        return pd.DataFrame(data)
