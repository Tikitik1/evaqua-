"""
Módulo para integración de datos reales desde APIs y archivos locales
Utiliza OpenMeteo para temperatura y datos locales de glaciares
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de APIs
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Rutas de datos locales - Buscar en múltiples ubicaciones
_current_dir = Path(__file__).parent.parent
_possible_paths = [
    _current_dir / "backend" / "datos",  # Si app.py está en alertas_streamlit/
    _current_dir.parent / "backend" / "datos",  # Si app.py está en raíz
    Path("c:/Users/tikit/Desktop/proyecto glaciares/glaciares/backend/datos"),  # Ruta absoluta
]

BASE_DATA_PATH = None
for path in _possible_paths:
    if path.exists():
        BASE_DATA_PATH = path
        break

if BASE_DATA_PATH is None:
    # Usar la primera como default
    BASE_DATA_PATH = _possible_paths[0]


class RealDataProvider:
    """Proveedor de datos reales desde APIs y archivos locales"""

    # Glaciares principales con coordenadas
    GLACIARES_COORDS = {
        "San Rafael": {"lat": -46.3892, "lon": -73.7822, "region": "Aysén"},
        "Perito Moreno": {"lat": -50.4926, "lon": -73.1895, "region": "Santa Cruz"},
        "Upsala": {"lat": -50.4500, "lon": -73.3500, "region": "Santa Cruz"},
        "Pio XI": {"lat": -49.3333, "lon": -73.6667, "region": "Aysén"},
        "Bernardo": {"lat": -51.3621, "lon": -72.3263, "region": "Magallanes"},
    }
    
    # Zonas de Aysén (31 comunas aproximadamente)
    # Dividimos la región en una cuadrícula de zonas
    AYSEN_ZONES = [
        {"zone_id": f"Z{i:02d}", "lat_center": lat, "lon_center": lon}
        for i, (lat, lon) in enumerate([
            (-45.0, -72.0), (-45.0, -72.5), (-45.0, -73.0), (-45.0, -73.5),
            (-45.5, -72.0), (-45.5, -72.5), (-45.5, -73.0), (-45.5, -73.5),
            (-46.0, -72.0), (-46.0, -72.5), (-46.0, -73.0), (-46.0, -73.5),
            (-46.5, -72.0), (-46.5, -72.5), (-46.5, -73.0), (-46.5, -73.5),
            (-47.0, -72.0), (-47.0, -72.5), (-47.0, -73.0), (-47.0, -73.5),
            (-47.5, -72.0), (-47.5, -72.5), (-47.5, -73.0), (-47.5, -73.5),
            (-48.0, -72.0), (-48.0, -72.5), (-48.0, -73.0), (-48.0, -73.5),
            (-48.5, -72.0), (-48.5, -72.5), (-48.5, -73.0),
        ], 1)
    ]
    
    # Cache de temperaturas por zona
    _temperature_cache = {}
    _aysen_region = None  # Cache para la región
    _aysen_zones = None   # Cache para las zonas

    @staticmethod
    def load_aysen_region() -> Optional[gpd.GeoDataFrame]:
        """Carga el shapefile de la región de Aysén"""
        if RealDataProvider._aysen_region is not None:
            return RealDataProvider._aysen_region
        
        try:
            # Cargar desde P00_RegProvCom_SIRGAS2000_fat
            aysen_path = BASE_DATA_PATH / "P00_RegProvCom_SIRGAS2000_fat"
            if aysen_path.exists():
                shp_files = list(aysen_path.glob("*.shp"))
                if shp_files:
                    gdf = gpd.read_file(shp_files[0])
                    gdf = gdf.to_crs(epsg=4326)
                    
                    # Filtrar solo Aysén - buscar por nombre de región
                    if 'NOM_REGION' in gdf.columns:
                        gdf = gdf[gdf['NOM_REGION'].str.contains('AYSÉN', case=False, na=False)]
                    
                    RealDataProvider._aysen_region = gdf
                    logger.info(f"✅ Región de Aysén cargada: {len(gdf)} geometrías")
                    return gdf
            
            logger.warning("No se encontró shapefile de región")
            return None
        except Exception as e:
            logger.error(f"Error cargando región de Aysén: {e}")
            return None

    @staticmethod
    def create_zone_grid(n_zones: int = 30) -> List[dict]:
        """
        Crea una cuadrícula de zonas dentro de la región de Aysén
        
        Args:
            n_zones: Número de zonas deseadas
            
        Returns:
            Lista de zonas con center y bounds
        """
        if RealDataProvider._aysen_zones is not None:
            return RealDataProvider._aysen_zones
        
        try:
            # Cargar región de Aysén
            aysen_gdf = RealDataProvider.load_aysen_region()
            
            if aysen_gdf is None or aysen_gdf.empty:
                logger.warning("No se pudo cargar región de Aysén, usando cuadrícula predefinida")
                return RealDataProvider.AYSEN_ZONES
            
            # Obtener bounds de Aysén
            total_bounds = aysen_gdf.total_bounds  # [minx, miny, maxx, maxy]
            min_lon, min_lat, max_lon, max_lat = total_bounds
            
            logger.info(f"Bounds de Aysén: lat [{min_lat:.2f}, {max_lat:.2f}], lon [{min_lon:.2f}, {max_lon:.2f}]")
            
            # Crear cuadrícula de zonas
            # Para 30 zonas: aproximadamente 6 columnas x 5 filas
            cols = 6
            rows = 5
            
            lon_step = (max_lon - min_lon) / cols
            lat_step = (max_lat - min_lat) / rows
            
            zones = []
            zone_id = 1
            
            for i in range(cols):
                for j in range(rows):
                    lon_center = min_lon + (i + 0.5) * lon_step
                    lat_center = min_lat + (j + 0.5) * lat_step
                    
                    zones.append({
                        'zone_id': f"Z{zone_id:02d}",
                        'lat_center': lat_center,
                        'lon_center': lon_center,
                        'bounds': {
                            'north': lat_center + lat_step / 2,
                            'south': lat_center - lat_step / 2,
                            'east': lon_center + lon_step / 2,
                            'west': lon_center - lon_step / 2
                        }
                    })
                    zone_id += 1
            
            RealDataProvider._aysen_zones = zones
            logger.info(f"✅ Creadas {len(zones)} zonas basadas en shapefile de Aysén")
            return zones
            
        except Exception as e:
            logger.error(f"Error creando zonas: {e}")
            return RealDataProvider.AYSEN_ZONES

    @staticmethod
    def get_temperature_data(lat: float, lon: float, days: int = 30) -> Dict:
        """
        Obtiene datos de temperatura real de OpenMeteo

        Args:
            lat: Latitud
            lon: Longitud
            days: Número de días históricos a obtener

        Returns:
            Diccionario con datos de temperatura
        """
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
                "timezone": "America/Santiago",
            }

            response = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "daily" in data:
                daily_data = data["daily"]
                current_temp = daily_data["temperature_2m_mean"][-1] if daily_data["temperature_2m_mean"] else 0
                avg_temp = np.mean(daily_data["temperature_2m_mean"]) if daily_data["temperature_2m_mean"] else 0

                return {
                    "current_temperature": float(current_temp),
                    "average_temperature": float(avg_temp),
                    "max_temperature": float(max(daily_data["temperature_2m_max"])) if daily_data["temperature_2m_max"] else 0,
                    "min_temperature": float(min(daily_data["temperature_2m_min"])) if daily_data["temperature_2m_min"] else 0,
                    "temperature_trend": float(daily_data["temperature_2m_mean"][-1] - daily_data["temperature_2m_mean"][0])
                    if len(daily_data["temperature_2m_mean"]) > 1
                    else 0,
                    "data_source": "OpenMeteo Archive",
                    "timestamp": datetime.now().isoformat(),
                }
        except requests.RequestException as e:
            logger.warning(f"Error obteniendo datos de OpenMeteo: {e}")
            # Retornar datos por defecto si falla
            return {
                "current_temperature": None,
                "average_temperature": None,
                "error": str(e),
            }

    @staticmethod
    def get_zone_temperature(zone_id: str, lat: float, lon: float) -> Dict:
        """
        Obtiene temperatura para una zona (cachea resultados)
        
        Args:
            zone_id: ID de la zona
            lat: Latitud del centro de la zona
            lon: Longitud del centro de la zona
            
        Returns:
            Diccionario con datos de temperatura
        """
        # Verificar cache
        if zone_id in RealDataProvider._temperature_cache:
            return RealDataProvider._temperature_cache[zone_id]
        
        try:
            # Obtener temperatura actual (más rápido que histórico)
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": ["temperature_2m", "relative_humidity_2m"],
                "timezone": "America/Santiago",
            }
            
            response = requests.get(OPENMETEO_URL, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            current = data.get("current", {})
            temp = float(current.get("temperature_2m", 0))
            
            result = {
                "zone_id": zone_id,
                "temperature": temp,
                "average_temperature": temp,
                "data_source": "OpenMeteo Current",
                "timestamp": datetime.now().isoformat(),
            }
            
            # Cachear por 30 minutos
            RealDataProvider._temperature_cache[zone_id] = result
            return result
            
        except Exception as e:
            logger.warning(f"Error obteniendo temperatura para zona {zone_id}: {e}")
            return {
                "zone_id": zone_id,
                "temperature": 0,
                "average_temperature": 0,
                "error": str(e),
            }

    @staticmethod
    def get_forecast_data(lat: float, lon: float) -> Dict:
        """
        Obtiene pronóstico de temperatura de OpenMeteo

        Args:
            lat: Latitud
            lon: Longitud

        Returns:
            Diccionario con pronóstico
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": ["temperature_2m_max", "temperature_2m_min"],
                "timezone": "America/Santiago",
                "forecast_days": 16,
            }

            response = requests.get(OPENMETEO_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "daily" in data:
                return {
                    "forecast_max": float(max(data["daily"]["temperature_2m_max"])),
                    "forecast_min": float(min(data["daily"]["temperature_2m_min"])),
                    "data_source": "OpenMeteo Forecast",
                }
        except requests.RequestException as e:
            logger.warning(f"Error obteniendo pronóstico: {e}")
            return {"error": str(e)}

        return {}

    @staticmethod
    def load_glaciares_geojson() -> Optional[gpd.GeoDataFrame]:
        """Carga datos de glaciares desde archivos GeoJSON locales (solo Aysén)"""
        try:
            # Buscar archivos GeoJSON en el directorio de datos
            geojson_files = list(BASE_DATA_PATH.glob("*.geojson"))

            if not geojson_files:
                logger.warning("No se encontraron archivos GeoJSON")
                return None

            # Filtrar solo archivos que contienen datos de glaciares (excluir comunas)
            glacier_files = [f for f in geojson_files if 'glaciar' in f.name.lower() or 'comunas' not in f.name.lower()]
            
            if not glacier_files:
                logger.warning("No se encontraron archivos GeoJSON de glaciares (se encontraron comunas)")
                return None

            # Cargar el archivo GeoJSON de glaciares
            gdf = gpd.read_file(glacier_files[0])
            gdf = gdf.to_crs(epsg=4326)  # Asegurar CRS
            
            # Filtrar solo glaciares de Aysén si la columna existe
            if 'REGION' in gdf.columns:
                gdf = gdf[gdf['REGION'].str.contains('AISEN|AYSEN|Aysén|Aysen', case=False, na=False)]
            
            logger.info(f"Cargados {len(gdf)} glaciares desde {glacier_files[0].name}")
            return gdf if len(gdf) > 0 else None

        except Exception as e:
            logger.error(f"Error cargando GeoJSON: {e}")
            return None

    @staticmethod
    def load_glaciares_shapefile() -> Optional[gpd.GeoDataFrame]:
        """Carga datos de glaciares desde shapefiles locales (solo Aysén)"""
        try:
            # Prioridad de shapefiles: Aysén-Magallanes > 2022 > Inventario
            shapefile_dirs = [
                (BASE_DATA_PATH / "Glaciares Aysen-Magallanes", "Aysén-Magallanes"),
                (BASE_DATA_PATH / "inventario_glaciares_2022", "Inventario 2022"),
                (BASE_DATA_PATH / "inventario-de-glaciares_shapefile", "Inventario General"),
                (BASE_DATA_PATH / "Glaciares_aysen_antiguos", "Glaciares Antiguos"),
            ]

            for shapefile_dir, source_name in shapefile_dirs:
                if shapefile_dir.exists():
                    shp_files = list(shapefile_dir.glob("*.shp"))
                    if shp_files:
                        try:
                            gdf = gpd.read_file(shp_files[0])
                            gdf = gdf.to_crs(epsg=4326)  # Asegurar CRS
                            
                            # Filtrar solo glaciares de Aysén
                            if 'REGION' in gdf.columns:
                                gdf = gdf[gdf['REGION'].str.contains('AISEN|AYSEN|Aysén|Aysen', case=False, na=False)]
                            
                            if len(gdf) > 0:
                                logger.info(f"Cargados {len(gdf)} glaciares de Aysén desde {source_name}: {shp_files[0].name}")
                                return gdf
                            else:
                                logger.warning(f"No hay glaciares de Aysén en {source_name}")
                        except Exception as e:
                            logger.warning(f"Error cargando {source_name}: {e}")
                            continue

            logger.warning("No se encontraron shapefiles de glaciares de Aysén")
            return None

        except Exception as e:
            logger.error(f"Error cargando shapefile: {e}")
            return None

    @staticmethod
    def load_all_glacier_data() -> Optional[gpd.GeoDataFrame]:
        """
        Carga todos los datos de glaciares desde archivos geoespaciales
        Prioridad: Shapefiles de Glaciares > GeoJSON > Fallback

        Returns:
            GeoDataFrame con todos los glaciares de Aysén
        """
        try:
            # PRIMERO: Intentar cargar desde Shapefiles (tienen datos de glaciares reales)
            gdf = RealDataProvider.load_glaciares_shapefile()

            if gdf is not None and not gdf.empty:
                logger.info(f"✅ Cargados {len(gdf)} glaciares desde Shapefile")
                return gdf

            # SEGUNDO: Si no hay shapefiles, intentar GeoJSON
            logger.info("No se encontraron shapefiles, intentando GeoJSON...")
            gdf = RealDataProvider.load_glaciares_geojson()

            if gdf is not None and not gdf.empty:
                logger.info(f"✅ Cargados {len(gdf)} glaciares desde GeoJSON")
                return gdf

            logger.warning("❌ No se encontraron datos de glaciares de Aysén")
            return None

        except Exception as e:
            logger.error(f"Error cargando datos de glaciares: {e}")
            return None

    @staticmethod
    def enrich_glacier_data_with_temperature(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Enriquece datos de glaciares con temperatura de OpenMeteo

        Args:
            gdf: GeoDataFrame con datos de glaciares

        Returns:
            GeoDataFrame enriquecido con temperatura y parámetros
        """
        try:
            if gdf is None or gdf.empty:
                return gdf

            # Copiar para no modificar original
            gdf = gdf.copy()

            # Convertir a CRS proyectado para calcular centroide correctamente
            gdf_projected = gdf.to_crs(epsg=3857)  # Web Mercator proyectado
            gdf['centroid'] = gdf_projected.geometry.centroid
            
            # Convertir centroides de vuelta a WGS84
            centroid_gdf = gpd.GeoDataFrame(geometry=gdf['centroid'], crs='EPSG:3857')
            centroid_gdf = centroid_gdf.to_crs(epsg=4326)
            
            gdf['lat'] = centroid_gdf.geometry.y.values
            gdf['lon'] = centroid_gdf.geometry.x.values

            # Obtener temperatura para cada glaciar
            temperatures = []
            melt_rates = []
            forecasts_max = []

            for idx, row in gdf.iterrows():
                try:
                    lat, lon = row['lat'], row['lon']

                    # Obtener datos de OpenMeteo
                    temp_data = RealDataProvider.get_temperature_data(lat, lon, days=30)
                    forecast_data = RealDataProvider.get_forecast_data(lat, lon)

                    current_temp = temp_data.get('current_temperature', 0) or 0
                    melt_rate = max(0, current_temp * 2.5) if current_temp else 0
                    forecast_max = forecast_data.get('forecast_max', 0) or 0

                    temperatures.append(float(current_temp))
                    melt_rates.append(float(melt_rate))
                    forecasts_max.append(float(forecast_max))

                except Exception as e:
                    logger.warning(f"Error obteniendo temperatura para glaciar {idx}: {e}")
                    temperatures.append(0)
                    melt_rates.append(0)
                    forecasts_max.append(0)

            # Agregar columnas
            gdf['temperature'] = temperatures
            gdf['melt_rate'] = melt_rates
            gdf['forecast_max'] = forecasts_max
            gdf['volume_loss_percent'] = gdf['temperature'].apply(lambda t: max(0, min(100, t * 3)))
            gdf['velocity'] = gdf['temperature'].apply(lambda t: max(0, t * 40))

            logger.info(f"Datos de {len(gdf)} glaciares enriquecidos con temperatura")
            return gdf

        except Exception as e:
            logger.error(f"Error enriqueciendo datos: {e}")
            return gdf

    @classmethod
    def get_glacier_data_with_temperature(cls, glacier_name: str) -> Dict:
        """
        Obtiene datos completos de un glaciar incluyendo temperatura real

        Args:
            glacier_name: Nombre del glaciar

        Returns:
            Diccionario con datos del glaciar y temperatura
        """
        glacier_info = cls.GLACIARES_COORDS.get(glacier_name, {})

        if not glacier_info:
            logger.warning(f"Glaciar {glacier_name} no encontrado")
            return None

        temp_data = cls.get_temperature_data(glacier_info["lat"], glacier_info["lon"])
        forecast_data = cls.get_forecast_data(glacier_info["lat"], glacier_info["lon"])

        # Calcular parámetros de deshielo basado en temperatura
        current_temp = temp_data.get("current_temperature", 0) or 0
        melt_rate = max(0, current_temp * 2.5)  # Factor de conversión estimado

        return {
            "name": glacier_name,
            "region": glacier_info.get("region", ""),
            "lat": glacier_info["lat"],
            "lon": glacier_info["lon"],
            "temperature": float(current_temp),
            "avg_temperature": float(temp_data.get("average_temperature", 0) or 0),
            "max_temperature": float(temp_data.get("max_temperature", 0) or 0),
            "min_temperature": float(temp_data.get("min_temperature", 0) or 0),
            "temperature_trend": float(temp_data.get("temperature_trend", 0) or 0),
            "melt_rate": float(melt_rate),
            "forecast_max": float(forecast_data.get("forecast_max", 0) or 0),
            "forecast_min": float(forecast_data.get("forecast_min", 0) or 0),
            "data_source": "OpenMeteo + Local Data",
            "timestamp": datetime.now().isoformat(),
        }

    @classmethod
    def get_all_glaciers_with_data(cls) -> pd.DataFrame:
        """
        Obtiene datos de todos los glaciares principales

        Returns:
            DataFrame con datos de todos los glaciares
        """
        all_data = []

        for glacier_name in cls.GLACIARES_COORDS.keys():
            try:
                glacier_data = cls.get_glacier_data_with_temperature(glacier_name)
                if glacier_data:
                    all_data.append(glacier_data)
            except Exception as e:
                logger.error(f"Error obteniendo datos de {glacier_name}: {e}")

        df = pd.DataFrame(all_data)

        # Agregar parámetros derivados
        if not df.empty:
            df["volume_loss_percent"] = df["temperature"].apply(lambda t: max(0, min(100, t * 3)))
            df["velocity"] = df["temperature"].apply(lambda t: max(0, t * 40))

        return df

    @classmethod
    def get_all_glaciers_from_geospatial(cls) -> Optional[pd.DataFrame]:
        """
        Obtiene TODOS los glaciares desde archivos geoespaciales RÁPIDAMENTE
        Divide Aysén en 31 zonas y enriquece cada zona con clima real
        
        Returns:
            DataFrame con TODOS los glaciares + temperatura por zona
        """
        try:
            logger.info("🏔️ Cargando glaciares por zonas...")
            
            # Cargar datos geoespaciales
            gdf = cls.load_all_glacier_data()

            if gdf is None or gdf.empty:
                logger.warning("❌ No se encontraron datos geoespaciales")
                return None

            total_glaciares = len(gdf)
            logger.info(f"✅ {total_glaciares} glaciares cargados")
            
            # Copiar para no modificar original
            gdf = gdf.copy()

            # Convertir a CRS proyectado para calcular centroide correctamente
            gdf_projected = gdf.to_crs(epsg=3857)
            gdf['centroid'] = gdf_projected.geometry.centroid
            
            # Convertir centroides de vuelta a WGS84
            centroid_gdf = gpd.GeoDataFrame(geometry=gdf['centroid'], crs='EPSG:3857')
            centroid_gdf = centroid_gdf.to_crs(epsg=4326)
            
            gdf['lat'] = centroid_gdf.geometry.y.values
            gdf['lon'] = centroid_gdf.geometry.x.values
            
            # Obtener temperaturas por zona (caché)
            logger.info("🌡️  Cargando clima por zonas...")
            zone_temps = {}
            for zone in cls.AYSEN_ZONES:
                zone_id = zone['zone_id']
                temp_data = cls.get_zone_temperature(
                    zone_id, 
                    zone['lat_center'], 
                    zone['lon_center']
                )
                zone_temps[zone_id] = temp_data.get('temperature', 0)
            
            logger.info(f"✅ Clima cargado para {len(zone_temps)} zonas")

            # Asignar zona a cada glaciar basado en distancia al centro más cercano
            def get_closest_zone(lat, lon):
                min_dist = float('inf')
                closest_zone_id = 'Z00'
                
                for zone in cls.AYSEN_ZONES:
                    dist = ((lat - zone['lat_center'])**2 + (lon - zone['lon_center'])**2)**0.5
                    if dist < min_dist:
                        min_dist = dist
                        closest_zone_id = zone['zone_id']
                
                return closest_zone_id
            
            logger.info("🗺️  Asignando glaciares a zonas...")
            gdf['zone_id'] = gdf.apply(lambda row: get_closest_zone(row['lat'], row['lon']), axis=1)
            gdf['zone_temperature'] = gdf['zone_id'].map(zone_temps).fillna(0)
            
            # Extraer atributos importantes
            result_data = []

            for idx, row in gdf.iterrows():
                try:
                    # Obtener nombre del glaciar
                    glacier_name = None
                    for col in ['NOMBRE_GLAC', 'NOMBRE_GLACIAR', 'NAME', 'nombre', 'name', 'NOMBRE', 'Nombre']:
                        if col in gdf.columns and pd.notna(row[col]):
                            glacier_name = str(row[col]).strip()
                            if glacier_name and glacier_name not in ['', 'S/N', 'Sin Nombre']:
                                break

                    if not glacier_name:
                        glacier_name = f"Glaciar_{idx}"

                    # Obtener región
                    region = "Aysén"
                    for col in ['REGION', 'region', 'PROVINCIA', 'Region']:
                        if col in gdf.columns and pd.notna(row[col]):
                            region = str(row[col])
                            break

                    # Coordenadas
                    lat = float(row['lat']) if pd.notna(row['lat']) else 0
                    lon = float(row['lon']) if pd.notna(row['lon']) else 0
                    
                    # Temperatura de la zona
                    temperature = float(row['zone_temperature']) or 0

                    result_data.append({
                        'name': glacier_name,
                        'region': region,
                        'lat': lat,
                        'lon': lon,
                        'temperature': temperature,
                        'melt_rate': max(0, temperature * 2.5),
                        'volume_loss_percent': max(0, min(100, temperature * 3)),
                        'velocity': max(0, temperature * 40),
                        'forecast_max': temperature + 2,
                        'zone_id': row['zone_id'],
                        'data_source': 'Geospatial + OpenMeteo (Zonas)',
                        'timestamp': datetime.now().isoformat(),
                        'id': int(idx)
                    })

                except Exception as e:
                    logger.warning(f"Error procesando glaciar {idx}: {e}")
                    continue

            if result_data:
                logger.info(f"✅ Retornando {len(result_data)} glaciares enriquecidos por zona")
                return pd.DataFrame(result_data)

            return None

        except Exception as e:
            logger.error(f"❌ Error obteniendo glaciares geoespaciales: {e}")
            return None

    @classmethod
    def get_temperature_time_series(cls, glacier_name: str, days: int = 30) -> pd.DataFrame:
        """
        Obtiene serie de tiempo de temperatura para un glaciar

        Args:
            glacier_name: Nombre del glaciar
            days: Número de días

        Returns:
            DataFrame con serie de tiempo
        """
        glacier_info = cls.GLACIARES_COORDS.get(glacier_name, {})

        if not glacier_info:
            return pd.DataFrame()

        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            params = {
                "latitude": glacier_info["lat"],
                "longitude": glacier_info["lon"],
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
                "timezone": "America/Santiago",
            }

            response = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "daily" in data:
                df = pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(data["daily"]["time"]),
                        "temperature": data["daily"]["temperature_2m_mean"],
                        "temperature_max": data["daily"]["temperature_2m_max"],
                        "temperature_min": data["daily"]["temperature_2m_min"],
                    }
                )

                # Calcular parámetros derivados
                df["melt_rate"] = df["temperature"].apply(lambda t: max(0, t * 2.5))
                df["volume_loss_percent"] = df["temperature"].apply(lambda t: max(0, min(100, t * 3)))
                df["velocity"] = df["temperature"].apply(lambda t: max(0, t * 40))

                return df

        except Exception as e:
            logger.error(f"Error obteniendo serie de tiempo: {e}")

        return pd.DataFrame()
