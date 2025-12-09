"""
EVAQUA - Sistema de Evaluación de Escorrentía Glaciar y Riesgo de Inundación
Aysén, Chile

Módulo principal que integra:
- Topografía (OpenTopography)
- Clima (Open-Meteo)
- Derretimiento glaciar (Modelo Grado-Día)
- Escorrentía (Modelo Racional)
- Riesgo de desborde
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import streamlit as st
from pathlib import Path
import logging
import io
from datetime import datetime, timedelta
import concurrent.futures
import requests
from shapely.geometry import box, Point
import warnings
from . import config

@st.cache_data(ttl=3600)
def load_shapefile_cached(path):
    """Carga shapefile con caché"""
    return gpd.read_file(path).to_crs(epsg=4326)

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

# ==================== CONSTANTES ====================

# Factor Grado-Día (mm/día/°C)
DEGREE_DAY_FACTOR_ICE = 8.0  # Hielo es más sensible
DEGREE_DAY_FACTOR_SNOW = 4.0  # Nieve menos sensible
BASE_TEMP_THRESHOLD = 0.0  # Temperatura base para derretimiento

# Coeficientes de escorrentía según pendiente
RUNOFF_COEFFICIENTS = {
    'muy_baja': 0.20,    # Pendiente < 5%
    'baja': 0.35,         # Pendiente 5-10%
    'media': 0.50,        # Pendiente 10-20%
    'alta': 0.70,         # Pendiente > 20%
}

# Pesos para riesgo de desborde
RISK_WEIGHTS = {
    'melt': 0.40,         # Derretimiento
    'runoff': 0.40,       # Escorrentía
    'precip_72h': 0.20    # Precipitación acumulada 72h
}

# Clasificación de riesgo
RISK_LEVELS = {
    'bajo': (0.0, 0.25, '#2ecc71', 'Bajo'),          # Verde
    'medio': (0.25, 0.50, '#f39c12', 'Medio'),        # Amarillo
    'alto': (0.50, 0.75, '#e67e22', 'Alto'),          # Naranja
    'critico': (0.75, 1.0, '#e74c3c', 'Crítico')      # Rojo
}


class EVAQUACalculator:
    """
    Calculador central de EVAQUA
    
    Responsabilidades:
    1. Cargar y preparar datos geoespaciales
    2. Obtener topografía de OpenTopography
    3. Obtener clima de Open-Meteo
    4. Calcular derretimiento, escorrentía y riesgo
    5. Mantener resultados en cache
    """
    
    def __init__(self):
        self.glaciers_gdf = None
        self.grids_gdf = None
        self.aysen_gdf = None  # Polígono de la región de Aysén
        self.aysen_boundary = None  # Union de polígonos de Aysén
        self.cuencas_gdf = None  # Cuencas hidrográficas
        self.subcuencas_gdf = None  # Subcuencas
        self.results_gdf = None
        self.climate_data = None
        self.topo_data = None
        self.last_update = None
        logger.info("✅ EVAQUA inicializado")
    
    # ==================== PASO 1: CARGAR DATOS BASE ====================
    
    def load_base_data(self, glaciers_shapefile: str, regions_shapefile: str,
                       cuencas_file: str = None, subcuencas_file: str = None):
        """
        Carga y prepara datos base.
        
        Paso 1: Cargar shapefiles (glaciares, región, cuencas, subcuencas)
        Paso 2: Convertir a WGS84 (EPSG:4326)
        Paso 3: Intersectar glaciares con cuadrículas y subcuencas
        """
        logger.info("🔄 Cargando datos base...")
        
        try:
            # Cargar glaciares
            self.glaciers_gdf = load_shapefile_cached(glaciers_shapefile)
            # self.glaciers_gdf = self.glaciers_gdf.to_crs(epsg=4326) # Ya se hace en el loader
            logger.info(f"✅ {len(self.glaciers_gdf)} glaciares cargados")
            
            # Cargar shapefile regional
            base_gdf = load_shapefile_cached(regions_shapefile)
            # base_gdf = base_gdf.to_crs(epsg=4326)
            
            # Filtrar SOLO la región de Aysén
            # Filtrar SOLO la región de Aysén
            logger.info("🗺️ Filtrando región de Aysén...")
            if 'NOM_REGION' in base_gdf.columns:
                aysen_gdf = base_gdf[base_gdf['NOM_REGION'].str.contains('Ays', case=False, na=False)]
                if len(aysen_gdf) == 0:
                    logger.warning("⚠️ No se encontró 'Aysén' en NOM_REGION. Usando todo el shapefile.")
                    aysen_gdf = base_gdf
                else:
                    logger.info(f"✅ Región Aysén encontrada ({len(aysen_gdf)} polígonos)")
            else:
                logger.warning("⚠️ Columna NOM_REGION no existe. Usando todo el shapefile.")
                aysen_gdf = base_gdf
            
            # Definir límite de Chile (Aysén)
            chile_boundary = aysen_gdf.unary_union
            
            # Guardar región de Aysén para el mapa
            self.aysen_gdf = aysen_gdf
            self.aysen_boundary = chile_boundary
            
            # 1. Cargar cuencas y subcuencas PRIMERO (necesarias para HRUs)
            if cuencas_file:
                logger.info("🌊 Cargando cuencas hidrográficas...")
                try:
                    self.cuencas_gdf = load_shapefile_cached(cuencas_file)
                    # self.cuencas_gdf = self.cuencas_gdf.to_crs(epsg=4326)
                    logger.info(f"✅ {len(self.cuencas_gdf)} cuencas cargadas")
                except Exception as e:
                    logger.error(f"❌ Error loading cuencas: {e}")
            
            if subcuencas_file:
                logger.info("💧 Cargando subcuencas...")
                try:
                    self.subcuencas_gdf = load_shapefile_cached(subcuencas_file)
                    # self.subcuencas_gdf = self.subcuencas_gdf.to_crs(epsg=4326)
                    logger.info(f"✅ {len(self.subcuencas_gdf)} subcuencas cargadas")
                except Exception as e:
                    logger.error(f"❌ Error loading subcuencas: {e}")

            # 2. GENERAR HRUs desde subcuencas
            logger.info("🏔️ Generando HRUs desde subcuencas...")
            
            # Filtrar glaciares dentro de Aysén primero
            logger.info("🧊 Filtrando glaciares dentro de región de Aysén...")
            
            # Simplificar geometría de la región para acelerar
            chile_boundary = aysen_gdf.simplify(0.01).unary_union
            
            glaciers_in_aysen = self.glaciers_gdf[self.glaciers_gdf.intersects(chile_boundary)]
            logger.info(f"✅ {len(glaciers_in_aysen)} glaciares en región de Aysén")
            
            # Importar y usar HRU Generator
            from src.hru_generator import HRUGenerator
            
            # Generar HRUs usando subcuencas (ahora sí están cargadas)
            if self.subcuencas_gdf is not None and not self.subcuencas_gdf.empty:
                # Intersectar subcuencas con territorio chileno (Aysén)
                logger.info("🗺️ Filtrando subcuencas a territorio chileno (Optimizado)...")
                subcuencas_chile = self.subcuencas_gdf.copy()
                
                # Solo mantener las que intersectan, SIN recortar geometría costosa -> AHORA SÍ RECORTAR
                # Usamos intersection para cortar lo que se salga (ej. al mar)
                logger.info("✂️ Recortando subcuencas al límite regional exacto...")
                
                # Alinear CRS
                if subcuencas_chile.crs != aysen_gdf.crs:
                    subcuencas_chile = subcuencas_chile.to_crs(aysen_gdf.crs)
                
                # Overlay intersección (gpd.clip o overlay)
                # overlay es más robusto para conservar atributos que intersect directo
                subcuencas_chile = gpd.overlay(subcuencas_chile, aysen_gdf[['geometry']], how='intersection')
                
                # Eliminar geometrías vacías
                subcuencas_chile = subcuencas_chile[~subcuencas_chile.geometry.is_empty]
                logger.info(f"✅ {len(subcuencas_chile)} subcuencas recortadas en territorio chileno")
                
                # Generar HRUs con criterios hidrológicos (Static Method)
                self.grids_gdf = HRUGenerator.generate_hrus(
                    subcuencas_chile, 
                    glaciers_in_aysen,
                    small_km2=50,
                    medium_km2=200,
                    glacier_density_threshold=5
                )
                
                # Asignar glaciares a HRUs (Static Method)
                self.grids_gdf = HRUGenerator.assign_glaciers_to_hrus(self.grids_gdf, glaciers_in_aysen)
                
                # Renombrar columna hru_id a grid_id para compatibilidad
                if 'hru_id' in self.grids_gdf.columns:
                    self.grids_gdf['grid_id'] = self.grids_gdf['hru_id']
                
                logger.info(f"✅ {len(self.grids_gdf)} HRUs generados (sistema hidrológico)")
            else:
                logger.warning("⚠️ No hay subcuencas cargadas, generando cuadrículas como fallback...")
                # Fallback a cuadrículas si no hay subcuencas
                self.grids_gdf = self._generate_grid_from_bounds(aysen_gdf.total_bounds, n_cells=30)
                self.grids_gdf['grid_id'] = range(len(self.grids_gdf))
                logger.info(f"✅ {len(self.grids_gdf)} cuadrículas generadas (fallback)")
            
            # Actualizar glaciers_gdf con solo los de Aysén
            self.glaciers_gdf = glaciers_in_aysen
            
            # 3. Intersectar glaciares si es necesario
            # Si se usó HRU Generator, self.grids_gdf ya tiene los HRUs
            # Pero self.glaciers_gdf NO tiene necesariamente la columna grid_id
            # Necesitamos asignar a cada glaciar su grid_id (HRU) correspondiente
            
            if 'grid_id' not in self.glaciers_gdf.columns:
                 logger.info("📊 Asignando HRU IDs (grid_id) a glaciares...")
                 
                  # Spatial Join para asignar grid_id a cada glaciar
                 glaciers_with_grid = gpd.sjoin(self.glaciers_gdf, self.grids_gdf[['geometry', 'grid_id']], how='inner', predicate='intersects')
                 
                 # Limpiar columnas duplicadas
                 if 'index_right' in glaciers_with_grid.columns:
                     glaciers_with_grid = glaciers_with_grid.drop(columns=['index_right'])

                 # --- CÁLCULO DE ÁREA EXACTA (INTERSECCIÓN) ---
                 # 1. Traer geometría del grid
                 grids_geom = self.grids_gdf[['grid_id', 'geometry']].rename(columns={'geometry': 'grid_geom'})
                 glaciers_merged = glaciers_with_grid.merge(grids_geom, on='grid_id', how='left')
                 
                 # 2. Calcular intersección real (para no sobreestimar glaciares grandes)
                 # Usar make_valid para evitar errores topológicos
                 logger.info("📐 Calculando áreas exactas de intersección (esto puede tomar un momento)...")
                 
                 # Proyectar para cálculo de área (EPSG:32718)
                 gdf_proj = glaciers_merged.to_crs(epsg=32718)
                 
                 # Calcular intersección elemento a elemento
                 # intersection() en geopandas alinea por índice, pero aquí están en las mismas filas
                 # Usamos apply para seguridad o alineación directa
                 # Optimización vectorizada:
                 # buffer(0) repara geometrías inválidas (self-intersections)
                 glacier_geoms = gdf_proj.geometry.buffer(0)
                 # IMPORTANTE: Usar CRS de grids_gdf, no de glaciers_gdf
                 grid_crs = self.grids_gdf.crs if self.grids_gdf.crs else "EPSG:4326"
                 grid_geoms = gpd.GeoSeries(gdf_proj['grid_geom'], crs=grid_crs).to_crs(epsg=32718).buffer(0)
                 
                 intersection_geoms = glacier_geoms.intersection(grid_geoms)
                 
                 # 3. Asignar área en m2
                 areas = intersection_geoms.area
                 glaciers_with_grid['area_in_grid'] = areas
                 
                 # LOGGING DE DEBUG
                 non_zero_areas = areas[areas > 0]
                 logger.info(f"🔍 DEBUG: {len(non_zero_areas)} intersecciones con área > 0")
                 if not non_zero_areas.empty:
                     logger.info(f"🔍 DEBUG: Área promedio: {non_zero_areas.mean():.2f} m2, Max: {non_zero_areas.max():.2f} m2")
                 else:
                     logger.warning("⚠️ DEBUG: TODAS LAS ÁREAS SON 0 - Revise proyección")
                     
                 # Verificar tipo de grid_id
                 logger.info(f"🔍 DEBUG: Tipo de grid_id en glaciares: {glaciers_with_grid['grid_id'].dtype}")

                 self.glaciers_gdf = glaciers_with_grid
                 
                 # --- AGREGACION INMEDIATA ---
                 # Calcular suma por HRU ahora mismo para evitar problemas después
                 glacier_areas_sum = self.glaciers_gdf.groupby('grid_id')['area_in_grid'].sum().reset_index()
                 glacier_areas_sum.rename(columns={'area_in_grid': 'glacier_area_m2'}, inplace=True)
                 glacier_areas_sum['glacier_area_km2'] = glacier_areas_sum['glacier_area_m2'] / 1e6
                 
                 # Unir a grids_gdf
                 if 'glacier_area_km2' in self.grids_gdf.columns:
                     self.grids_gdf = self.grids_gdf.drop(columns=['glacier_area_km2'])
                     
                 self.grids_gdf = self.grids_gdf.merge(glacier_areas_sum[['grid_id', 'glacier_area_km2']], on='grid_id', how='left')
                 self.grids_gdf['glacier_area_km2'] = self.grids_gdf['glacier_area_km2'].fillna(0)
                 
                 logger.info(f"✅ Áreas agregadas a grids_gdf. Total área glaciar: {self.grids_gdf['glacier_area_km2'].sum():.2f} km2")

                 logger.info(f"✅ {len(self.glaciers_gdf)} fragmentos de glaciares procesados con área exacta")
            else:
                 logger.info("✅ Glaciares ya tienen asignado grid_id")
            
            return True
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            return False
    
    def _generate_grid_from_bounds(self, bounds, n_cells=30):
        """Genera una cuadrícula de n_cells sobre los bounds dados"""
        minx, miny, maxx, maxy = bounds
        
        # Calcular filas y columnas (aprox raiz cuadrada)
        ratio = (maxx - minx) / (maxy - miny)
        cols = int(np.sqrt(n_cells * ratio))
        rows = int(n_cells / cols)
        
        # Ajustar paso
        x_step = (maxx - minx) / cols
        y_step = (maxy - miny) / rows
        
        grids = []
        for i in range(cols):
            for j in range(rows):
                x1 = minx + i * x_step
                x2 = minx + (i + 1) * x_step
                y1 = miny + j * y_step
                y2 = miny + (j + 1) * y_step
                
                poly = box(x1, y1, x2, y2)
                grids.append({'geometry': poly, 'grid_id': i * rows + j})
                
        return gpd.GeoDataFrame(grids, crs="EPSG:4326")

    
    def _intersect_glaciers_with_watersheds(self):
        """
        Asigna cada glaciar (o fragmento) a su subcuenca correspondiente.
        Agrega columnas: COD_SUBC, NOM_SUBC, COD_CUEN, area_subcuenca_km2
        """
        logger.info("💧 Asignando glaciares a subcuencas...")
        
        if self.subcuencas_gdf is None or self.subcuencas_gdf.empty:
            logger.warning("⚠️ No hay datos de subcuencas disponibles")
            return
        
        # Spatial join - asignar la subcuenca que contiene cada glaciar
        glaciers_with_watershed = gpd.sjoin(
            self.glaciers_gdf,
            self.subcuencas_gdf[['COD_CUEN', 'COD_SUBC', 'NOM_SUBC', 'Shape_Area', 'geometry']],
            how='left',
            predicate='intersects'
        )
        
        # Eliminar columna de índice del join
        if 'index_right' in glaciers_with_watershed.columns:
            glaciers_with_watershed = glaciers_with_watershed.drop(columns=['index_right'])
        
        # Renombrar Shape_Area a area_subcuenca_km2
        if 'Shape_Area' in glaciers_with_watershed.columns:
            glaciers_with_watershed['area_subcuenca_km2'] = glaciers_with_watershed['Shape_Area'] * 12365
            glaciers_with_watershed = glaciers_with_watershed.drop(columns=['Shape_Area'])
        
        # Actualizar glaciers_gdf
        self.glaciers_gdf = glaciers_with_watershed
        
        # Estadísticas
        glaciers_con_subcuenca = glaciers_with_watershed[glaciers_with_watershed['COD_SUBC'].notna()]
        logger.info(f"✅ {len(glaciers_con_subcuenca)} glaciares asignados a subcuencas")
        if len(glaciers_with_watershed) > len(glaciers_con_subcuenca):
            logger.info(f"   {len(glaciers_with_watershed) - len(glaciers_con_subcuenca)} glaciares sin subcuenca")

    def _intersect_glaciers_with_grids(self):
        """Intersecta glaciares con cuadrículas y calcula áreas usando overlay"""
        logger.info("🔄 Intersectando glaciares con cuadrículas...")
        
        # Los glaciares ya fueron filtrados a Aysén en load_base_data
        # Asegurar CRS
        if self.glaciers_gdf.crs != self.grids_gdf.crs:
            self.grids_gdf = self.grids_gdf.to_crs(self.glaciers_gdf.crs)
            
        # Usar overlay para intersección real (corta los polígonos)
        # Esto crea nuevas filas para cada pedazo de glaciar en cada celda
        try:
            # Strategía para ID estable
            cid = None
            for candidate in ['COMUNA', 'COD_COMUNA', 'ID', 'OBJECTID', 'CODIGO']:
                if candidate in self.grids_gdf.columns:
                    cid = candidate
                    break
            
            if cid:
                logger.info(f"✅ Usando columna '{cid}' como grid_id estable")
                self.grids_gdf['grid_id'] = self.grids_gdf[cid]
            elif 'grid_id' not in self.grids_gdf.columns:
                logger.warning("⚠️ Sin columna ID estable. Usando índice secuencial (frágil)")
                self.grids_gdf['grid_id'] = range(1, len(self.grids_gdf) + 1)

            # Mantener columnas relevantes
            cols_glaciers = [c for c in self.glaciers_gdf.columns if c not in ['geometry', 'grid_id']]
            cols_grids = ['grid_id', 'geometry']
            
            # Intersection
            self.glaciers_gdf = gpd.overlay(
                self.glaciers_gdf, 
                self.grids_gdf[['grid_id', 'geometry']], 
                how='intersection'
            )
            
            # Calcular nueva área por pedazo
            # Para lat/lon, el área sale en grados cuadrados (mal).
            # Debemos proyectar temporalmente para calcular área.
            # Usar proyección equivalente de área (ej. Cylindrical Equal Area o UTM local)
            # Para Chile/Aysén UTM 18S (EPSG:32718) o 19S (EPSG:32719). 
            # Usamos 32718 (Zona 18S) promedio.
            
            gdf_proj = self.glaciers_gdf.to_crs(epsg=32718)
            self.glaciers_gdf['area_in_grid'] = gdf_proj.geometry.area
            
            logger.info(f"✅ Glaciares intersectados: {len(self.glaciers_gdf)} fragmentos creados")
            
        except Exception as e:
            logger.error(f"❌ Error en intersección: {e}")
            # Fallback simple (centroide)
            pass
    
    # ==================== PASO 2: TOPOGRAFÍA ====================
    
    def get_topography_for_grids(self):
        """
        Obtiene topografía (OpenTopoData) para todas las cuadrículas en BATCH.
        Evita saturar la API con cientos de requests paralelos.
        """
        logger.info("🏔️  Obteniendo topografía (Batch Mode)...")
        
        topo_data = []
        grid_specs = []
        all_points = []
        
        # 1. Preparar todos los puntos de muestreo
        for idx, row in self.grids_gdf.iterrows():
            grid_id = row['grid_id'] if 'grid_id' in row else idx
            bounds = row.geometry.bounds
            
            # Muestreo reducido a 1 punto (centroide) para velocidad máxima
            points_list, shape = self._generate_sample_points(bounds, n=1)
            
            start_idx = len(all_points)
            all_points.extend(points_list)
            
            grid_specs.append({
                'grid_id': grid_id,
                'bounds': bounds,
                'shape': shape,
                'start_idx': start_idx,
                'count': len(points_list)
            })
            
        logger.info(f"📍 Total puntos a consultar: {len(all_points)}")
        
        # 2. Consultar elevaciones en batches de 100 puntos
        all_elevations = self._fetch_elevation_batch_optimized(all_points)
        
        # 3. Procesar resultados por grid
        for spec in grid_specs:
            start = spec['start_idx']
            end = start + spec['count']
            grid_elevs = all_elevations[start:end]
            
            # Reconstruir matriz
            if len(grid_elevs) == spec['count']:
                try:
                    elevation_matrix = np.array(grid_elevs).reshape(spec['shape'])
                    topo_info = self._calculate_topography_metrics(elevation_matrix, spec['bounds'])
                    
                    if topo_info:
                        topo_info['grid_id'] = spec['grid_id']
                        topo_data.append(topo_info)
                except Exception as e:
                    logger.debug(f"Error procesando grid {spec['grid_id']}: {e}")
        
        self.topo_data = pd.DataFrame(topo_data)
        logger.info(f"✅ Topografía obtenida para {len(topo_data)} cuadrículas")
        return self.topo_data
    
    def _generate_sample_points(self, bounds, n=5):
        """Genera una grilla de puntos de muestreo"""
        minx, miny, maxx, maxy = bounds
        
        # Optimización n=1: solo centroide
        if n == 1:
            return [{"latitude": (miny+maxy)/2, "longitude": (minx+maxx)/2}], (1, 1)
            
        lats = np.linspace(miny, maxy, n)
        lons = np.linspace(minx, maxx, n)
        points = []
        for lat in lats:
            for lon in lons:
                points.append({"latitude": lat, "longitude": lon})
        return points, (n, n)

    @staticmethod
    @st.cache_data(ttl=3600)
    def _fetch_elevation_batch_optimized(points):
        """
        Consulta OpenTopoData en batches secuenciales.
        Rate limit friendly: 1 request / sec approx.
        """
        elevations = []
        batch_size = 100 # Límite de la API
        
        import time
        
        total_batches = (len(points) + batch_size - 1) // batch_size
        
        for i in range(0, len(points), batch_size):
            batch = points[i:i+batch_size]
            locations = "|".join([f"{p['latitude']},{p['longitude']}" for p in batch])
            
            try:
                url = "https://api.opentopodata.org/v1/aster30m"
                params = {"locations": locations}
                
                response = requests.get(url, params=params, timeout=20)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "OK":
                        results = data.get("results", [])
                        batch_elevs = [r.get("elevation", 0) for r in results]
                        # Handling nulls
                        batch_elevs = [e if e is not None else 0 for e in batch_elevs]
                        elevations.extend(batch_elevs)
                    else:
                        elevations.extend([0] * len(batch))
                else:
                    logger.warning(f"⚠️ OpenTopoData Error {response.status_code}")
                    elevations.extend([0] * len(batch))
                    
                # Rate limiting simple
                time.sleep(0.1)
                
                # Feedback visual en consola
                print(f"  > Batch topo {i//batch_size + 1} completado")
                
            except Exception as e:
                logger.error(f"Error en batch elevación: {e}")
                elevations.extend([0] * len(batch))
                
        return elevations
    
    def _generate_synthetic_elevations(self, n):
        return list(np.random.normal(loc=1000, scale=300, size=n).clip(0, 3000))
    
    def _calculate_topography_metrics(self, elev_matrix, bounds):
        # (Sin cambios en este método)
        return super(EVAQUACalculator, self)._calculate_topography_metrics(elev_matrix, bounds) if hasattr(super(EVAQUACalculator, self), '_calculate_topography_metrics') else self._calculate_topography_metrics_impl(elev_matrix, bounds)

    def _calculate_topography_metrics_impl(self, elev_matrix, bounds):
        """Implementación local de cálculo de métricas"""
        try:
            if elev_matrix is None or elev_matrix.size == 0: return None
            elev_mean = np.mean(elev_matrix)
            elev_min = np.min(elev_matrix)
            elev_max = np.max(elev_matrix)
            
            minx, miny, maxx, maxy = bounds
            rows, cols = elev_matrix.shape
            
            # Caso n=1 o muy pequeño: no hay gradiente
            if rows < 2 or cols < 2:
                slope_mean = 0
                aspect_mean = 0
                aspect_dominant = '-'
                
                return {
                    'elevation_mean': float(elev_mean),
                    'elevation_min': float(elev_min),
                    'elevation_max': float(elev_max),
                    'slope_mean': float(slope_mean),
                    'aspect': aspect_dominant,
                    'aspect_deg': float(aspect_mean)
                }

            dy = ((maxy - miny) * 111000) / rows
            dx = ((maxx - minx) * 111000 * np.cos(np.radians(miny))) / cols
            
            if dx == 0 or dy == 0:
                slope_mean = 0
                aspect_dominant = '-'
                aspect_mean = 0
            else:
                gy, gx = np.gradient(elev_matrix, dy, dx)
                slope_rad = np.arctan(np.sqrt(gx**2 + gy**2))
                slope_mean = np.mean(np.degrees(slope_rad))
                
                aspect_rad = np.arctan2(-gy, gx)
                aspect_deg = np.degrees(aspect_rad)
                aspect_deg = np.where(aspect_deg < 0, aspect_deg + 360, aspect_deg)
                aspect_mean = np.mean(aspect_deg)
                
                directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
                aspect_idx = int(((aspect_mean + 22.5) % 360) / 45)
                aspect_dominant = directions[aspect_idx]
            
            return {
                'elevation_mean': float(elev_mean),
                'elevation_min': float(elev_min),
                'elevation_max': float(elev_max),
                'slope_mean': float(slope_mean),
                'aspect': aspect_dominant,
                'aspect_deg': float(aspect_mean)
            }
        except: return None

    # ==================== PASO 3: CLIMA ====================
    
    def get_climate_data(self):
        """
        Obtiene datos climáticos de Open-Meteo en BATCH.
        """
        logger.info("🌡️  Obteniendo clima (Batch Mode)...")
        
        climate_data = []
        lats = []
        lons = []
        grid_ids = []
        
        for idx, row in self.grids_gdf.iterrows():
            centroid = row.geometry.centroid
            lats.append(centroid.y)
            lons.append(centroid.x)
            grid_ids.append(row['grid_id'] if 'grid_id' in row else idx)
            
        # Batching: Open-Meteo soporta muchos, pero vamos de 50 en 50
        batch_size = 50
        
        for i in range(0, len(lats), batch_size):
            batch_lats = lats[i:i+batch_size]
            batch_lons = lons[i:i+batch_size]
            batch_ids = grid_ids[i:i+batch_size]
            
            batch_results = self._fetch_openmeteo_batch(batch_lats, batch_lons)
            
            for j, result in enumerate(batch_results):
                if result:
                    result['grid_id'] = batch_ids[j]
                    climate_data.append(result)
        
        self.climate_data = pd.DataFrame(climate_data)
        logger.info(f"✅ Clima obtenido para {len(climate_data)} cuadrículas")
        return self.climate_data
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def _fetch_openmeteo_batch(lats, lons):
        """
        Consulta Open-Meteo para múltiples localizaciones.
        """
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': ",".join(map(str, lats)),
                'longitude': ",".join(map(str, lons)),
                'hourly': 'temperature_2m,precipitation,rain,snowfall,shortwave_radiation,wind_speed_10m',
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,snowfall_sum',
                'timezone': 'UTC',
                'forecast_days': 7
            }
            
            # logger.info(f"OpenMeteo Request: {len(lats)} locations")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ OpenMeteo API Error: {response.text}")
                
            response.raise_for_status()
            data = response.json()
            
            # Response es una lista de objetos si hay múltiples locaciones
            # O un solo objeto si hay una sola locación
            if not isinstance(data, list):
                data = [data]
                
            results = []
            for d in data:
                results.append(EVAQUACalculator._process_meteo_json(d))
                
            return results
            
        except Exception as e:
            logger.error(f"⚠️ Error batch Open-Meteo: {e}")
            # Retornar vacíos
            return [EVAQUACalculator._process_meteo_json({}) for _ in lats]

    @staticmethod
    def _process_meteo_json(data):
        """Procesa un JSON de respuesta de Open-Meteo a nuestro formato"""
        try:
            hourly = data.get('hourly', {})
            daily = data.get('daily', {})
            
            if not hourly: return EVAQUACalculator._empty_climate_dict()

            temps_hourly = hourly.get('temperature_2m', [])
            temps_daily = daily.get('temperature_2m_max', [])
            precip_daily = daily.get('precipitation_sum', [])
            snow_daily = daily.get('snowfall_sum', [])
            rain_daily = daily.get('rain_sum', [])
            radiation_hourly = hourly.get('shortwave_radiation', [])
            wind_hourly = hourly.get('wind_speed_10m', [])
            
            return {
                'temp_current': temps_hourly[0] if temps_hourly else 0,
                'temp_avg_today': np.mean(temps_daily) if temps_daily else 0,
                'temp_max_today': np.max(temps_daily) if temps_daily else 0,
                'precip_24h': np.sum(precip_daily[:1]) if precip_daily else 0,
                'precip_72h': np.sum(precip_daily[:3]) if precip_daily else 0,
                'rain_24h': np.sum(rain_daily[:1]) if rain_daily else 0,
                'snow_24h': np.sum(snow_daily[:1]) if snow_daily else 0,
                'snow_72h': np.sum(snow_daily[:3]) if snow_daily else 0,
                'radiation_current': radiation_hourly[0] if radiation_hourly else 0,
                'wind_speed_current': wind_hourly[0] if wind_hourly else 0,
                'rain_intensity_max': 0,
                'elevation': data.get('elevation', 0),
                # Series temporales para gráficos (primeras 72h)
                'temp_series': temps_hourly[:72] if temps_hourly else [],
                'precip_series': hourly.get('precipitation', [])[:72] if hourly else []
            }
        except:
             return EVAQUACalculator._empty_climate_dict()

    @staticmethod
    def _empty_climate_dict():
        return {
            'temp_current': 0, 'temp_avg_today': 0, 'temp_max_today': 0,
            'precip_24h': 0, 'precip_72h': 0, 'rain_24h': 0,
            'snow_24h': 0, 'snow_72h': 0, 'radiation_current': 0,
            'wind_speed_current': 0, 'rain_intensity_max': 0,
            'temp_series': [], 'precip_series': []
        }


    
    # ==================== PASO 4: DERRETIMIENTO ====================
    
    def calculate_melt(self):
        """
        Calcula derretimiento usando modelo Grado-Día.
        
        Formula:
        Melt = Factor_GradoDía × (Temp_actual - Base_temp) × Area_glaciar
        
        Considera:
        - Factor diferente para hielo vs nieve
        - Umbral de temperatura (0°C)
        - Área glaciar en cada cuadrícula
        """
        logger.info("❄️ Calculando derretimiento...")
        
        if self.climate_data is None or len(self.climate_data) == 0:
            logger.warning("⚠️ Datos de clima no disponibles")
            return
        
        melt_data = []
        
        # FAILSAFE: Asegurar que glaciers_gdf tiene grid_id
        if 'grid_id' not in self.glaciers_gdf.columns:
             logger.warning("⚠️ grid_id no encontrado en glaciares. Ejecutando asignación de emergencia...")
             glaciers_with_grid = gpd.sjoin(self.glaciers_gdf, self.grids_gdf[['geometry', 'grid_id']], how='inner', predicate='intersects')
             if 'index_right' in glaciers_with_grid.columns:
                 glaciers_with_grid = glaciers_with_grid.drop(columns=['index_right'])
             
             # Recalcular área
             gdf_proj = glaciers_with_grid.to_crs(epsg=32718)
             glaciers_with_grid['area_in_grid'] = gdf_proj.geometry.area
             self.glaciers_gdf = glaciers_with_grid
        
        for idx, climate in self.climate_data.iterrows():
            grid_id = climate['grid_id']
            
            # Área total de glaciar en este grid (Ya pre-calculada en load_base_data)
            # Primero intentar obtener de grids_gdf con lookup
            grid_row = self.grids_gdf[self.grids_gdf['grid_id'] == grid_id]

            if not grid_row.empty:
                total_area_km2 = grid_row.iloc[0].get('glacier_area_km2', 0)
            else:
                # Fallback
                glaciers_in_grid = self.glaciers_gdf[self.glaciers_gdf['grid_id'] == grid_id]
                total_area_m2 = glaciers_in_grid['area_in_grid'].sum()
                total_area_km2 = total_area_m2 / 1e6

            # Calcular exceso de temperatura
            # Calcular exceso de temperatura
            temp = climate['temp_current']
            
            # === CORRECCIÓN POR LAPSE RATE (V2 - SAFE) ===
            # Ajuste: -0.65°C por cada 100m de diferencia positiva (más alto = más frío)
            lapse_rate = -0.0065
            model_elev = climate.get('elevation', 0)
            hru_elev = 0
            
            # Intentar obtener elevación del HRU
            if 'elevation_mean' in grid_row:
                 hru_elev = grid_row.iloc[0]['elevation_mean']
            # Fallback a to-do data si no está en grid_row
            elif self.topo_data is not None and not self.topo_data.empty:
                 topo_match = self.topo_data[self.topo_data['grid_id'] == grid_id]
                 if not topo_match.empty:
                     hru_elev = topo_match.iloc[0]['elevation_mean']
            
            # Aplicar corrección solo si tenemos datos válidos
            if model_elev is not None and hru_elev > 0:
                diff_elev = hru_elev - model_elev
                adjustment = diff_elev * lapse_rate
                temp = temp + adjustment
                
                # logger.debug(f"Combined T: {temp:.1f} (Base: {climate['temp_current']:.1f} + Adj: {adjustment:.1f}) | Elevs: {hru_elev:.0f}/{model_elev:.0f}")

            temp_excess = max(0, temp - BASE_TEMP_THRESHOLD)
            
            # Aplicar factor grado-día (mm/día/°C * °C = mm/día)
            melt_rate = DEGREE_DAY_FACTOR_ICE * temp_excess
            
            # Si hay nieve, ajustar factor
            if climate['snow_24h'] > 0:
                melt_rate = DEGREE_DAY_FACTOR_SNOW * temp_excess
            
            melt_data.append({
                'grid_id': grid_id,
                'melt_rate_mm_day': melt_rate,
                'temp_for_melt': temp,
                'glacier_area_km2': total_area_km2,
                'snow_recent': climate['snow_24h'] > 0
            })
        
        melt_df = pd.DataFrame(melt_data)
        logger.info(f"✅ Derretimiento calculado para {len(melt_df)} cuadrículas")
        
        return melt_df
    
    # ==================== PASO 5: ESCORRENTÍA ====================
    
    def calculate_runoff(self, melt_df):
        """
        Calcula escorrentía superficial usando modelo Racional.
        
        Formula:
        Q = C × I × A
        
        Donde:
        - C: Coeficiente de escorrentía (según pendiente)
        - I: Intensidad de precipitación (mm/h)
        - A: Área de la cuadrícula
        """
        logger.info("💧 Calculando escorrentía...")
        
        runoff_data = []
        
        for idx, grid in self.grids_gdf.iterrows():
            grid_id = grid['grid_id'] if 'grid_id' in grid else idx
            
            # Obtener datos de clima y topografía para esta cuadrícula
            climate_row = self.climate_data[self.climate_data['grid_id'] == grid_id]
            
            # Acceso seguro a topografía
            topo_row = None
            if self.topo_data is not None and not self.topo_data.empty and 'grid_id' in self.topo_data.columns:
                topo_matches = self.topo_data[self.topo_data['grid_id'] == grid_id]
                if not topo_matches.empty:
                    topo_row = topo_matches
            
            if climate_row.empty:
                continue
            
            # Determinar coeficiente según pendiente
            if topo_row is not None and not topo_row.empty:
                slope = topo_row.iloc[0]['slope_mean']
                if slope < 5:
                    runoff_coeff = RUNOFF_COEFFICIENTS['muy_baja']
                elif slope < 10:
                    runoff_coeff = RUNOFF_COEFFICIENTS['baja']
                elif slope < 20:
                    runoff_coeff = RUNOFF_COEFFICIENTS['media']
                else:
                    runoff_coeff = RUNOFF_COEFFICIENTS['alta']
            else:
                runoff_coeff = RUNOFF_COEFFICIENTS['media']
            
            # Intensidad de precipitación (mm/h)
            precip_24h = climate_row.iloc[0]['precip_24h']
            intensity = precip_24h / 24 if precip_24h > 0 else 0
            
            # Área de la cuadrícula (en km²)
            # Área de la cuadrícula (en km²)
            # Usar valor real calculado en hru_generator
            if 'area_km2' in grid:
                area_km2 = grid['area_km2']
            else:
                 # Fallback: Calcular dinámicamente si no existe
                 # Asumimos que geometry está en WGS84, proyectamos rapido
                grid_gs = gpd.GeoSeries([grid.geometry], crs="EPSG:4326")
                area_km2 = grid_gs.to_crs(epsg=32719).area.iloc[0] / 1e6
            
            # Fallback seguro si area es 0 o inválida
            if area_km2 <= 0: area_km2 = 1.0
            
            # Calcular caudal (m³/s, aproximado)
            # Q = C × I × A (en unidades consistentes)
            runoff_m3s = runoff_coeff * intensity * area_km2 * 0.278  # Factor de conversión
            
            runoff_data.append({
                'grid_id': grid_id,
                'runoff_m3s': runoff_m3s,
                'runoff_coeff': runoff_coeff,
                'slope_percent': topo_row.iloc[0]['slope_mean'] if topo_row is not None else 0,
                'precip_intensity_mm_h': intensity
            })
        
        runoff_df = pd.DataFrame(runoff_data)
        logger.info(f"✅ Escorrentía calculada para {len(runoff_df)} cuadrículas")
        
        return runoff_df
    
    # ==================== PASO 6.5: PROYECCIÓN 3 DÍAS (NUEVO) ====================

    def calculate_projected_risk_3d(self, melt_df, runoff_df):
        """
        Proyecta el riesgo a 3 días asumiendo que las condiciones actuales se mantienen.
        "Si esto sigue así 3 días..."
        """
        logger.info("🔮 Calculando proyección a 3 días...")
        
        proj_data = []
        
        for idx, grid in self.grids_gdf.iterrows():
            gid = grid['grid_id'] if 'grid_id' in grid else idx
            
            # Obtener datos actuales
            melt_row = melt_df[melt_df['grid_id'] == gid]
            runoff_row = runoff_df[runoff_df['grid_id'] == gid]
            climate_row = self.climate_data[self.climate_data['grid_id'] == gid]
            
            if climate_row.empty:
                continue
                
            # Extracción de valores base (0 si no existen)
            melt_rate_day = melt_row.iloc[0]['melt_rate_mm_day'] if not melt_row.empty else 0
            runoff_m3s = runoff_row.iloc[0]['runoff_m3s'] if not runoff_row.empty else 0
            precip_24h = climate_row.iloc[0]['precip_24h']
            
            # --- CÁLCULO PROYECTADO (3 DÍAS) ---
            # Asumimos persistencia: lo de hoy se repite 3 veces
            melt_3d_mm = melt_rate_day * 3
            precip_3d_mm = precip_24h * 3 
            
            # Agua total acumulada equivalente (mm)
            # Escorrentía volumétrica total en 3 días (m3) = caudal * segundos
            runoff_total_m3 = runoff_m3s * (3 * 24 * 3600)
            
            # Recalcular score de riesgo proyectado
            # Normalizamos con umbrales más altos pues es acumulado
            melt_comp_3d = min(1.0, melt_3d_mm / 150)       # Max 150 mm en 3 días
            precip_comp_3d = min(1.0, precip_3d_mm / 300)   # Max 300 mm en 3 días
            runoff_comp_3d = min(1.0, runoff_m3s / 1200)    # Escorrentía sostenida (usamos m3s pico como proxy de impacto continuo)

            # Pesos ajustados para proyección (más peso al acumulado)
            risk_score_3d = (0.35 * melt_comp_3d) + (0.35 * runoff_comp_3d) + (0.30 * precip_comp_3d)
            
            proj_data.append({
                'grid_id': gid,
                'melt_3d_mm': melt_3d_mm,
                'precip_3d_mm': precip_3d_mm,
                'water_total_3d_mm': melt_3d_mm + precip_3d_mm,
                'risk_score_3d': risk_score_3d,
                'risk_class_3d': self._classify_risk(risk_score_3d)
            })
            
        return pd.DataFrame(proj_data)

    # ==================== PASO 6: RIESGO DE DESBORDE ====================
    
    def calculate_flood_risk(self, melt_df, runoff_df):
        """
        Calcula riesgo de desborde combinando componentes.
        AHORA CON PESOS DINÁMICOS: Si no hay glaciar, la lluvia pesa más.
        """
        logger.info("⚠️ Calculando riesgo de desborde...")
        
        risk_data = []
        
        for idx, grid in self.grids_gdf.iterrows():
            gid = grid['grid_id'] if 'grid_id' in grid else idx
            
            melt_row = melt_df[melt_df['grid_id'] == gid]
            runoff_row = runoff_df[runoff_df['grid_id'] == gid]
            climate_row = self.climate_data[self.climate_data['grid_id'] == gid]
            
            if climate_row.empty:
                continue

            # Valores
            melt_val = melt_row.iloc[0]['melt_rate_mm_day'] if not melt_row.empty else 0
            runoff_val = runoff_row.iloc[0]['runoff_m3s'] if not runoff_row.empty else 0
            precip_val = climate_row.iloc[0]['precip_72h']
            
            # --- PESOS DINÁMICOS ---
            # Si no hay derretimiento significativo, re-balancear pesos hacia lluvia
            if melt_val < 0.1:
                # Caso "Solo Lluvia"
                w_melt = 0.05
                w_runoff = 0.55
                w_precip = 0.40
            else:
                # Caso Glaciar Activo (Original)
                w_melt = 0.40
                w_runoff = 0.40
                w_precip = 0.20
            
            # Normalizar
            melt_component = min(1.0, melt_val / 50)
            runoff_component = min(1.0, runoff_val / 1000)
            precip_component = min(1.0, precip_val / 300)
            
            # Score
            risk_score = (w_melt * melt_component) + (w_runoff * runoff_component) + (w_precip * precip_component)
            
            risk_data.append({
                'grid_id': gid,
                'risk_score': risk_score,
                'risk_class': self._classify_risk(risk_score),
                'melt_component': melt_component,
                'runoff_component': runoff_component,
                'precip_component': precip_component
            })
        
        risk_df = pd.DataFrame(risk_data)
        logger.info(f"✅ Riesgo calculado para {len(risk_df)} cuadrículas")
        
        return risk_df
    
    def _classify_risk(self, score: float) -> str:
        """Clasifica el riesgo según el score"""
        for risk_level, (min_val, max_val, color, label) in RISK_LEVELS.items():
            if min_val <= score < max_val:
                return risk_level
        return 'critico'
    
    def _identify_flood_zones(self):
        """
        Identifica zonas de posible inundación dentro de cada cuadrícula.
        Basado en: elevación baja, pendiente baja, y alto riesgo de escorrentía.
        """
        logger.info("🌊 Identificando zonas de inundación potencial...")
        
        if self.results_gdf is None or self.results_gdf.empty:
            return
        
        flood_zones = []
        
        for idx, row in self.results_gdf.iterrows():
            # Criterios de zona inundable:
            # 1. Elevación por debajo del promedio de la zona
            # 2. Pendiente baja (< 5°)
            # 3. Alta escorrentía (> 100 m³/s)
            
            elev_mean = row.get('elevation_mean', 0)
            slope = row.get('slope_mean', 0)
            runoff = row.get('runoff_m3s', 0)
            risk_score = row.get('risk_score', 0)
            
            # Determinar si hay riesgo de inundación
            is_flood_prone = False
            flood_description = "Bajo riesgo"
            
            if elev_mean < 500 and slope < 5 and runoff > 100:
                is_flood_prone = True
                flood_description = "Alto riesgo - Zona baja con alta escorrentía"
            elif elev_mean < 800 and runoff > 200:
                is_flood_prone = True
                flood_description = "Riesgo medio - Elevación moderada con escorrentía significativa"
            elif risk_score > 0.5:
                is_flood_prone = True
                flood_description = "Atención - Alto índice de riesgo general"
            
            flood_zones.append({
                'grid_id': row.get('grid_id'),
                'flood_prone': is_flood_prone,
                'flood_description': flood_description
            })
        
        # Agregar columnas al results_gdf
        flood_df = pd.DataFrame(flood_zones)
        self.results_gdf = self.results_gdf.merge(flood_df, on='grid_id', how='left')
        
        flood_count = len([z for z in flood_zones if z['flood_prone']])
        logger.info(f"✅ {flood_count} zonas con riesgo de inundación identificadas")
    
    # ==================== FLUJO COMPLETO ====================
    
    def run_full_analysis(self, glaciers_shapefile: str, regions_shapefile: str,
                          cuencas_file: str = None, subcuencas_file: str = None):
        """
        Ejecuta el análisis completo de EVAQUA.
        
        Retorna GeoDataFrame con todas las métricas
        """
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO ANÁLISIS COMPLETO DE EVAQUA")
        logger.info("=" * 60)
        
        # 1. Cargar datos (incluyendo cuencas si están disponibles)
        if not self.load_base_data(glaciers_shapefile, regions_shapefile, 
                                    cuencas_file=cuencas_file, 
                                    subcuencas_file=subcuencas_file):
            return None
        
        # 2. Topografía
        self.get_topography_for_grids()
        
        # 3. Clima
        self.get_climate_data()
        
        # 4. Derretimiento
        melt_df = self.calculate_melt()
        
        # 5. Escorrentía
        runoff_df = self.calculate_runoff(melt_df)
        
        # 6. Riesgo
        risk_df = self.calculate_flood_risk(melt_df, runoff_df)
        
        # 7. Combinar resultados
        # 7. Combinar resultados
        self.results_gdf = self.grids_gdf.copy()
        
        # Merge Climate
        if self.climate_data is not None and not self.climate_data.empty:
            if 'grid_id' in self.climate_data.columns:
                self.results_gdf = self.results_gdf.merge(self.climate_data, on='grid_id', how='left')
        
        # Merge Topo
        if self.topo_data is not None and not self.topo_data.empty:
            if 'grid_id' in self.topo_data.columns:
                self.results_gdf = self.results_gdf.merge(self.topo_data, on='grid_id', how='left')
        
        # Merge Melt
        if melt_df is not None and not melt_df.empty:
            # Eliminar columnas duplicadas para evitar sufijos _x, _y
            cols_to_drop = [c for c in melt_df.columns if c in self.results_gdf.columns and c != 'grid_id']
            if cols_to_drop:
                self.results_gdf = self.results_gdf.drop(columns=cols_to_drop)
                
            self.results_gdf = self.results_gdf.merge(melt_df, on='grid_id', how='left')
        
        # Merge Runoff
        if runoff_df is not None and not runoff_df.empty:
            self.results_gdf = self.results_gdf.merge(runoff_df, on='grid_id', how='left')
        
        # Merge Risk
        if risk_df is not None and not risk_df.empty:
            self.results_gdf = self.results_gdf.merge(risk_df, on='grid_id', how='left')

        # 6.5. Proyección a 3 Días (Nuevo)
        proj_df = self.calculate_projected_risk_3d(melt_df, runoff_df)
        if proj_df is not None and not proj_df.empty:
            # Merge simple (asumimos que no hay colisiones graves aparte de grid_id)
            cols_to_drop = [c for c in proj_df.columns if c in self.results_gdf.columns and c != 'grid_id']
            if cols_to_drop:
                self.results_gdf = self.results_gdf.drop(columns=cols_to_drop)
            self.results_gdf = self.results_gdf.merge(proj_df, on='grid_id', how='left')
        
        # Calcular zonas de inundación potencial dentro de cada cuadrícula
        self._identify_flood_zones()
        
        self.last_update = datetime.now()
        
        logger.info("=" * 60)
        logger.info("✅ ANÁLISIS COMPLETADO")
        logger.info(f"⏰ Actualizado: {self.last_update}")
        logger.info("=" * 60)
        
        return self.results_gdf
    
    def get_grid_details(self, grid_id: int) -> dict:
        """Retorna detalles completos de una cuadrícula"""
        if self.results_gdf is None or self.results_gdf.empty:
            return {}
        
        grid_data = self.results_gdf[self.results_gdf['grid_id'] == grid_id]
        
        if grid_data.empty:
            return {}
        
        row = grid_data.iloc[0]
        
        return {
            'grid_id': grid_id,
            'temperature': row.get('temp_current', 0),
            'precipitation_24h': row.get('precip_24h', 0),
            'precipitation_72h': row.get('precip_72h', 0),
            'snow_24h': row.get('snow_24h', 0),
            'rain_24h': row.get('rain_24h', 0),
            'radiation': row.get('radiation_current', 0),
            'wind_speed': row.get('wind_speed_current', 0),
            'melt_rate': row.get('melt_rate_mm_day', 0),
            'runoff': row.get('runoff_m3s', 0),
            'risk_score': row.get('risk_score', 0),
            'risk_class': row.get('risk_class', 'unknown'),
            'elevation_mean': row.get('elevation_mean', 0),
            'slope_mean': row.get('slope_mean', 0),
            'aspect': row.get('aspect', '-')
        }
