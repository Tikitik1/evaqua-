"""
EVAQUA Dashboard - Versión Profesional con UI/UX Rediseñada
Estilo Google FloodHub con tema azul oscuro y Material Design
"""

import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from datetime import datetime
import logging
import os

# Imports locales
from src.evaqua import EVAQUACalculator, RISK_LEVELS

# Configuración de logging - WARNING en producción para evitar spam
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
GEMINI_API_KEY = st.secrets["GEMINI_API_KEY"]

# Paleta de colores para niveles de riesgo
RISK_COLORS = {
    'muy_bajo': '#66bb6a',    # Verde
    'bajo': '#ffeb3b',        # Amarillo
    'moderado': '#ff9800',    # Naranja
    'alto': '#f44336',        # Rojo
    'extremo': '#9c27b0'      # Púrpura
}

# ==================== FUNCIONES DE CARGA ====================

@st.cache_resource
def init_evaqua():
    """Inicializa calculador EVAQUA"""
    return EVAQUACalculator()

@st.cache_resource(show_spinner=False, ttl=3600)
def load_evaqua_analysis():
    """Carga análisis completo de EVAQUA con caché persistente y lazy loading"""
    try:
        # Mostrar progreso para mantener vivo el health check
        progress_placeholder = st.empty()
        
        progress_placeholder.info("🔄 Inicializando EVAQUA...")
        calculator = init_evaqua()
        
        import os
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, "data")
        
        glaciers_shp = os.path.join("data", "glaciares", "Glaciares_aysen-magallanes.shp")
        regions_shp = os.path.join("data", "region", "P00_RegProvCom_SIRGAS2000_fat.shp")
        cuencas_file = os.path.join("data", "cuencas", "cuencas.geojson")
        subcuencas_file = os.path.join("data", "subcuencas", "Subcuencas_Aysen_Magallanes.shp")
        
        # Paso 1: Cargar datos base
        progress_placeholder.info("📂 Cargando datos geoespaciales (1/4)...")
        calculator.load_base_data(glaciers_shp, regions_shp, cuencas_file, subcuencas_file)
        
        # Paso 2: Topografía
        progress_placeholder.info("🏔️ Obteniendo topografía (2/4)...")
        calculator.get_topography_for_grids()
        
        # Paso 3: Clima
        progress_placeholder.info("🌡️ Consultando datos climáticos (3/4)...")
        calculator.get_climate_data()
        
        # Paso 4: Cálculos finales
        progress_placeholder.info("⚙️ Calculando riesgos (4/4)...")
        
        # Derretimiento (retorna DataFrame)
        melt_df = calculator.calculate_melt()
        
        # Escorrentía (requiere melt_df)
        runoff_df = calculator.calculate_runoff(melt_df)
        
        # Riesgo (requiere melt_df y runoff_df)
        risk_df = calculator.calculate_flood_risk(melt_df, runoff_df)
        
        # Combinar resultados (igual que run_full_analysis)
        calculator.results_gdf = calculator.grids_gdf.copy()
        
        # Merge Climate
        if calculator.climate_data is not None and not calculator.climate_data.empty:
            if 'grid_id' in calculator.climate_data.columns:
                calculator.results_gdf = calculator.results_gdf.merge(calculator.climate_data, on='grid_id', how='left')
        
        # Merge Topo
        if calculator.topo_data is not None and not calculator.topo_data.empty:
            if 'grid_id' in calculator.topo_data.columns:
                calculator.results_gdf = calculator.results_gdf.merge(calculator.topo_data, on='grid_id', how='left')
        
        # Merge Melt
        if melt_df is not None and not melt_df.empty:
            cols_to_drop = [c for c in melt_df.columns if c in calculator.results_gdf.columns and c != 'grid_id']
            if cols_to_drop:
                calculator.results_gdf = calculator.results_gdf.drop(columns=cols_to_drop)
            calculator.results_gdf = calculator.results_gdf.merge(melt_df, on='grid_id', how='left')
        
        # Merge Runoff
        if runoff_df is not None and not runoff_df.empty:
            calculator.results_gdf = calculator.results_gdf.merge(runoff_df, on='grid_id', how='left')
        
        # Merge Risk
        if risk_df is not None and not risk_df.empty:
            calculator.results_gdf = calculator.results_gdf.merge(risk_df, on='grid_id', how='left')
        
        # Proyección 3D
        proj_df = calculator.calculate_projected_risk_3d(melt_df, runoff_df)
        if proj_df is not None and not proj_df.empty:
            cols_to_drop = [c for c in proj_df.columns if c in calculator.results_gdf.columns and c != 'grid_id']
            if cols_to_drop:
                calculator.results_gdf = calculator.results_gdf.drop(columns=cols_to_drop)
            calculator.results_gdf = calculator.results_gdf.merge(proj_df, on='grid_id', how='left')
        
        # Identificar zonas de inundación
        calculator._identify_flood_zones()
        
        results_gdf = calculator.results_gdf
        
        progress_placeholder.success("✅ Datos cargados exitosamente")
        
        return calculator, results_gdf
    except Exception as e:
        logger.error(f"Error loading EVAQUA analysis: {e}")
        import traceback
        logger.error(traceback.format_exc())
        st.error(f"❌ Error al cargar datos: {str(e)}")
        raise

# ==================== TEMA Y ESTILO ====================

def apply_custom_theme():
    """Aplica tema azul oscuro profesional"""
    # CSS en bloque separado para evitar problemas de renderizado
    css = """
    <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined" rel="stylesheet">
    <style>
    :root {
        --primary-bg: #0a1929;
        --secondary-bg: #132f4c;
        --accent: #3399ff;
        --text-primary: #e3f2fd;
        --text-secondary: #90caf9;
        --success: #66bb6a;
        --warning: #ffa726;
        --danger: #ef5350;
        --border: #1e3a5f;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0a1929 0%, #132f4c 100%);
        color: var(--text-primary);
    }
    

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: var(--secondary-bg);
        padding: 10px;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        color: var(--text-secondary);
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: var(--accent);
        color: white;
    }
    
    div[data-testid="stMetricValue"] {
        color: var(--accent);
        font-size: 2rem;
        font-weight: 700;
    }
    
    .stButton button {
        background-color: var(--accent);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 600;
        transition: all 0.3s;
    }
    
    .stButton button:hover {
        background-color: #2576cc;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(51, 153, 255, 0.4);
    }
    
    .white-emoji {
        filter: grayscale(100%) brightness(1000%); /* Force white */
        font-size: 1.2em;
        vertical-align: middle;
        margin-right: 8px;
        display: inline-block;
    }
    
    .info-card {
        background-color: var(--secondary-bg);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid var(--border);
        margin: 10px 0;
    }
    
    h1, h2, h3 {
        color: var(--text-primary);
        font-weight: 700;
        display: flex;
        align-items: center; /* Flex alignment for headers */
    }
    
    .stSelectbox label, .stRadio label {
        color: var(--text-primary);
    }
    </style>
    """
    
    # Usar st.markdown con unsafe_allow_html
    st.markdown(css, unsafe_allow_html=True)



# ==================== MAIN ====================

def main():
    """Aplicación principal EVAQUA - Dashboard profesional"""
    
    # Configuración de página
    st.set_page_config(
        page_title="EVAQUA - Sistema de monitoreo de inundaciones (prototipo 2.0 region de Aysen)",
        page_icon="🌊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Aplicar CSS directamente aquí
    st.markdown("""
    <style>
    .stApp {
        background: linear-gradient(135deg, #0a1929 0%, #132f4c 100%) !important;
    }
    

    .stMarkdown, .stText, p, label {
        color: #e3f2fd !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        background-color: #132f4c !important;
        padding: 10px !important;
        border-radius: 10px !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #90caf9 !important;
        border-radius: 8px !important;
        padding: 10px 20px !important;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #3399ff !important;
        color: white !important;
    }
    
    div[data-testid="stMetricValue"] {
        color: #3399ff !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }
    
    .stButton button {
        background-color: #3399ff !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 10px 24px !important;
        font-weight: 600 !important;
        border: none !important;
    }
    
    .stButton button:hover {
        background-color: #2576cc !important;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(51, 153, 255, 0.4) !important;
    }
    
    h1, h2, h3 {
        color: #e3f2fd !important;
    }

    .white-emoji {
        /* Filtro Pastel: Menos saturación, más brillo, ligeramente translúcido */
        filter: saturate(60%) brightness(130%) contrast(90%);
        font-size: 1.2em;
        vertical-align: middle;
        margin-right: 8px;
        display: inline-block;
        opacity: 0.9;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header principal
    import base64
    def get_base64_image(image_path):
        try:
            with open(image_path, "rb") as img_file:
                return base64.b64encode(img_file.read()).decode()
        except Exception:
            return ""

    logo_b64 = get_base64_image("assets/logo.png")
    logo_html = f'<img src="data:image/png;base64,{logo_b64}" style="height: 80px; margin-right: 20px; vertical-align: middle;">' if logo_b64 else ""

    st.markdown(f"""
    <div style='text-align: center; padding: 20px; background: rgba(19, 47, 76, 0.6); border-radius: 12px; margin-bottom: 20px; display: flex; flex-direction: column; align-items: center; justify-content: center;'>
        <div style='display: flex; align-items: center; justify-content: center; margin-bottom: 5px;'>
            {logo_html}
            <h1 style='margin: 0; color: #3399ff; font-size: 3rem; line-height: 1.2;'>
                EVAQUA
            </h1>
        </div>
        <p style='margin: 0; color: #90caf9; font-size: 1.1rem;'>
            Sistema de Evaluación de Riesgo de Inundación (Prototipo 2.0 - Region de Aysén)
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Cargar datos
    # Cache clear removido para evitar recarga constante
    # st.cache_resource.clear()
    
    try:
        with st.spinner("🔄 Cargando modelos hidrológicos y datos climáticos..."):
            calculator, results_gdf = load_evaqua_analysis()
        
        if results_gdf is None or results_gdf.empty:
            st.error("❌ Error al cargar datos")
            st.info("Por favor, intenta recargar la página o contacta al administrador.")
            return
        
        # Configuración por defecto de capas (sin sidebar)
        layers = {
            'hru_risk': True,
            'subwatershed': True,
            'watershed': True,
            'glaciers': True,
            'slope': False
        }

        
        # === MÓDULO ALERTA AI (GEMINI) ===
        # Insertar bloque de AI al inicio
        from src.ai_alerts import render_ai_alert_section
        
        # Contenedor destacado
        with st.container():
            render_ai_alert_section(results_gdf, GEMINI_API_KEY)
            
        st.markdown("---")
        
        # Banners de alerta tradicional
        critical_count = len(results_gdf[results_gdf.get('risk_class', 'bajo') == 'critico'])
        if critical_count > 0:
            st.markdown(f"""
            <div style="background-color: #f44336; color: white; padding: 12px; border-radius: 8px; margin-bottom: 20px; display: flex; align-items: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <span class="white-emoji" style="margin-right: 12px; font-size: 32px;">⚠️</span>
                <div>
                    <strong style="font-size: 1.1em;">ALERTA CRÍTICA</strong><br>
                    {critical_count} HRUs detectados en riesgo extremo. Revise el mapa inmediatamente.
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Tabs principales
        tab_map, tab_watershed, tab_glacier, tab_climate = st.tabs([
            "Mapa de Riesgo", 
            "Riesgo por Cuencas", 
            "Análisis Glaciar", 
            "Clima y Pronóstico"
        ])
        
        with tab_map:
            # st.markdown("## Mapa de Riesgo", unsafe_allow_html=True)
            render_map_tab(results_gdf, layers, calculator)
        
        with tab_watershed:
            # st.markdown("## Riesgo por Cuencas", unsafe_allow_html=True)
            render_watershed_tab(results_gdf)
        
        with tab_glacier:
            # st.markdown("## Análisis Glaciar", unsafe_allow_html=True)
            render_glacier_tab(results_gdf)
        
        with tab_climate:
            # st.markdown("## Clima y Pronóstico", unsafe_allow_html=True)
            render_climate_tab(results_gdf)
            
        # === FOOTER ===
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: #90caf9; padding: 20px; font-size: 0.9em; background-color: rgba(19, 47, 76, 0.4); border-radius: 8px;'>
            <p style='margin-bottom: 10px;'>
                <b>Tecnologías:</b><br>
                EVAQUA integra modelos hidrológicos avanzados basados en Unidades de Respuesta Hidrológica (HRU) con procesamiento geoespacial en tiempo real y predicción climática de alta resolución.<br>
                Diseñado para la evaluación continua del riesgo de inundación.
            </p>
            <p style='margin-bottom: 5px;'>
                <b>Desarrolladores Principales (Prototipo 2.0):</b><br>
                Juan Pablo Cardenas • Florencia Castillo<br>
                <br>
                <b>Agradecimientos al equipo EVAQUA</b>
            </p>
            <p style='font-size: 0.8em; color: #64b5f6;'>
                EVAQUA © 2025 - Sistema de Monitoreo de Inundaciones
            </p>
        </div>
        """, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error: {e}")
        import traceback
        st.code(traceback.format_exc())

# ==================== UTILIDADES ====================

@st.fragment
def create_hru_detailed_analysis(results_gdf, selected_hru_id):
    """
    Sección completa de análisis detallado del HRU
    Incluye gráficos, métricas, y análisis completo
    """
    hru_data = results_gdf[results_gdf['grid_id'] == selected_hru_id]
    
    if hru_data.empty:
        st.warning(f"HRU #{selected_hru_id} no encontrado")
        return
    
    hru = hru_data.iloc[0]
    
    # Header de la sección
    st.markdown("---")
    st.markdown(f"## 📊 Análisis Detallado: HRU #{selected_hru_id}")
    st.markdown(f"### {hru.get('subcuenca_nom', 'N/A')} - Banda {hru.get('elevation_band', 'N/A')}")
    
    # Fila 1: Métricas clave
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Área Total", f"{hru.get('area_km2', 0):.1f} km²")
    
    with col2:
        st.metric("Glaciares", f"{hru.get('glacier_count', 0):.0f}")
    
    with col3:
        glacier_pct = hru.get('glacier_pct', 0)
        if pd.isna(glacier_pct):
            glacier_pct = 0
        st.metric("Cobertura Glaciar", f"{glacier_pct:.1f}%")
    
    with col4:
        st.metric("Elevación", f"{hru.get('elevation_mean', 0):.0f} m")
    
    with col5:
        risk_score = hru.get('risk_score', 0)
        if pd.isna(risk_score):
            risk_score = 0
        risk_score = max(0.0, min(1.0, float(risk_score)))
        st.metric("Riesgo", f"{risk_score:.2f}", delta=None)
    
    st.divider()
    
    # Fila 2: Tabs con diferentes análisis
    tab1, tab2, tab3, tab4 = st.tabs(["🌡️ Clima e Hidrología", "⛰️ Topografía", "🧊 Glaciares", "⚠️ Evaluación de Riesgo"])
    
    with tab1:
        st.subheader("Condiciones Climáticas e Hidrológicas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🌡️ Clima Actual**")
            st.write(f"• Temperatura: **{hru.get('temp_current', 0):.1f}°C**")
            st.write(f"• Precipitación (24h): **{hru.get('rain_24h', 0):.0f} mm**")
            st.write(f"• Nieve (24h): **{hru.get('snow_24h', 0):.0f} cm**")
            st.write(f"• Viento: **{hru.get('wind_speed_current', 0):.1f} km/h**")
            st.write(f"• Radiación: **{hru.get('radiation_current', 0):.0f} W/m²**")
        
        with col2:
            st.markdown("**💧 Hidrología**")
            st.write(f"• Deshielo glaciar: **{hru.get('melt_rate_mm_day', 0):.2f} mm/día**")
            st.write(f"• Escorrentía: **{hru.get('runoff_m3s', 0):.2f} m³/s**")
            
            # Calcular contribución glaciar
            area = hru.get('area_km2', 0)
            if area > 0 and hru.get('glacier_area_km2', 0) > 0:
                melt_contrib = (hru.get('glacier_area_km2', 0) / area) * hru.get('melt_rate_mm_day', 0)
                st.write(f"• Contribución glaciar al deshielo: **{melt_contrib:.2f} mm/día**")
    
    with tab2:
        st.subheader("Características Topográficas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Elevación Media", f"{hru.get('elevation_mean', 0):.0f} m")
            st.metric("Pendiente Media", f"{hru.get('slope_mean', 0):.1f}°")
            st.write(f"**Orientación:** {hru.get('aspect', 'N/A')}")
        
        with col2:
            st.info("Zona de " + str(hru.get('elevation_band', 'N/A')) + " elevación en la subcuenca")
    
    with tab3:
        st.subheader("Información de Glaciares")
        
        glacier_count = hru.get('glacier_count', 0)
        glacier_area = hru.get('glacier_area_km2', 0)
        
        if glacier_count > 0:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total de Glaciares", f"{glacier_count:.0f}")
            
            with col2:
                st.metric("Área Glaciar Total", f"{glacier_area:.2f} km²")
            
            with col3:
                area = hru.get('area_km2', 0)
                if area > 0:
                    pct = (glacier_area / area) * 100
                    st.metric("% del HRU", f"{pct:.1f}%")
            
            st.info(f"Este HRU contiene {glacier_count:.0f} glaciares que cubren {glacier_area:.2f} km² del territorio.")
        else:
            st.warning("Este HRU no contiene glaciares.")
    
    with tab4:
        st.subheader("Evaluación de Riesgo Detallada")
        
        risk_class = hru.get('risk_class', 'bajo')
        if not isinstance(risk_class, str):
            risk_class = 'bajo'
        
        # Barra de riesgo grande
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.progress(risk_score, text=f"Índice de Riesgo: {risk_score:.3f}")
        
        with col2:
            # Emoji según riesgo
            risk_emoji = {
                'bajo': '🟢',
                'moderado': '🟡',
                'medio': '🟡',
                'alto': '🟠',
                'muy_alto': '🔴',
                'critico': '🔴',
                'extremo': '🔴'
            }
            emoji = risk_emoji.get(risk_class, '⚪')
            st.markdown(f"# <span class='white-emoji'>{emoji}</span>", unsafe_allow_html=True)
            st.markdown(f"**{risk_class.upper()}**")
        
        # Detalles del riesgo
        st.markdown("---")
        st.markdown("**Factores de Riesgo:**")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Deshielo:**")
            melt = hru.get('melt_rate_mm_day', 0)
            if melt > 10:
                st.markdown(f"<div style='color: #d32f2f; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>🔴</span> <b>Alto:</b> {melt:.1f} mm/día</div>", unsafe_allow_html=True)
            elif melt > 5:
                st.markdown(f"<div style='color: #f57c00; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>⚠️</span> <b>Moderado:</b> {melt:.1f} mm/día</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='color: #388e3c; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>✅</span> <b>Bajo:</b> {melt:.1f} mm/día</div>", unsafe_allow_html=True)
        
        with col2:
            st.write("**Escorrentía:**")
            runoff = hru.get('runoff_m3s', 0)
            if runoff > 20:
                st.markdown(f"<div style='color: #d32f2f; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>🔴</span> <b>Alta:</b> {runoff:.1f} m³/s</div>", unsafe_allow_html=True)
            elif runoff > 10:
                st.markdown(f"<div style='color: #f57c00; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>⚠️</span> <b>Moderada:</b> {runoff:.1f} m³/s</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='color: #388e3c; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>✅</span> <b>Baja:</b> {runoff:.1f} m³/s</div>", unsafe_allow_html=True)
        
        with col3:
            st.write("**Precipitación:**")
            precip = hru.get('rain_24h', 0)
            if precip > 50:
                 st.markdown(f"<div style='color: #d32f2f; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>🔴</span> <b>Alta:</b> {precip:.0f} mm</div>", unsafe_allow_html=True)
            elif precip > 20:
                 st.markdown(f"<div style='color: #f57c00; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>⚠️</span> <b>Moderada:</b> {precip:.0f} mm</div>", unsafe_allow_html=True)
            else:
                 st.markdown(f"<div style='color: #388e3c; display: flex; align-items: center; margin-bottom: 5px;'><span class='white-emoji' style='font-size: 20px; margin-right: 5px;'>✅</span> <b>Baja:</b> {precip:.0f} mm</div>", unsafe_allow_html=True)

# ==================== TAB 1: MAPA ====================

def render_map_tab(results_gdf, layers, calculator):
    """Tab del mapa interactivo"""
    # Header ya se mostró en el tab, pero si se llama independiente:
    # st.markdown("## <span class='material-symbols-outlined'>map</span> Mapa de Riesgo", unsafe_allow_html=True)
    
    # st.info("ℹ️ Selecciona capas en la barra lateral. Haz clic en un HRU para detalles.", icon="👆")
    st.markdown("""
    <div style="background-color: #e8f5e9; color: #2e7d32; padding: 10px; border-radius: 8px; border: 1px solid #c8e6c9; display: flex; align-items: center; margin-bottom: 10px;">
        <span class="white-emoji" style="margin-right: 10px; font-size: 20px;">👆</span>
        <span style="font-size: 0.9em;">Selecciona capas en la barra lateral. Haz clic en un HRU para ver detalles.</span>
    </div>
    """, unsafe_allow_html=True)
    # Streamlit info icon is fixed, but we can remove emoji from text

    
    import folium
    from folium import plugins
    import matplotlib.colors as mcolors

    # Centrar en Aysén
    m = folium.Map(location=[-45.5712, -72.0685], zoom_start=7, tiles="OpenStreetMap")
    
    # Capa base (opcional, OSM ya es el default)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satélite (Esri)',
        overlay=False,
        control=True
    ).add_to(m)
    
    # === CAPA 1: GLACIARES (Fondo) - OPTIMIZADO ===
    if layers.get('glaciers', True) and calculator.glaciers_gdf is not None:
        try:
            # OPTIMIZACIÓN AGRESIVA: Limitar número de glaciares y simplificar más
            glaciers_to_show = calculator.glaciers_gdf.copy()
            
            # Solo mostrar glaciares grandes (> 0.1 km²) para reducir carga
            if 'area_in_grid' in glaciers_to_show.columns:
                glaciers_to_show = glaciers_to_show[glaciers_to_show['area_in_grid'] > 100000]  # > 0.1 km²
            
            # Limitar a máximo 500 glaciares más grandes
            if len(glaciers_to_show) > 500:
                if 'area_in_grid' in glaciers_to_show.columns:
                    glaciers_to_show = glaciers_to_show.nlargest(500, 'area_in_grid')
                else:
                    glaciers_to_show = glaciers_to_show.head(500)
            
            # Simplificar geometría AGRESIVAMENTE (0.001 grados ≈ 100m)
            glaciers_to_show['geometry'] = glaciers_to_show.simplify(0.001)

            folium.GeoJson(
                glaciers_to_show,
                name='Glaciares',
                style_function=lambda x: {
                    'fillColor': '#e3f2fd',
                    'color': '#90caf9',
                    'weight': 1,
                    'fillOpacity': 0.7
                },
                tooltip="Glaciar"
            ).add_to(m)
        except Exception as e:
            pass # logger.warning(f"No se pudo cargar capa glaciares: {e}")

    # === CAPA 2: CUENCAS (Medio) - OPTIMIZADO ===
    if layers.get('watershed', False) and calculator.cuencas_gdf is not None:
         # Simplificar geometría
         cuencas_simplified = calculator.cuencas_gdf.copy()
         cuencas_simplified['geometry'] = cuencas_simplified.simplify(0.001)
         
         folium.GeoJson(
            cuencas_simplified,
            name='Cuencas',
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#3399ff',
                'weight': 2,
                'dashArray': '5, 5'
            },
            tooltip=folium.GeoJsonTooltip(fields=['nom_cuen'], aliases=['Cuenca:'])
         ).add_to(m)

    # === CAPA 3: SUBCUENCAS (Medio-Alto) - OPTIMIZADO ===
    if layers.get('subwatershed', True) and calculator.subcuencas_gdf is not None:
         # Simplificar geometría
         subcuencas_simplified = calculator.subcuencas_gdf.copy()
         subcuencas_simplified['geometry'] = subcuencas_simplified.simplify(0.001)
         
         folium.GeoJson(
            subcuencas_simplified,
            name='Subcuencas',
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#4fc3f7', # Azul más claro
                'weight': 1,
                'dashArray': '3, 3'
            },
            tooltip=folium.GeoJsonTooltip(fields=['NOM_SUBC'], aliases=['Subcuenca:'])
         ).add_to(m)

    # === CAPA 4: HRUs (RIESGO) - ENCIMA - OPTIMIZADO ===
    if layers.get('hru_risk', True):
        # Simplificar geometrías de HRUs
        results_gdf_simplified = results_gdf.copy()
        results_gdf_simplified['geometry'] = results_gdf_simplified.simplify(0.001)
        # Función de estilo
        def style_function(feature):
            risk_class = feature['properties'].get('risk_class', 'bajo')
            # Mapear clase a color
            color_map = {
                'critico': RISK_COLORS['extremo'],
                'alto': RISK_COLORS['alto'],
                'medio': RISK_COLORS['moderado'],
                'bajo': RISK_COLORS['bajo'],
                'muy_bajo': RISK_COLORS['muy_bajo']
            }
            # Fallback para valores numéricos o desconocidos
            if risk_class not in color_map:
                try:
                    val = feature['properties'].get('risk_score')
                    risk_score = float(val) if val is not None else 0.0
                except (ValueError, TypeError):
                    risk_score = 0.0
                
                if risk_score >= 0.8: color = RISK_COLORS['extremo']
                elif risk_score >= 0.6: color = RISK_COLORS['alto']
                elif risk_score >= 0.4: color = RISK_COLORS['moderado']
                elif risk_score >= 0.2: color = RISK_COLORS['bajo']
                else: color = RISK_COLORS['muy_bajo']
            else:
                color = color_map.get(risk_class, RISK_COLORS['bajo'])
                
            return {
                'fillColor': color,
                'color': 'white',
                'weight': 1,
                'fillOpacity': 0.6
            }

        # Función de highlight
        highlight_function = lambda x: {
            'fillColor': '#000000', 
            'color': '#000000', 
            'fillOpacity': 0.50, 
            'weight': 0.1
        }
        
        # Popup Rico en HTML
        popup_fields = folium.GeoJsonPopup(
            fields=[
                'grid_id', 'subcuenca_nom', 'elevation_band', 'risk_class', 'risk_score',
                'glacier_area_km2', 'temp_current', 'rain_24h', 
                'melt_rate_mm_day', 'runoff_m3s'
            ],
            aliases=[
                'ID', 'Cuenca', 'Banda', 'Riesgo', 'Score',
                'Área Glaciar (km²)', 'Temp (°C)', 'Lluvia (mm)', 
                'Deshielo (mm/d)', 'Escorrentía (m³/s)'
            ],
            localize=True,
            labels=True,
            style="min-width: 300px; background-color: #1e3a5f; color: white; border-radius: 4px;"
        )
        
        folium.GeoJson(
            results_gdf_simplified,
            name='Riesgo por HRU',
            style_function=style_function,
            highlight_function=highlight_function,
            popup=popup_fields,
            tooltip=folium.GeoJsonTooltip(
                fields=['grid_id', 'subcuenca_nom', 'risk_class'],
                aliases=['HRU:', 'Cuenca:', 'Riesgo:'],
                style="background-color: #f0f0f0; color: black; border: 1px solid black;"
            )
        ).add_to(m)

    # Control de capas (UNA SOLA VEZ AL FINAL)
    folium.LayerControl(position='topright').add_to(m)
    
    # Renderizar mapa
    st_folium(m, width="100%", height=600, returned_objects=[])
    
    # === ANÁLISIS DETALLADO ===
    st.markdown("---")
    
    # === ANÁLISIS DETALLADO ===
    st.markdown("---")
    st.markdown("### Análisis de Zonas (HRUs)")
    
    # Crear dos pestañas: Tabla General y Detalle Individual
    # tab_table, tab_detail = st.tabs(["📋 Planilla General", "🔍 Ficha Detallada"])
    
    # with tab_table:
    # === SECCIÓN 1: ESTADO ACTUAL ===
    st.markdown("### Situación Actual")
    
    # Preparar dataframe PRINCIPAL (Solo datos actuales)
    main_cols = [
        'grid_id', 'subcuenca_nom', 'risk_class', 'risk_score', 
        'glacier_area_km2', 'melt_rate_mm_day', 'runoff_m3s', 
        'temp_current', 'rain_24h'
    ]
    
    df_main = results_gdf[main_cols].copy() if set(main_cols).issubset(results_gdf.columns) else results_gdf.copy()
    
    # Renombrar
    map_main = {
        'grid_id': 'ID', 'subcuenca_nom': 'Subcuenca', 'risk_class': 'Nivel Riesgo',
        'risk_score': 'Índice Riesgo', 'glacier_area_km2': 'Área Glaciar (km²)',
        'melt_rate_mm_day': 'Deshielo (mm/d)', 'runoff_m3s': 'Escorrentía (m³/s)',
        'temp_current': 'Temp (°C)', 'rain_24h': 'Lluvia 24h (mm)'
    }
    df_main = df_main.rename(columns=map_main)
    
    st.dataframe(
        df_main,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Índice Riesgo': st.column_config.ProgressColumn(
                "Índice Riesgo", help="Riesgo 0-1", format="%.2f", min_value=0, max_value=1
            ),
            'Nivel Riesgo': st.column_config.TextColumn("Nivel Riesgo")
        }
    )

    # === SECCIÓN 2: PROYECCIONES ===
    st.markdown("---")
    st.markdown("### Proyección a 3 Días (Detalle por HRU)")
    st.info("ℹ️ Escenario hipotético: Si las condiciones de hoy se repiten por 72 horas.")
    
    if 'water_total_3d_mm' in results_gdf.columns:
        # Preparar dataframe PROYECCIÓN
        proj_cols = [
            'grid_id', 'subcuenca_nom', 'risk_class_3d', 'risk_score_3d',
            'melt_3d_mm', 'precip_3d_mm', 'water_total_3d_mm'
        ]
        
        # Verificar que existan las columnas
        valid_cols = [c for c in proj_cols if c in results_gdf.columns]
        df_proj = results_gdf[valid_cols].copy()
        
        # Renombrar
        map_proj = {
            'grid_id': 'ID', 'subcuenca_nom': 'Subcuenca', 
            'risk_class_3d': 'Riesgo 3D', 'risk_score_3d': 'Índice 3D',
            'melt_3d_mm': 'Deshielo 3D (mm)', 'precip_3d_mm': 'Lluvia 3D (mm)',
            'water_total_3d_mm': 'Agua Total 3D'
        }
        df_proj = df_proj.rename(columns=map_proj)
        
        st.dataframe(
            df_proj,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Índice 3D': st.column_config.ProgressColumn(
                    "Índice 3D", format="%.2f", min_value=0, max_value=1
                ),
                'Agua Total 3D': st.column_config.BarChartColumn(
                    "Agua Acum. (Gráfico)",
                    help="Volumen total proyectado (Deshielo + Lluvia) en 72h",
                    y_min=0,
                    y_max=df_proj['Agua Total 3D'].max() if not df_proj.empty else 100
                ),
                'Deshielo 3D (mm)': st.column_config.NumberColumn(format="%.1f"),
                'Lluvia 3D (mm)': st.column_config.NumberColumn(format="%.1f")
            }
        )
    
    # with tab_detail:
    #     # Selector de HRU para análisis (Lógica original)
    #     pass


# ==================== TAB 2: CUENCAS ====================


def render_watershed_tab(results_gdf):
    """Tab de análisis por cuencas"""
    st.markdown("## 📊 Análisis por Cuencas")
    
    if results_gdf is None or results_gdf.empty:
        st.warning("No hay datos disponibles para analizar cuencas.")
        return

    # 1. Agregación de datos por Cuenca/Subcuenca
    # Usamos 'subcuenca_nom' si existe, sino tratamos de usar otra
    group_col = 'subcuenca_nom' if 'subcuenca_nom' in results_gdf.columns else 'grid_id'
    
    # Agrupar
    watershed_stats = results_gdf.groupby(group_col).agg({
        'runoff_m3s': 'sum',
        'melt_rate_mm_day': 'mean', # Promedio de tasa de derretimiento
        'glacier_area_km2': 'sum',
        'risk_score': 'max',  # Riesgo máximo encontrado en la cuenca
        'grid_id': 'count'    # Número de HRUs
    }).reset_index()
    
    # Renombrar para visualización
    watershed_stats.rename(columns={
        group_col: 'Cuenca',
        'runoff_m3s': 'Caudal Total (m³/s)',
        'melt_rate_mm_day': 'Tasa Deshielo Prom. (mm/d)',
        'glacier_area_km2': 'Área Glaciar (km²)',
        'risk_score': 'Riesgo Máx',
        'grid_id': 'Num. HRUs'
    }, inplace=True)
    
    # Ordenar por Riesgo y Caudal
    watershed_stats = watershed_stats.sort_values(by=['Riesgo Máx', 'Caudal Total (m³/s)'], ascending=False)
    
    # 2. Métricas Resumen
    c1, c2, c3 = st.columns(3)
    c1.metric("Cuencas Analizadas", len(watershed_stats))
    
    # Calcular promedio por cuenca en lugar de total
    avg_flow_basin = watershed_stats['Caudal Total (m³/s)'].mean() if not watershed_stats.empty else 0
    c2.metric("Caudal Promedio (Cuenca)", f"{avg_flow_basin:.1f} m³/s", help=f"Caudal medio por cuenca hidrográfica. Total Regional: {watershed_stats['Caudal Total (m³/s)'].sum():.1f} m³/s")
    top_risk_basin = watershed_stats.iloc[0]['Cuenca'] if not watershed_stats.empty else "-"
    c3.metric("Cuenca Más Crítica", str(top_risk_basin))
    
    st.markdown("---")
    
    # 3. Gráficos Comparativos
    col_chart, col_table = st.columns([1, 1])
    
    with col_chart:
        # st.subheader("🌊 Top Cuencas por Caudal")
        st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>🌊</span> Caudal por Cuenca", unsafe_allow_html=True)
        # Bar chart simple
        top_flow = watershed_stats.head(10)
        st.bar_chart(top_flow.set_index('Cuenca')['Caudal Total (m³/s)'], color="#0288d1")
        
    with col_table:
        # st.subheader("🚨 Ranking de Riesgo")
        st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>⚠️</span> Zonas Críticas", unsafe_allow_html=True)
        # Mostrar tabla simplificada
        st.dataframe(
            watershed_stats[['Cuenca', 'Riesgo Máx', 'Caudal Total (m³/s)', 'Área Glaciar (km²)']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'Riesgo Máx': st.column_config.ProgressColumn(
                    "Riesgo Máx",
                    format="%.2f",
                    min_value=0,
                    max_value=1,
                    help="Riesgo máximo detectado en cualquier HRU de la cuenca"
                ),
                'Caudal Total (m³/s)': st.column_config.NumberColumn(format="%.1f"),
                'Área Glaciar (km²)': st.column_config.NumberColumn(format="%.1f")
            }
        )
    # 3. Gráficos Comparativos (Agua por Subcuenca)
    st.markdown("---")
    st.markdown("### <span class='white-emoji' style='vertical-align: middle;'>💧</span> Aporte Glaciar vs Lluvia", unsafe_allow_html=True)
    
    # Preparar datos para gráfico apilado
    # Necesitamos agrupar por subcuenca y sumar melt (m3/s) y rain runoff (m3/s)
    # Aproximación rápida: Caudal Total vs Caudal Glaciar Estimado
    
    # Calcular runoff solo de lluvia (Total - Glaciar)
    # Nota: runoff_m3s ya incluye todo. Estimamos componente glaciar como antes.
    
    watershed_comp = results_gdf.groupby('subcuenca_nom').agg({
        'runoff_m3s': 'sum',
        'melt_rate_mm_day': 'mean', # Solo referencia
        'glacier_area_km2': 'sum'
    }).reset_index()
    
    # Recalcular componente glaciar por cuenca (aproximado)
    # Suma de (melt_rate * area * 1000 / 86400) para cada HRU en la cuenca
    # O más fácil: iterar el GDF original
    
    glacier_runoff = results_gdf.copy()
    glacier_runoff['glacier_flow_m3s'] = (glacier_runoff['melt_rate_mm_day'] * glacier_runoff['glacier_area_km2'] * 1000) / 86400
    
    ws_flow_comp = glacier_runoff.groupby('subcuenca_nom')[['runoff_m3s', 'glacier_flow_m3s']].sum().reset_index()
    ws_flow_comp['rain_flow_m3s'] = ws_flow_comp['runoff_m3s'] - ws_flow_comp['glacier_flow_m3s']
    ws_flow_comp['rain_flow_m3s'] = ws_flow_comp['rain_flow_m3s'].clip(lower=0)
    
    # Graficar
    chart_data = ws_flow_comp.set_index('subcuenca_nom')[['glacier_flow_m3s', 'rain_flow_m3s']]
    chart_data.columns = ['Aporte Glaciar', 'Aporte Lluvia']
    
    st.bar_chart(chart_data, color=['#4fc3f7', '#0277bd'])
    st.caption("Comparación de fuentes de caudal estimado (m³/s)")

# ==================== TAB 3: GLACIARES ====================

def render_glacier_tab(results_gdf):
    """Tab de análisis glaciar"""
    st.markdown("## 🧊 Análisis de Glaciares")
    
    if 'glacier_area_km2' not in results_gdf.columns:
        st.warning("Datos de área glaciar no disponibles.")
        return

    # 1. Estadísticas Globales
    col1, col2, col3, col4 = st.columns(4)
    
    total_area = results_gdf['glacier_area_km2'].sum()
    
    # Cálculo CLEAN de aporte caudal (m3/s)
    # Melt Rate (mm/d) * Area (km2) * 1000 (m3/mm*km2) / 86400 (s/d)
    
    total_vol_m3_d = (results_gdf['melt_rate_mm_day'] * results_gdf['glacier_area_km2']).sum() * 1000
    total_melt_m3s = total_vol_m3_d / 86400

    avg_temp = results_gdf[results_gdf['glacier_area_km2'] > 0]['temp_current'].mean()
    
    # Cálculos de Promedios
    glacier_zones = results_gdf[results_gdf['glacier_area_km2'] > 0.01]
    count_glaciers = len(glacier_zones)
    
    avg_melt_rate = glacier_zones['melt_rate_mm_day'].mean() if count_glaciers > 0 else 0
    
    # Caudal promedio por zona (del total calculado antes)
    avg_melt_flow = total_melt_m3s / count_glaciers if count_glaciers > 0 else 0
    
    with col1:
        st.metric("Tasa Deshielo Prom.", f"{avg_melt_rate:.1f} mm/d", help=f"Tasa media de fusión vertical. Área Total: {total_area:.1f} km²")
    with col2:
        st.metric("Caudal Deshielo Prom.", f"{avg_melt_flow:.2f} m³/s", help=f"Aporte promedio por zona glaciar. Total: {total_melt_m3s:.1f} m³/s")
    with col3:
        st.metric("Temp. Promedio (Glaciar)", f"{avg_temp:.1f}°C")
    with col4:
        st.metric("Zonas con Glaciares", f"{count_glaciers}")
        
    st.markdown("---")
    
    # 2. Análisis Visual: Scatter y Pie Chart
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # st.subheader("🔥 Deshielo vs Altitud")
        st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>🌡️</span> Deshielo vs Altitud", unsafe_allow_html=True)
        if 'elevation_mean' in results_gdf.columns:
            chart_df = results_gdf[results_gdf['glacier_area_km2'] > 0].copy()
            st.scatter_chart(
                chart_df,
                x='elevation_mean',
                y='melt_rate_mm_day',
                size='glacier_area_km2',
                color='#3399ff',
            )
            st.caption("Mayor deshielo en zonas bajas (Eje X: Elevación, Y: Tasa mm/d)")

    with col_chart2:
        # st.subheader("💧 Contribución Glaciar Total")
        st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>💧</span> Contribución Glaciar Total", unsafe_allow_html=True)
        # Calcular % global
        total_rain_runoff = results_gdf['runoff_m3s'].sum()
        total_system_flow = total_melt_m3s + total_rain_runoff
        
        pct_glacier = (total_melt_m3s / total_system_flow * 100) if total_system_flow > 0 else 0
        pct_rain = 100 - pct_glacier
        
        # Pie chart con plotly express
        labels = ['Aporte Glaciar', 'Lluvia/Base']
        values = [pct_glacier, pct_rain]
        
        fig = px.pie(values=values, names=labels, hole=0.4, 
                     color_discrete_sequence=['#4fc3f7', '#01579b'])
        fig.update_layout(showlegend=True, margin=dict(t=0, b=0, l=0, r=0), height=300)
        st.plotly_chart(fig, use_container_width=True)
        
        st.metric("% Caudal Glaciar", f"{pct_glacier:.1f}%", delta="Impacto actual")
            
    st.markdown("---")
    # st.subheader("🧊 Top Zonas de Aporte")
    st.markdown("### <span class='white-emoji' style='vertical-align: middle;'>🧊</span> Top Zonas de Aporte", unsafe_allow_html=True)
    
    # Ranking de HRUs por volumen de deshielo (Area * Rate)
    ranking_df = results_gdf.copy()
    ranking_df['vol_melt'] = ranking_df['melt_rate_mm_day'] * ranking_df['glacier_area_km2']
    top_hru = ranking_df.sort_values(by='vol_melt', ascending=False).head(10)
    
    st.dataframe(
        top_hru[['grid_id', 'melt_rate_mm_day', 'glacier_area_km2']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'grid_id': 'ID HRU',
                'melt_rate_mm_day': st.column_config.NumberColumn("Tasa (mm/d)", format="%.1f"),
                'glacier_area_km2': st.column_config.NumberColumn("Área (km²)", format="%.1f")
            }
        )

# ==================== TAB 4: CLIMA ====================

def render_climate_tab(results_gdf):
    """Tab de clima y pronósticos"""
    st.markdown("## <span class='white-emoji' style='vertical-align: bottom;'>🌡️</span> Pronóstico y Clima", unsafe_allow_html=True)
    
    # 1. Métricas Regionales
    col1, col2, col3 = st.columns(3)
    
    avg_temp = results_gdf['temp_current'].mean() if 'temp_current' in results_gdf.columns else 0
    avg_precip = results_gdf['rain_24h'].mean() if 'rain_24h' in results_gdf.columns else 0
    max_precip_loc = results_gdf['rain_24h'].max() if 'rain_24h' in results_gdf.columns else 0
    
    with col1:
        st.metric("Temp. Regional Promedio", f"{avg_temp:.1f}°C")
    with col2:
        st.metric("Precipitación Promedio 24h", f"{avg_precip:.1f} mm")
    with col3:
        st.metric("Pico Máximo (Local)", f"{max_precip_loc:.1f} mm")
        
    st.markdown("---")
    
    # 2. Gráficos de Pronóstico
    # Necesitamos las series temporales que guardamos en evaqua.py
    if 'temp_series' in results_gdf.columns:
        # st.subheader("📈 Pronóstico Regional (Próximos 3 Días)")
        st.markdown("### <span class='white-emoji' style='vertical-align: middle;'>📈</span> Pronóstico Regional (3 Días)", unsafe_allow_html=True)
        
        # Agregación: Promedio horario de todas las celdas
        # Extraer listas y convertir a array numpy 2D (grids x hours)
        # Filtramos filas donde temp_series sea válido (no vacío/NaN)
        valid_series = results_gdf[results_gdf['temp_series'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
        
        if not valid_series.empty:
            # Temperatura
            temps_array = np.array(valid_series['temp_series'].tolist())
            mean_temps = np.mean(temps_array, axis=0) # Promedio por hora
            
            # Precipitación
            precip_array = np.array(valid_series['precip_series'].tolist())
            mean_precip = np.mean(precip_array, axis=0) # Promedio por hora (o suma según se quiera ver, usa promedio para intensidad regional)
            
            # Crear DataFrame temporal
            hours = range(len(mean_temps))
            forecast_df = pd.DataFrame({
                "Hora": hours,
                "Temperatura (°C)": mean_temps,
                "Precipitación (mm/h)": mean_precip
            })
            
            # Gráfico Combinado
            tab_temp, tab_rain = st.tabs(["Temperatura", "Precipitación"])
            
            with tab_temp:
                 st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>🌡️</span> Evolución Térmica", unsafe_allow_html=True)
                 st.line_chart(forecast_df, x="Hora", y="Temperatura (°C)", color="#ff9800")
                 st.caption("Promedio regional de temperatura horaria.")
                 
            with tab_rain:
                 st.markdown("##### <span class='white-emoji' style='vertical-align: middle;'>💧</span> Intensidad de Lluvia", unsafe_allow_html=True)
                 st.bar_chart(forecast_df, x="Hora", y="Precipitación (mm/h)", color="#2196f3")
                 st.caption("Intensidad promedio de precipitación regional.")
        else:
             st.markdown("""
             <div style="background-color: #e3f2fd; color: #0d47a1; padding: 10px; border-radius: 8px; display: flex; align-items: center;">
                <span class="white-emoji" style="margin-right: 10px;">ℹ️</span>
                <span>No hay datos de series temporales disponibles para gráficos.</span>
             </div>
             """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background-color: #e3f2fd; color: #0d47a1; padding: 10px; border-radius: 8px; display: flex; align-items: center;">
           <span class="white-emoji" style="margin-right: 10px;">ℹ️</span>
           <span>Datos de series temporales no cargados. Recarga la aplicación para actualizar.</span>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
