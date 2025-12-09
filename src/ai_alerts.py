import streamlit as st
import pandas as pd
import logging
from google import genai
from datetime import datetime

logger = logging.getLogger(__name__)

# Configuración del Modelo
MODEL_ID = "gemini-2.5-flash"

def _get_risk_level_name(score):
    if score >= 0.8: return "EXTREMO"
    if score >= 0.6: return "ALTO"
    if score >= 0.4: return "MEDIO"
    if score >= 0.2: return "BAJO"
    return "MUY BAJO"

def _format_hru_data(row):
    """Formatea los datos de un HRU para el prompt"""
    causes = []
    if row.get('rain_24h', 0) > 20: causes.append(f"Lluvia intensa ({row['rain_24h']:.0f}mm)")
    if row.get('melt_rate_mm_day', 0) > 10: causes.append(f"Deshielo acelerado ({row['melt_rate_mm_day']:.1f}mm/d)")
    if row.get('risk_score', 0) > 0.7: causes.append("Saturación probable")
    
    causes_text = ", ".join(causes) if causes else "Sin factores críticos evidentes"
    
    # Coordenadas aproximadas (se asume que vienen procesadas en el dump json)
    lat, lon = "N/A", "N/A"
    if 'lat' in row and 'lon' in row:
        lat, lon = f"{row['lat']:.4f}", f"{row['lon']:.4f}"
        
    return f"""
    - ID HRU: {row['grid_id']}
      Subcuenca: {row.get('subcuenca_nom', 'Desconocida')}
      Nivel Riesgo: {_get_risk_level_name(row.get('risk_score', 0))} (Score: {row.get('risk_score', 0):.2f})
      Coordenadas: ({lat}, {lon})
      Factores: {causes_text}
    """

@st.cache_data(ttl=3600)  # Cache por 1 hora
def generate_ai_report(results_json_dump, api_key):
    """
    Genera el reporte usando el SDK de Google GenAI.
    Recibe un JSON stringificado del DF para gestionar el cache.
    """
    try:
        # Reconstruir DataFrame
        data = pd.read_json(results_json_dump)
        
        # 1. Análisis Estadístico
        total_hrus = len(data)
        riesgo_alto = len(data[data['risk_score'] >= 0.6])
        riesgo_medio = len(data[(data['risk_score'] >= 0.4) & (data['risk_score'] < 0.6)])
        riesgo_bajo = len(data[data['risk_score'] < 0.4])
        
        # 2. Filtrar HRUs relevantes (Medio o Alto)
        relevant_hrus = data[data['risk_score'] >= 0.4].sort_values('risk_score', ascending=False)
        
        # Si hay muchos, cortamos a los top 10 para no saturar el prompt
        if len(relevant_hrus) > 10:
            relevant_hrus = relevant_hrus.head(10)
            note = "(Mostrando Top 10 críticos)"
        else:
            note = ""
            
        hru_details = "\n".join([_format_hru_data(row) for _, row in relevant_hrus.iterrows()])
        
        # 3. Construir Prompt OPTIMIZADO PARA BREVEDAD
        prompt_text = f"""
        Actúa como un hidrólogo experto del sistema de alerta temprana EVAQUA en la región de Aysén, Chile.
        Genera un **BOLETÍN DE ALERTA RESUMIDO** en formato **HTML** para integrarse en un dashboard.
        
        OBJETIVO: Ser extremadamente conciso y directo.
        
        DATOS DE CONTEXTO:
        {hru_details}
        
        ESTADÍSTICAS:
        - Total HRUs: {total_hrus} | Media: {riesgo_medio} | Alta: {riesgo_alto}
        
        INSTRUCCIONES DE FORMATO (HTML Puro):
        1. NO uses Markdown. Usa tags HTML: <h3>, <b>, <ul>, <li>, <p>.
        2. NO incluyas tags <html>, <head> o <body>. Solo el contenido del div (fragmento).
        3. Estructura:
           - <h3>Resumen Regional</h3>: (Párrafo corto <p>).
           - <h3>Zonas Afectadas</h3>: <ul> con <li>. Cada li debe tener <b>Zona</b>: Detalle.
           - <p><i>Cierre con referencia al mapa.</i></p>
        4. Sé conciso. Agrupa por zonas geográficas.
        
        EJEMPLO SALIDA:
        <h3>Resumen Regional</h3>
        <p>Se registra alerta media en el 90% de la región por deshielos...</p>
        <h3>Zonas Afectadas</h3>
        <ul>
            <li><b>Campos de Hielo</b>: Aumento de caudal por fusión.</li>
            <li><b>Zona Costera</b>: Riesgo de deslizamientos.</li>
        </ul>
        <p><i>Ver mapa para detalle.</i></p>
        """
        
        # 4. Usar google-genai SDK
        client = genai.Client(api_key=api_key)
        
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt_text
        )
        
        return response.text
        
    except Exception as e:
        logger.error(f"Error generando reporte AI: {e}")
        return "<p>⚠️ Servicio de inteligencia artificial temporalmente no disponible.</p>"

def render_ai_alert_section(results_gdf, api_key):
    """Renderiza la sección de alertas AI"""
    if 'risk_score' not in results_gdf.columns:
        return
        
    # Preparar datos (Simplificado para JSON dump)
    cols_to_keep = ['grid_id', 'hru_id', 'subcuenca_nom', 'risk_score', 
                   'melt_rate_mm_day', 'rain_24h', 'lat', 'lon']
    
    # Asegurar que existan columnas
    df_export = results_gdf.copy()
    for col in cols_to_keep:
        if col not in df_export.columns:
            df_export[col] = 0
            
    # Top 20 HRUs más riesgosos para el prompt
    df_export = df_export.sort_values('risk_score', ascending=False).head(20)
    
    if not df_export.empty:
        # Serializar a JSON string
        # Importante: Convertir a DataFrame de pandas puro para soportar orient='records'
        # GeoPandas to_json no soporta orient
        json_dump = pd.DataFrame(df_export[cols_to_keep]).to_json(orient='records')
        
        report_html = generate_ai_report(json_dump, api_key)
        
        # Renderizado CUSTOM HTML (Control total de estilo y tamaño de icono)
        st.markdown(f"""
        <div style="
            background-color: rgba(33, 150, 243, 0.1); 
            border: 1px solid rgba(33, 150, 243, 0.3); 
            border-radius: 8px; 
            padding: 20px; 
            margin-bottom: 20px;
        ">
            <h2 style="
                margin-top: 0; 
                margin-bottom: 15px; 
                display: flex; 
                align-items: center; 
                gap: 10px; 
                color: #e3f2fd; 
                font-size: 1.4rem;
            ">
                <span class="white-emoji" style="font-size: 1.0em;">🤖</span> 
                Boletín de Riesgo (AI)
            </h2>
            <div style="font-size: 0.95rem; line-height: 1.6; color: #e3f2fd;">
                {report_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
