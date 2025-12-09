# 🔧 Guía de Troubleshooting - EVAQUA Deploy

## Problema: EOF en Health Check de Streamlit Cloud

### ✅ Soluciones Implementadas

#### 1. **Desactivar File Watcher** (`.streamlit/config.toml`)
```toml
[server]
fileWatcherType = "none"
```
**Por qué:** El watchdog/inotify genera eventos constantemente por `__pycache__`, causando reinicios en bucle.

#### 2. **Reducir Logging** 
```python
logging.basicConfig(level=logging.WARNING)
```
**Por qué:** Los logs DEBUG generan mucha I/O que puede saturar el sistema.

#### 3. **Lazy Loading Progresivo**
La función `load_evaqua_analysis()` ahora muestra progreso paso a paso:
- 📂 Cargando datos geoespaciales (1/4)
- 🏔️ Obteniendo topografía (2/4)
- 🌡️ Consultando datos climáticos (3/4)
- ⚙️ Calculando riesgos (4/4)

**Por qué:** Mantiene vivo el health check mostrando que la app está activa.

#### 4. **Caché Optimizado**
```python
@st.cache_data(ttl=3600, show_spinner="Cargando...")
```
**Por qué:** Evita recargar datos en cada request, pero muestra progreso.

#### 5. **`.gitignore` Actualizado**
Excluye `__pycache__/` y otros archivos temporales.

**Por qué:** Evita que archivos temporales se suban al repo y causen problemas.

---

## 📋 Checklist de Deploy

### Antes de hacer deploy a Streamlit Cloud:

- [ ] Verificar que `.streamlit/config.toml` tiene `fileWatcherType = "none"`
- [ ] Verificar que `requirements.txt` tiene versiones específicas
- [ ] Verificar que `packages.txt` existe con dependencias del sistema
- [ ] Verificar que `.gitignore` excluye `__pycache__/`
- [ ] Verificar que `secrets.toml` NO está en el repo (solo en Streamlit Cloud)
- [ ] Limpiar caché local: `streamlit cache clear`

### En Streamlit Cloud:

1. **Configurar Secrets:**
   - Ir a App Settings → Secrets
   - Agregar: `GEMINI_API_KEY = "tu_api_key"`

2. **Configuración Avanzada:**
   - Python version: 3.10 o superior
   - Main file path: `app.py`

3. **Monitorear Logs:**
   - Ir a "Manage app" → "Logs"
   - Buscar errores específicos
   - Verificar que NO aparezca spam de inotify

---

## 🚨 Si el problema persiste:

### Opción 1: Aumentar recursos
Streamlit Cloud Free tiene límites. Considera:
- Streamlit Cloud Pro (más recursos)
- Deploy en otro servicio (Render, Railway, Heroku)

### Opción 2: Reducir carga inicial
Si los datos son muy pesados:

```python
# En app.py, línea ~44
@st.cache_resource(show_spinner=False, ttl=7200)  # 2 horas
def load_evaqua_analysis():
    # Cargar solo datos esenciales primero
    # Lazy load el resto bajo demanda
```

### Opción 3: Pre-procesar datos
Generar archivos `.parquet` pre-procesados:

```python
# Script separado: preprocess.py
results_gdf.to_parquet("data/preprocessed_results.parquet")

# En app.py:
@st.cache_data
def load_preprocessed():
    return gpd.read_parquet("data/preprocessed_results.parquet")
```

---

## 📊 Métricas de Salud

### Indicadores de que la app está saludable:

✅ **Logs limpios:** Sin spam de inotify/watchdog
✅ **Tiempo de carga:** < 30 segundos en primera carga
✅ **Health check:** Responde sin EOF
✅ **Memoria:** < 1GB en uso

### Comandos útiles (local):

```bash
# Limpiar caché
streamlit cache clear

# Correr sin file watcher
streamlit run app.py --server.fileWatcherType none

# Ver uso de memoria
# (En Windows PowerShell)
Get-Process streamlit | Select-Object WorkingSet64
```

---

## 🔍 Debug Avanzado

### Si necesitas más información:

1. **Activar logging temporal:**
```python
# En app.py (solo para debug)
logging.basicConfig(level=logging.INFO)
```

2. **Verificar tamaño de datos:**
```python
# En Python
import os
for root, dirs, files in os.walk("data"):
    for file in files:
        path = os.path.join(root, file)
        size = os.path.getsize(path) / (1024*1024)  # MB
        print(f"{path}: {size:.2f} MB")
```

3. **Profiling:**
```python
import time
start = time.time()
# ... código ...
print(f"Tiempo: {time.time() - start:.2f}s")
```

---

## 📞 Contacto

Si el problema persiste después de aplicar todas las soluciones:

1. Revisar logs de Streamlit Cloud
2. Verificar límites de recursos
3. Considerar alternativas de deploy
4. Contactar soporte de Streamlit Cloud

---

**Última actualización:** 2025-12-09
**Versión:** 2.0
