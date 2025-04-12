
# Workshop002 - ETL con Spotify, MusicBrainz y Grammy Awards

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) usando **Airflow** y **Python**, donde se integran tres fuentes de datos relacionadas con la industria musical:

- Datos de canciones de **Spotify**
- Información de artistas desde la **API de MusicBrainz**
- Registros históricos de los **Grammy Awards**

---

## Objetivo del Proyecto

Analizar cómo características musicales como **tempo** y **bailabilidad** influyen en el reconocimiento de artistas en los Grammy Awards, enriqueciendo los datos con información adicional desde una API pública (MusicBrainz).

---

## Tecnologías utilizadas

- **Apache Airflow**: orquestación de tareas ETL
- **Python + Pandas**: procesamiento y limpieza de datos
- **PostgreSQL**: almacenamiento de datos estructurados
- **Google Drive API**: almacenamiento externo (opcional)
- **Power BI**: visualización y análisis final

---

## Estructura del proyecto

```
Workshop002/
├── dags/
│   ├── WorkshopDAG.py              # DAG principal de Airflow
│   ├── Extract.py                  # Extracción desde CSV, PostgreSQL y API
│   ├── transform_spotify_data.py   # Limpieza y normalización de Spotify
│   ├── transform_grammy_data.py    # Transformaciones de premios Grammy
│   ├── transform_API_data.py       # (opcional) Limpieza de datos API
│   ├── merge_datasets.py           # Unión de los tres datasets
│   ├── load.py                     # Carga final a PostgreSQL
│   └── Store.py                    # Subida del CSV final a Google Drive
├── dataset.csv                     # Dataset original de Spotify
├── requirements.txt                # Librerías requeridas
├── README.md                       # Este archivo
└── .gitignore                      # Exclusión de archivos temporales y claves
```

---

## Flujo ETL

1. **Extract**:
   - Se cargan los datos desde PostgreSQL (Grammy)
   - Se lee el archivo dataset.csv (Spotify)
   - Se hace llamada a la API de MusicBrainz para obtener el ID y tipo de artista

2. **Transform**:
   - Se limpian y filtran los datos
   - Se estandarizan las columnas
   - Se normaliza información relevante para análisis

3. **Merge**:
   - Se combinan las 3 fuentes en un solo dataframe unificado

4. **Load**:
   - Se inserta el resultado en una base de datos PostgreSQL

5. **Store**:
   - Se exporta como CSV y se sube automáticamente a Google Drive

---

## Cómo ejecutar el proyecto

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu_usuario/Workshop002.git
   cd Workshop002
   ```

2. Crea un entorno virtual e instala dependencias:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Define la variable de entorno de Airflow:
   ```bash
   export AIRFLOW_HOME=~/Workshop002
   airflow db init
   ```

4. Coloca tu archivo service_account.json (credenciales de Google Drive) en la carpeta correspondiente.

5. Ejecuta Airflow en modo standalone:
   ```bash
   airflow standalone
   ```

6. Habilita el DAG llamado WorkshopDAG desde la interfaz web de Airflow.

---

## Visualizaciones

Una vez procesados los datos, puedes importarlos a Power BI (u otro BI) para explorar:
- Distribución de tempo en artistas ganadores vs no ganadores
- Comparativa entre tipos de artistas
- Evolución de características musicales por año
