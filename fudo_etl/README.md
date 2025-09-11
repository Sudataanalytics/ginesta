# Proyecto ETL para Grupo Ginesta (Datos Gastronómicos Fudo)

Este repositorio contiene la solución de Extracción, Transformación y Carga (ETL) desarrollada para Grupo Ginesta, con el objetivo de centralizar y procesar los datos de sus unidades gastronómicas que utilizan el sistema **Fu.do**.

La solución extrae datos crudos de la API de Fu.do, los almacena en una base de datos PostgreSQL, los transforma en un modelo relacional optimizado (DER), y automatiza su actualización para el consumo de Power BI.

---

## 🚀 **Visión General de la Solución**

El ETL procesa datos de ventas, ítems de venta, pagos, productos, categorías y sucursales de Fu.do para todas las unidades gastronómicas (Nebraska, Quito, Punto Criollo, Chalé).

**Componentes Clave:**
- **Origen de Datos:** Fu.do API (múltiples sucursales).
- **Base de Datos de Destino:** PostgreSQL en Donweb (`ginesta` DB).
- **Capas de Datos:**
    - **RAW Layer:** Tablas `fudo_raw_*` (datos crudos, optimizados para cambios de contenido).
    - **Analytic Layer (DER):** Vistas Materializadas `mv_*` (modelo relacional optimizado para Power BI).
- **Orquestación:** Python script (`main.py`) dockerizado.
- **Plataforma Cloud:** Google Cloud Platform (GCP) - Cloud Run Jobs, Cloud Scheduler, Secret Manager.
- **Consumo:** Microsoft Power BI (via Power BI Gateway a Donweb).

---

## 🏗️ **Estructura del Proyecto**
grupo ginesta/
├── .github/
│ └── workflows/ # GitHub Actions (futuro CI/CD)
├── .gitignore # Archivos ignorados por Git
├── fudo_etl/ # Contenedor del ETL de Fu.do
│ ├── modules/ # Módulos Python internos (config, db, auth, api_client, metadata)
│ │ ├── init.py
│ │ └── ...
│ ├── sql/ # Scripts SQL para despliegue de estructura
│ │ └── deploy_fudo_structure.sql
│ ├── main.py # Script principal del ETL (Extract, Load, Transform)
│ ├── deploy_db.py # Script para la creación inicial de la DB en Donweb
│ ├── Dockerfile # Definición de la imagen Docker para el ETL
│ └── requirements.txt # Dependencias de Python
├── brie-etl/ # Futura integración para Minimercados Brie (estructura similar)
│ └── ...
├── venv/ # Entorno virtual (ignorada)
├── .env # Variables de entorno locales (ignorada - ¡CRÍTICO!)
└── README.md # Este documento
code
Code
---

## ⚙️ **Configuración y Despliegue**

### **1. Entorno de Desarrollo Local**

Para ejecutar el ETL localmente (contra tu PostgreSQL local o Donweb para pruebas):

1.  **Clonar el Repositorio:**
    git clone https://github.com/Sudataanalytics/ginesta.git
    cd ginesta

2.  **Crear y Activar Entorno Virtual:**
    python3 -m venv venv
    source venv/bin/activate  # macOS/Linux
 
3.  **Instalar Dependencias:**
    
    pip install -r fudo_etl/requirements.txt

4.  **Configurar `.env`:**
    Crea un archivo `.env` en la **raíz del proyecto** (`grupo ginesta/`) con las credenciales (¡no lo subas a Git!):
    ```
    DB_CONNECTION_STRING="postgresql://fudo_user:mysecretpassword@localhost:5432/fudo_etl_db"
    DONWEB_ADMIN_CONNECTION_STRING="postgresql://sudata_owner:6w3zAQa4sXs6z@vps-4657831-x.dattaweb.com:5432/postgres"
    TARGET_DATABASE_NAME="ginesta" # O el nombre de tu DB local para pruebas
    FUDO_AUTH_ENDPOINT="https://auth.fu.do/api"
    FUDO_API_BASE_URL="https://api.fu.do"
    GCP_PROJECT_ID="local-dev-project" # ID de tu proyecto GCP, o dummy para local

    # Credenciales de Fu.do (todas las sucursales)
    FUDO_CHALE_APIKEY="MUAxNTk0MzM="
    FUDO_CHALE_APISECRET="oHxOKeifSqdOMt6nICBuOJHsBq4ZfBsA"
    FUDO_QUITO_BAR_APIKEY="..."
    FUDO_QUITO_BAR_APISECRET="..."
    FUDO_NEBRASKA_SAS_APIKEY="..."
    FUDO_NEBRASKA_SAS_APISECRET="..."
    FUDO_PUNTO_CRIOLLO_APIKEY="..."
    FUDO_PUNTO_CRIOLLO_APISECRET="..."
    ```
5.  **Preparar Base de Datos PostgreSQL (Local o Donweb):**
    *   **Local:** Asegúrate de tener PostgreSQL ejecutándose y una DB `fudo_etl_db` con el usuario `fudo_user`.
    *   **Donweb:** Si apuntas a Donweb, asegúrate de que la DB `ginesta` exista y que tu IP local esté en el firewall.

### **2. Ejecución Local del ETL**

**Para crear la DB `ginesta` en Donweb (¡SOLO UNA VEZ!):**
(Asegúrate de que `DONWEB_ADMIN_CONNECTION_STRING` en `.env` apunte a la DB `postgres` de Donweb).
```bash
python -m fudo_etl.deploy_db
Para desplegar la estructura de tablas/vistas en la DB ginesta (¡SOLO UNA VEZ por DB!):
(Asegúrate de que DB_CONNECTION_STRING en .env apunte a la DB ginesta de Donweb o tu DB local).
code
Bash
python -m fudo_etl.main # Esto ejecutará la fase de despliegue y luego la ETL RAW/Transformación
Para ejecuciones regulares (Extracción y Transformación):
(Asegúrate de que DB_CONNECTION_STRING en .env apunte a la DB ginesta de Donweb o tu DB local).
code
Bash
# Comentar la sección de despliegue de estructura en fudo_etl/main.py
# Y luego ejecutar:
python -m fudo_etl.main
☁️ Despliegue en Google Cloud Platform (GCP)
1. Configuración de GCP
Proyecto: ginestafudo
Región: us-central1 (para Cloud Run Jobs, Artifact Registry, Cloud Scheduler).
Habilitar APIs:
Cloud Build API, Cloud Run API, Secret Manager API, Cloud Scheduler API, Artifact Registry API, Cloud Storage API, IAM API.
Crear Secretos en Secret Manager:
Crea un secreto para cada variable mapeada en fudo_etl/cloudbuild.yaml (--set-secrets). Los nombres de los secretos deben ser exactos (ej. fudo-chale-apikey, donweb-db-connection-string, gcp-project-id con valor ginestafudo).
Configurar Repositorio de Artifact Registry:
Crea un repositorio Docker llamado ginesta-fudo-etl-repo en la región us-central1.
Configurar Permisos de Cuentas de Servicio:
Cloud Build Service Account ([PROJECT_NUMBER]-compute@developer.gserviceaccount.com):
Administrador de Cloud Run
Usuario de cuenta de servicio
Accesor de secretos de Secret Manager
Escritor de Artifact Registry
Escritor de registros (Logs Writer)
Lector de imágenes de Container Registry (si aún hubiera necesidad de leer de gcr.io)
Cloud Run Job Service Account (la misma que Cloud Build en este proyecto):
Accesor de secretos de Secret Manager
Invocador de Cloud Run (Este se añade a la SA que dispara el Scheduler, pero si el Job se invoca a sí mismo o se usa esta SA, también lo necesita).
2. Disparador de Cloud Build (CI/CD)
Configurar Disparador:
En Cloud Build > Disparadores, crea/edita el disparador ginesta-fudo-etl-deployment-trigger.
Repositorio: Sudataanalytics/ginesta (GitHub).
Rama: main.
Archivo de configuración de Cloud Build: fudo_etl/cloudbuild.yaml (¡ruta correcta dentro del repo!).
Cuenta de Servicio: 595327888136-compute@developer.gserviceaccount.com (la que configuraste con todos los permisos).
Ejecutar Disparador:
Haz un git push a main, o ejecuta manualmente el disparador para desplegar el ginesta-fudo-etl-job.
3. Programación con Cloud Scheduler
Crear Tarea:
En Cloud Scheduler, crea la tarea ginesta-fudo-etl-scheduler.
Región: us-central1.
Frecuencia: 0 8-23/2 * * * (Cada 2 horas, de 8 AM a 11 PM).
Zona Horaria: America/Argentina/Buenos_Aires.
Objetivo: Cloud Run.
Servicio de Cloud Run: Selecciona ginesta-fudo-etl-job (y su región us-central1).
Autenticación: OIDC token con la cuenta de servicio 595327888136-compute@developer.gserviceaccount.com (o la que tenga Cloud Run Invoker).
✅ Validación Final
Ejecutar Cloud Run Job Manualmente: Desde Cloud Run > Trabajos, haz clic en ginesta-fudo-etl-job y luego en "Ejecutar".
Monitorear Logs: En Cloud Logging, filtra por Resource: Cloud Run Job, ginesta-fudo-etl-job. Verifica que el ETL se ejecute de principio a fin sin errores.
Verificar DB en Donweb: Con pgAdmin, conecta a ginesta y verifica las tablas fudo_raw_* y mv_* para confirmar que los datos se están actualizando.
📊 Integración con Power BI
Una vez que el ETL esté operando en GCP y actualizando Donweb:
Configurar Power BI Gateway: En un servidor de la empresa, instala y configura el On-premises Data Gateway. Crea una fuente de datos PostgreSQL que apunte a vps-4657831-x.dattaweb.com:5432/ginesta con las credenciales de sudata_owner.
Publicar .pbix: La BA publicará el GrupoGinesta_Gastronomia_Dashboard_V1.pbix en Power BI Service.
Configurar Actualización Programada: En Power BI Service, en la configuración del conjunto de datos, asocia la fuente de datos con el Gateway y configura la actualización cada 2 horas (coincidiendo con el Scheduler).