import os
from dotenv import load_dotenv

def load_config() -> dict:
    """Carga la configuración desde variables de entorno."""
    load_dotenv() # Carga variables del archivo .env
    
    config = {
        "db_connection_string": os.getenv("DB_CONNECTION_STRING"),
        "fudo_auth_endpoint": os.getenv("FUDO_AUTH_ENDPOINT"),
        "fudo_api_base_url": os.getenv("FUDO_API_BASE_URL"),
        "gcp_project_id": os.getenv("GCP_PROJECT_ID") # Valor dummy para desarrollo local. En GCP será tu project ID real.
    }
    # Validaciones básicas para asegurar que las variables esenciales estén seteadas
    for key, value in config.items():
        if value is None:
            raise ValueError(f"Missing configuration: {key} environment variable not set. Check your .env file or system environment variables.")
    return config