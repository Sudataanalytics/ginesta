# fudo_etl/modules/get_secret.py
import os
import logging
from google.cloud import secretmanager # <--- ¡DESCOMENTAR ESTA LÍNEA!

logger = logging.getLogger(__name__)

def get_secret(secret_name: str, project_id: str = None) -> str:
    """
    Accede a un secreto. En local (GCP_PROJECT_ID="local-dev-project"), lee de variables de entorno.
    En GCP (project_id real), usa Google Secret Manager.
    """
    # Si estamos en entorno local de desarrollo (GCP_PROJECT_ID en .env es "local-dev-project")
    if project_id == "local-dev-project":
        env_value = os.getenv(secret_name)
        if env_value:
            logger.debug(f"Secret '{secret_name}' obtenido de variable de entorno local.")
            return env_value
        else:
            raise ValueError(f"Secret '{secret_name}' not found in local .env file. Please add it.")
    
    # Si no estamos en entorno local, intentamos Secret Manager (para GCP)
    # --- ESTA SECCIÓN DEBE ESTAR DESCOMENTADA PARA GCP ---
    else: # Esto se ejecutará si project_id NO es "local-dev-project" (es decir, es un ID real de GCP)
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(name=name)
            logger.info(f"Secret '{secret_name}' obtenido de Google Secret Manager.")
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to access secret '{secret_name}' from Secret Manager: {e}", exc_info=True)
            raise ConnectionError(f"Failed to access secret '{secret_name}' from Secret Manager.")
    # -----------------------------------------------------------------------------------------------------
    
    # Esta línea solo se alcanzaría si la lógica de arriba no cubre un caso.
    # En el flujo normal, no debería alcanzarse en GCP.
    raise RuntimeError(f"Unexpected state for secret '{secret_name}'. Neither local .env nor GCP Secret Manager could be accessed.")