# fudo_etl/modules/get_secret.py
import os
import logging
# from google.cloud import secretmanager # Mantener comentado para ejecución local

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
    # --- DESCOMENTAR ESTA SECCIÓN Y ASEGURAR 'google-cloud-secret-manager' en requirements.txt para GCP ---
    # else: # Esto se ejecutará si project_id NO es "local-dev-project"
    #     try:
    #         client = secretmanager.SecretManagerServiceClient()
    #         name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    #         response = client.access_secret_version(name=name)
    #         logger.info(f"Secret '{secret_name}' obtenido de Google Secret Manager.")
    #         return response.payload.data.decode("UTF-8")
    #     except Exception as e:
    #         logger.error(f"Failed to access secret '{secret_name}' from Secret Manager: {e}", exc_info=True)
    #         raise ConnectionError(f"Failed to access secret '{secret_name}' from Secret Manager.")
    # -----------------------------------------------------------------------------------------------------
    
    # Si GCP_PROJECT_ID no es "local-dev-project" y la sección de Secret Manager está comentada/falla,
    # esto debería lanzar un error.
    raise RuntimeError(f"Unexpected state for secret '{secret_name}'. Neither local .env nor GCP Secret Manager could be accessed.")