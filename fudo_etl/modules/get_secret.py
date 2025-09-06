import os
import logging

# Comenta o elimina esta línea para pruebas locales sin la lib de GCP
# from google.cloud import secretmanager 

logger = logging.getLogger(__name__)

def get_secret(secret_name: str, project_id: str = None) -> str:
    """
    Accede a un secreto. En local, intenta leer de variables de entorno.
    En un entorno de Cloud con GCP_PROJECT_ID configurado, intentará Google Secret Manager (si la lib está instalada).
    """
    # 1. Intentar leer de variables de entorno (para desarrollo local)
    env_value = os.getenv(secret_name)
    if env_value:
        logger.debug(f"Secret '{secret_name}' obtenido de variable de entorno local.")
        return env_value

    # 2. Si no se encuentra en ENV y se proporcionó un project_id real,
    # intenta con Google Secret Manager (para despliegue en GCP).
    # Descomenta esta parte cuando vayas a desplegar en GCP o probar con Secret Manager.
    # Asegúrate de instalar 'google-cloud-secret-manager' si lo descomentas.
    """
    if project_id and project_id != "local-dev-project": # Evita usar el dummy project_id para Secret Manager
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(name=name)
            logger.info(f"Secret '{secret_name}' obtenido de Google Secret Manager.")
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to access secret '{secret_name}' from Secret Manager: {e}", exc_info=True)
            raise ConnectionError(f"Failed to access secret '{secret_name}' from Secret Manager.")
    """
    
    # Si no se encontró ni en ENV local ni se intentó Secret Manager
    raise ValueError(f"Secret '{secret_name}' not found in local environment variables and no GCP Secret Manager access attempted/successful.")