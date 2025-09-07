# Usa una imagen base de Python ligera
FROM python:3.10-slim-buster

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia los archivos de requerimientos e instálalos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código de tu aplicación fudo-etl al directorio de trabajo
# Esto copia main.py, deploy_db.py, modules/, sql/, etc. a /app/
COPY . . 

# Comando para ejecutar el ETL principal de Fudo
# Asumiendo que 'main.py' está directamente en el WORKDIR /app
CMD ["python", "main.py"] 