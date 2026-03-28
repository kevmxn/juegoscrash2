# Usa una imagen oficial de Python
FROM python:3.10-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de dependencias primero (mejora caché)
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código
COPY main.py .

# Expone el puerto que usará Render (se asigna automáticamente)
EXPOSE 10000

# Comando para ejecutar la aplicación
CMD ["python", "main.py"]
