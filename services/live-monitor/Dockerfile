# services/live-monitor/Dockerfile
FROM sports-signals-system-base:latest

WORKDIR /app

# Copia TODO o diretório 'src' da raiz do host para /app/src no container.
# Isso inclui src/data-collector, src/live-monitor e src/shared.
COPY src/ /app/src/

# Define o PYTHONPATH para que Python encontre os módulos dentro de /app/src
ENV PYTHONPATH=/app/src

# Comando para rodar a aplicação
# O main.py está em /app/src/live-monitor/main.py
CMD ["python", "/app/src/live-monitor/main.py"]