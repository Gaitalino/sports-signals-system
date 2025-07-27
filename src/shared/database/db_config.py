# src/shared/database/db_config.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
from dotenv import load_dotenv # Importe para carregar variáveis do .env

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carrega variáveis de ambiente do arquivo .env (se existir)
# Garanta que esta linha seja chamada no ponto de entrada da aplicação também
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# Define a URL de conexão com o banco de dados
# postgresql+psycopg2://user:password@host:port/dbname
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    # Cria o engine do SQLAlchemy
    # pool_pre_ping verifica se a conexão está viva antes de usá-la do pool
    # echo=False para não logar cada SQL gerado (mude para True para debug)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, echo=False)
    logging.info("SQLAlchemy Engine criado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao criar o SQLAlchemy Engine: {e}")
    raise # Re-lança o erro fatal

# Cria uma SessionLocal
# autoflush=False: Dados não são enviados automaticamente para o DB até o commit ou flush explícito
# autocommit=False: Desativa o autocommit, permitindo controle transacional explícito
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logging.info("SQLAlchemy SessionLocal configurada.")

# Função utilitária para obter uma sessão de banco de dados
# Isso é útil para injeção de dependência ou uso em scripts
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        logging.debug("Sessão de banco de dados fechada.")