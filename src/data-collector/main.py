# src/data-collector/main.py
import logging
import time
from datetime import datetime, timedelta

# Importa a nova estratégia de anti-bloqueio e a interface
from shared.core.anti_block import AntiBlockStrategy, TokenBucketAntiBlockStrategy
from shared.adapters.sofascore_adapter import SofascoreAdapter
from shared.adapters.thesportsdb_adapter import TheSportsDBAdapter # Novo adaptador
from shared.core.normalizer import DataNormalizer

# Importa a nova classe de acesso a dados e a função get_db
from shared.database.data_access import DataAccess
from shared.database.db_config import get_db, engine # Importa engine para create_all
from shared.database.models import Base # Importa Base para create_all

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_database():
    """Garante que as tabelas do SQLAlchemy sejam criadas."""
    logging.info("Inicializando o banco de dados (criando tabelas se não existirem)...")
    Base.metadata.create_all(engine)
    logging.info("Inicialização do banco de dados concluída.")

def collect_and_save_data():
    """
    Coleta dados de múltiplas fontes (Sofascore, TheSportsDB) e os salva/atualiza
    no banco de dados canônico usando a lógica do SQLAlchemy ORM.
    """
    logging.info("Iniciando o serviço de Coleta de Dados (Data Collector)...")

    # Configura as estratégias de anti-bloqueio
    sofascore_anti_block = TokenBucketAntiBlockStrategy(capacity=20, fill_rate=1.0) # Ajustado para maior taxa
    thesportsdb_anti_block = TokenBucketAntiBlockStrategy(capacity=10, fill_rate=0.5) # TheSportsDB pode ser mais restritivo

    # Inicializa adaptadores e normalizador
    sofascore_adapter = SofascoreAdapter(anti_block_strategy=sofascore_anti_block)
    thesportsdb_adapter = TheSportsDBAdapter() # Anti-block não está no construtor do TheSportsDBAdapter ainda

    normalizer = DataNormalizer()

    # Obter uma sessão de DB usando o padrão de yield (context manager)
    # Isso garante que a sessão seja fechada automaticamente
    db_session_generator = get_db()
    db_session = next(db_session_generator) # Obtém a sessão do gerador
    data_access = DataAccess(session=db_session)

    try:
        # 1. Coleta e processa dados do Sofascore (principal para live/detalhes)
        logging.info("Coletando dados do Sofascore...")
        all_sofascore_events = sofascore_adapter.get_todays_and_tomorrows_matches_events() 

        if not all_sofascore_events:
            logging.info("Nenhum evento do Sofascore encontrado para coletar.")
        else:
            logging.info(f"Encontrados {len(all_sofascore_events)} eventos do Sofascore para processamento.")
            for event_data_sofascore in all_sofascore_events:
                normalized_event, source_mapping = normalizer.normalize_sofascore_match(event_data_sofascore)
                if normalized_event and source_mapping:
                    persisted_event = data_access.save_or_update_event(normalized_event, source_mapping)
                    if not persisted_event:
                        logging.error(f"Falha ao salvar/atualizar evento Sofascore {source_mapping.get('source_event_id')}.")
                else:
                    logging.warning(f"Não foi possível normalizar dados do Sofascore para o evento {event_data_sofascore.get('id')}. Pulando.")

        # 2. Coleta e processa dados do TheSportsDB (complementar, principalmente agendados)
        # Para TheSportsDB, você pode querer iterar por ligas ou buscar um conjunto limitado de eventos.
        # Este é um exemplo simplificado:
        logging.info("Coletando dados do TheSportsDB (exemplo - buscando eventos da liga 4328 - Premier League 2024-2025)...")
        # Substitua '4328' e '2024-2025' por lógica dinâmica ou configuração
        thesportsdb_events = thesportsdb_adapter.get_events_by_league_id(league_id="4328", season="2024-2025") # Exemplo Premier League

        if not thesportsdb_events:
            logging.info("Nenhum evento do TheSportsDB encontrado para coletar.")
        else:
            logging.info(f"Encontrados {len(thesportsdb_events)} eventos do TheSportsDB para processamento.")
            for event_data_thesportsdb in thesportsdb_events:
                # Chame o adaptador fetch_event_details para obter dados mais completos se necessário
                # thesportsdb_full_details = thesportsdb_adapter.fetch_event_details(event_data_thesportsdb.get('idEvent'))
                # if thesportsdb_full_details:
                #     event_data_thesportsdb.update(thesportsdb_full_details) # Mescla os detalhes

                normalized_event, source_mapping = normalizer.normalize_thesportsdb_match(event_data_thesportsdb)
                if normalized_event and source_mapping:
                    persisted_event = data_access.save_or_update_event(normalized_event, source_mapping)
                    if not persisted_event:
                        logging.error(f"Falha ao salvar/atualizar evento TheSportsDB {source_mapping.get('source_event_id')}.")
                else:
                    logging.warning(f"Não foi possível normalizar dados do TheSportsDB para o evento {event_data_thesportsdb.get('idEvent')}. Pulando.")

    except Exception as e:
        logging.critical(f"Erro fatal no Data Collector: {e}", exc_info=True)
    finally:
        # Garante que a sessão seja fechada, mesmo em caso de erro
        try:
            db_session_generator.close()
        except RuntimeError:
            # Gerador já foi fechado ou não foi usado
            pass
        logging.info("Serviço de Coleta de Dados finalizado.")

if __name__ == "__main__":
    initialize_database() # Garante que as tabelas existam na inicialização
    collect_and_save_data()