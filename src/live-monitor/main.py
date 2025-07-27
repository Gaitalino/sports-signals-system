import logging
import time
import redis
import json
import os
from datetime import datetime, timedelta

# Importa as novas classes de estratégia anti-bloqueio e acesso a dados
from shared.core.anti_block import AntiBlockStrategy, TokenBucketAntiBlockStrategy
from shared.adapters.sofascore_adapter import SofascoreAdapter
from shared.core.normalizer import DataNormalizer
from shared.database.data_access import DataAccess
from shared.database.db_config import SessionLocal


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def monitor_live_matches():
    """
    Monitora partidas ao vivo e próximas, atualiza o DB e publica notificações no Redis.
    Implementa lógica de hibernação inteligente.
    """
    logging.info("Iniciando o serviço de Monitoramento ao Vivo (Live Monitor)...")

    # Configurações de polling
    # Intervalo de polling quando há partidas ativas (em andamento ou próximas)
    ACTIVE_POLL_INTERVAL_SECONDS = 15
    # Intervalo de polling quando o monitor está em "hibernação"
    HIBERNATION_POLL_INTERVAL_SECONDS = 300 # 5 minutos
    # Buffer de tempo para considerar partidas 'agendadas' como 'próximas'
    # Ex: 30 minutos antes do início para começar a monitorar de perto
    MATCH_PROXIMITY_BUFFER_MINUTES = 30

    # Inicializa as dependências
    anti_block_strategy = TokenBucketAntiBlockStrategy(capacity=5, fill_rate=0.2) # Ajuste a capacidade e fill_rate
    sofascore_adapter = SofascoreAdapter(anti_block_strategy=anti_block_strategy)
    normalizer = DataNormalizer()
     # ONDE ESTAVA: data_access = DataAccess() # Nova instância do DataAccess

    # Substitua a linha acima por:
    # A sessão será criada a cada ciclo do loop principal
    # e fechada no finally.
    # Esta abordagem é mais robusta para garantir sessões frescas por ciclo.

     # Loop principal de monitoramento
    while True:
        db_session = SessionLocal() # Cria uma nova sessão para este ciclo
        try:
            data_access = DataAccess(session=db_session)

            # ... seu código principal do live_monitor_service (tudo isso DEVE estar indentado)
            # ...
            
        # ESTES BLOCOS 'except' e 'finally' DEVEM TER O MESMO NÍVEL DE IDENTAÇÃO DO 'try'
        except redis.exceptions.ConnectionError as e: # Linha 58 (exemplo)
            # O conteúdo deste bloco (logging, reconexão) DEVE ESTAR IDENTADO
            logging.error(f"Conexão com Redis perdida ou falhou: {e}. Tentando reconectar no próximo ciclo.")
            redis_client = None
            try:
                redis_host = os.getenv("REDIS_HOST", "localhost")
                redis_port = int(os.getenv("REDIS_PORT", 6379))
                redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
                redis_client.ping()
                logging.info("Redis reconectado com sucesso.")
            except Exception as reconnect_e:
                logging.error(f"Falha ao reconectar ao Redis: {reconnect_e}")
                time.sleep(30)
        except Exception as e: # Linha 60 (exemplo)
            # O conteúdo deste bloco (logging, sleep) DEVE ESTAR IDENTADO
            logging.critical(f"Erro fatal no Live Monitor: {e}", exc_info=True)
            time.sleep(30)
        finally: # Este 'finally' DEVE TER O MESMO NÍVEL DE IDENTAÇÃO DO 'try'
            # O conteúdo deste bloco (db_session.close(), logging) DEVE ESTAR IDENTADO
            db_session.close()
            logging.debug("Sessão do banco de dados fechada.")

    # Conexão com Redis
    redis_client = None
    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        redis_client.ping() # Testa a conexão
        logging.info(f"Conectado ao Redis em {redis_host}:{redis_port}.")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Não foi possível conectar ao Redis: {e}. As notificações não serão publicadas.")
        redis_client = None # Garante que o cliente Redis seja None se a conexão falhar

    # Loop principal de monitoramento
    while True:
        try:
            # 1. Identificar partidas ativas (em andamento ou próximas)
            active_matches = data_access.get_upcoming_and_live_matches(
                time_buffer_minutes=MATCH_PROXIMITY_BUFFER_MINUTES
            )
            
            poll_interval = ACTIVE_POLL_INTERVAL_SECONDS
            
            if active_matches:
                logging.info(f"Monitorando {len(active_matches)} partidas ativas (ao vivo/próximas).")
                for match_info in active_matches:
                    source_id = match_info['source_id']
                    current_status = match_info['status']
                    
                    logging.info(f"Coletando dados detalhados para partida {source_id} (status: {current_status})...")
                    raw_match_data = sofascore_adapter.get_match_data(source_id) # Obtenha dados detalhados
                    
                    if raw_match_data:
                        normalized_data = normalizer.normalize_sofascore_data(raw_match_data)
                        if normalized_data:
                            # Salva/Atualiza no DB. save_match_data já faz UPSERT.
                            success = data_access.save_match_data(normalized_data)
                            
                            if success:
                                logging.info(f"Partida {source_id} atualizada no DB. Publicando no Redis...")
                                if redis_client:
                                    # Crie uma mensagem para o Redis. Adicione todos os campos relevantes.
                                    message = {
                                        "source_id": normalized_data.get("source_id"),
                                        "status": normalized_data.get("status"),
                                        "home_team_name": normalized_data.get("home_team_name"),
                                        "away_team_name": normalized_data.get("away_team_name"),
                                        "home_score": normalized_data.get("home_score"),
                                        "away_score": normalized_data.get("away_score"),
                                        "minute": normalized_data.get("minute"),
                                        "start_time": normalized_data.get("start_time"),
                                        "updated_at": int(time.time())
                                    }
                                    redis_client.publish("match_updates", json.dumps(message))
                                    logging.info(f"Publicada atualização para partida {source_id} no Redis.")
                                else:
                                    logging.warning(f"Redis não conectado. Não foi possível publicar atualização para partida {source_id}.")
                            else:
                                 logging.warning(f"Partida {source_id} não foi atualizada no DB (pode ser que os dados não mudaram ou houve erro).")
                        else:
                            logging.warning(f"Não foi possível normalizar dados atualizados para partida {source_id}.")
                    else:
                        logging.warning(f"Não foi possível coletar dados atualizados para partida {source_id}.")
                
                # Após processar todas as partidas ativas, aguarde o intervalo de polling ativo
                logging.info(f"Ciclo de monitoramento de partidas ativas concluído. Próximo ciclo em {poll_interval} segundos.")
                time.sleep(poll_interval)

            else:
                # Nenhuma partida ativa/próxima, entrar em modo de hibernação
                logging.info("Nenhuma partida ativa ou próxima. Entrando em modo de hibernação...")
                
                next_match_start_time_unix = data_access.get_next_scheduled_match_start_time()
                
                if next_match_start_time_unix:
                    next_match_datetime = datetime.fromtimestamp(next_match_start_time_unix)
                    current_datetime = datetime.now()
                    
                    # Calcula o tempo até o buffer antes do próximo jogo
                    # Queremos acordar 'MATCH_PROXIMITY_BUFFER_MINUTES' antes do jogo
                    wake_up_time = next_match_datetime - timedelta(minutes=MATCH_PROXIMITY_BUFFER_MINUTES)
                    
                    time_to_wait_seconds = (wake_up_time - current_datetime).total_seconds()
                    
                    if time_to_wait_seconds > 0:
                        # Se o tempo de espera calculado é muito longo, limite-o ao HIBERNATION_POLL_INTERVAL_SECONDS
                        # para garantir que o monitor ainda verifique periodicamente em caso de falha de agendamento ou nova partida.
                        wait_duration = min(time_to_wait_seconds, HIBERNATION_POLL_INTERVAL_SECONDS)
                        logging.info(f"Próxima partida agendada para {next_match_datetime.strftime('%Y-%m-%d %H:%M:%S')}. Hibernando por {wait_duration:.0f} segundos.")
                        time.sleep(wait_duration)
                    else:
                        # O tempo calculado já passou ou é negativo (jogo já deveria ter começado ou está muito próximo)
                        # Então, apenas espera o intervalo de hibernação padrão.
                        logging.info(f"Próxima partida ({next_match_datetime.strftime('%Y-%m-%d %H:%M:%S')}) já está muito próxima ou passou. Aguardando {HIBERNATION_POLL_INTERVAL_SECONDS} segundos.")
                        time.sleep(HIBERNATION_POLL_INTERVAL_SECONDS)
                else:
                    # Nenhuma partida agendada no futuro, apenas hiberna pelo intervalo padrão
                    logging.info(f"Nenhuma partida agendada no futuro. Hibernando por {HIBERNATION_POLL_INTERVAL_SECONDS} segundos.")
                    time.sleep(HIBERNATION_POLL_INTERVAL_SECONDS)

        except redis.exceptions.ConnectionError as e:
            logging.error(f"Conexão com Redis perdida ou falhou: {e}. Tentando reconectar no próximo ciclo.")
            redis_client = None # Reseta o cliente para tentar reconectar no próximo loop
            # Tenta reconectar imediatamente para não esperar um ciclo completo
            try:
                redis_host = os.getenv("REDIS_HOST", "localhost")
                redis_port = int(os.getenv("REDIS_PORT", 6379))
                redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
                redis_client.ping()
                logging.info("Redis reconectado com sucesso.")
            except Exception as reconnect_e:
                logging.error(f"Falha ao reconectar ao Redis: {reconnect_e}")
                time.sleep(30) # Espera um pouco mais antes de tentar novamente
        except Exception as e:
            logging.critical(f"Erro fatal no Live Monitor: {e}", exc_info=True)
            time.sleep(30) # Espera mais em caso de erro para evitar loops rápidos e sobrecarga

if __name__ == "__main__":
    monitor_live_matches()