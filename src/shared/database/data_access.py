# src/shared/database/data_access.py
import logging
from shared.database.db_config import SessionLocal, engine # Importe SessionLocal e engine
from shared.database.models import Base, Event, EventSourceMapping # Importe os novos modelos
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import pytz # Para timestamps conscientes de fuso horário
from sqlalchemy import or_ # Para a cláusula OR em consultas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataAccess:
    def __init__(self, session: Session): # Agora recebe uma sessão SQLAlchemy
        self.session = session

    def create_tables(self):
        """
        Cria todas as tabelas definidas nos modelos usando o engine do SQLAlchemy.
        Deve ser chamado uma vez na inicialização do coletor de dados.
        """
        try:
            Base.metadata.create_all(engine) # Usa o engine importado
            logging.info("Tabelas do banco de dados verificadas/criadas via SQLAlchemy.")
        except Exception as e:
            logging.error(f"Erro ao criar tabelas no banco de dados: {e}")
            raise # Re-lança o erro

    def save_or_update_event(self, normalized_event_data: dict, source_mapping_data: dict) -> Event | None:
        """
        Salva ou atualiza um evento canônico e seu mapeamento de fonte,
        garantindo que dados mais antigos não sobrescrevam dados mais novos.

        :param normalized_event_data: Dicionário com os dados normalizados para o modelo Event.
        :param source_mapping_data: Dicionário com os dados para o modelo EventSourceMapping.
        :return: O objeto Event persistido ou None em caso de falha.
        """
        session = self.session

        # --- Dados de entrada para o Evento Canônico ---
        # Assegure-se de que esses campos vêm do normalizador
        event_timestamp = normalized_event_data.get('event_timestamp')
        home_team_name = normalized_event_data.get('home_team_name')
        away_team_name = normalized_event_data.get('away_team_name')
        league_id = normalized_event_data.get('league_id') # Pode ser None, se a fonte não tiver league_id

        incoming_last_updated_timestamp = normalized_event_data.get('last_updated_timestamp')
        incoming_current_game_time = normalized_event_data.get('current_game_time')

        # --- Dados de entrada para o Mapeamento da Fonte ---
        source_name = source_mapping_data.get('source_name')
        source_event_id = source_mapping_data.get('source_event_id')

        if not all([event_timestamp, home_team_name, away_team_name, source_name, source_event_id, incoming_last_updated_timestamp]):
            logging.error(f"Dados essenciais faltando para save_or_update_event. Normalized: {normalized_event_data}, Source Mapping: {source_mapping_data}")
            return None

        existing_event = None

        try:
            # 1. Tentar encontrar o mapeamento da fonte primeiro
            # Isso é eficiente se o evento já foi processado por esta fonte antes
            existing_mapping = session.query(EventSourceMapping).filter_by(
                source_name=source_name,
                source_event_id=source_event_id
            ).first()

            if existing_mapping:
                existing_event = existing_mapping.event # Obtém o evento canônico associado
                logging.debug(f"Mapeamento existente encontrado para {source_name}:{source_event_id}. Evento canônico ID: {existing_event.id}")
            else:
                # Se o mapeamento não existe, tentar encontrar o evento canônico
                # por seus atributos principais (para evitar duplicidade do EVENTO REAL)
                # É importante que league_id seja considerado na unicidade se ele for consistente entre fontes
                # Se league_id puder ser diferente entre fontes para o MESMO evento canônico, ajuste a UniqueConstraint no models.py
                existing_event = session.query(Event).filter(
                    Event.event_timestamp == event_timestamp,
                    Event.home_team_name == home_team_name,
                    Event.away_team_name == away_team_name,
                    Event.league_id == league_id # Considere remover ou ajustar se league_id não for canônico
                ).first()
                if existing_event:
                    logging.debug(f"Evento canônico existente encontrado por atributos: ID {existing_event.id}. Preparando para criar novo mapeamento.")

            if existing_event:
                # Lógica de atualização para o evento canônico
                should_update = False

                # 1a. Prioridade: last_updated_timestamp mais recente
                if incoming_last_updated_timestamp > existing_event.last_updated_timestamp:
                    should_update = True
                    logging.debug(f"  -> Evento ID {existing_event.id}: Atualizando por timestamp de processamento mais recente ({incoming_last_updated_timestamp} > {existing_event.last_updated_timestamp}).")
                elif incoming_last_updated_timestamp == existing_event.last_updated_timestamp:
                    # 1b. Segundo desempate: current_game_time mais avançado (para jogos em andamento)
                    # Só compara se o status atual ou o recebido for 'inprogress' ou 'live'
                    if (normalized_event_data.get('event_status') == 'inprogress' or existing_event.event_status == 'inprogress') and \
                       incoming_current_game_time is not None and \
                       (existing_event.current_game_time is None or incoming_current_game_time > existing_event.current_game_time):
                        should_update = True
                        logging.debug(f"  -> Evento ID {existing_event.id}: Atualizando por tempo de jogo mais avançado ({incoming_current_game_time} > {existing_event.current_game_time}).")
                    else:
                        logging.debug(f"  -> Evento ID {existing_event.id}: Ignorando atualização (dados existentes são iguais ou mais novos e sem tempo de jogo relevante).")
                else:
                    logging.debug(f"  -> Evento ID {existing_event.id}: Ignorando atualização (dados recebidos são mais antigos ({incoming_last_updated_timestamp} < {existing_event.last_updated_timestamp})).")

                if should_update:
                    # Atualizar campos do evento canônico com os novos dados
                    # Certifique-se de que normalized_event_data contêm todos os campos do modelo Event
                    for key, value in normalized_event_data.items():
                        if hasattr(existing_event, key): # Evita erro se a chave não for um atributo do modelo
                            setattr(existing_event, key, value)
                    # Garante que a fonte e o timestamp de quem fez a última atualização são registrados
                    existing_event.last_data_source = source_name
                    existing_event.last_updated_timestamp = incoming_last_updated_timestamp # Atualiza com o timestamp do dado recebido
                    session.add(existing_event) # Marca para atualização
                    logging.info(f"Evento ID {existing_event.id} ({existing_event.event_name}) atualizado com sucesso pela fonte {source_name}.")
            else:
                # Criar um novo evento canônico se não foi encontrado
                logging.info(f"Criando novo evento canônico para: {normalized_event_data.get('event_name')} da fonte {source_name}.")
                new_event = Event(**normalized_event_data)
                new_event.last_data_source = source_name # Define a primeira fonte que o criou
                session.add(new_event)
                session.flush() # Importante para que 'new_event.id' seja populado antes do commit

                existing_event = new_event # O evento recém-criado é agora o evento "existente"
                logging.info(f"Novo evento canônico ID: {existing_event.id} criado.")

            # 2. Salvar/Atualizar mapeamento da fonte
            if not existing_mapping:
                # Se não há mapeamento para esta fonte/ID, crie um
                logging.info(f"Criando mapeamento para {source_name}:{source_event_id} -> Evento ID: {existing_event.id}")
                new_mapping = EventSourceMapping(
                    event_id=existing_event.id,
                    source_name=source_name,
                    source_event_id=source_event_id
                )
                session.add(new_mapping)

            session.commit()
            logging.info(f"Operação de persistência concluída para evento: {existing_event.event_name} (ID: {existing_event.id})")
            return existing_event

        except IntegrityError as e:
            session.rollback()
            logging.error(f"Erro de integridade ao salvar/atualizar evento {source_event_id} ({source_name}): {e}. Tentativa de criar evento/mapeamento duplicado.", exc_info=True)
            # Tenta recuperar o evento existente se o erro for por unique constraint do evento canônico
            existing_event_after_rollback = session.query(Event).filter(
                Event.event_timestamp == event_timestamp,
                Event.home_team_name == home_team_name,
                Event.away_team_name == away_team_name,
                Event.league_id == league_id
            ).first()
            if existing_event_after_rollback:
                logging.warning(f"Conflito resolvido: Evento canônico já existia. Retornando evento existente ID: {existing_event_after_rollback.id}.")
                return existing_event_after_rollback
            return None # Falha em encontrar ou resolver

        except Exception as e:
            session.rollback()
            logging.critical(f"Erro inesperado e crítico ao salvar/atualizar evento {source_event_id} ({source_name}): {e}", exc_info=True)
            return None

    def get_events_for_monitoring(self, time_buffer_minutes: int = 60) -> list[Event]:
        """
        Busca eventos que estão 'inprogress' ou 'scheduled' e começarão/continuarão
        dentro de um período de tempo (ex: próximos 60 minutos ou já em andamento).
        """
        current_time_utc = datetime.now(pytz.utc)
        # Considera eventos que começaram até `time_buffer_minutes` atrás (para 'inprogress' que pode ter ficado como 'scheduled' por um tempo)
        # e eventos que começarão até `time_buffer_minutes` no futuro.
        time_threshold = current_time_utc - timedelta(minutes=time_buffer_minutes)

        events = self.session.query(Event).filter(
            or_(
                Event.event_status == 'inprogress',
                (Event.event_status == 'scheduled') & (Event.event_timestamp >= time_threshold)
            )
        ).order_by(Event.event_timestamp.asc()).all()

        logging.info(f"Encontrados {len(events)} eventos para monitoramento (ao vivo/próximos).")
        return events

    def get_next_scheduled_event_start_time(self) -> int | None:
        """
        Busca o timestamp Unix de início do próximo evento com status 'scheduled'.
        Usado para a lógica de hibernação.
        """
        current_time_utc = datetime.now(pytz.utc)
        next_event = self.session.query(Event).filter(
            Event.event_status == 'scheduled',
            Event.event_timestamp > current_time_utc
        ).order_by(Event.event_timestamp.asc()).first()

        if next_event:
            next_timestamp = int(next_event.event_timestamp.timestamp())
            logging.info(f"Próximo timestamp de evento agendado: {next_timestamp}")
            return next_timestamp

        logging.info("Nenhum evento futuro agendado encontrado no banco de dados.")
        return None

    # Métodos de consulta adicionais podem ser adicionados aqui conforme necessário
    def get_event_by_id(self, event_id: int) -> Event | None:
        """Busca um evento canônico pelo seu ID primário."""
        return self.session.query(Event).get(event_id)

    def get_event_by_source_id(self, source_name: str, source_event_id: str) -> Event | None:
        """Busca um evento canônico através do seu mapeamento de fonte."""
        mapping = self.session.query(EventSourceMapping).filter_by(
            source_name=source_name,
            source_event_id=source_event_id
        ).first()
        return mapping.event if mapping else None