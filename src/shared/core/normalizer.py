# src/shared/core/normalizer.py
import logging
from datetime import datetime, timezone # Importa datetime e timezone para gerenciar UTC
import pytz # Para manipulação de fuso horário em timestamps Unix
import json # Usado para estatísticas, embora JSONB gerencie internamente no ORM

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataNormalizer:
    def __init__(self):
        # Define o fuso horário UTC para garantir consistência
        self.utc_timezone = pytz.utc

    def _get_current_utc_timestamp(self) -> int:
        """Retorna o timestamp Unix UTC atual."""
        return int(datetime.now(self.utc_timezone).timestamp())

    def _convert_timestamp_to_utc_datetime(self, unix_timestamp: int) -> datetime:
        """Converte um timestamp Unix para um objeto datetime UTC."""
        return datetime.fromtimestamp(unix_timestamp, tz=self.utc_timezone)

    def _normalize_sofascore_statistics(self, raw_stats: dict) -> dict:
        """
        Normaliza as estatísticas brutas do Sofascore para o formato JSONB padrão.
        Isso é um exemplo e deve ser expandido para incluir todas as estatísticas relevantes.
        """
        normalized = {
            "home": {},
            "away": {},
            "total": {} # Para estatísticas totais se aplicável
        }

        if not raw_stats or not raw_stats.get('periods'):
            return normalized

        # Exemplo: Agregando estatísticas de "allPeriods" se disponível
        all_periods_stats = raw_stats.get('periods', [])
        if all_periods_stats:
            # O Sofascore muitas vezes tem estatísticas por período.
            # Para uma visão geral, podemos pegar as estatísticas finais (geralmente no último período ou em 'overall')
            # ou agregar de todos os períodos. Por simplicidade, vamos pegar o 'overall' se disponível,
            # ou o primeiro/último período mais relevante.
            overall_stats = next((p for p in all_periods_stats if p.get('type') == 'overall'), None)

            stats_to_process = overall_stats if overall_stats else (all_periods_stats[0] if all_periods_stats else {})

            # Processa os grupos de estatísticas
            groups = stats_to_process.get('groups', [])
            for group in groups:
                group_name = group.get('groupName', '').lower()

                for stat in group.get('statisticsItems', []):
                    name = stat.get('name')
                    home_value = stat.get('home')
                    away_value = stat.get('away')

                    if name and (home_value is not None or away_value is not None):
                        # Exemplo de mapeamento:
                        if name == 'ball_possession':
                            normalized['home']['possession'] = home_value
                            normalized['away']['possession'] = away_value
                        elif name == 'total_shots':
                            normalized['home']['total_shots'] = home_value
                            normalized['away']['total_shots'] = away_value
                        elif name == 'shots_on_target':
                            normalized['home']['shots_on_target'] = home_value
                            normalized['away']['shots_on_target'] = away_value
                        # Adicione mais mapeamentos conforme necessário para outras estatísticas
                        # Ex: 'goals', 'corners', 'yellow_cards', 'red_cards', 'offsides', etc.
                        # Alguns dados podem ser numéricos, outros strings. Converta conforme apropriado.
                        # Para "goals", você pode querer extrair de outro lugar se não estiver aqui.
        return normalized

    def _normalize_thesportsdb_statistics(self, raw_data: dict) -> dict:
        """
        Normaliza as estatísticas brutas do TheSportsDB para o formato JSONB padrão.
        NOTA: TheSportsDB geralmente tem dados de estatísticas muito limitados ou ausentes
        na API gratuita, especialmente para eventos ao vivo. Esta função pode retornar
        muitos valores padrão.
        """
        normalized = {
            "home": {},
            "away": {},
            "total": {}
        }
        # TheSportsDB geralmente não tem estatísticas detalhadas no endpoint principal de eventos.
        # Se você usar um endpoint de detalhes de evento ou estatísticas (lookupEventStats.php),
        # precisará analisar a estrutura específica.
        # Por enquanto, preenche com o básico.
        return normalized

    def normalize_sofascore_match(self, raw_data: dict) -> tuple[dict | None, dict | None]:
        """
        Normaliza os dados brutos de partida do Sofascore para os modelos Event e EventSourceMapping.

        :param raw_data: Dados brutos de uma partida do Sofascore.
        :return: Uma tupla (normalized_event_data, source_mapping_data) ou (None, None) se falhar.
        """
        if not raw_data:
            logging.warning("Dados brutos do Sofascore são nulos ou vazios para normalização.")
            return None, None

        try:
            # Extrai o ID da fonte e o nome da fonte
            source_event_id = str(raw_data.get("id"))
            source_name = "sofascore"

            # Campos para EventSourceMapping
            source_mapping_data = {
                "source_name": source_name,
                "source_event_id": source_event_id
            }

            # Campos para Event
            status_info = raw_data.get("status", {})
            # Mapeia status do Sofascore para um padrão mais genérico
            sofascore_status_type = status_info.get("type")

            event_status_map = {
                "notstarted": "scheduled",
                "inprogress": "inprogress",
                "finished": "finished",
                "canceled": "cancelled", # A ortografia no Sofascore pode ser 'canceled' ou 'cancelled'
                "postponed": "postponed",
                "interrupted": "paused"
                # Adicione outros mapeamentos conforme necessário
            }
            event_status = event_status_map.get(sofascore_status_type, "unknown")


            # Horário de início
            start_timestamp_unix = raw_data.get("startTimestamp")
            if start_timestamp_unix:
                event_timestamp = self._convert_timestamp_to_utc_datetime(start_timestamp_unix)
            else:
                logging.error(f"Timestamp de início ausente para evento Sofascore {source_event_id}. Pulando.")
                return None, None

            # Minuto atual do jogo (se em andamento)
            current_game_time = raw_data.get("time", {}).get("currentPeriodStartTimestamp") # Pode ser None
            # O Sofascore pode ter o 'minute' em outro lugar, como 'changes.minute' ou 'time.minute'
            # Verifique a estrutura real do payload para ser mais preciso
            # current_game_time = raw_data.get("changes", {}).get("minute") 
            # Ou: current_game_time = raw_data.get("time", {}).get("minute")

            # Placar
            home_score = raw_data.get("homeScore", {}).get("current")
            away_score = raw_data.get("awayScore", {}).get("current")

            # Nomes dos times
            home_team_name = raw_data.get("homeTeam", {}).get("name")
            away_team_name = raw_data.get("awayTeam", {}).get("name")

            # Liga/Torneio
            league_name = raw_data.get("tournament", {}).get("name")
            if not league_name:
                league_name = raw_data.get("uniqueTournament", {}).get("name")
            league_id = str(raw_data.get("tournament", {}).get("id")) # Use o ID do torneio como league_id

            # Nome do Esporte
            # O Sofascore pode ter 'sport.name' ou 'category.name'
            sport_name = raw_data.get("sport", {}).get("name", "football") # Default para football

            # Normaliza as estatísticas
            statistics = self._normalize_sofascore_statistics(raw_data.get('statistics', {}))

            # Geração do last_updated_timestamp (timestamp Unix no momento do processamento)
            last_updated_timestamp = self._get_current_utc_timestamp()

            # Nome do evento canônico (combinação de times)
            event_name = f"{home_team_name} vs {away_team_name}"

            normalized_event_data = {
                "event_name": event_name,
                "sport_name": sport_name,
                "event_status": event_status,
                "current_game_time": current_game_time if current_game_time is not None else 0, # Garante que seja um int ou None
                "home_team_name": home_team_name,
                "away_team_name": away_team_name,
                "home_score": home_score if home_score is not None else 0,
                "away_score": away_score if away_score is not None else 0,
                "league_name": league_name,
                "league_id": league_id,
                "event_timestamp": event_timestamp,
                "last_data_source": source_name, # Será sobrescrito pelo DataAccess, mas útil para o primeiro save
                "last_updated_timestamp": last_updated_timestamp,
                "statistics": statistics
                # created_at e updated_at serão definidos pelo ORM
            }

            # Validação básica de campos essenciais para o Evento Canônico
            if not all([event_name, sport_name, home_team_name, away_team_name, event_timestamp]):
                logging.error(f"Dados essenciais faltando após normalização do Sofascore para evento {source_event_id}. Evento normalizado: {normalized_event_data}")
                return None, None

            return normalized_event_data, source_mapping_data

        except Exception as e:
            logging.critical(f"Erro inesperado ao normalizar dados do Sofascore: {e}. Dados brutos: {raw_data}", exc_info=True)
            return None, None

    def normalize_thesportsdb_match(self, raw_data: dict) -> tuple[dict | None, dict | None]:
        """
        Normaliza os dados brutos de partida do TheSportsDB para os modelos Event e EventSourceMapping.
        NOTA: TheSportsDB fornece dados mais limitados para 'live' status e estatísticas detalhadas.
        """
        if not raw_data:
            logging.warning("Dados brutos do TheSportsDB são nulos ou vazios para normalização.")
            return None, None

        try:
            # Campos para EventSourceMapping
            source_event_id = str(raw_data.get("idEvent"))
            source_name = "thesportsdb"

            source_mapping_data = {
                "source_name": source_name,
                "source_event_id": source_event_id
            }

            # Campos para Event
            # TheSportsDB geralmente usa strStatus para status de evento.
            # Ex: "Match Finished", "Fixture", "In Progress"
            thesportsdb_status = raw_data.get("strStatus", "Fixture").lower()
            event_status_map = {
                "fixture": "scheduled",
                "in progress": "inprogress",
                "match finished": "finished",
                "cancelled": "cancelled",
                "postponed": "postponed"
                # Adicione outros mapeamentos conforme a API do TheSportsDB se apresentar
            }
            event_status = event_status_map.get(thesportsdb_status, "unknown")

            # Times e placares
            home_team_name = raw_data.get("strHomeTeam")
            away_team_name = raw_data.get("strAwayTeam")
            home_score = int(raw_data["intHomeScore"]) if raw_data.get("intHomeScore") else 0
            away_score = int(raw_data["intAwayScore"]) if raw_data.get("intAwayScore") else 0

            # Horário de início
            # TheSportsDB fornece strTimestamp (Unix) e strDate/strTime/strTimeLocal
            start_timestamp_unix = int(raw_data.get("intEventLiveTime")) if raw_data.get("intEventLiveTime") else int(raw_data.get("idTimestamp")) # Prioriza live time, senão o timestamp do evento

            if start_timestamp_unix:
                event_timestamp = self._convert_timestamp_to_utc_datetime(start_timestamp_unix / 1000) # TheSportsDB timestamp pode ser em milissegundos
            else:
                # Alternativa se o intEventLiveTime/idTimestamp não estiver disponível (ex: eventos antigos ou futuros)
                # Converte de strDate e strTime para datetime. TheSportsDB geralmente usa "YYYY-MM-DD" e "HH:MM:SS"
                date_str = raw_data.get("dateEvent")
                time_str = raw_data.get("strTime")
                if date_str and time_str:
                    # Assumimos UTC ou o fuso horário da API, o ideal é converter para UTC
                    # Ex: "2024-07-25" "15:00:00"
                    try:
                        # A API do TheSportsDB muitas vezes não especifica fuso horário, assuma UTC ou adicione lógica de conversão se souber.
                        # Para simplificar, trataremos como ingênuo e depois atribuiremos UTC
                        dt_naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                        event_timestamp = dt_naive.replace(tzinfo=timezone.utc) # Força UTC, adapte se a fonte especificar outro TZ

                        # Atualiza o timestamp Unix se foi convertido de string
                        start_timestamp_unix = int(event_timestamp.timestamp())
                    except ValueError:
                        logging.warning(f"Não foi possível parsear data/hora para evento TheSportsDB {source_event_id}. Usando None.")
                        event_timestamp = None
                else:
                    event_timestamp = None # Nenhuma informação de tempo disponível

            if not event_timestamp:
                logging.error(f"Timestamp de início ausente para evento TheSportsDB {source_event_id}. Pulando.")
                return None, None

            # Minuto atual (TheSportsDB geralmente não fornece ou é inconsistente)
            # intEventProgress é um campo que pode indicar progresso, mas não é um minuto direto
            current_game_time = None 

            # Liga/Torneio
            league_name = raw_data.get("strLeague")
            league_id = raw_data.get("idLeague")

            # Nome do Esporte
            sport_name = raw_data.get("strSport", "football")

            # Normaliza as estatísticas (altamente limitado para TheSportsDB)
            statistics = self._normalize_thesportsdb_statistics(raw_data) # Passa os dados brutos para extrair o que puder

            # Geração do last_updated_timestamp
            last_updated_timestamp = self._get_current_utc_timestamp()

            event_name = f"{home_team_name} vs {away_team_name}"

            normalized_event_data = {
                "event_name": event_name,
                "sport_name": sport_name,
                "event_status": event_status,
                "current_game_time": current_game_time,
                "home_team_name": home_team_name,
                "away_team_name": away_team_name,
                "home_score": home_score,
                "away_score": away_score,
                "league_name": league_name,
                "league_id": league_id,
                "event_timestamp": event_timestamp,
                "last_data_source": source_name,
                "last_updated_timestamp": last_updated_timestamp,
                "statistics": statistics
            }

            # Validação básica
            if not all([event_name, sport_name, home_team_name, away_team_name, event_timestamp]):
                logging.error(f"Dados essenciais faltando após normalização do TheSportsDB para evento {source_event_id}. Evento normalizado: {normalized_event_data}")
                return None, None

            return normalized_event_data, source_mapping_data

        except Exception as e:
            logging.critical(f"Erro inesperado ao normalizar dados do TheSportsDB: {e}. Dados brutos: {raw_data}", exc_info=True)
            return None, None