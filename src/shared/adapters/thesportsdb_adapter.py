# src/shared/adapters/thesportsdb_adapter.py
import requests
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TheSportsDBAdapter:
    def __init__(self):
        self.api_key = os.getenv("THESPORTSDB_API_KEY")
        if not self.api_key:
            logging.error("THESPORTSDB_API_KEY not found in environment variables.")
            raise ValueError("THESPORTSDB_API_KEY not found in environment variables.")
        self.base_url = f"https://www.thesportsdb.com/api/v1/json/{self.api_key}"

    def get_all_leagues(self):
        endpoint = f"{self.base_url}/all_leagues.php"
        try:
            response = requests.get(endpoint)
            response.raise_for_status()
            data = response.json()
            logging.info("Ligas do TheSportsDB buscadas com sucesso.")
            return data.get('leagues', [])
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar ligas do TheSportsDB: {e}", exc_info=True)
            return []

    def get_events_by_league_id(self, league_id, round_number=None, season=None):
        """
        Busca eventos de uma liga específica por ID, opcionalmente por rodada e temporada.
        Este endpoint é mais para eventos agendados/passados, não para live.
        Documentação: https://www.thesportsdb.com/api/v1/json/{APIKEY}/eventsround.php?id={ID}&r={round}&s={season}
        """
        params = {'id': league_id}
        if round_number is not None:
            params['r'] = round_number
        if season is not None:
            params['s'] = season

        endpoint = f"{self.base_url}/eventsround.php"
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Eventos da liga {league_id} do TheSportsDB buscados com sucesso.")
            return data.get('events', [])
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar eventos da liga {league_id} do TheSportsDB: {e}", exc_info=True)
            return []

    def fetch_event_details(self, event_id: str) -> dict | None:
        """
        Busca detalhes de um evento específico usando o ID do evento do TheSportsDB.
        Documentação: https://www.thesportsdb.com/api/v1/json/{APIKEY}/lookupevent.php?id={ID}
        """
        endpoint = f"{self.base_url}/lookupevent.php"
        params = {'id': event_id}
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            # O endpoint retorna uma lista 'events', mesmo que seja um único evento
            events = data.get('events')
            if events and len(events) > 0:
                logging.info(f"Detalhes do evento {event_id} do TheSportsDB buscados com sucesso.")
                return events[0] # Retorna o primeiro e único evento
            logging.warning(f"Nenhum detalhe encontrado para o evento {event_id} do TheSportsDB.")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar detalhes do evento {event_id} do TheSportsDB: {e}", exc_info=True)
            return None