import requests
import json
import logging
import time
from datetime import datetime, timedelta

# Importa a nova classe de estratégia anti-bloqueio
from shared.core.anti_block import AntiBlockStrategy, TokenBucketAntiBlockStrategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SofascoreAdapter:
    """
    Adaptador para coletar dados de futebol do Sofascore via web scraping (API).
    Prioriza endpoints JSON da API Sofascore quando possível.
    """
    BASE_API_URL = "https://api.sofascore.com/api/v1"
    SPORT = "football"

    def __init__(self, anti_block_strategy: AntiBlockStrategy = None):
        """
        Inicializa o adaptador Sofascore.
        :param anti_block_strategy: Uma instância de uma estratégia anti-bloqueio (preferencialmente TokenBucketAntiBlockStrategy).
        """
        self.anti_block_strategy = anti_block_strategy if anti_block_strategy else TokenBucketAntiBlockStrategy(capacity=10, fill_rate=0.5)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36", # Exemplo de Chrome atual
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Host": "api.sofascore.com",
            "Origin": "https://www.sofascore.com",
            "Referer": "https://www.sofascore.com/"
        }
        logging.info(f"SofascoreAdapter inicializado. Usando estratégia: {self.anti_block_strategy.__class__.__name__}")

    def _make_api_request(self, endpoint: str) -> dict | None:
        """
        Faz uma requisição genérica à API do Sofascore com a estratégia anti-bloqueio.
        """
        url = f"{self.BASE_API_URL}/{endpoint}"
        
        self.anti_block_strategy.wait_before_request() # Espera antes da requisição
        logging.info(f"Fazendo requisição à API Sofascore: {url}") # Log para ver qual URL está sendo chamada
        
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status() # Lança um HTTPError para respostas de erro (4xx ou 5xx)
            data = response.json()
            self.anti_block_strategy.record_request() # Registra a requisição após o sucesso
            logging.debug(f"Requisição bem-sucedida para: {endpoint}")
            return data
        except requests.exceptions.HTTPError as e:
            logging.error(f"Erro HTTP ao requisitar {url}: {e.response.status_code} - {e.response.text}")
            self.anti_block_strategy.record_request() # Ainda registra, pois a tentativa foi feita
            return None
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Erro de conexão ao requisitar {url}: {e}")
            self.anti_block_strategy.record_request()
            return None
        except requests.exceptions.Timeout as e:
            logging.error(f"Timeout ao requisitar {url}: {e}")
            self.anti_block_strategy.record_request()
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição para {url}: {e}")
            self.anti_block_strategy.record_request()
            return None
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar JSON da resposta de {url}. Conteúdo: {response.text[:200]}...")
            self.anti_block_strategy.record_request()
            return None
        except Exception as e:
            logging.error(f"Erro inesperado ao requisitar {url}: {e}", exc_info=True)
            self.anti_block_strategy.record_request()
            return None

    def get_todays_and_tomorrows_matches_events(self) -> list[dict]: # Mudança no nome e retorno
        """
        Coleta objetos de eventos agendados para hoje e amanhã.
        Este endpoint é o ponto de entrada para o data-collector, fornecendo dados de alto nível.
        :return: Lista de dicionários, cada um representando um evento.
        """
        all_events = []
        today = datetime.now()
        tomorrow = today + timedelta(days=1)

        dates_to_fetch = [today, tomorrow]

        for date_obj in dates_to_fetch:
            date_str = date_obj.strftime("%Y-%m-%d")
            endpoint = f"sport/{self.SPORT}/scheduled-events/{date_str}"
            
            data = self._make_api_request(endpoint)
            
            if data and 'events' in data:
                logging.info(f"Eventos encontrados para a data {date_str}: {len(data['events'])}")
                all_events.extend(data['events']) # Adiciona os objetos de evento completos
            else:
                logging.warning(f"Nenhum evento encontrado para a data {date_str} ou estrutura da resposta inesperada.")

        logging.info(f"Encontrados {len(all_events)} eventos de partidas para hoje e amanhã.")
        
        if not all_events:
            logging.warning("Nenhum evento de partida foi coletado. Verifique o endpoint da API, os headers ou se realmente há jogos para as datas.")

        return all_events

    # O método get_event_summary não é mais necessário para o data-collector
    # pois ele obterá os dados de alto nível diretamente de get_todays_and_tomorrows_matches_events.
    # No entanto, se o live-monitor precisar de um "summary" diferente de "details",
    # poderíamos mantê-lo ou renomeá-lo. Por simplicidade, vamos removê-lo aqui,
    # já que o live-monitor usará get_match_data.

    def get_match_data(self, match_id: str) -> dict | None:
        """
        Obtém dados completos de uma partida, incluindo estatísticas detalhadas.
        Ideal para o live-monitor.
        """
        endpoint = f"event/{match_id}" # Endpoint para detalhes completos
        data = self._make_api_request(endpoint)
        if data and 'event' in data:
            event_data = data['event']
            if 'statistics' in data: 
                event_data['statistics'] = data['statistics']
            elif 'statistics' in event_data:
                pass 
            else:
                event_data['statistics'] = {} 

            logging.debug(f"Dados completos para partida {match_id} coletados com sucesso do endpoint de detalhes.")
            return event_data
        logging.warning(f"Não foi possível obter dados completos para partida {match_id} do endpoint de detalhes.")
        return None