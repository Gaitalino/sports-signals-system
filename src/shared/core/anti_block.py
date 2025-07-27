import time
import logging
import requests
import threading
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AntiBlockStrategy:
    """Interface base para estratégias anti-bloqueio."""
    def wait_before_request(self):
        """Método para aguardar antes de fazer uma requisição."""
        raise NotImplementedError("O método 'wait_before_request' deve ser implementado pelas subclasses.")

    def record_request(self):
        """Método para registrar uma requisição feita."""
        raise NotImplementedError("O método 'record_request' deve ser implementado pelas subclasses.")

class TokenBucketAntiBlockStrategy(AntiBlockStrategy):
    """
    Estratégia anti-bloqueio que usa o algoritmo Token Bucket.
    Permite um certo número de requisições (tokens) por unidade de tempo,
    com a capacidade de acumular tokens até um limite máximo.
    """
    def __init__(self, capacity: int, fill_rate: float, initial_tokens: int = None):
        """
        Inicializa a estratégia Token Bucket.
        :param capacity: Capacidade máxima de tokens no balde.
        :param fill_rate: Taxa de preenchimento de tokens por segundo.
        :param initial_tokens: Número inicial de tokens. Se None, será igual à capacidade.
        """
        if capacity <= 0 or fill_rate <= 0:
            raise ValueError("Capacidade e taxa de preenchimento devem ser maiores que zero.")

        self.capacity = capacity
        self.fill_rate = fill_rate  # tokens por segundo
        self.tokens = initial_tokens if initial_tokens is not None else capacity
        self.last_refill_time = time.time()
        self.lock = threading.Lock() # Para garantir thread-safety

        logging.info(f"Estratégia Anti-Bloqueio: Token Bucket ativada. Capacidade: {capacity}, Taxa de preenchimento: {fill_rate} tps.")

    def _refill_tokens(self):
        """Recarrega os tokens com base no tempo decorrido."""
        now = time.time()
        time_passed = now - self.last_refill_time
        new_tokens = time_passed * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill_time = now

    def wait_before_request(self):
        """
        Bloqueia até que haja tokens disponíveis para fazer uma requisição.
        """
        with self.lock:
            while True:
                self._refill_tokens() # Sempre tenta reabastecer antes de verificar
                if self.tokens >= 1:
                    self.tokens -= 1 # Consome um token
                    logging.debug(f"Token consumido. Tokens restantes: {self.tokens}/{self.capacity}")
                    break
                else:
                    # Calcula quanto tempo falta para ter pelo menos 1 token
                    tokens_needed = 1 - self.tokens
                    time_to_wait = tokens_needed / self.fill_rate
                    logging.debug(f"Sem tokens. Aguardando {time_to_wait:.2f}s para o próximo token. Tokens: {self.tokens:.2f}")
                    time.sleep(time_to_wait + 0.01) # Adiciona um pequeno buffer para evitar erros de ponto flutuante

    def record_request(self):
        """
        Este método é mantido para compatibilidade com a interface,
        mas o consumo de tokens já é feito em wait_before_request.
        Pode ser usado para futuras lógicas de feedback da API, se necessário.
        """
        pass # A lógica de token já consome em wait_before_request