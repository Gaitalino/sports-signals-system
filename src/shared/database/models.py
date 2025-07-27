# src/shared/database/models.py
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, JSON, ForeignKey, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB # Específico para JSONB no PostgreSQL
from datetime import datetime
import pytz # Para fusos horários

# Base declarativa para seus modelos SQLAlchemy
Base = declarative_base()

class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, index=True) # ID primário do evento canônico
    event_name = Column(String(500), nullable=False) # Nome do evento (ex: Time A vs Time B)
    sport_name = Column(String(50), nullable=False) # Ex: 'football', 'basketball'

    # Detalhes do evento
    event_status = Column(String(50), nullable=False) # Ex: 'scheduled', 'inprogress', 'finished', 'cancelled'
    current_game_time = Column(Integer, nullable=True) # Minuto atual da partida (se aplicável, para 'inprogress')

    # Times e placares
    home_team_name = Column(String(255), nullable=False)
    away_team_name = Column(String(255), nullable=False)
    home_score = Column(Integer, default=0)
    away_score = Column(Integer, default=0)

    # Informações da liga/torneio
    league_name = Column(String(255), nullable=True)
    league_id = Column(String(255), nullable=True) # ID da liga na fonte (ex: Sofascore Tournament ID)

    # Timestamps
    # Usamos DateTime com timezone=True para armazenar datetimes UTC e evitar problemas de fuso horário
    event_timestamp = Column(DateTime(timezone=True), nullable=False) # Horário oficial do início do evento

    # Controle de versão dos dados
    last_data_source = Column(String(50), nullable=True) # Ex: 'sofascore', 'thesportsdb'
    # last_updated_timestamp deve ser um timestamp Unix (BIGINT) para comparação simples
    last_updated_timestamp = Column(BigInteger, default=lambda: int(datetime.now(pytz.utc).timestamp())) 

    # Dados JSONB para estatísticas detalhadas e dinâmicas
    # Ex: {'home': {'shots': 10, 'possession': 60}, 'away': {'shots': 5, 'possession': 40}}
    statistics = Column(JSONB, default={}) 

    # Timestamps de controle do próprio registro no banco de dados
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(pytz.utc), onupdate=lambda: datetime.now(pytz.utc))

    # Relação com EventSourceMapping
    source_mappings = relationship("EventSourceMapping", back_populates="event", cascade="all, delete-orphan")

    # Garante a unicidade de um evento canônico pela combinação de atributos chave
    __table_args__ = (
        UniqueConstraint('event_timestamp', 'home_team_name', 'away_team_name', 'league_id', name='uq_event_canonical'),
        Index('idx_event_status', 'event_status'),
        Index('idx_event_timestamp', 'event_timestamp'),
    )

    def __repr__(self):
        return f"<Event(id={self.id}, name='{self.event_name}', status='{self.event_status}')>"

class EventSourceMapping(Base):
    __tablename__ = 'event_source_mappings'

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey('events.id'), nullable=False) # FK para a tabela events
    source_name = Column(String(50), nullable=False) # Ex: 'sofascore', 'thesportsdb'
    source_event_id = Column(String(255), nullable=False) # ID do evento na fonte externa

    # Relação com Event
    event = relationship("Event", back_populates="source_mappings")

    # Garante que um par (source_name, source_event_id) seja único
    # e também que um event_id só tenha uma entrada para uma dada source_name
    __table_args__ = (
        UniqueConstraint('source_name', 'source_event_id', name='uq_source_event'),
        UniqueConstraint('event_id', 'source_name', name='uq_event_id_source'),
        Index('idx_source_name_event_id', 'source_name', 'source_event_id'),
    )

    def __repr__(self):
        return f"<EventSourceMapping(id={self.id}, event_id={self.event_id}, source='{self.source_name}', source_id='{self.source_event_id}')>"