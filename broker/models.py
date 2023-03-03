from sqlalchemy import Column, Integer, String

from .database import Base


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    content = Column(String, index=True)
    topic = Column(String, index=True)
    partition = Column(Integer, index=True)
