from pydantic import BaseModel


class MessageBase(BaseModel):
    content: str


class Message(MessageBase):
    id: int

    class Config:
        orm_mode = True


class MessageCreate(MessageBase):
    topic: str
    partition: int


class TopicCreate(BaseModel):
    topic: str
    partition: int
    partners: list
