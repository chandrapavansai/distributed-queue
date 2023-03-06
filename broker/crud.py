from sqlalchemy.orm import Session

import models, schemas


def get_message(db: Session, topic: str, partition: int, offset: int):
    return db.query(models.Message).filter(
        models.Message.topic == topic, models.Message.partition == partition
    ).offset(offset).first()


def create_message(db: Session, message: schemas.MessageCreate):
    db_message = models.Message(**message.dict())
    db.add(db_message)
    db.commit()
    db.refresh(db_message)
    return db_message


def get_message_count(db: Session, topic: str, partition: int, offset: int):
    return db.query(models.Message).filter(
        models.Message.topic == topic, models.Message.partition == partition
    ).offset(offset).count()


def clear_messages(db: Session):
    db.query(models.Message).delete()
    db.commit()
