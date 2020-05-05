import datetime
import typing

from sqlalchemy import (
    and_,
    create_engine,
    Column,
    ForeignKey,
    LargeBinary,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON

from core.utils import DateTimeRange

Base = declarative_base()


class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    sensor_data = relationship(
        "SensorData",
        uselist=True,
        lazy="noload"
    )


class SensorData(Base):
    __tablename__ = "sensor_data"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    data = Column(JSON, nullable=False)
    sign = Column(LargeBinary)
    signer = Column(LargeBinary)

    sensor = relationship(
        "Sensor",
        uselist=False,
        lazy="noload",
    )


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    pwd_hash = Column(String, nullable=False)


class UserSocialTokens(Base):
    __tablename__ = 'users_social_tokens'

    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True, nullable=False)
    yandex_disk = Column(String)


class DatabaseManager:
    def __init__(self, db_uri):
        self._db_uri = db_uri

    def _create_session(self):
        if not hasattr(self, "__session"):
            setattr(self, "__session", Session(create_engine(self._db_uri)))
        return getattr(self, "__session")

    def get_sensors(self) -> typing.List[Sensor]:
        return self._create_session().query(Sensor).all()

    def get_sensor_data(self, sensor_id: int, datetime_range: DateTimeRange) -> typing.List[SensorData]:
        return self._create_session().query(SensorData).filter(
            and_(
                SensorData.sensor_id == sensor_id,
                SensorData.data["timestamp"].astext.cast(DateTime) >= datetime_range.start,
                SensorData.data["timestamp"].astext.cast(DateTime) <= datetime_range.end,
            ),
        ).all()

    def get_first_sensor_data_date(self, sensor_id: int) -> datetime.datetime:
        data = self._create_session().query(SensorData.data["timestamp"].astext.cast(DateTime)) \
            .filter(SensorData.sensor_id == sensor_id) \
            .order_by(SensorData.id.asc()) \
            .limit(1) \
            .all()
        if len(data):
            return data[0][0]

    def get_encryption_key(self):
        key, = self._create_session().query(User.pwd_hash).one()
        return key

    def get_tokens(self):
        return self._create_session().query(UserSocialTokens).one()
