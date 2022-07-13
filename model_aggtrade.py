from datetime import datetime
from typing import List, Optional
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import (
    ArrayField,
    DateTime64Field,
    StringField,
    UInt8Field,
    Float64Field,
    UInt64Field,
    LowCardinalityField,
)
from infi.clickhouse_orm.database import Database, DatabaseException
from infi.clickhouse_orm.engines import MergeTree, ReplacingMergeTree
from infi.clickhouse_orm.funcs import F
from enum import IntEnum
from config import CONFIG
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)


class LoggingLevel(IntEnum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class Logger:
    def __init__(self, database: Database):
        self.db = database

    def log_msg(
        self,
        msg: str,
        level: LoggingLevel,
        payload: str = "",
        silence: Optional[bool] = None,
    ) -> None:
        silence = silence if silence is not None else CONFIG.log_to_console
        if silence:
            logging.log(level, msg)
        self.db.insert(
            [
                LoggingMsg(
                    timestamp=datetime.utcnow(), msg=msg, level=level, payload=payload
                )
            ]
        )


class LoggingMsg(Model):
    timestamp = DateTime64Field(codec="Delta,ZSTD")
    msg = StringField()
    level = UInt8Field(codec="Delta, LZ4")
    payload = StringField(default="")

    engine = MergeTree("timestamp", order_by=("timestamp",))




class AggtradeSteam(Model):
    time_exchange = UInt64Field(codec="Delta,ZSTD")
    time_coinapi = UInt64Field(codec="Delta,ZSTD")
    uuid = Float64Field()
    price = StringField()
    size = StringField()
    taker_side = StringField()
    symbol_id = LowCardinalityField(StringField())

    engine = ReplacingMergeTree(
        partition_key=('time_exchange', "symbol_id"),
        order_by=("time_exchange",),
    )


class AggtradeSteamDispatcher:
    def __init__(self, database: Database, logger: Logger):
        self.buffer = []
        self.db = database
        self.batch_size = CONFIG.dispatcher_buffer_size
        self.logger = logger

    def insert(
        self,
        time_exchange: int,
        time_coinapi: int,
        symbol_id: str,
        uuid: int,
        price: str,
        size: str,
        taker_side: str,
    ) -> None:
        self.buffer.append(
            AggtradeSteam(
                time_exchange=time_exchange,
                time_coinapi=time_coinapi,
                symbol_id=symbol_id,
                uuid=uuid,
                price=price,
                size=size,
                taker_side=taker_side,
            )
        )
        if len(self.buffer) >= self.batch_size:
            self.insert_to_db()

    def insert_to_db(self) -> None:
        try:
            self.db.insert(self.buffer)
            self.buffer = []
        except DatabaseException as e:
            self.logger.log_msg(e)
            self.logger.log_msg(
                f"{self.__repr__()} error, retrying:", LoggingLevel.WARNING, repr(e)
            )

    def __len__(self) -> int:
        return len(self.buffer)

    def __repr__(self) -> str:
        return f"DiffDepthStreamDispatcher(len(buffer)={len(self.buffer)})"


if __name__ == "__main__":
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    for model in [LoggingMsg, AggtradeSteam]:
        db.create_table(model)
