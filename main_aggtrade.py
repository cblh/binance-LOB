from asyncio.events import AbstractEventLoop
from typing import List, Optional
from aiohttp.client import ClientSession
from infi.clickhouse_orm.database import Database
from model_aggtrade import (
    AggtradeSteamDispatcher,
    Logger,
    LoggingLevel,
    AggtradeSteam,
    LoggingMsg,
)
import asyncio
import aiohttp
from pydantic import BaseModel, ValidationError
from time import time
from config import CONFIG
from enum import Enum


class AssetType(Enum):
    SPOT = ""
    USD_M = "USD_F_"
    COIN_M = "COIN_F_"


class AggtradeStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    s: str  # Symbol
    a: int     # 归集成交 ID
    p: str     # 成交价格
    q: str     # 成交量
    f: int     # 被归集的首个交易ID
    l: int     # 被归集的末次交易ID
    T: int     # 成交时间
    m: bool     # 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。


def aggtrade_stream_url(symbol: str, asset_type: AssetType) -> str:
    speed = CONFIG.aggtrade_stream_interval
    assert speed in (1000, 100), "speed must be 1000 or 100"
    symbol = symbol.lower()
    endpoint = f"{symbol}@aggTrade"
    if asset_type == AssetType.SPOT:
        return f"wss://stream.binance.com:9443/ws/{endpoint}"
    elif asset_type == AssetType.USD_M:
        return f"wss://fstream.binance.com/ws/{endpoint}"
    else:
        return f"wss://dstream.binance.com/ws/{endpoint}"



async def handle_aggtrade_stream(
    symbol: str,
    session: ClientSession,
    dispatcher: AggtradeSteamDispatcher,
    database: Database,
    logger: Logger,
    loop: AbstractEventLoop,
    asset_type: AssetType,
):
    logger.log_msg(
        f"Connecting to {asset_type.value + symbol} stream", LoggingLevel.INFO, symbol
    )
    while True:
        async with session.ws_connect(aggtrade_stream_url(symbol, asset_type)) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data_raw = AggtradeStreamMsg(**msg.json())
                    except ValidationError:
                        print(msg.data)
                        break

                    timestamp = data_raw.E
                    symbol_id = data_raw.s


                    symbol_full = asset_type.value + symbol
                    
                    time_exchange = timestamp
                    time_coinapi = timestamp
                    uuid = data_raw.a
                    price = data_raw.p
                    size = data_raw.q
                    # 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
                    taker_side = 'SELL' if data_raw.m else 'BUY'

                    
                    dispatcher.insert(
                        time_exchange,
                        time_coinapi,
                        symbol_full,
                        uuid,
                        price,
                        size,
                        taker_side,
                    )
                    logger.log_msg(
                        f"aggtrade insert for {symbol_full}",
                        LoggingLevel.INFO,
                        symbol,
                    )
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
        logger.log_msg(
            f"Connection closed for {symbol} stream, retrying.",
            LoggingLevel.INFO,
            symbol,
        )

async def setup():
    session = aiohttp.ClientSession()
    loop = asyncio.get_event_loop()
    database = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    logger = Logger(database)
    dispatcher = AggtradeSteamDispatcher(database, logger)
    logger.log_msg("Starting event loop...", LoggingLevel.INFO)
    for symbol in CONFIG.symbols_sorted_by_trade:
        if "USD_" in symbol :
            inst = symbol[6:]
            loop.create_task(
                handle_aggtrade_stream(
                    inst,
                    session,
                    dispatcher,
                    database,
                    logger,
                    loop,
                    AssetType.USD_M,
                )
            )
        elif "COIN_" in symbol:
            loop.create_task(
                handle_aggtrade_stream(
                    symbol[5:],
                    session,
                    dispatcher,
                    database,
                    logger,
                    loop,
                    AssetType.COIN_M,
                )
            )
        else:
            loop.create_task(
                handle_aggtrade_stream(
                    symbol, session, dispatcher, database, logger, loop, AssetType.SPOT
                )
            )


if __name__ == "__main__":
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    for model in [LoggingMsg, AggtradeSteam]:
        db.create_table(model)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    loop.run_forever()
