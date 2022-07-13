""" Replay modules.

Inlcude all useful function and classes for reconstructing orderbook from database
"""
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple
from infi.clickhouse_orm.database import Database
import sys
import os
sys.path.insert(
    0, str(os.path.abspath(__file__ + "/../"))
)  # Allows relative imports from model_aggtrade
from model_aggtrade import AggtradeSteam
from clickhouse_driver import Client
from config import CONFIG
from sortedcontainers import SortedDict
from itertools import chain
from dataclasses import dataclass


def diff_depth_stream_generator(
    timestamp: int, symbol: str, block_size: Optional[int] = None
) -> Generator[
    Tuple[datetime, int, int, List[float], List[float], List[float], List[float], str],
    None,
    None,
]:
    database = CONFIG.db_name
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    qs = (
        AggtradeSteam.objects_in(db)
        .filter(
            AggtradeSteam.symbol_id == symbol.upper(),
            AggtradeSteam.time_exchange >= timestamp,
        )
        .order_by("time_exchange")
    )

    if block_size is None:
        for row in client.execute(qs.as_sql()):
            yield row
    else:
        settings = {"max_block_size": block_size}
        rows_gen = client.execute_iter(qs.as_sql(), settings=settings)
        for row in rows_gen:
            yield row


@dataclass
class Aggtrade:
    time_exchange: int
    time_coinapi: int
    symbol_id: str
    uuid: int
    price: str
    size: str
    taker_side: str

def aggtrade_generator(
    last_update_id: int, symbol_id: str,  block_size: Optional[int] = 5_000
) -> Generator[Aggtrade, None, None]:
    """Similar to orderbook_generator but instead of yielding a full constructed orderbook
    while maintaining a full local orderbook, a partial orderbook with level for both bids and
    asks are yielded and only a partial orderbook is maintained. This generator should be much
    faster than orderbook_generator.

    Args:
        last_update_id (int): target update id to begin iterator. The first item
            from the iterator will be the first snapshot with last update id that
            is strictly greater than the one applied. Sucessive item will be constructed
            with diff stream while a local orderbook is maintained.
            See the link below for detail
            https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
            for more detail.
        symbol (str): symbol for orderbook to reconstruct
        level (int, optional): levels of orderbook to return. Defaults to 10.
        block_size (Optional[int], optional): pagniate size for executing SQL queries. None
            means all data are retrived at once. Defaults to 5000.

    Raises:
        ValueError: ignore

    Yields:
        PartialBook: Partial Orderbook object representing reconstructed orderbook
    """
    for diff_stream in diff_depth_stream_generator(last_update_id, symbol_id, block_size):
        (
            time_exchange,
            time_coinapi,
            uuid,
            price,
            size,
            taker_side,
            symbol_id,
        ) = diff_stream

        yield Aggtrade(
            time_exchange=time_exchange,
            time_coinapi=time_coinapi,
            symbol_id=symbol_id,
            uuid=uuid,
            price=price,
            size=size,
            taker_side=taker_side,
        )


def lists_to_dict(price: List[float], quantity: List[float]) -> Dict[float, float]:
    return {p: q for p, q in zip(price, quantity)}



def get_all_symbols() -> List[str]:
    database = CONFIG.db_name
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    return [s[0] for s in client.execute("SELECT DISTINCT symbol FROM diffdepthstream")]


if __name__ == "__main__":
    for r in aggtrade_generator(0, "AVAXBUSD", block_size=5000):
        print(r)
