
import os
import json
from pathlib import Path

def get_BINANCE_history_trade_count():
    __DIR__ = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(__DIR__, "BINANCE_history_trade_count.json")
    return json.loads(Path(path).read_text())