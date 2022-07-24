import json
from pathlib import Path
from typing import Dict, Any, List
import os
from pydantic import BaseSettings


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    A simple settings source that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """
    encoding = settings.__config__.env_file_encoding
    __DIR__ = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(__DIR__, "config.json")
    return json.loads(Path(path).read_text(encoding))


class Config(BaseSettings):
    api_key: str = ""
    api_secret: str = ""
    symbols: List[str]
    full_fetch_interval: int = 60 * 60
    full_fetch_limit: int = 1000
    stream_interval: int = 100
    log_to_console: bool = True
    dispatcher_buffer_size: int = 1000
    db_name: str = "archive"
    host_name_docker: str = "clickhouse"
    host_name_default: str = "localhost"

    @property
    def host_name(self) -> str:
        if os.environ.get('AM_I_IN_DOCKER', False):
            return self.host_name_docker
        else:
            return self.host_name_default
    @property
    def BINANCE_history_trade_count(self) -> List[str]:
        path = 'BINANCE_history_trade_count.json'
        return json.loads(Path(path).read_text())
    @property
    def symbols_sorted_by_trade(self) -> List[str]:
        return list(map(lambda x: x['symbol'], self.BINANCE_history_trade_count))

    class Config:
        env_file_encoding = "utf-8"

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                json_config_settings_source,
                env_settings,
                file_secret_settings,
            )


CONFIG = Config()

if __name__ == "__main__":
    print(Config())
    print(CONFIG.BINANCE_history_trade_count)
    print(CONFIG.symbols_sorted_by_trade)
