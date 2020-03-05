from configparser import ConfigParser
from pathlib import Path
from typing import List, Optional

from airflow import settings


class _Remotes:
    @property
    def remotes_config_path(self) -> Path:
        return Path(settings.AIRFLOW_HOME) / '.remotes'

    @property
    def remotes_config(self) -> ConfigParser:
        config = ConfigParser()
        config.read(str(self.remotes_config_path))
        return config

    @property
    def remote_names(self) -> List[str]:
        return [remote for remote in self.remotes_config.keys() if remote != 'DEFAULT']

    def sql_alchemy_conn(self, remote: Optional[str]) -> str:
        return self.remotes_config[remote]['sql-alchemy-conn'] if remote else None

    def fernet_key(self, remote: str) -> str:
        return self.remotes_config[remote]['fernet-key']

    def add_remote(self, remote: str, sql_alchemy_conn: str, fernet_key: str):
        config = self.remotes_config
        config[remote] = {}
        config[remote]['sql-alchemy-conn'] = sql_alchemy_conn
        config[remote]['fernet-key'] = fernet_key
        with open(str(self.remotes_config_path), 'w') as f:
            config.write(f)

    def remove_remote(self, remote: str):
        config = self.remotes_config
        del config[remote]
        with open(str(self.remotes_config_path), 'w') as f:
            config.write(f)


Remotes = _Remotes()
