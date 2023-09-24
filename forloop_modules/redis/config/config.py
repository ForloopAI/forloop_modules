

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings



class RedisConfig(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    # None values for Host and Password are correct options:
    # in-memory local caching will be used instead of a Redis server
    HOST: Optional[str] = None
    USERNAME: str = "default"
    PASSWORD: Optional[str] = None
    PORT: int = 6379
    DB: int = 0

    JOB_KEY: str = "jobs"
    JOB_INDEX_NAME: str = "index"
    JOB_LOCK_NAME: str = "jobs:lock"
    JOB_PRIMARY_KEY: str = "jobs:pk"

@lru_cache
def get_redis_config() -> RedisConfig:
    return RedisConfig()


redis_config = RedisConfig()
