

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings



class RedisConfig(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    # None values for Host and Password are correct options:
    # in-memory local caching will be used instead of a Redis server
    HOST: Optional[str] = "localhost" # "localhost" #localhost doesn't work for devs with DummyKVRedis
    USERNAME: str = "default"
    PASSWORD: Optional[str] = None
    PORT: int = 6379
    DB: int = 0

    VARIABLE_KEY: str = "stored_variable_"  # key prefix to be concatenated with variable name
    INITIAL_VARIABLE_KEY: str = "stored_initial_variable_"  # key prefix to be concatenated with variable name

    JOB_KEY: str = "jobs"
    JOB_INDEX_NAME: str = "index"
    JOB_LOCK_NAME: str = "jobs:lock"
    JOB_PRIMARY_KEY: str = "jobs:pk"

    # Redis key for storing temporary scraping results
    SCRAPING_ACTION_KEY_TEMPLATE: str = "pipeline:{pipeline_uid}:scraping:action"

    # Complete nonsense name --> to avoid accidental selection as a variable
    PRIVATE_ENCRYPTION_KEY_KEY: str = "ForLoop_unicorn_2025_key_for_key_1998_CraZy_FRog"

@lru_cache
def get_redis_config() -> RedisConfig:
    return RedisConfig()


redis_config = RedisConfig()
