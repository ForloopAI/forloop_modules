

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings



class RedisConfig(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

    # None values for Host and Password are correct options:
    # in-memory local caching will be used instead of a Redis server
    HOST: Optional[str] = None # "localhost" #localhost doesn't work for devs with DummyKVRedis
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
    
    LAST_ACTIVE_DF_NODE_UID_KEY_TEMPLATE: str = "project:{project_uid}:last_active_df_node_uid"

    # Redis key for storing temporary scraping results
    SCRAPING_ACTION_KEY_TEMPLATE: str = "pipeline:{pipeline_uid}:scraping:action"

    # Redis key for storing password encryption private key
    PASSWORD_ENCRYPTION_KEY_TEMPLATE: str = "project:{project_uid}:password:encryption"
    
    LAST_ACTIVE_SCRIPT_KEY_TEMPLATE: str = "project:{project_uid}:last_active_script_uid"

@lru_cache
def get_redis_config() -> RedisConfig:
    return RedisConfig()


redis_config = RedisConfig()
