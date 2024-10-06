from typing import Optional

from dataclasses import dataclass
from forloop_modules.utils import synchronization_flags


@dataclass
class RedisConfig:
    # None values for Host and Password are correct options:
    # in-memory local caching will be used instead of a Redis server
    FORLOOP_REDIS_HOST: Optional[str] = None # localhost doesn't work for devs with DummyKVRedis
    FORLOOP_REDIS_USERNAME: str = "default"
    FORLOOP_REDIS_PASSWORD: Optional[str] = None
    FORLOOP_REDIS_PORT: int = 6379
    FORLOOP_REDIS_DB: int = 0

    VARIABLE_KEY: str = "pipeline_job:{pipeline_job_uid}:variable:{variable_name}"
    INITIAL_VARIABLE_KEY: str = "pipeline:{pipeline_uid}:initial_variable:{variable_name}"

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


redis_config = RedisConfig()


if synchronization_flags.REDIS_CONFIG_HOST is not None:
    redis_config.FORLOOP_REDIS_HOST = synchronization_flags.REDIS_CONFIG_HOST 
    
if synchronization_flags.REDIS_CONFIG_USERNAME != "default":
    redis_config.FORLOOP_REDIS_USERNAME = synchronization_flags.REDIS_CONFIG_USERNAME
    
if synchronization_flags.REDIS_CONFIG_PASSWORD is not None:
    redis_config.FORLOOP_REDIS_PASSWORD = synchronization_flags.REDIS_CONFIG_PASSWORD



