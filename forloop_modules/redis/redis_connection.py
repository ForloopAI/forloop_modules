from typing import Union
import datetime

import keepvariable.keepvariable_core as kv
import redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from forloop_modules.redis.config.config import RedisConfig, redis_config
import forloop_modules.flog as flog
from forloop_modules.errors.errors import InitializationError


def check_modules(kv_redis: kv.KeepVariableRedisServer) -> None:
    """Check if RedisSearch and ReJSON modules are loaded in Redis DB."""
    loaded_modules = {module.get("name") for module in kv_redis.redis.module_list()}
    if ("search" not in loaded_modules):
        raise InitializationError("RedisSearch module is not loaded on the connected Redis server")
    if ("ReJSON" not in loaded_modules):
        raise InitializationError("RedisJSON module is not loaded on the connected Redis server")
    flog.info("RedisJSON and RedisSearch modules are loaded")


def validate_job_index(kv_redis: kv.KeepVariableRedisServer, redis_config: RedisConfig) -> bool:
    """
    Check if the expected index for job querying is present in Redis. If not, create one.

    :return: True is successfully validated
    :rtype: bool
    """
    try:
        index: dict = kv_redis.redis.ft(f"{redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME}"
                                       ).info()
    except redis.exceptions.ResponseError:
        flog.info(f"Redis index {redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME} was not found")
    else:
        # Check that the index used is indexing 'status' and 'created_at' variables
        # Those variables are used for searching and sorting of Redis queries
        indexed_attrs = index.get("attributes")

        try:
            assert redis_config.JOB_KEY in index["index_definition"][
                3]  # check correct indexing prefix
            assert indexed_attrs is not None
            assert len(indexed_attrs) >= 4  # check that at least 4 columns are indexed

            # Check that both columns is present in the indexed columns
            for column in [
                {"status", "TAG", "SORTABLE"},
                {"created_at", "NUMERIC", "SORTABLE"},
                {"completed_at", "NUMERIC", "SORTABLE"},
                {"pipeline_uid", "TEXT", "SORTABLE"},
            ]:
                indexed_column_exists = False
                for indexed_column in [set(indexed_column) for indexed_column in indexed_attrs]:
                    if column <= indexed_column:
                        indexed_column_exists = True
                assert indexed_column_exists
        except AssertionError:
            return False
        else:
            flog.info("Redis is already correctly indexing Jobs")
            return True


def recreate_job_index(kv_redis: kv.KeepVariableRedisServer, redis_config: RedisConfig) -> None:
    """Check if the expected index for job querying is present in Redis. If not, create one."""
    # If the expected index is not defined in Redis, create one
    schema = (
        TagField("$.status", as_name="status", sortable=True),
        NumericField("$.created_at", as_name="created_at", sortable=True),
        NumericField("$.completed_at", as_name="completed_at", sortable=True),
        TextField("$.pipeline_uid", as_name="pipeline_uid", sortable=True),
    )

    # FT.CREATE job:index
    #     ON JSON
    #         PREFIX 1 "jobs:"
    #     SCHEMA
    #         $.status AS status TAG
    #         $.created_at AS created_at NUMERIC SORTABLE
    #         $.completed_at AS completed_at NUMERIC SORTABLE
    #         $.pipeline_uid AS pipeline_uid TEXT SORTABLE
    kv_redis.redis.ft(f"{redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME}").create_index(
        schema,
        definition=IndexDefinition(prefix=[redis_config.JOB_KEY], index_type=IndexType.JSON),
    )
    flog.info("Redis index has been created")


def check_job_primary_key(
    kv_redis: Union[kv.KeepVariableRedisServer, kv.KeepVariableDummyRedisServer],
    redis_config: RedisConfig
) -> None:
    if kv_redis.get(redis_config.JOB_PRIMARY_KEY) is not None:
        flog.info("Jobs PK already exists")
    else:
        kv_redis.set(redis_config.JOB_PRIMARY_KEY, 0)
        flog.info("Jobs PK created")


if redis_config.HOST is None:
    kv_redis = kv.KeepVariableDummyRedisServer()
    check_job_primary_key(kv_redis, redis_config)
else:
    kv_redis = kv.KeepVariableRedisServer(
        host=redis_config.HOST, port=redis_config.PORT, username=redis_config.USERNAME,
        password=redis_config.PASSWORD, db=redis_config.DB
    )
    try:
        kv_redis.set("forloop_redis_connection_test_metadata",str(datetime.datetime.now())+", Host: "+kv_redis.host+", Username: "+kv_redis.username)
        flog.info("Redis connection was successfully established and metadata were inserted")
    except Exception as e:
        flog.warning("forloop_redis_connection_test_metadata couldn't be sent to Redis, check your connection")
    
    # check_modules(kv_redis)
    # check_job_primary_key(kv_redis, redis_config)

    # if not validate_job_index(kv_redis, redis_config):
    #     flog.error(
    #         f"Dropping index '{redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME}' as it is incorrectly defined."
    #     )
    #     kv_redis.redis.ft(f"{redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME}").dropindex()
    #     recreate_job_index(kv_redis, redis_config)

    # # Revalidate to double check for correct initialization logic
    # if not validate_job_index(kv_redis, redis_config):
    #     raise InitializationError(
    #         f"Discrepancy between '{redis_config.JOB_KEY}:{redis_config.JOB_INDEX_NAME}' initialization and validation rules. Could not initialize"
    #     )

def create_redis_key_for_project_db_private_key(project_uid: str):
    redis_key = redis_config.PASSWORD_ENCRYPTION_KEY_TEMPLATE.format(project_uid=project_uid)
    
    return redis_key
