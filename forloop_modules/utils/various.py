import inspect

from forloop_modules.utils.definitions import JSON_SERIALIZABLE_TYPES, REDIS_STORED_TYPES


def is_value_serializable(value) -> bool:
    return True if type(value) in JSON_SERIALIZABLE_TYPES else False


def is_value_redis_compatible(value) -> bool:
    return (
        type(value) in REDIS_STORED_TYPES or inspect.isfunction(value) or inspect.isclass(value)
    )
