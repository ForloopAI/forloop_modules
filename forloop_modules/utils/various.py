import inspect

from forloop_modules.utils.definitions import JSON_SERIALIZABLE_TYPES, REDIS_STORED_TYPES


def is_value_serializable(value) -> bool:
    is_value_serializable = type(value) in JSON_SERIALIZABLE_TYPES
    return is_value_serializable


def is_value_redis_compatible(value) -> bool:
    is_value_callable = inspect.isfunction(value)
    is_value_class = inspect.isclass(value)
    is_value_redis_compatible = type(value) in REDIS_STORED_TYPES
    return is_value_redis_compatible or is_value_callable or is_value_class


def is_list_of_strings(var) -> bool:
    """Check for list[str] variable type."""
    return isinstance(var, list) and not all(isinstance(v, str) for v in var)
