import sys
import os
import traceback

from datetime import datetime
from typing import Tuple

from enum import Enum, unique
from pathlib import Path


DEVELOPER_MODE=True #originally in thread flags - it is here because of packaging - maybe rename locally to PACKAGING_MODE
MESSAGE_CATEGORIES=["*"]

if DEVELOPER_MODE:
    from configparser import ConfigParser

#from src.function_handlers.abstract_function_handler import AbstractFunctionHandler #NO! Antipattern! Circular import!


if DEVELOPER_MODE:
    # instantiate
    config = ConfigParser()
    config.optionxform = str


    # parse existing file
    config.read(Path('config/flog_config.ini'))

    try:
        output_stream = config.get("LOGGER", "Output stream")
        if output_stream == "file":
            if not os.path.exists('../logs'):
                os.makedirs('../logs')
    
            OUTPUT = open("../logs/flog.out", "a+")
        else:
            OUTPUT = sys.stdout
    except Exception as e:
        print("forloop_modules.flog: Warning: Output stream couldn't be defined - ignoring ",e)
        OUTPUT = sys.stdout
else:
    OUTPUT = sys.stdout


@unique
class LogColor(Enum):
    """
    Helper class for coloring log output
    """
    OKGREEN = '\033[92m'
    ERROR = '\033[31m'
    WARNING = '\033[93m'
    BOLD = '\033[1m'
    COLOROFF = '\033[0m'


# Logging enum
# Constants reproducing logging look-a-like syntax
@unique
class FlogLevel(Enum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    MINORINFO = 15
    DEBUG = 10
    NOTSET = 0


# Logging levels for specific classes, logs with values higher than config are printed
# If class name is not found, parent class may be used, otherwise DEFAULT config is sed
FLOG_CONFIG = {
    "DEFAULT": FlogLevel.WARNING,
    "Wizard": FlogLevel.INFO,
    "Scanner": FlogLevel.INFO,
    "CleaningUtility": FlogLevel.INFO,
    "DfToListHandler": FlogLevel.DEBUG
}

if DEVELOPER_MODE:
    # update FLOG_CONFIG with config.ini settings
    try:
        ini_flog_config = dict(config.items("LOGGER.FLOG_CONFIG"))
        for key, value in ini_flog_config.items():
            FLOG_CONFIG[key] = eval(f"FlogLevel.{value}")
            
    except Exception as e:
        print("forloop_modules.flog: Warning: FLOG_CONFIG wasn't redefined - ignoring ",e)
        


class EmptyClass:
    """
    Dummy class with empty name for flogger calls from outside of any class
    """

    def __init__(self):
        self.__class__.__name__ = ""


def augment_message(message: str, color: LogColor, header: str = "") -> str:
    message = f"{header}{message}"
    if OUTPUT == sys.stdout:
        message = f'{color.value}{message}{LogColor.COLOROFF.value}'
    return message


def get_class_name_config_key_pair(class_instance: object) -> Tuple[str, str]:
    """
    Helper function to get class name and corresponding key in flog config dictionary
    Corresponding key may be class' name, class' parent name or 'DEFAULT'

    :param class_instance: class from which logger was called
    :return: class name and corresponding key in flog config dictionary
    :rtype: str, str
    """
    cls_name = type(class_instance).__name__
    flog_config_key = "DEFAULT"
    if cls_name in FLOG_CONFIG:
        flog_config_key = cls_name

    if flog_config_key == "DEFAULT":
        ancestor_cls_name = type(class_instance.__class__.__bases__[0]).__name__
        if ancestor_cls_name in FLOG_CONFIG:
            flog_config_key = ancestor_cls_name

    return cls_name, flog_config_key


def get_cls_loglevel(class_name: str) -> int:
    """
    Get minimum log level for specified class

    :param class_name: flog config key corresponding to log calling class, empty if called outside of class
    :type class_name: str
    :return: Minimum log level is the minimum the flogger is allowed to output
    :rtype: int
    """
    log_level = FLOG_CONFIG[class_name].value

    return log_level


def get_callers_class_instance():
    # return instance of a class that flog has been called from
    # sys._getframe(2) returns fourth frame from stack (get_callers_class_instance - error - {caller}
    try:
        class_instance = sys._getframe(2).f_locals["self"]
    except KeyError:
        class_instance = EmptyClass()

    return class_instance


def wrap_add_class_name(func):
    # automatically add class instance to flogger method call
    def add_class_name(message: str, class_instance=None, message_category = "*"):
        if class_instance is None:
            class_instance = get_callers_class_instance()

        return func(message, class_instance, message_category = message_category)
    return add_class_name


def is_error_raised():
    _, exc_value, _ = sys.exc_info()
    return exc_value is not None

def print_exception_if_raised():
    if is_error_raised():
        traceback.print_exc()


@wrap_add_class_name
def critical(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print red colored critical message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.CRITICAL.value:
        if is_error_raised():
            traceback.print_exc()
        flog(message, class_name, color=LogColor.ERROR, message_category=message_category)


@wrap_add_class_name
def error(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print red colored error message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.ERROR.value:

        #print_exception_if_raised()

        if is_error_raised():
            traceback.print_exc()

        # For better debugging in handlers (will show handler's params)
        # NO! CIRCULAR IMPORT! FLOG CANNOT IMPORT ABSTRACT FUNCTION HANDLER!
        #if isinstance(class_instance, AbstractFunctionHandler):
        #    message += f"\nNode's form_dict_list: {class_instance.make_form_dict_list()}"

        flog(message, class_name, color=LogColor.ERROR, message_category=message_category)


@wrap_add_class_name
def warning(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print yellow colored warning message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.WARNING.value:
        flog(message, class_name, color=LogColor.WARNING, message_category=message_category)


@wrap_add_class_name
def info(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print info message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.INFO.value:
        flog(message, class_name, message_category=message_category)


@wrap_add_class_name
def minor_info(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print minor_info message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.MINORINFO.value:
        flog(message, class_name, message_category=message_category)


@wrap_add_class_name
def debug(message="", class_instance: object = EmptyClass(), message_category="*"):
    """
    print debug message
    """

    message = str(message)

    class_name, class_flog_config_key = get_class_name_config_key_pair(class_instance)
    cls_min_log_level = get_cls_loglevel(class_flog_config_key)

    if cls_min_log_level <= FlogLevel.DEBUG.value:
        flog(message, class_name, message_category=message_category)


def flog(message: str, class_name: str, color: LogColor = LogColor.COLOROFF, message_category="*"):
    """
    print a specified message prepended with datetime and class name(or empty string in case of EmptyClass)

    When thread_flags.DEVELOPER_MODE == False ==> DISABLED! (colored printing disrupts packaged versions)
    """

    if DEVELOPER_MODE:
        header = f"{datetime.now().strftime('%H:%M:%S')} "
        if class_name:
            header += f"{class_name}: "
            
        colored_message = augment_message(message, color, header)
        if message_category in MESSAGE_CATEGORIES:
            print(colored_message, file=OUTPUT)


if __name__ == '__main__':
    debug(message="debug test: you should not see this")
    minor_info(message="minor_info test: you should not see this")
    info(message="info test")
    warning(message="warning test")
    error(message="error test")
    critical(message="critical test")

    FLOG_CONFIG["DEFAULT"] = FlogLevel.MINORINFO
    debug(message="debug test: you should not see this")
    minor_info(message="minor_info test: you should see this")

    FLOG_CONFIG["DEFAULT"] = FlogLevel.DEBUG
    debug(message="debug test - you should see this")
    minor_info(message="minor_info test - you should see this")

    class Test:
        def __init__(self):
            info("testing info logging from class Test")

    Test()
