# This file serves for synchronizing variables in different threads, 
# It's main advantage is that it can be imported from anywhere

IS_API_LOADED=False
IS_MAIN_APP_LOADED=False
IS_PIPELINE_EXECUTION_THREAD_RUNNING=False


# Following variables serve to distinguish whether Forloop is running in a single process or from multiple processes and 
# It also serves to determine what is the launch file (__main__.py, forloop_fastapi.py, execution_core.py, job_scheduler.py)
IS_MODULE_MAIN_INITIALIZED = False
IS_MODULE_FORLOOP_FASTAPI_INITIALIZED = False
IS_MODULE_EXECUTION_CORE_INITIALIZED = False
IS_MODULE_JOB_SCHEDULER_INITIALIZED = False


# Following variables are used to pass redis configs defined in the platform to modules used in Forloop_modules
REDIS_CONFIG_HOST = None
REDIS_CONFIG_USERNAME = None
REDIS_CONFIG_PASSWORD = None

E2B_API_KEY = None # Used for E2B connection in RunPythonScriptHandler