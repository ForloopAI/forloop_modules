class CriticalPipelineError(Exception):
    """
    Exception used for handling pipeline-breaking errors during function_handler runtime.

    It should be raised only in case of a critical error, when the ExecutionCore cannot recover the
    pipeline flow, and it's execution must be terminated.

    Must always be raised with 'raise ... from error' clause to retain the information about
    original error.
    """


class SoftPipelineError(Exception):
    """
    Exception used for handling node-breaking, but non-pipeline-breaking errors during
    function_handler runtime.

    It should be raised only in case of a recoverable errors, when the ExecutionCore can omit
    the current node execution without any pipeline flow disruption.

    Must always be raised with 'raise ... from error' clause to retain the information about
    original error.
    """


class MalformedPipelineError(Exception):
    """
    Exception for handling errors related to graph construction and validation rule violations.

    Exception used in two cases:
    - graph initialization fails due to incompatible pipeline elements
    - not passing a pipeline validation rule (no logic implemented yet)

    Must always be raised with 'raise ... from error' clause to retain the information about original error.
    """


class InitializationError(Exception):
    """
    Exception raised during application/web_server start up.

    Raised whenever any essential module/component of the application has not been initialized
    properly. This can mean failure while setting DB/Redis connection, initialization assertions, etc.
    """
