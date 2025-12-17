# Borrowed from https://github.com/libp2p/py-libp2p/blob/main/libp2p/utils/logging.py.
from __future__ import annotations

import atexit
import logging
import logging.handlers
import queue
import sys
import threading
from typing import Any

from . import envs

_LOG_QUEUE: queue.Queue[Any] = queue.Queue()

_LOG_LISTENER: logging.handlers.QueueListener | None = None

_LOG_LISTENER_READY = threading.Event()

DEFAULT_LOG_FORMAT = (
    "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
)


def _parse_module_levels(level_str: str) -> dict[str, int]:
    """
    Parse the GPUSTACK_RUNTIME_LOG_LEVEL environment variable to determine module-specific log levels.

    Examples:
        - "DEBUG"                                                           # All modules at DEBUG
        - "gpustack_runtime.module_a:DEBUG"                                 # Only module_a module at DEBUG, other modules at INFO
        - "module_a:DEBUG"                                                  # Same as above
        - "module_a=DEBUG"                                                  # Using '=' instead of ':'
        - "gpustack_runtime.module_a:DEBUG;gpustack_runtime.module_b:INFO"  # Multiple modules
        - "ERROR;runtime.module_a:DEBUG"                                    # All modules at ERROR, only module_a module at DEBUG

    """
    module_levels: dict[str, int] = {}  # {"module_name": log_level}

    if not level_str or level_str.isspace():
        return module_levels

    levels = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    if ":" not in level_str and level_str.upper() in levels:
        return {"": getattr(logging, level_str.upper())}

    for p in level_str.split(";"):
        module = ""
        level = ""
        if ":" in p:
            module, level = p.split(":", 1)
        elif "=" in p:
            module, level = p.split("=", 1)

        level = level.upper()
        if level not in levels:
            continue

        module = module.strip()
        module = module.replace(f"{__package__}.", "")
        module = module.replace("/", ".").strip(".")

        module_levels[module] = getattr(logging, level)

    return module_levels


def setup_logging():
    """
    Set up logging configuration based on environment variables.

    Environment Variables:
        GPUSTACK_RUNTIME_LOG_LEVEL
            Controls logging levels. Examples:
            - "DEBUG"                                                           # All modules at DEBUG
            - "gpustack_runtime.module_a:DEBUG"                                 # Only module_a module at DEBUG, other modules at INFO
            - "module_a:DEBUG"                                                  # Same as above
            - "gpustack_runtime.module_a:DEBUG;gpustack_runtime.module_b:INFO"  # Multiple modules
            - "ERROR;gpustack_runtime.module_a:DEBUG"                           # All modules at ERROR, only module_a module at DEBUG

        GPUSTACK_RUNTIME_LOG_TO_FILE
            If set, specifies the file path for log output. When this variable is set,
            logs will only be written to the specified file. If not set, logs will be
            written to stderr (console output).

    The logging system uses Python's native hierarchical logging:
        - Loggers are organized in a hierarchy using dots
          (e.g., runtime.module_a.submodule_1)
        - Child loggers inherit their parent's level unless explicitly set
        - The root runtime logger controls the default level: INFO
    """
    global _LOG_LISTENER, _LOG_LISTENER_READY

    _LOG_LISTENER_READY.clear()
    if _LOG_LISTENER is not None:
        _LOG_LISTENER.stop()
        _LOG_LISTENER = None

    level_str = envs.GPUSTACK_RUNTIME_LOG_LEVEL or "INFO"
    module_levels = _parse_module_levels(level_str)

    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
    level = module_levels.get("", logging.INFO)
    handlers: list[logging.StreamHandler[Any] | logging.FileHandler] = []
    queue_handler = logging.handlers.QueueHandler(_LOG_QUEUE)

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # File handler (if configured)
    if log_file := envs.GPUSTACK_RUNTIME_LOG_TO_FILE:
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure package logger
    package_logger = logging.getLogger(__package__)
    package_logger.handlers.clear()
    package_logger.setLevel(level)
    package_logger.addHandler(queue_handler)
    package_logger.propagate = False

    # Configure module loggers
    for module, module_level in module_levels.items():
        if module:  # Skip default level
            module_logger = logging.getLogger(f"{__package__}.{module}")
            module_logger.handlers.clear()
            module_logger.addHandler(queue_handler)
            module_logger.setLevel(module_level)
            module_logger.propagate = False

    # Configure 3rd-party loggers, set slightly higher level than package level
    for _3rd in ["docker", "kubernetes"]:
        _3rd_logger = logging.getLogger(_3rd)
        _3rd_logger.handlers.clear()
        _3rd_logger.addHandler(queue_handler)
        _3rd_logger.setLevel(max(logging.NOTSET, min(level + 10, logging.CRITICAL)))
        _3rd_logger.propagate = False

    _LOG_LISTENER = logging.handlers.QueueListener(
        _LOG_QUEUE,
        *handlers,
        respect_handler_level=True,
    )
    _LOG_LISTENER.start()
    _LOG_LISTENER_READY.set()


# Register cleanup function
@atexit.register
def cleanup_logging() -> None:
    """
    Clean up logging resources on exit.
    """
    global _LOG_LISTENER

    if _LOG_LISTENER is not None:
        _LOG_LISTENER.stop()
        _LOG_LISTENER = None


def debug_log_warning(logger: logging.Logger, msg: str, *args: Any):
    """
    Log a warning message,
    if the logger is enabled for DEBUG and GPUSTACK_RUNTIME_LOG_WARNING is enabled.

    Args:
        logger: The logger instance to use.
        msg: The message format string.
        *args: Arguments to be formatted into the message.

    """
    if logger.isEnabledFor(logging.DEBUG) and envs.GPUSTACK_RUNTIME_LOG_WARNING:
        logger.warning(msg, *args)


def debug_log_exception(logger: logging.Logger, msg: str, *args: Any):
    """
    Log an exception message,
    if the logger is enabled for DEBUG and GPUSTACK_RUNTIME_LOG_EXCEPTION is enabled.

    Args:
        logger: The logger instance to use.
        msg: The message format string.
        *args: Arguments to be formatted into the message.

    """
    if logger.isEnabledFor(logging.DEBUG) and envs.GPUSTACK_RUNTIME_LOG_EXCEPTION:
        logger.exception(msg, *args)
