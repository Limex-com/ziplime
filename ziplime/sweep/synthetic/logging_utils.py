"""Logging utils for synthetic."""

import contextlib
import logging
import structlog


@contextlib.contextmanager
def quiet_logs():
    """The kernel logs every batch it runs; once per path is noise."""
    saved = structlog.get_config()
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))
    try:
        yield
    finally:
        structlog.configure(**saved)
