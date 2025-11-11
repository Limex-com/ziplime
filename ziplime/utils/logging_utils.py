import logging
import sys

import structlog


def configure_logging(level: int = logging.INFO, file_name: str = None) -> None:
    """
    Configures logging for both the standard library logging and structlog.

    This function sets up a logging configuration to enable consistent and
    structured logging across an application. It configures the root logger from
    the standard library as well as structlog with appropriate handlers,
    formatters, and processors.

    Args:
        level (int): The logging level to be used (default is logging.INFO).
        file_name (str, optional): The name of the file to log messages to. If not
            provided, logging will be restricted to the console.

    Returns:
        None
    """
    # 1) Configure the standard library root logger
    #    All handlers you add will inherit this level.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    # Create formatter
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(name)s: %(message)s')

    # Create standard output handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)

    if file_name:
        file_handler = logging.FileHandler(file_name)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set the overall logging level for the root logger
    root_logger.setLevel(level)

    # 2) Tell structlog to filter by level first,
    #    then add any other processors you need.
    structlog.configure(
        processors=[
            # If log level is too low, abort pipeline and throw away log entry.
            structlog.stdlib.filter_by_level,
            # Add the name of the logger to event dict.
            structlog.stdlib.add_logger_name,
            # Add log level to event dict.
            structlog.stdlib.add_log_level,
            # Perform %-style formatting.
            structlog.stdlib.PositionalArgumentsFormatter(),
            # Add a timestamp in ISO 8601 format.
            structlog.processors.TimeStamper(fmt="iso"),
            # If the "stack_info" key in the event dict is true, remove it and
            # render the current stack trace in the "stack" key.
            structlog.processors.StackInfoRenderer(),
            # If the "exc_info" key in the event dict is either true or a
            # sys.exc_info() tuple, remove "exc_info" and render the exception
            # with traceback into the "exception" key.
            structlog.processors.format_exc_info,
            # If some value is in bytes, decode it to a Unicode str.
            structlog.processors.UnicodeDecoder(),
            # Add callsite parameters.
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            # Render the final event dict as JSON.
            structlog.processors.JSONRenderer()
        ],
        # `wrapper_class` is the bound logger that you get back from
        # get_logger(). This one imitates the API of `logging.Logger`.
        wrapper_class=structlog.stdlib.BoundLogger,
        # `logger_factory` is used to create wrapped loggers that are used for
        # OUTPUT. This one returns a `logging.Logger`. The final value (a JSON
        # string) from the final processor (`JSONRenderer`) will be passed to
        # the method of the same name as that you've called on the bound logger.
        logger_factory=structlog.stdlib.LoggerFactory(),
        # logger_factory=structlog.WriteLoggerFactory(
        #     file=Path(file_name).open("wt")
        # ),
        # Effectively freeze configuration after creating the first bound
        # logger.
        cache_logger_on_first_use=True,
    )
