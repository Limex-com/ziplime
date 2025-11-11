# Logging in Ziplime

Ziplime uses structured logging through the `structlog` library to provide consistent, contextual logging across the
application. This approach enables better debugging, monitoring, and log analysis capabilities.

## Configuration

The simplest way to configure logging is using the `configure_logging` utility:

```python
import logging
from ziplime.utils.logging_utils import configure_logging

# Use ERROR log level and output logs (in addition to console output) to file named `mylog.log`
configure_logging(level=logging.ERROR, file_name="mylog.log")
```

It's important to call `configure_logging` function before using logger as configuration is applied only to logs
that are printed after configuration.