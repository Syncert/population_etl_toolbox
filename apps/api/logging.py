"""One logging configuration, owned by the application (API-143).

The API's only request-level operational signal is the completion line
``apps.api.telemetry`` writes at ``INFO``: method, route shape, status,
duration, cache disposition, and the ``X-Request-ID`` the consumer guide
promises a client can quote "with the server's logs". Nothing configured
logging for the deployed process, and that promise was not kept.

Uvicorn's default configuration attaches handlers to its own ``uvicorn``,
``uvicorn.error`` and ``uvicorn.access`` loggers and to nothing else. The root
logger stays at ``WARNING`` with the last-resort handler, which writes the
bare message with no timestamp, no level and no logger name. So in a deployed
container every ``INFO`` record under ``apps.api`` was dropped -- the whole of
request observability -- and the ``WARNING``/``ERROR`` records from the
middleware, the freshness reader and the dependencies arrived unformatted and
untimed.

The configuration is deliberately small and deliberately local:

- It attaches **one** stream handler to ``apps.api``, the parent of every
  logger this application writes through, and leaves uvicorn's own loggers
  exactly as uvicorn configured them. Two processes' logging conventions do
  not need to be merged to fix one of them.
- It does not disable propagation. With no handler on the root logger nothing
  is duplicated, the last-resort handler is never reached because a handler
  was found, and a test that captures at the root still sees these records --
  which is how every existing assertion about this line is written.
- The request id is not a formatter field. It is already inside the
  completion line's own message, and a ``%(request_id)s`` in the format would
  print a placeholder on every record that does not carry one, which is more
  noise than the id is worth. What the formatter adds is what the records had
  no way to carry: when, how bad, and from where.
"""

from __future__ import annotations

import logging
import sys
from typing import IO

#: The logger every ``apps.api`` module writes through, by way of its
#: children: ``apps.api.request``, ``apps.api.middleware`` and the rest.
APPLICATION_LOGGER_NAME = "apps.api"

#: Named so that re-configuring replaces this handler rather than stacking
#: another beside it. ``create_app`` runs once in a deployed process and many
#: times in a test session.
HANDLER_NAME = "apps.api.stream"

#: Timestamp, level, logger, message. The completion line carries its own
#: request id; see the module docstring.
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging(
    level: str = "INFO", *, stream: IO[str] | None = None
) -> logging.Logger:
    """Attach the application's one handler to ``apps.api`` and set its level.

    Returns the configured logger. Idempotent: calling it again replaces the
    handler it installed rather than adding a second one, so a process that
    builds the application twice does not log every line twice.
    """
    logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    logger.setLevel(level)

    for existing in [
        handler for handler in logger.handlers if handler.name == HANDLER_NAME
    ]:
        logger.removeHandler(existing)
        existing.close()

    # stderr, where uvicorn writes its own lines, so one `docker compose logs`
    # shows the process's output in one order.
    handler = logging.StreamHandler(sys.stderr if stream is None else stream)
    handler.name = HANDLER_NAME
    handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logger.addHandler(handler)
    return logger
