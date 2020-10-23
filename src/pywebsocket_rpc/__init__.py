# Set default logging handler to avoid "No handler found" warnings.
import logging
from logging import NullHandler, StreamHandler

log = logging.getLogger(__name__)
log.addHandler(NullHandler())
