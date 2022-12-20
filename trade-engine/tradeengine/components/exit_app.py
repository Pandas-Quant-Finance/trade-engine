from __future__ import annotations

import logging

from circuits import Component, handler

from ..events import *

_log = logging.getLogger(__name__)


class ExitApp(Component):

    @handler(TakePoisonPillEvent.__name__)
    def suicide(self):
        _log.warning("Received poison pill")
        raise SystemExit(0)
