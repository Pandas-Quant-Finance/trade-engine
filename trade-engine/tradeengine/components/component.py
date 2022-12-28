from collections import defaultdict
from typing import Dict, List


__HANDLERS__: Dict[type, List[callable]] = defaultdict(list)


class Component(object):

    def __init__(self):
        pass

    def register(self, *events: type, handler: callable):
        for event in events:
            __HANDLERS__[event].append(handler)

    def fire(self, event):
        for handler in __HANDLERS__[type(event)]:
            handler(event)
