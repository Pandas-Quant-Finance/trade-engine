from collections import defaultdict
from typing import Dict, List



class Component(object):

    def __init__(self):
        self._handlers: Dict[type, List[callable]] = defaultdict(list)

    def register(self, parent: 'Component'):
        # pass all event hanlers to the parent
        for k, v in self._handlers.items():
            v.extend(parent._handlers[k])
            parent._handlers[k] = v

        # clear all own event handlers
        self._handlers.clear()

        # make sure self fire acutally fires the parents fire
        self.fire = parent.fire

        return self

    def register_event(self, *events: type, handler: callable):
        for event in events:
            self._handlers[event].append(handler)

    def fire(self, event):
        for handler in self._handlers[type(event)]:
            handler(event)

    def start(self):
        pass

    def stop(self):
        pass

    def get_handlers(self):
        return self._handlers

