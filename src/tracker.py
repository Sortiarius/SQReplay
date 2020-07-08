from event import Event
import typing


class TrackedUnits:
    def __init__(self):
        self._tracked = {}

    def add(self, event: Event) -> None:
        key = "{m_unitTagIndex}-{m_unitTagRecycle}".format(**event)
        self._tracked[key] = event

    def fetch(self, event: Event, killer=False) -> typing.Optional[Event]:
        try:
            if killer:
                key = "{m_killerUnitTagIndex}-{m_killerUnitTagRecycle}".format(**event)
            else:
                key = "{m_unitTagIndex}-{m_unitTagRecycle}".format(**event)
            return self._tracked.get(key)
        except KeyError:
            return

    def delete(self, event: Event) -> None:
        key = "{m_unitTagIndex}-{m_unitTagRecycle}".format(**event)
        try:

            del self._tracked[key]
        except KeyError:
            print("Tried to delete non-existing key.")
