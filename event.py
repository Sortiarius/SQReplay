import typing, datetime
from decimal import Decimal


class Event(dict):

    def __init__(self, *args, **kwargs):
        super(Event, self).__init__(*args, **kwargs)
        self._event = self.get('_event')

        # The default return value is bytes for quite a few of the units.
        # Encode them to strings for easier parsing.
        for key, value in self.items():
            if isinstance(value, bytes):
                value:bytes
                self[key] = value.decode(encoding='utf-8')

    @property
    def unit(self) -> typing.Optional[str]:
        """Returns the unit name if any"""
        return self.get('m_unitTypeName')

    @property
    def unit_born(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitBornEvent'

    @property
    def unit_init(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitInitEvent'

    @property
    def unit_done(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitDoneEvent'

    @property
    def unit_died(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitDiedEvent'

    @property
    def stats_update(self) -> bool:
        return self._event == "NNet.Replay.Tracker.SPlayerStatsEvent"

    @property
    def time_event(self) -> bool:
        return self._event == "NNet.Game.SSetSyncLoadingTimeEvent"

    @property
    def player_setup(self) -> bool:
        return self._event == "NNet.Replay.Tracker.SPlayerSetupEvent"

    @property
    def upgrade_event(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUpgradeEvent'

    @property
    def unit_owner_transferred(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitOwnerChangeEvent'

    @property
    def unit_type_changed(self) -> bool:
        return self._event == 'NNet.Replay.Tracker.SUnitTypeChangeEvent'

    @property
    def message_received(self) -> bool:
        return self._event == 'NNet.Game.SChatMessage'

    @property
    def position(self) -> typing.Optional[int]:
        x, y = self.get('m_x'), self.get('m_y')
        if x is None or y is None:
            return
        x = round((x - 20.5) / 10.0)
        y = round((90.0 - y) / 10)
        return x + (round(y * 8.0))

    @property
    def game_time(self):
        game_time = Decimal(self.get('_gameloop', 0))
        game_time = game_time / Decimal("16")   # Trigger update time
        game_time = game_time / Decimal("1.4")  # Map speed Faster
        return str(round(game_time, 4))           # in seconds

    @property
    def formatted_game_time(self):
        return str(datetime.timedelta(seconds=float(self.game_time)))

    @property
    def trackable_unit(self):
        tracked_units = [
            'Bunker',
            'SCV',
            'Nuke',
            'Tank',
        ]
        return any([self.is_unit(u) for u in tracked_units])

    def is_unit(self, unit_name: str, key='m_unitTypeName') -> bool:
        unit = self.get(key)
        encoded = unit_name.encode('utf-8')
        return unit == encoded or unit == unit_name
