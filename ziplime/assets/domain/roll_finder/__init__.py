"""Deciding which contract a continuous future holds on a given date.

``DataBundle`` consulted ``self._roll_finders`` but nothing ever defined the class or assigned the
attribute, so continuous futures could not resolve to a contract at all. These are the
implementations behind the two roll styles a
:class:`~ziplime.assets.domain.continuous_future.ContinuousFuture` may ask for.
"""

from ziplime.assets.domain.roll_finder.roll_finder import RollFinder  # noqa: F401
from ziplime.assets.domain.roll_finder.calendar_roll_finder import (  # noqa: F401
    CalendarRollFinder,
)
from ziplime.assets.domain.roll_finder.volume_roll_finder import VolumeRollFinder  # noqa: F401
from ziplime.assets.domain.roll_finder.support import ROLL_FINDERS  # noqa: F401


__all__ = [
    'RollFinder',
    'CalendarRollFinder',
    'VolumeRollFinder',
    'ROLL_FINDERS',
]
