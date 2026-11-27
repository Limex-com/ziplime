"""Support for roll_finder."""

from ziplime.assets.domain.roll_finder.calendar_roll_finder import CalendarRollFinder
from ziplime.assets.domain.roll_finder.volume_roll_finder import VolumeRollFinder


ROLL_FINDERS = {
    "calendar": CalendarRollFinder,
    "volume": VolumeRollFinder,
}
