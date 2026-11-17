#
# Copyright 2019 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Base contract for scheduled-event rules."""

from abc import ABC, abstractmethod
import datetime

from exchange_calendars import ExchangeCalendar


class EventRule(ABC):
    """A rule defining when a scheduled function should execute."""

    # Instances of EventRule are assigned a calendar instance when scheduling
    # a function.
    _cal = None

    @property
    def cal(self):
        return self._cal

    @cal.setter
    def cal(self, value: 'ExchangeCalendar') -> None:
        self._cal = value

    @abstractmethod
    def should_trigger(self, dt: 'datetime.datetime'):
        """
        Checks if the rule should trigger with its current state.
        This method should be pure and NOT mutate any state on the object.
        """
        raise NotImplementedError("should_trigger")
