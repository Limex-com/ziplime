#
# Copyright 2015 Quantopian, Inc.
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

from pandas import Timestamp

EPOCH = Timestamp(0, tz='UTC')


def _lookup_dt(dt_cache: dict,
               dt: int,
               fallback):
    if dt not in dt_cache:
        dt_cache[dt] = fallback.searchsorted(dt, side='right')
    return dt_cache[dt]
