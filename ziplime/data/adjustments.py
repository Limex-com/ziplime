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

from itertools import chain

import pandas as pd
from numpy import (
    int64,
    uint32,
    zeros,
)
from pandas import Timestamp

from ziplime.lib.adjustment import Float64Multiply

from ziplime.utils.pandas_utils import timedelta_to_integral_seconds
from ziplime.utils.sqlite_utils import SQLITE_MAX_VARIABLE_NUMBER

_SID_QUERY_TEMPLATE = """
SELECT DISTINCT sid FROM {0}
WHERE effective_date >= ? AND effective_date <= ?
"""
SID_QUERIES = {
    tablename: _SID_QUERY_TEMPLATE.format(tablename)
    for tablename in ('splits', 'dividends', 'mergers')
}

ADJ_QUERY_TEMPLATE = """
SELECT sid, ratio, effective_date
FROM {0}
WHERE sid IN ({1}) AND effective_date >= {2} AND effective_date <= {3}
"""

EPOCH = Timestamp(0, tz='UTC')





def _lookup_dt(dt_cache: dict,
               dt: int,
               fallback):
    if dt not in dt_cache:
        dt_cache[dt] = fallback.searchsorted(dt, side='right')
    return dt_cache[dt]
