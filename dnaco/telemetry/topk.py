# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque
from time import time_ns

from dnaco.util import humans


class TopK:
    COLLECTOR_TYPE = 'TOP_K'

    class _Entry:
        def __init__(self, key):
            self.key = key
            self.vmin = None
            self.vmax = None
            self.vmax_ts = None
            self.vsum = 0
            self.freq = 0
            self.trace_ids = deque([], 5)

        def update(self, value, trace_id):
            if self.vmax is None or value >= self.vmax:
                self.trace_ids.append(trace_id)
                self.vmax_ts = time_ns()
                self.vmax = value
            self.vmin = min(value, self.vmin) if self.vmin else value
            self.vsum += value
            self.freq += 1

        def snapshot(self):
            return {
                'key': self.key,
                'max_ts': self.vmax_ts,
                'max': self.vmax,
                'min': self.vmin,
                'avg': (self.vsum // self.freq),
                'freq': self.freq,
                'trace_ids': list(self.trace_ids)
            }

        def human_report(self, human_converter):
            return [
                self.key, humans.human_date_time_ns(self.vmax_ts),
                human_converter(self.vmax), human_converter(self.vmin),
                human_converter((self.vsum // self.freq)), self.freq,
                str(list(self.trace_ids))
            ]

    def __init__(self, k):
        self.data = {}
        self.k = k

    def clear(self):
        self.data = {}

    def add(self, key, value, trace_id=None):
        entry = self.data.get(key)
        if entry is None:
            entry = self._Entry(key)
            self.data[key] = entry
        entry.update(value, trace_id)

        if len(self.data) > (self.k * 2):
            self.data = self._compute(self.k * 2)

    def _compute(self, n):
        data = {}
        entries = sorted(self.data.values(), key=lambda e: e.vmax, reverse=True)
        for entry in entries[:n]:
            data[entry.key] = entry
        return data

    def snapshot(self):
        data = []
        for entry in self._compute(self.k).values():
            data.append(entry.snapshot())
        return data

    def human_report(self, human_converter):
        table = humans.HumansTableView()
        table.add_columns(['', 'Max Timestamp', 'Max', 'Min', 'Avg', 'Freq', 'Trace Ids'])
        for entry in self._compute(self.k).values():
            table.add_row(entry.human_report(human_converter))
        return table.human_view()
