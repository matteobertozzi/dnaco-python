import math
from time import time_ns

from dnaco.util import humans


class TimeRangeCounter:
    COLLECTOR_TYPE = 'TIME_RANGE_COUNTER'

    def __init__(self, max_interval, window):
        slots = int(math.ceil(max_interval / window))
        self.counters = [0] * slots
        self.window = window
        self.next = 0
        self._set_last_interval(time_ns())

    def clear(self):
        for i in range(len(self.counters)):
            self.counters[i] = 0
        self._set_last_interval(time_ns())
        self.next = 0

    def inc(self, now=None):
        self.add(1, now if now else time_ns())

    def add(self, amount=1, now=None):
        now = now if now else time_ns()
        delta = now - self.last_interval
        if delta < self.window:
            self.counters[self.next % len(self.counters)] += amount
            return

        self._inject_zeros(now)
        self._set_last_interval(now)
        self.next += 1
        self.counters[self.next % len(self.counters)] = amount

    def _set_last_interval(self, now):
        self.last_interval = (now - (now % self.window))

    def _inject_zeros(self, now, keep_prev=False):
        delta = (now - self.last_interval)
        if delta < self.window: return

        slots = int(delta / self.window) - 1
        if slots > 0:
            value = self.counters[self.next % len(self.counters)] if keep_prev else 0
            for i in range(slots):
                self.next += 1
                self.counters[self.next % len(self.counters)] = value
            self._set_last_interval(now)

    def snapshot(self):
        index = self.next % len(self.counters)
        return {
            'window': self.window,
            'last_interval': self.last_interval,
            'counters': (self.counters[index + 1:] + self.counters[:index + 1])[-(self.next + 1):]
        }

    def human_report(self, human_converter):
        index = self.next % len(self.counters)
        data = (self.counters[index + 1:] + self.counters[:index + 1])[-(self.next + 1):]
        return 'window %s - %s - [%s] - %s' % (
            humans.human_time_diff_ns(self.window),
            humans.human_date_time_ns(self.last_interval - (self.window * len(data))),
            ','.join(human_converter(v) for v in data),
            humans.human_date_time_ns(self.last_interval)
        )
