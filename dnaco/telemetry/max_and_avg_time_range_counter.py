import math
from time import time_ns

from dnaco.util import humans


class MaxAndAvgTimeRangeGauge:
    COLLECTOR_TYPE = 'MAX_AND_AVG_TIME_RANGE_GAUGE'

    def __init__(self, max_interval, window):
        slots = int(math.ceil(max_interval / window))
        self.ring_max = [0] * slots
        self.ring_avg = [0] * slots
        self.window = window
        self.count = 0
        self.vmax = 0
        self.vsum = 0
        self.next = 0
        self._set_last_interval(time_ns())

    def clear(self):
        for i in range(len(self.ring_max)):
            self.ring_max[i] = 0
            self.ring_avg[i] = 0
        self.count = 0
        # self.vmax = 0
        # self.vsum = 0
        self.next = 0
        self._set_last_interval(time_ns())

    def _set_last_interval(self, now):
        self.last_interval = (now - (now % self.window))

    def update(self, value, now=None):
        self.set_value(now if now else time_ns(), value)

    def set_value(self, now, value):
        delta = now - self.last_interval
        if delta < self.window:
            self.vmax = max(value, self.vmax)
            self.vsum += value
            self.count += 1
            return

        self._inject_zeros(now)
        self._set_last_interval(now)
        self._save_snapshot()

    def _save_snapshot(self, next_inc=1):
        self.next += next_inc
        index = (self.next % len(self.ring_avg))
        avg = (self.vsum // self.count) if self.count > 0 else 0
        self.ring_avg[index] = avg
        self.ring_max[index] = self.vmax
        self.count = 1
        self.vsum = avg
        self.vmax = avg

    def _inject_zeros(self, now, keep_prev=False):
        delta = (now - self.last_interval)
        if delta < self.window: return

        slots = int(delta / self.window) - 1
        for _ in range(slots):
            self._save_snapshot()

    def snapshot(self):
        self._save_snapshot(0)
        index = self.next % len(self.ring_avg)
        return {
            'events': {
                'avg': (self.ring_avg[index + 1:] + self.ring_avg[:index + 1])[-(self.next + 1):],
                'max': (self.ring_max[index + 1:] + self.ring_max[:index + 1])[-(self.next + 1):]
            },
            'slots': len(self.ring_avg),
            'window': self.window,
            'last_interval': self.last_interval
        }

    def human_report(self, human_converter):
        self._save_snapshot(0)
        index = self.next % len(self.ring_avg)
        data_avg = (self.ring_avg[index + 1:] + self.ring_avg[:index + 1])[-(self.next + 1):]
        data_max = (self.ring_max[index + 1:] + self.ring_max[:index + 1])[-(self.next + 1):]
        return 'window %s - %s - [%s] - %s' % (
            humans.human_time_diff_ns(self.window),
            humans.human_date_time_ns(self.last_interval - (self.window * len(data_avg))),
            ','.join(human_converter(vavg) + '/' + human_converter(vmax) for vavg, vmax in zip(data_avg, data_max)),
            humans.human_date_time_ns(self.last_interval)
        )
