from dnaco.util import humans


def min_value(bounds, events, value_max):
    for i in range(len(bounds)):
        if events[i] > 0:
            return bounds[i]
    return value_max


def max_value(bounds, events, value_max):
    for i in reversed(range(len(bounds))):
        if events[i] > 0:
            return bounds[i]
    return value_max


def mean(bounds, events, value_max, nevents):
    if nevents == 0: return 0
    xsum = 0
    for i in range(len(bounds)):
        xsum += bounds[i] * events[i]
    if events[-1] > 0:
        xsum += value_max * events[-1]
    return min(value_max, xsum / nevents)


def percentile(bounds, events, value_max, nevents, p):
    if nevents == 0: return 0

    threshold = nevents * (p * 0.01)
    xsum = 0
    i = 0
    while (i < len(bounds)) and (value_max > bounds[i]):
        xsum += events[i]
        if xsum >= threshold:
            # Scale linearly within this bucket
            left_point = bounds[0 if (i == 0) else (i - 1)]
            right_point = bounds[i]
            left_xsum = xsum - events[i]
            right_xsum = xsum
            pos = 0
            right_left_diff = right_xsum - left_xsum
            if right_left_diff != 0:
                pos = (threshold - left_xsum) / right_left_diff
            r = left_point + ((right_point - left_point) * pos)
            return value_max if (r > value_max) else r
        i += 1
    return value_max


class Histogram:
    COLLECTOR_TYPE = 'HISTOGRAM'

    DEFAULT_MS_DURATION_BOUNDS = [
        5, 10, 25, 50, 75, 100, 150, 250, 350, 500, 750,  # msec
        1000, 2500, 5000, 10000, 25000, 50000, 60000,  # sec
        75000, 120000,  # min
    ]

    DEFAULT_SIZE_BOUNDS = [
        0, 128, 256, 512,
        1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10,  # kb
        1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20, 128 << 20, 256 << 20, 512 << 20,  # mb
    ]

    def __init__(self, bounds):
        self.bounds = bounds
        self.events = [0] * (1 + len(bounds))
        self.vmax = 0

    def clear(self):
        for i in range(len(self.events)):
            self.events[i] = 0
        self.vmax = 0

    def add(self, value, num_events=1):
        index = 0
        bounds = self.bounds
        while (index < len(bounds)) and (value > bounds[index]):
            index += 1
        self.events[index] += num_events
        self.vmax = max(self.vmax, value)

    def snapshot(self):
        return {
            'bounds': self.bounds[:],
            'events': self.events[:],
            'nevents': sum(self.events),
            'max_value': self.vmax
        }

    def human_report(self, human_converter):
        nevents = sum(self.events)
        if nevents == 0: return '(no data)'

        buf = []
        buf.append('Count:%s Min:%s Mean:%s Max:%s' % (
            humans.human_count(nevents),
            human_converter(min_value(self.bounds, self.events, self.vmax)),
            human_converter(mean(self.bounds, self.events, self.vmax, nevents)),
            human_converter(max_value(self.bounds, self.events, self.vmax))
        ))
        buf.append('Percentiles: P50:%s P75:%s P99:%s P99.9:%s P99.99:%s' % (
            human_converter(percentile(self.bounds, self.events, self.vmax, nevents, 50)),
            human_converter(percentile(self.bounds, self.events, self.vmax, nevents, 75)),
            human_converter(percentile(self.bounds, self.events, self.vmax, nevents, 99)),
            human_converter(percentile(self.bounds, self.events, self.vmax, nevents, 99.9)),
            human_converter(percentile(self.bounds, self.events, self.vmax, nevents, 99.99))
        ))
        buf.append('----------------------------------------------------------------------')

        mult = 100.0 / nevents
        cumulative_sum = 0
        for b in range(len(self.bounds)):
            bucket_value = self.events[b]
            if bucket_value == 0: continue
            cumulative_sum += bucket_value
            marks = int(round(mult * bucket_value / 5 + 0.5))
            buf.append("[%15s, %15s) %7s %7.3f%% %7.3f%% %s" % (
                human_converter(0 if b == 0 else self.bounds[b - 1]),  # left
                human_converter(self.bounds[b]),  # right
                humans.human_count(bucket_value),  # count
                (mult * bucket_value),  # percentage
                (mult * cumulative_sum),  # cumulative percentage
                ('#' * marks)
            ))

        if self.events[-1] > 0:
            cumulative_sum += self.events[-1]
            marks = int(round(mult * bucket_value / 5 + 0.5))
            buf.append("[%15s, %15s) %7s %7.3f%% %7.3f%% %s" % (
                human_converter(self.bounds[-1]),  # left
                human_converter(self.vmax),  # right
                humans.human_count(self.events[-1]),  # count
                (mult * bucket_value),  # percentage
                (mult * cumulative_sum),  # cumulative percentage
                ('#' * marks)
            ))
        return '\n'.join(buf)
