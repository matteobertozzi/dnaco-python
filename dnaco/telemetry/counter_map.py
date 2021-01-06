class CounterMap:
    COLLECTOR_TYPE = 'COUNTER_MAP'

    def __init__(self):
        self.data = {}

    def clear(self):
        self.data = {}

    def inc(self, key, amount=1):
        count = self.data.get(key, 0)
        self.data[key] = count + amount

    def snapshot(self):
        return self.data

    def human_report(self, human_converter):
        buf = []
        total = sum(self.data.values())
        for key, value in sorted(self.data.items(), key=lambda x: x[1], reverse=True):
            buf.append(' - %5.2f%% (%7s) - %s' % (
                (100 * (value / total)),
                human_converter(value),
                key
            ))
        return '\n'.join(buf)
