class TelemetryCollectorGroup:
    def __init__(self):
        self.collectors = {}

    def register(self, collector_info):
        self.collectors[collector_info.name] = collector_info

    def register_hourly(self, collector_info):
        self.collectors[collector_info.name] = collector_info

    def snapshot(self):
        data = {}
        for collector_info in self.collectors.values():
            data[collector_info.name] = collector_info.snapshot()
        return data

    def human_report(self):
        buf = []
        for collector_info in self.collectors.values():
            buf.append(collector_info.human_report())
        return '\n'.join(buf)


class TelemetryCollectorRegistry(TelemetryCollectorGroup):
    pass


TELEMETRY_COLLECTOR_REGISTRY = TelemetryCollectorRegistry()


class TelemetryCollector:
    def __init__(self, name, label, help_descr, unit, collector):
        self.name = name
        self.label = label
        self.unit = unit
        self.help = help_descr
        self.collector = collector

    def snapshot(self):
        return {
            # 'name': self.name,
            'label': self.label,
            'help': self.help,
            'unit': self.unit['name'],
            'type': self.collector.COLLECTOR_TYPE,
            'data': self.collector.snapshot()
        }

    def human_report(self):
        buf = ['--- %s (%s) ---' % (self.label, self.name)]
        if self.help: buf.append(self.help)
        buf.append(self.collector.human_report(self.unit['converter']))
        buf.append('')
        return '\n'.join(buf)

    @staticmethod
    def register(name, label, unit, collector, help_descr=None):
        collector_info = TelemetryCollector(name, label, help_descr, unit, collector)
        TELEMETRY_COLLECTOR_REGISTRY.register(collector_info)
        return collector
