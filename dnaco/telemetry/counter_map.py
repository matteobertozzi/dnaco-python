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
