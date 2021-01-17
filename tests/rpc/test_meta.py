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

from unittest import TestCase

from dnaco.rpc.meta import Meta


class TestRpcMeta(TestCase):
    def test_simple(self):
        meta = Meta()
        meta.add_null('gnul')
        meta.add_bool('bult', True)
        meta.add_bool('bulf', False)
        meta.add_int('inte', 123)
        meta.add_float('flot', 15.34)
        meta.add_bytes('bitez', b'\xABCD')
        meta.add_string('strinz', 'stringha')
        meta.add_array('arrai', [1, 2, 3])
        meta.add_object('obje', {'a': 10, 'b': 'bbb'})

        data = meta.get_data()
        self.assertEqual(len(data), 118)

        meta_map = Meta.parse(data)
        print(meta_map)
        self.assertEqual(meta_map['gnul'], None)
        self.assertEqual(meta_map['bult'], True)
        self.assertEqual(meta_map['bulf'], False)
        self.assertEqual(meta_map['inte'], 123)
        self.assertEqual(meta_map['flot'], 15.34)
        self.assertEqual(meta_map['bitez'], b'\xABCD')
        self.assertEqual(meta_map['strinz'].decode('utf-8'), 'stringha')
        self.assertEqual(meta_map['arrai'], [1, 2, 3])
        self.assertEqual(meta_map['obje'], {'a': 10, 'b': 'bbb'})

