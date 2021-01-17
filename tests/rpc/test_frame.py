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

from random import randrange, random
from unittest import TestCase

import dnaco.rpc.frame


class TestRpcFrame(TestCase):
    def test_header_simple(self):
        header = dnaco.rpc.frame.build_frame_header(1, False, 3)
        self.assertEqual(header, b'\x10\x00\x00\x03')
        rev, write_to_wal, length = dnaco.rpc.frame.parse_frame_header(header)
        self.assertEqual(rev, 1)
        self.assertEqual(write_to_wal, False)
        self.assertEqual(length, 3)
        # TODO

    def test_header_encode_decode(self):
        for test_rev in range(16):
            test_length = randrange(0, 128 << 20)
            test_write_to_wal = True if random() > 0.5 else False
            header = dnaco.rpc.frame.build_frame_header(test_rev, test_write_to_wal, test_length)
            rev, write_to_wal, length = dnaco.rpc.frame.parse_frame_header(header)
            self.assertEqual(rev, test_rev)
            self.assertEqual(write_to_wal, test_write_to_wal)
            self.assertEqual(length, test_length)

