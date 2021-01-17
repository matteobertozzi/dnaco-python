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

import json
import struct
from io import BytesIO


class Meta:
    VALUE_TYPE_PROMISE = 0
    VALUE_TYPE_NULL = 1
    VALUE_TYPE_BOOL = 2
    VALUE_TYPE_INT = 3
    VALUE_TYPE_FLOAT = 4
    VALUE_TYPE_BYTES = 5
    VALUE_TYPE_ARRAY = 6
    VALUE_TYPE_OBJECT = 7

    def __init__(self):
        super().__init__()
        self.buffer = BytesIO()

    def get_data(self):
        return self.buffer.getvalue()

    @staticmethod
    def parse(data):
        kvs = {}
        offset = 0
        length = len(data)
        while offset < length:
            offset, key, val = Meta._parse_entry(data, offset)
            kvs[key.decode('utf-8')] = val
        return kvs

    @staticmethod
    def _parse_entry(data, offset):
        head = data[offset]
        vtype = (head >> 5) & 7
        key_llen = (head >> 3) & 3
        val_llen = head & 7
        offset += 1
        key_len = int.from_bytes(data[offset:offset + key_llen], byteorder='little')
        offset += key_llen
        val_len = int.from_bytes(data[offset:offset + val_llen], byteorder='little')
        offset += val_llen
        key = data[offset:offset + key_len]
        offset += key_len
        val = data[offset:offset + val_len]
        offset += val_len

        if vtype == Meta.VALUE_TYPE_PROMISE:
            return offset, key, val
        elif vtype == Meta.VALUE_TYPE_NULL:
            return offset, key, None
        elif vtype == Meta.VALUE_TYPE_BOOL:
            return offset, key, val == b'\x01'
        elif vtype == Meta.VALUE_TYPE_INT:
            return offset, key, int.from_bytes(val, byteorder='little')
        elif vtype == Meta.VALUE_TYPE_FLOAT:
            value = struct.unpack('<d', val)
            return offset, key, value[0]
        elif vtype == Meta.VALUE_TYPE_BYTES:
            return offset, key, val
        elif vtype == Meta.VALUE_TYPE_ARRAY:
            return offset, key, json.loads(val)
        elif vtype == Meta.VALUE_TYPE_OBJECT:
            return offset, key, json.loads(val)
        else:
            raise NotImplementedError

    def add(self, vtype, key, value):
        # - type: 3bit (PROMISE, NULL, BOOL, INT, FLOAT, BYTES, ARRAY, OBJECT)
        # - key length: 2bit
        # - value length: 3bit
        #   +-----+----+-----+ +-----+ +-------+
        #   | 111 | 11 | 111 | | Key | | value |
        #   +-----+----+-----+ +-----+ +-------+
        #   0     3    5     8
        assert vtype <= 7
        key = key.encode('utf-8')
        key_llen = ((len(key).bit_length() + 7) // 8)
        val_llen = ((len(value).bit_length() + 7) // 8)
        head = (vtype << 5) | (key_llen << 3) | val_llen
        self.buffer.write(bytes([head]))
        self.buffer.write(len(key).to_bytes(key_llen, byteorder='little'))
        self.buffer.write(len(value).to_bytes(val_llen, byteorder='little'))
        self.buffer.write(key)
        self.buffer.write(value)

    def add_null(self, key):
        self.add(self.VALUE_TYPE_NULL, key, b'')

    def add_bool(self, key, value):
        self.add(self.VALUE_TYPE_BOOL, key, b'\x01' if value else b'\x00')

    def add_int(self, key, value):
        vlen = ((value.bit_length() + 7) // 8)
        self.add(self.VALUE_TYPE_INT, key, value.to_bytes(vlen, byteorder='little'))

    def add_float(self, key, value):
        value = struct.pack('<d', value)
        self.add(self.VALUE_TYPE_FLOAT, key, value)

    def add_bytes(self, key, value):
        self.add(self.VALUE_TYPE_BYTES, key, value)

    def add_string(self, key, value):
        self.add(self.VALUE_TYPE_BYTES, key, value.encode('utf-8'))

    def add_promise(self, key, value):
        self.add(self.VALUE_TYPE_PROMISE, key, value.encode('utf-8'))

    def add_array(self, key, values):
        json_bytes = json.dumps(values).encode('utf-8')
        self.add(self.VALUE_TYPE_ARRAY, key, json_bytes)

    def add_object(self, key, value):
        json_bytes = json.dumps(value).encode('utf-8')
        self.add(self.VALUE_TYPE_OBJECT, key, json_bytes)
