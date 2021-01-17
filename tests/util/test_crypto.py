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

from random import randbytes
from unittest import TestCase

from dnaco.util import crypto

import string


class TestCrypto(TestCase):
    def test_aes_ctr(self):
        key = randbytes(32)
        for i in range(len(string.printable)):
            plain_text = string.printable[:1 + i].encode('utf-8')
            cipher_text = crypto.encrypt_AES_CTR(key, plain_text)
            self.assertEqual(16 + i + 1, len(cipher_text))
            dec_plain_text = crypto.decrypt_AES_CTR(key, cipher_text)
            self.assertEqual(plain_text, dec_plain_text)

    def test_rsa(self):
        keypair = crypto.generate_key_RSA(2048)
        for _ in range(128):
            plain_text = randbytes(32)
            cipher_text = crypto.encrypt_RSA_ECB_OEAP(keypair.publickey(), plain_text)
            self.assertEqual(256, len(cipher_text))
            dec_plain_text = crypto.decrypt_RSA_ECB_OEAP(keypair, cipher_text)
            self.assertEqual(plain_text, dec_plain_text)

