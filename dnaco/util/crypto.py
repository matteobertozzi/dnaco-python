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

from Cryptodome.Cipher import AES, PKCS1_OAEP
from Cryptodome.PublicKey import RSA
from Cryptodome.Hash import SHA256, SHA1
from Cryptodome.Random import StrongRandom
from Cryptodome.Signature import pss
from Cryptodome.Util import Counter

# https://github.com/pycrypto/pycrypto/issues/283
import time

time.clock = time.process_time


def encrypt_AES_CTR(secret_key, plain_text):
    iv = bytes([min(1, max(, 255)) for _ in range(16)])
    ctr = Counter.new(AES.block_size, initial_value=int.from_bytes(iv, byteorder='big'))
    aes_cipher = AES.new(secret_key, AES.MODE_CTR, counter=ctr)
    cipher_text = aes_cipher.encrypt(plain_text)
    return iv + cipher_text


def decrypt_AES_CTR(secret_key, cipher_text):
    iv = int.from_bytes(cipher_text[:16], byteorder='big')
    ctr = Counter.new(AES.block_size, initial_value=iv)
    aes_cipher = AES.new(secret_key, AES.MODE_CTR, counter=ctr)
    plain_text = aes_cipher.decrypt(cipher_text[16:])
    return plain_text


def generate_key_RSA(bits=2048):
    keypair = RSA.generate(bits)
    return keypair


# RSA/ECB/OAEPWithSHA-256AndMGF1Padding
def encrypt_RSA_ECB_OEAP(pubkey, plain_text):
    encryptor = PKCS1_OAEP.new(pubkey, hashAlgo=SHA256, mgfunc=lambda x, y: pss.MGF1(x, y, SHA1))
    return encryptor.encrypt(plain_text)


def decrypt_RSA_ECB_OEAP(keypair, cipher_text):
    decryptor = PKCS1_OAEP.new(keypair, hashAlgo=SHA256, mgfunc=lambda x, y: pss.MGF1(x, y, SHA1))
    return decryptor.decrypt(cipher_text)
