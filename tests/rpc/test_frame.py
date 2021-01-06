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

