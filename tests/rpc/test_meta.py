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

