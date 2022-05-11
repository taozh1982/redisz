from src.unit_test import RedisTestCase


class TestExt(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'ext'

    def test_global(self):
        rdz = self.rdz

        self.assertTrue(rdz.set_value('test:str', 'a'))  # str
        self.assertTrue(rdz.set_value('test:str-number', 1.0))  # number
        self.assertEqual(rdz.set_value('test:list', [1, 2, 3]), 3)  # list
        self.assertEqual(rdz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3}), 3)  # hash
        self.assertEqual(rdz.set_value('test:set', {'x', 'y', 'z'}), 3)  # set
        self.assertEqual(rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset'), 3)  # zset

        self.assertEqual(rdz.get_value('test:str'), 'a')  # str
        self.assertEqual(rdz.get_value('test:str-number'), '1.0')  # number
        self.assertListEqual(rdz.get_value('test:list'), ['1', '2', '3'])  # list
        self.assertDictEqual(rdz.get_value('test:hash'), {'a': '1', 'b': '2', 'c': '3'})  # hash
        self.assertSetEqual(rdz.get_value('test:set'), {'x', 'y', 'z'})  # set
        self.assertListEqual(rdz.get_value('test:zset'), [('x', 1.0), ('y', 2.0), ('z', 3.0)])  # zset

        self.assertSetEqual(set(rdz.get_names()), {'test:str', 'test:str-number', 'test:list', 'test:hash', 'test:set', 'test:zset'})
        self.assertSetEqual(set(rdz.get_names('*set')), {'test:set', 'test:zset'})
        self.assertSetEqual(set(rdz.get_names('test:*set')), {'test:set', 'test:zset'})
        self.assertSetEqual(set(rdz.get_names('test:str*')), {'test:str', 'test:str-number'})

    def test_list(self):
        rdz = self.rdz
        rdz.set_value('test:list', [1, 2, 3])
        self.assertListEqual(rdz.list_getall('test:list'), ['1', '2', '3'])

        self.assertListEqual(rdz.list_getall('test:not-exist'), [])
        self.assertIsNone(rdz.list_getall('test:not-exist', nx_none=True))

        self.assertTrue(rdz.list_exists('test:list', 1))
        self.assertFalse(rdz.list_exists('test:list', 10))
        self.assertFalse(rdz.list_exists('test:not-exist', 1))

    def test_set(self):
        rdz = self.rdz
        rdz.set_value('test:set', {'x', 'y', 'z'})

        self.assertEqual(rdz.set_len('test:set'), 3)
        self.assertEqual(rdz.set_len('test:not-exist'), 0)

        self.assertTrue(rdz.set_exists('test:set', 'x'))
        self.assertFalse(rdz.set_exists('test:set', 'm'))
        self.assertFalse(rdz.set_exists('test:not-exist', 'x'))

        self.assertSetEqual(rdz.set_getall('test:set'), {'x', 'y', 'z'})
        self.assertSetEqual(rdz.set_getall('test:not-exist'), set())

    def test_zset(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')
        self.assertEqual(rdz.zset_index('test:zset', 'x'), 0)
        self.assertEqual(rdz.zset_index('test:zset', 'z'), 2)
        self.assertIsNone(rdz.zset_index('test:zset', 'm'))
        self.assertIsNone(rdz.zset_index('test:not-exist', 'm'))

        self.assertEqual(rdz.zset_len('test:zset'), 3)
        self.assertEqual(rdz.zset_len('test:not-exist'), 0)

        self.assertListEqual(rdz.zset_getall('test:zset'), [('x', 1.0), ('y', 2.0), ('z', 3.0)])
        self.assertListEqual(rdz.zset_getall('test:zset', withscores=False), ['x', 'y', 'z'])

        self.assertTrue(rdz.zset_exists('test:zset', 'x'))
        self.assertTrue(rdz.zset_exists('test:zset', 'y'))
        self.assertFalse(rdz.zset_exists('test:zset', 'm'))
        self.assertFalse(rdz.zset_exists('test:not-exist', 'x'))
