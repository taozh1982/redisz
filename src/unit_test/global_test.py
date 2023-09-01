import time

from redis import ResponseError

from src.unit_test import RedisTestCase


class TestStr(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'global'

    def test_type(self):
        rdz = self.rdz
        rdz.set_value('test:string', 'a')  # str
        rdz.set_value('test:list', [1, 2, 3])  # list
        rdz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})  # hash
        rdz.set_value('test:set', {'x', 'y', 'z'})  # set
        rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')  # zset

        self.assertEqual(rdz.get_type('test:string'), 'string')
        self.assertEqual(rdz.get_type('test:list'), 'list')
        self.assertEqual(rdz.get_type('test:hash'), 'hash')
        self.assertEqual(rdz.get_type('test:set'), 'set')
        self.assertEqual(rdz.get_type('test:zset'), 'zset')
        self.assertEqual(rdz.get_type('test:not-exist'), 'none')

    def test_exists(self):
        rdz = self.rdz
        rdz.set_value('test:string', 'a')  # str
        rdz.set_value('test:list', [1, 2, 3])  # list
        rdz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})  # hash
        rdz.set_value('test:set', {'x', 'y', 'z'})  # set
        rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')  # zset

        self.assertTrue(rdz.exists('test:string'))
        self.assertTrue(rdz.exists('test:string', 'test:list'))
        self.assertTrue(rdz.exists(['test:string', 'test:list']))
        self.assertFalse(rdz.exists('test:string', 'test:not-exist'))

        self.assertEqual(rdz.exists('test:string', 'test:list', return_number=True), 2)
        self.assertEqual(rdz.exists('test:string', 'test:not-exist', return_number=True), 1)

    def test_keys(self):
        rdz = self.rdz
        for i in range(3):
            rdz.set_value('test:x' + str(i), i)
            rdz.set_value('test:y' + str(i), i)

        self.assertSetEqual(set(rdz.keys()), {'test:x0', 'test:x1', 'test:x2', 'test:y0', 'test:y1', 'test:y2'})
        self.assertSetEqual(set(rdz.keys('test:x*')), {'test:x0', 'test:x1', 'test:x2'})
        self.assertSetEqual(set(rdz.keys('*2')), {'test:x2', 'test:y2'})

    def test_delete(self):
        rdz = self.rdz
        for i in range(6):
            rdz.set_value('test:i' + str(i), i)

        self.assertEqual(rdz.delete('test:i0'), 1)
        self.assertEqual(rdz.delete('test:i1', 'test:i2'), 2)
        self.assertEqual(rdz.delete(['test:i3', 'test:i4']), 2)
        self.assertEqual(rdz.delete(['test:i5', 'test:i6']), 1)

    def test_rname(self):
        rdz = self.rdz
        for i in range(6):
            rdz.set_value('test:{i}' + str(i), i)

        self.assertTrue(rdz.rename('test:{i}0', 'test:{i}1'))
        self.assertEqual(rdz.str_get('test:{i}1'), "0")
        self.assertFalse(rdz.rename('test:{i}2', 'test:{i}3', nx=True))
        self.assertEqual(rdz.str_get('test:{i}3'), "3")

    def test_expire(self):
        rdz = self.rdz
        for i in range(6):
            rdz.set_value('test:i' + str(i), i)

        self.assertTrue(rdz.expire('test:i1', 1))
        self.assertFalse(rdz.expire('test:not-exist', 1))

        self.assertEqual(rdz.ttl('test:i1'), 1)
        self.assertEqual(rdz.ttl('test:i2'), -1)
        self.assertEqual(rdz.ttl('test:not-exist'), -2)

        self.assertTrue(rdz.expire('test:i2', 1))
        self.assertTrue(rdz.persist('test:i2'))
        self.assertFalse(rdz.persist('test:not-exist'))
        self.assertEqual(rdz.ttl('test:i2'), -1)

        current_time = int(time.time())
        self.assertTrue(rdz.expireat('test:i3', current_time + 2))  # 确保运行test的客户端跟redis所在server时间一致

        self.assertFalse(rdz.expireat('test:not-exist', current_time + 2))
        self.assertLessEqual(rdz.ttl('test:i3'), 2)
        self.assertGreaterEqual(rdz.ttl('test:i3'), 1)

        self.assertTrue(rdz.exists('test:i3'))
        self.assertTrue(rdz.expireat('test:i3', current_time - 10))
        self.assertFalse(rdz.exists('test:i3'))

        time.sleep(2)
        self.assertFalse(rdz.exists('test:ex1'))

    def test_sort(self):
        rdz = self.rdz
        # if rdz.get_ha_mode() == 'cluster':
        #     print('\n', __name__ + ":test_sort doesn't work with clusters")
        #     return
        rdz.set_value('test:{abc}:sort', [6, 88, 112, 18, 36])
        rdz.set_value('test:{abc}:sort-weight', {'d-6': 1, 'd-88': 2, 'd-112': 3, 'd-18': 4, 'd-36': 5, })
        self.assertListEqual(rdz.sort('test:{abc}:sort'), ['6', '18', '36', '88', '112'])
        self.assertListEqual(rdz.sort('test:{abc}:sort', desc=True), ['112', '88', '36', '18', '6'])
        self.assertListEqual(rdz.sort('test:{abc}:sort', alpha=True), ['112', '18', '36', '6', '88'])
        self.assertListEqual(rdz.sort('test:{abc}:sort', start=1, num=3), ['18', '36', '88'])

        rdz.set_value('test:{abc}:sort-alpha', ['a', 'c', 'b'])
        self.assertListEqual(rdz.sort('test:{abc}:sort-alpha', alpha=True), ['a', 'b', 'c'])
        with self.assertRaises(ResponseError):
            rdz.sort('test:{abc}:sort-alpha')
        #
        self.assertEqual(rdz.sort('test:{abc}:sort', store='test:{abc}:sort-store'), 5)
        self.assertListEqual(rdz.get_value('test:{abc}:sort-store'), ['6', '18', '36', '88', '112'])

        rdz.set_value('test:{abc}:obj-ids', [1, 3, 2])
        rdz.set_value('test:{abc}:obj-1', {'name': '1a', 'weight': 33})
        rdz.set_value('test:{abc}:obj-2', {'name': '2b', 'weight': 22})
        rdz.set_value('test:{abc}:obj-3', {'name': '3c', 'weight': 11})

        if rdz.get_ha_mode() == 'cluster':
            print('\n', __name__ + ":test_sort - BY option of SORT denied in Cluster mode")
            return
        self.assertListEqual(rdz.sort('test:{abc}:obj-ids', by='test:{abc}:obj-*->weight'), ['3', '2', '1'])
        self.assertListEqual(rdz.sort('test:{abc}:obj-ids', by='test:{abc}:obj-*->weight', get='test:{abc}:obj-*->name'), ['3c', '2b', '1a'])
        self.assertListEqual(rdz.sort('test:{abc}:obj-ids', get='test:{abc}:obj-*->name'), ['1a', '2b', '3c'])
        self.assertListEqual(rdz.sort('test:{abc}:obj-ids', by='nosort', get='test:{abc}:obj-*->name'), ['1a', '3c', '2b'])
