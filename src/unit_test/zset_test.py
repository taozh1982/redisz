from src.unit_test import RedisTestCase


class TestZSet(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'zset'

    def test_add_rem(self):
        rdz = self.rdz
        self.assertEqual(rdz.zset_add('test:zset', {'a': 10, 'b': 20}), 2)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10), ('b', 20.0)])

        self.assertEqual(rdz.zset_add('test:zset', {'b': 30, 'c': 40}), 1)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10), ('b', 30.0), ('c', 40.0)])

        self.assertEqual(rdz.zset_add('test:zset', {'c': 50, 'd': 60}, nx=True), 1)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10), ('b', 30.0), ('c', 40.0), ('d', 60.0)])

        self.assertEqual(rdz.zset_add('test:zset', {'d': 70, 'e': 80}, xx=True), 0)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10), ('b', 30.0), ('c', 40.0), ('d', 70.0)])

        self.assertEqual(rdz.zset_add('test:zset', {'b': 30, 'c': 400, 'd': 700}, ch=True), 2)

        self.assertEqual(rdz.zset_rem('test:zset', 'a'), 1)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('b', 30.0), ('c', 400.0), ('d', 700.0)])

        self.assertEqual(rdz.zset_rem('test:zset', ['b', 'c', 'e']), 2)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('d', 700.0)])

        self.assertEqual(rdz.zset_rem('test:not-exist', 'a'), 0)

    def test_card_count_rank(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30}, type='zset')

        self.assertEqual(rdz.zset_count('test:zset', 20, 30), 2)
        self.assertEqual(rdz.zset_count('test:zset', 21, 30), 1)
        self.assertEqual(rdz.zset_count('test:not-exist', 0, 10), 0)
        self.assertEqual(rdz.zset_score('test:zset', 'a'), 10)

        self.assertIsNone(rdz.zset_score('test:zset', 'x'))
        self.assertIsNone(rdz.zset_score('test:not-exist', 'x'))

        self.assertEqual(rdz.zset_rank('test:zset', 'a'), 0)
        self.assertEqual(rdz.zset_rank('test:zset', 'b'), 1)
        self.assertIsNone(rdz.zset_rank('test:zset', 'x'))
        self.assertIsNone(rdz.zset_rank('test:not-exist', 'x'))

    def test_incr(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30}, type='zset')

        self.assertEqual(rdz.zset_incr('test:zset', 1, 'a'), 11)
        self.assertEqual(rdz.zset_incr('test:zset', 2.2, 'b'), 22.2)
        self.assertEqual(rdz.zset_incr('test:zset', -2, 'c'), 28)
        self.assertEqual(rdz.zset_incr('test:zset', 3, 'e'), 3)

        self.assertListEqual(rdz.zset_getall('test:zset'), [('e', 3), ('a', 11), ('b', 22.2), ('c', 28)])

    def test_decr(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30}, type='zset')

        self.assertEqual(rdz.zset_decr('test:zset', 1, 'a'), 9.0)
        self.assertEqual(rdz.zset_decr('test:zset', 2.2, 'b'), 17.8)
        self.assertEqual(rdz.zset_decr('test:zset', -2, 'c'), 32.0)
        self.assertEqual(rdz.zset_decr('test:zset', 3, 'e'), -3.0)

        self.assertListEqual(rdz.zset_getall('test:zset'), [('e', -3), ('a', 9), ('b', 17.8), ('c', 32)])

    def test_range(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30}, type='zset')

        self.assertListEqual(rdz.zset_range('test:zset', 0, -1), ['a', 'b', 'c'])
        self.assertListEqual(rdz.zset_range('test:zset', 0, 1), ['a', 'b'])

        self.assertListEqual(rdz.zset_range('test:zset', 0, -1, desc=True), ['c', 'b', 'a'])
        self.assertListEqual(rdz.zset_range('test:zset', 0, 1, desc=True), ['c', 'b'])

        self.assertListEqual(rdz.zset_range('test:zset', 0, -1, withscores=True), [('a', 10), ('b', 20.0), ('c', 30)])

        self.assertListEqual(rdz.zset_range('test:zset', 0, 20, withscores=True, byscore=True), [('a', 10), ('b', 20.0)])
        self.assertListEqual(rdz.zset_range('test:zset', 20, 0, desc=True, withscores=True, byscore=True), [('b', 20.0), ('a', 10)])
        self.assertListEqual(rdz.zset_range('test:zset', 0, 20, desc=True, withscores=True, byscore=True), [])

        self.assertListEqual(rdz.zset_range('test:zset', 0, -1, withscores=True, score_cast_func=lambda x: str(x) + '%'), [('a', '10%'), ('b', '20%'), ('c', '30%')])

        self.assertListEqual(rdz.zset_revrange('test:zset', 0, -1), ['c', 'b', 'a'])
        self.assertListEqual(rdz.zset_revrange('test:zset', 0, 1), ['c', 'b'])
        self.assertListEqual(rdz.zset_revrange('test:zset', 0, -1, withscores=True), [('c', 30), ('b', 20.0), ('a', 10)])
        self.assertListEqual(rdz.zset_revrange('test:zset', 0, -1, withscores=True, score_cast_func=lambda x: str(x) + '%'), [('c', '30%'), ('b', '20%'), ('a', '10%')])

    def test_range_by_score(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')

        self.assertListEqual(rdz.zset_rangebyscore('test:zset', 20, 50), ['b', 'c', 'd', 'e'])
        self.assertListEqual(rdz.zset_rangebyscore('test:zset', 20, 50, withscores=True), [('b', 20.0), ('c', 30.0), ('d', 40.0), ('e', 50.0)])
        self.assertListEqual(rdz.zset_rangebyscore('test:zset', 20, 50, 0, 1, withscores=True), [('b', 20.0)])
        self.assertListEqual(rdz.zset_rangebyscore('test:zset', 20, 50, 1, 2, withscores=True), [('c', 30.0), ('d', 40.0)])
        self.assertListEqual(rdz.zset_rangebyscore('test:zset', 20, 50, 1, 10, withscores=True), [('c', 30.0), ('d', 40.0), ('e', 50.0)])

        self.assertListEqual(rdz.zset_revrangebyscore('test:zset', 50, 20), ['e', 'd', 'c', 'b'])
        self.assertListEqual(rdz.zset_revrangebyscore('test:zset', 50, 20, withscores=True), [('e', 50.0), ('d', 40.0), ('c', 30.0), ('b', 20.0)])
        self.assertListEqual(rdz.zset_revrangebyscore('test:zset', 50, 20, 0, 1, withscores=True), [('e', 50.0)])
        self.assertListEqual(rdz.zset_revrangebyscore('test:zset', 50, 20, 1, 2, withscores=True), [('d', 40.0), ('c', 30.0)])
        self.assertListEqual(rdz.zset_revrangebyscore('test:zset', 50, 20, 1, 10, withscores=True), [('d', 40.0), ('c', 30.0), ('b', 20.0)])

    def test_rem_range(self):
        rdz = self.rdz
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')
        self.assertEqual(rdz.zset_remrangebyrank('test:zset', 0, 3), 4)
        self.assertEqual(rdz.zset_remrangebyrank('test:zset', 10, 20), 0)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('e', 50.0), ('f', 60.0)])

        rdz.delete('test:zset')
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')
        self.assertEqual(rdz.zset_remrangebyrank('test:zset', -3, -1), 3)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10.0), ('b', 20.0), ('c', 30.0)])

        rdz.delete('test:zset')
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')
        self.assertEqual(rdz.zset_remrangebyrank('test:zset', -3, -1), 3)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10.0), ('b', 20.0), ('c', 30.0)])

        self.assertEqual(rdz.zset_remrangebyrank('test:not-exist', 0, 2), 0)

        rdz.delete('test:zset')
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')
        self.assertEqual(rdz.zset_remrangebyscore('test:zset', 0, 40), 4)
        self.assertEqual(rdz.zset_remrangebyscore('test:zset', 100, 200), 0)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('e', 50.0), ('f', 60.0)])

        rdz.delete('test:zset')
        rdz.set_value('test:zset', {'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}, type='zset')
        self.assertEqual(rdz.zset_remrangebyscore('test:zset', 30, 50), 3)
        self.assertListEqual(rdz.zset_getall('test:zset'), [('a', 10.0), ('b', 20.0), ('f', 60.0)])

        self.assertEqual(rdz.zset_remrangebyscore('test:not-exist', 0, 100), 0)
