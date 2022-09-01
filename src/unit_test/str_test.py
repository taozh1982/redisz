import redis
from redis import ResponseError

from src.unit_test import RedisTestCase


class TestStr(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'str'

    def test_setget(self):
        rdz = self.rdz
        self.assertTrue(rdz.str_set('test:name', 'Zhang Tao'))
        self.assertTrue(rdz.str_set('test:age', 18))
        self.assertTrue(rdz.str_set('test:email', 'taozh@cisco.com'))
        self.assertFalse(rdz.str_set('test:name', 'Zhang Tao', nx=True))
        self.assertTrue(rdz.str_set('test:name', 'Zhang Tao', xx=True))

        self.assertEqual(rdz.str_get('test:name'), 'Zhang Tao')
        self.assertIsNone(rdz.str_get('test:not-exist'))

        self.assertEqual(rdz.str_getset('test:age', 19), '18')
        self.assertIsNone(rdz.str_getset('test:not-exist', 19), None)
        self.assertEqual(rdz.str_get('test:not-exist'), '19')

    def test_msetget(self):
        rdz = self.rdz
        self.assertTrue(rdz.str_mset({'test:{aa}name': 'Zhang Tao', 'test:{aa}age': '18', 'test:{aa}email': 'taozh@cisco.com'}))
        self.assertListEqual(rdz.str_mget('test:{aa}name'), ['Zhang Tao'])
        self.assertListEqual(rdz.str_mget('test:{aa}name', 'test:{aa}age'), ['Zhang Tao', '18'])

    def test_append(self):
        rdz = self.rdz
        rdz.str_set('test:email', 'taozh@cisco.com')
        self.assertEqual(rdz.str_append('test:email', None), 15)
        self.assertEqual(rdz.str_append('test:email', '.cn'), 18)
        self.assertEqual(rdz.str_append('test:not-exist', '.cn'), 3)

    def test_range(self):
        rdz = self.rdz
        rdz.str_set('test:email', 'taozh@cisco.com')
        rdz.str_set('test:study', '好好学习')
        self.assertEqual(rdz.str_getrange('test:email', 0, 4), 'taozh')
        self.assertEqual(rdz.str_getrange('test:email', -3, -1), 'com')
        self.assertEqual(rdz.str_getrange('test:study', 0, 2), '好')
        self.assertEqual(rdz.str_setrange('test:email', 6, '1982@gmail.com'), 20)
        self.assertEqual(rdz.str_setrange('test:study', 6, '工作'), 12)

    def test_len(self):
        rdz = self.rdz
        rdz.str_set('test:email', 'taozh@cisco.com')
        rdz.str_set('test:study', '好好学习')
        self.assertEqual(rdz.str_len('test:email'), 15)
        self.assertEqual(rdz.str_len('test:study'), 12)
        self.assertEqual(rdz.str_len('test:not-exist'), 0)

    def test_incr(self):
        rdz = self.rdz
        rdz.str_set('test:age', 18)
        rdz.str_set('test:email', 'taozh@cisco.com')
        rdz.str_set('test:float', 1.1)
        self.assertEqual(rdz.str_incr('test:age'), 19)
        self.assertEqual(rdz.str_incr('test:age', 2), 21)
        self.assertEqual(rdz.str_incr('test:age', -1), 20)
        self.assertEqual(rdz.str_incr('test:not-exist'), 1)

        with self.assertRaises(ResponseError):
            rdz.str_incr('test:email')
        with self.assertRaises(ResponseError):
            rdz.str_incr('test:float')


    def test_decr(self):
        rdz = self.rdz
        rdz.str_set('test:count', 10)
        rdz.str_set('test:email', 'taozh@cisco.com')
        rdz.str_set('test:float', 1.1)

        self.assertEqual(rdz.str_decr('test:count'), 9)
        self.assertEqual(rdz.str_decr('test:count', 2), 7)
        self.assertEqual(rdz.str_decr('test:count', -1), 8)
        self.assertEqual(rdz.str_decr('test:not-exist'), -1)

        with self.assertRaises(ResponseError):
            rdz.str_decr('test:email')
        with self.assertRaises(ResponseError):
            rdz.str_decr('test:float')

    def test_incrfloat(self):
        rdz = self.rdz
        rdz.str_set('test:age', 18)
        rdz.str_set('test:email', 'taozh@cisco.com')

        self.assertEqual(rdz.str_incrfloat('test:age'), 19)
        self.assertEqual(rdz.str_incrfloat('test:age', 2), 21)
        self.assertEqual(rdz.str_incrfloat('test:age', -1), 20)
        self.assertEqual(rdz.str_incrfloat('test:not-exist'), 1)

        with self.assertRaises(ResponseError):
            rdz.str_incrfloat('test:email')

    def test_decrfloat(self):
        rdz = self.rdz
        rdz.str_set('test:count', 10)
        rdz.str_set('test:email', 'taozh@cisco.com')

        self.assertEqual(rdz.str_decrfloat('test:count'), 9)
        self.assertEqual(rdz.str_decrfloat('test:count', 2), 7)
        self.assertEqual(rdz.str_decrfloat('test:count', -1), 8)
        self.assertEqual(rdz.str_decrfloat('test:not-exist'), -1)

        with self.assertRaises(ResponseError):
            rdz.str_decrfloat('test:email')
