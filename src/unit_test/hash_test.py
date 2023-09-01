from redis import ResponseError

from src.unit_test import RedisTestCase


class TestHash(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'hash'

    def test_set_get(self):
        rdz = self.rdz
        self.assertTrue(rdz.hash_set('test:taozh', 'name', 'Zhang Tao'))
        self.assertDictEqual(rdz.hash_getall('test:taozh'), {'name': 'Zhang Tao'})
        self.assertTrue(rdz.hash_set('test:taozh', mapping={'age': 18, 'email': 'taozh@cisco.com'}))
        self.assertDictEqual(rdz.hash_getall('test:taozh'), {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'})
        self.assertFalse(rdz.hash_set('test:taozh', 'email', 'zht@cisco.com', nx=True))
        self.assertDictEqual(rdz.hash_getall('test:taozh'), {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'})
        self.assertTrue(rdz.hash_set('test:taozh', 'company', 'cisco', nx=True))
        self.assertDictEqual(rdz.hash_getall('test:taozh'), {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com', 'company': 'cisco'})
        self.assertEqual(rdz.hash_get('test:taozh', 'name'), 'Zhang Tao')

        self.assertEqual(rdz.hash_get('test:taozh', 'nx'), None)
        self.assertEqual(rdz.hash_get('test:not-exist', 'name'), None)

        self.assertTrue(rdz.hash_mset('test:taozh', {'department': 'cx', 'email': 'zht@cisco.com'}))
        self.assertDictEqual(rdz.hash_getall('test:taozh'), {'name': 'Zhang Tao', 'age': '18', 'email': 'zht@cisco.com', 'company': 'cisco', 'department': 'cx'})

        self.assertListEqual(rdz.hash_mget('test:taozh', 'name'), ['Zhang Tao'])
        self.assertListEqual(rdz.hash_mget('test:taozh', 'name', 'age'), ['Zhang Tao', '18'])
        self.assertListEqual(rdz.hash_mget('test:taozh', ['name', 'age']), ['Zhang Tao', '18'])
        self.assertListEqual(rdz.hash_mget('test:taozh', ['name', 'age', 'not-exist']), ['Zhang Tao', '18', None])

    def test_del(self):
        rdz = self.rdz
        rdz.hash_set('test:kv', mapping={'k1': '1', 'k2': '2', 'k3': '3', 'k4': '4', 'k5': '5', 'k6': '6', 'k7': '7'})
        self.assertEqual(rdz.hash_del('test:kv', 'k1', 'k2'), 2)
        self.assertDictEqual(rdz.hash_getall('test:kv'), {'k3': '3', 'k4': '4', 'k5': '5', 'k6': '6', 'k7': '7'})
        self.assertEqual(rdz.hash_del('test:kv', ['k3', 'k4']), 2)
        self.assertDictEqual(rdz.hash_getall('test:kv'), {'k5': '5', 'k6': '6', 'k7': '7'})
        self.assertEqual(rdz.hash_del('test:kv', ['k5', 'k-nx']), 1)
        self.assertDictEqual(rdz.hash_getall('test:kv'), {'k6': '6', 'k7': '7'})

        self.assertEqual(rdz.hash_del('test:not-exist', 'k1'), 0)
        self.assertEqual(rdz.hash_del('test:kv', 'k-nx'), 0)

    def test_exist_len_keys_values(self):
        rdz = self.rdz
        rdz.hash_set('test:taozh', mapping={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'})

        self.assertTrue(rdz.hash_exists('test:taozh', 'name'))
        self.assertFalse(rdz.hash_exists('test:taozh', 'city'))
        self.assertFalse(rdz.hash_exists('test:not-exist', 'name'))

        self.assertEqual(rdz.hash_len('test:taozh'), 3)
        self.assertEqual(rdz.hash_len('test:not-exist'), 0)

        self.assertListEqual(rdz.hash_keys('test:taozh'), ['name', 'age', 'email'])
        self.assertListEqual(rdz.hash_keys('test:not-exist'), [])

        self.assertListEqual(rdz.hash_values('test:taozh'), ['Zhang Tao', '18', 'taozh@cisco.com'])
        self.assertListEqual(rdz.hash_keys('test:not-exist'), [])

    def test_incr(self):
        rdz = self.rdz
        rdz.hash_mset('test:taozh', {'name': 'Zhang Tao', 'email': 'taozh@cisco.com', 'score': 99.9, 'count': 18})
        self.assertEqual(rdz.hash_incr('test:taozh', 'count'), 19)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '19')
        self.assertEqual(rdz.hash_incr('test:taozh', 'count', 2), 21)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '21')
        self.assertEqual(rdz.hash_incr('test:taozh', 'count', -1), 20)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '20')
        self.assertEqual(rdz.hash_incr('test:taozh', 'not-exist'), 1)
        self.assertEqual(rdz.hash_get('test:taozh', 'not-exist'), '1')

        with self.assertRaises(ResponseError):
            rdz.hash_incr('test:taozh', 'email')
        with self.assertRaises(ResponseError):
            rdz.hash_incr('test:taozh', 'score')

    def test_decr(self):
        rdz = self.rdz
        rdz.hash_mset('test:taozh', {'name': 'Zhang Tao', 'email': 'taozh@cisco.com', 'score': 99.9, 'count': 10})
        self.assertEqual(rdz.hash_decr('test:taozh', 'count'), 9)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '9')
        self.assertEqual(rdz.hash_decr('test:taozh', 'count', 2), 7)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '7')
        self.assertEqual(rdz.hash_decr('test:taozh', 'count', -1), 8)
        self.assertEqual(rdz.hash_get('test:taozh', 'count'), '8')
        self.assertEqual(rdz.hash_decr('test:taozh', 'not-exist'), -1)
        self.assertEqual(rdz.hash_get('test:taozh', 'not-exist'), '-1')

        with self.assertRaises(ResponseError):  # Cluster模式下，不能try...except异常，redis.exceptions.ResponseError: hash value is not an integer
            rdz.hash_decr('test:taozh', 'email')

        with self.assertRaises(ResponseError):  # Cluster模式下，不能try...except异常，redis.exceptions.ResponseError: hash value is not an integer
            rdz.hash_decr('test:taozh', 'score')

    def test_incrfloat(self):
        rdz = self.rdz
        rdz.hash_mset('test:taozh', {'name': 'Zhang Tao', 'email': 'taozh@cisco.com', 'score': 88.8})
        self.assertEqual(rdz.hash_incrfloat('test:taozh', 'score'), 89.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '89.8')
        self.assertEqual(rdz.hash_incrfloat('test:taozh', 'score', 2), 91.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '91.8')
        self.assertEqual(rdz.hash_incrfloat('test:taozh', 'score', -1), 90.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '90.8')
        self.assertEqual(rdz.hash_incrfloat('test:taozh', 'not-exist'), 1)
        self.assertEqual(rdz.hash_get('test:taozh', 'not-exist'), '1')

        with self.assertRaises(ResponseError):
            rdz.hash_incrfloat('test:taozh', 'email')

    def test_decrfloat(self):
        rdz = self.rdz
        rdz.hash_mset('test:taozh', {'name': 'Zhang Tao', 'email': 'taozh@cisco.com', 'score': 88.8})
        self.assertEqual(rdz.hash_decrfloat('test:taozh', 'score'), 87.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '87.8')
        self.assertEqual(rdz.hash_decrfloat('test:taozh', 'score', 2), 85.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '85.8')
        self.assertEqual(rdz.hash_decrfloat('test:taozh', 'score', -1), 86.8)
        self.assertEqual(rdz.hash_get('test:taozh', 'score'), '86.8')
        self.assertEqual(rdz.hash_decrfloat('test:taozh', 'not-exist'), -1)
        self.assertEqual(rdz.hash_get('test:taozh', 'not-exist'), '-1')

        with self.assertRaises(ResponseError):
            rdz.hash_decrfloat('test:taozh', 'email')
