from redis import ResponseError, DataError

from src.unit_test import RedisTestCase


class TestList(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'list'

    def test_push(self):
        rdz = self.rdz
        self.assertEqual(rdz.list_push('test:numbers', 3, 4), 2)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['3', '4'])
        self.assertEqual(rdz.list_push('test:numbers', [2, 1], left=True), 4)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3', '4'])
        self.assertEqual(rdz.list_push('test:numbers', [5, 6], 7), 7)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3', '4', '5', '6', '7'])
        self.assertEqual(rdz.list_push('test:not-exist', 1, xx=True), 0)
        self.assertEqual(rdz.list_getall('test:not-exist'), [])
        self.assertIsNone(rdz.list_getall('test:not-exist', nx_none=True))

    def test_insert(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', [1, 3, 5])

        self.assertEqual(rdz.list_insert('test:numbers', 1, 2), 4)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3', '5'])
        self.assertEqual(rdz.list_insert('test:numbers', 5, 4, before=True), 5)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3', '4', '5'])
        self.assertEqual(rdz.list_insert('test:numbers', 10, 11), -1)
        self.assertEqual(rdz.list_insert('test:not-exist', 1, 2), 0)
        self.assertListEqual(rdz.list_getall('test:not-exist'), [])

    def test_set(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', ['1', 'b', 'c'])
        self.assertTrue(rdz.list_set('test:numbers', 1, 2))
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', 'c'])
        self.assertTrue(rdz.list_set('test:numbers', -1, 3))
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3'])
        with self.assertRaises(ResponseError):
            rdz.list_set('test:not-exist', 1, 2)
        with self.assertRaises(ResponseError):
            rdz.list_set('test:numbers', 10, 2)
        with self.assertRaises(DataError):
            rdz.list_set('test:numbers', 1, None)

    def test_pop(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', ['1', '2', '3', '4', '5', '6'])

        self.assertListEqual(rdz.list_pop('test:numbers', 0), [])

        self.assertEqual(rdz.list_pop('test:numbers'), '6')
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3', '4', '5'])
        self.assertListEqual(rdz.list_pop('test:numbers', 2), ['5', '4'])
        self.assertListEqual(rdz.list_getall('test:numbers'), ['1', '2', '3'])
        self.assertEqual(rdz.list_pop('test:numbers', left=True), '1')
        self.assertListEqual(rdz.list_getall('test:numbers'), ['2', '3'])
        self.assertListEqual(rdz.list_pop('test:numbers', 3), ['3', '2'])
        self.assertListEqual(rdz.list_getall('test:numbers'), [])

        self.assertListEqual(rdz.list_pop('test:not-exist'),[])

    def test_rem(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', ['1', '2', '3', '4', '5', '6', '5', '4', '3', '2', '1'])
        self.assertEqual(rdz.list_rem('test:numbers', 1), 1)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['2', '3', '4', '5', '6', '5', '4', '3', '2', '1'])
        self.assertEqual(rdz.list_rem('test:numbers', 2, -1), 1)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['2', '3', '4', '5', '6', '5', '4', '3', '1'])
        self.assertEqual(rdz.list_rem('test:numbers', 4, 0), 2)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['2', '3', '5', '6', '5', '3', '1'])
        self.assertEqual(rdz.list_rem('test:numbers', 10), 0)
        self.assertListEqual(rdz.list_getall('test:numbers'), ['2', '3', '5', '6', '5', '3', '1'])
        self.assertEqual(rdz.list_rem('test:not-exist', 10), 0)

    def test_index_len(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', ['1', '2', '3', '4', '5', '6'])
        self.assertEqual(rdz.list_index('test:numbers', 1), '2')
        self.assertEqual(rdz.list_index('test:numbers', -1), '6')
        self.assertEqual(rdz.list_index('test:numbers', 10), None)
        self.assertEqual(rdz.list_index('test:not-exist', 0), None)
        self.assertEqual(rdz.list_len('test:numbers'), 6)
        self.assertEqual(rdz.list_len('test:not-exist'), 0)

    def test_range(self):
        rdz = self.rdz
        rdz.list_push('test:numbers', ['1', '2', '3', '4', '5', '6'])
        self.assertListEqual(rdz.list_range('test:numbers', 0, 2), ['1', '2', '3'])
        self.assertListEqual(rdz.list_range('test:numbers', 0, -1), ['1', '2', '3', '4', '5', '6'])
        self.assertListEqual(rdz.list_range('test:numbers', 0, 100), ['1', '2', '3', '4', '5', '6'])
        self.assertListEqual(rdz.list_range('test:not-exist', 0, -1), [])

    def test_iter(self):
        rdz = self.rdz
        numbers = ['1', '2', '3', '4', '5', '6']
        rdz.list_push('test:numbers', numbers)
        index = 0
        for item in rdz.list_iter('test:numbers'):
            with self.subTest():
                self.assertEqual(item, numbers[index])
                index += 1

    def test_bpop(self):
        rdz = self.rdz
        rdz.list_push('test:{numbers}1', ['1', '2'])
        rdz.list_push('test:{numbers}2', ['3', '4'])
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2']), ('test:{numbers}1', '2'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2']), ('test:{numbers}1', '1'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2']), ('test:{numbers}2', '4'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2']), ('test:{numbers}2', '3'))

        rdz.list_push('test:{numbers}1', ['1', '2'])
        rdz.list_push('test:{numbers}2', ['3', '4'])
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2'], left=True), ('test:{numbers}1', '1'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2'], left=True), ('test:{numbers}1', '2'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2'], left=True), ('test:{numbers}2', '3'))
        self.assertTupleEqual(rdz.list_bpop(['test:{numbers}1', 'test:{numbers}2'], left=True), ('test:{numbers}2', '4'))

    def test_rpoplpush(self):
        rdz = self.rdz
        rdz.list_push('test:{numbers}1', ['1', '2'])
        rdz.list_push('test:{numbers}2', ['3', '4'])
        self.assertEqual(rdz.list_rpoplpush('test:{numbers}1', 'test:{numbers}2'), '2')
        self.assertListEqual(rdz.list_getall('test:{numbers}1'), ['1'])
        self.assertListEqual(rdz.list_getall('test:{numbers}2'), ['2', '3', '4'])
        self.assertEqual(rdz.list_rpoplpush('test:{numbers}1', 'test:{numbers}2'), '1')
        self.assertListEqual(rdz.list_getall('test:{numbers}1'), [])
        self.assertListEqual(rdz.list_getall('test:{numbers}2'), ['1', '2', '3', '4'])
