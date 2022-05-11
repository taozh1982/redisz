from src.unit_test import RedisTestCase


class TestSet(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'set'

    def test_add_rem_pop(self):
        rdz = self.rdz
        self.assertEqual(rdz.set_add('test:letters', 'a', 'b', 'c'), 3)
        self.assertSetEqual(rdz.set_members('test:letters'), {'a', 'b', 'c'})
        self.assertEqual(rdz.set_add('test:letters', ['b', 'c', 'd']), 1)
        self.assertSetEqual(rdz.set_members('test:letters'), {'a', 'b', 'c', 'd'})
        self.assertEqual(rdz.set_add('test:letters', ['c', 'd'], 'e', 'f'), 2)
        self.assertSetEqual(rdz.set_members('test:letters'), {'a', 'b', 'c', 'd', 'e', 'f'})

        self.assertEqual(rdz.set_rem('test:letters', 'a', 'b'), 2)
        self.assertSetEqual(rdz.set_members('test:letters'), {'c', 'd', 'e', 'f'})
        self.assertEqual(rdz.set_rem('test:letters', ['a', 'b', 'c', 'x']), 1)
        self.assertSetEqual(rdz.set_members('test:letters'), {'d', 'e', 'f'})
        self.assertEqual(rdz.set_rem('test:not-exist', ['a', 'b', 'c', 'x']), 0)

        self.assertIsInstance(rdz.set_pop('test:letters'), str)
        self.assertEqual(rdz.set_card('test:letters'), 2)
        self.assertListEqual(rdz.set_pop('test:letters', 0), [])
        self.assertEqual(rdz.set_card('test:letters'), 2)

        self.assertIsInstance(rdz.set_pop('test:letters', 2), list)
        self.assertEqual(rdz.set_card('test:letters'), 0)

        self.assertIsNone(rdz.set_pop('test:letters'))
        self.assertListEqual(rdz.set_pop('test:letters', 2), [])
        self.assertListEqual(rdz.set_pop('test:not-exist', 2), [])

    def test_card_member(self):
        rdz = self.rdz
        rdz.set_add('test:letters', ['a', 'b', 'c', 'd', 'e', 'f'])

        self.assertSetEqual(rdz.set_members('test:letters'), {'a', 'b', 'c', 'd', 'e', 'f'})

        self.assertEqual(rdz.set_card('test:letters'), 6)
        self.assertEqual(rdz.set_card('test:not-exist'), 0)

        self.assertTrue(rdz.set_ismember('test:letters', 'a'))
        self.assertFalse(rdz.set_ismember('test:letters', 'x'))
        self.assertFalse(rdz.set_ismember('test:not-exist', 'a'))

    def test_move(self):
        rdz = self.rdz
        rdz.set_add('test:letters1', {'a', 'b', 'c'})
        rdz.set_add('test:letters2', {'c', 'd', 'e'})
        self.assertTrue(rdz.set_move('test:letters1', 'test:letters2', 'a'))
        self.assertSetEqual(rdz.set_members('test:letters1'), {'b', 'c'})
        self.assertSetEqual(rdz.set_members('test:letters2'), {'a', 'c', 'd', 'e'})
        self.assertTrue(rdz.set_move('test:letters1', 'test:letters2', 'c'))
        self.assertSetEqual(rdz.set_members('test:letters1'), {'b'})
        self.assertSetEqual(rdz.set_members('test:letters2'), {'a', 'c', 'd', 'e'})
        self.assertFalse(rdz.set_move('test:letters1', 'test:letters2', 'f'))

        self.assertFalse(rdz.set_move('test:not-exist', 'test:not-exist-1', 'f'))

    def test_diff(self):
        rdz = self.rdz
        rdz.set_add('test:letters1', {'a', 'b', 'c'})
        rdz.set_add('test:letters2', {'b', 'm', 'n'})
        rdz.set_add('test:letters3', {'c', 'x', 'y'})

        self.assertSetEqual(rdz.set_diff('test:letters1', 'test:letters2'), {'c', 'a'})
        self.assertSetEqual(rdz.set_diff(['test:letters2', 'test:letters3']), {'b', 'm', 'n'})
        self.assertSetEqual(rdz.set_diff(['test:letters1', 'test:letters2', 'test:letters3']), {'a'})
        self.assertSetEqual(rdz.set_diff('test:letters1', ['test:letters2', 'test:letters3']), {'a'})

        self.assertSetEqual(rdz.set_diff('test:letters1', 'test:not-exist'), {'a', 'b', 'c'})
        self.assertSetEqual(rdz.set_diff('test:not-exist', 'test:letters1'), set())

        rdz.list_push('test:diff', [1, 2])
        self.assertEqual(rdz.set_diff(['test:letters1', 'test:letters2'], dst='test:diff'), 2)
        self.assertSetEqual(rdz.set_members('test:diff'), {'a', 'c'})

    def test_inter(self):
        rdz = self.rdz
        rdz.set_add('test:letters1', {'a', 'b', 'c'})
        rdz.set_add('test:letters2', {'b', 'c', 'd'})
        rdz.set_add('test:letters3', {'c', 'd', 'e'})

        self.assertSetEqual(rdz.set_inter(['test:letters1', 'test:letters2']), {'b', 'c'})
        self.assertSetEqual(rdz.set_inter(['test:letters2', 'test:letters3']), {'c', 'd'})
        self.assertSetEqual(rdz.set_inter(['test:letters1', 'test:letters2', 'test:letters3']), {'c'})

        self.assertSetEqual(rdz.set_inter('test:letters1', 'test:not-exist'), set())

        rdz.list_push('test:inter', [1, 2])
        self.assertEqual(rdz.set_inter(['test:letters1', 'test:letters2'], dst='test:inter'), 2)
        self.assertSetEqual(rdz.set_members('test:inter'), {'b', 'c'})

    def test_union(self):
        rdz = self.rdz
        rdz.set_add('test:letters1', {'a', 'b', 'c'})
        rdz.set_add('test:letters2', {'b', 'c', 'd'})
        rdz.set_add('test:letters3', {'c', 'd', 'e'})

        self.assertSetEqual(rdz.set_union('test:letters1', 'test:letters2'), {'a', 'b', 'c', 'd'})
        self.assertSetEqual(rdz.set_union(['test:letters2', 'test:letters3']), {'b', 'c', 'd', 'e'})
        self.assertSetEqual(rdz.set_union(['test:letters1', 'test:letters2', 'test:letters3']), {'a', 'b', 'c', 'd', 'e'})

        rdz.list_push('test:union', [1, 2])
        self.assertEqual(rdz.set_union(['test:letters1', 'test:letters2'], dst='test:union'), 4)
        self.assertSetEqual(rdz.set_members('test:union'), {'a', 'b', 'c', 'd'})
