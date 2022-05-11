import time

from src.unit_test import RedisTestCase


class TestLock(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'lock'

    def test_lock(self):
        rdz = self.rdz
        locked = rdz.acquire_lock('a-lock')
        self.assertNotEqual(locked, False)

        locked1 = rdz.acquire_lock('a-lock', acquire_seconds=1)
        self.assertFalse(locked1)

        rdz.release_lock('a-lock', locked)

        locked2 = rdz.acquire_lock('a-lock', lock_seconds=1)
        self.assertNotEqual(locked2, False)

        time.sleep(2)
        locked3 = rdz.acquire_lock('a-lock', acquire_seconds=1)
        self.assertNotEqual(locked3, False)

        rdz.release_lock('a-lock', locked3)
