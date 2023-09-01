from redis.exceptions import LockError

from src.unit_test import RedisTestCase


class TestLock(RedisTestCase):
    @classmethod
    def get_test_name(cls) -> str:
        return 'lock'

    # def test_lock(self):
    #     rdz = self.rdz
    #     locked = rdz.acquire_lock('a-lock')
    #     self.assertNotEqual(locked, False)
    #
    #     locked1 = rdz.acquire_lock('a-lock', acquire_seconds=1)
    #     self.assertFalse(locked1)
    #
    #     rdz.release_lock('a-lock', locked)
    #
    #     locked2 = rdz.acquire_lock('a-lock', lock_seconds=1)
    #     self.assertNotEqual(locked2, False)
    #
    #     time.sleep(2)
    #     locked3 = rdz.acquire_lock('a-lock', acquire_seconds=1)
    #     self.assertNotEqual(locked3, False)
    #
    #     rdz.release_lock('a-lock', locked3)

    def test_new_lock(self):
        rdz = self.rdz

        lock = rdz.lock('a-lock', blocking=True)
        lock1 = rdz.lock('a-lock', timeout=1)
        lock2 = rdz.lock('a-lock2', timeout=1)
        self.assertTrue(lock.acquire())
        self.assertFalse(lock1.acquire())
        self.assertTrue(lock2.acquire())
        lock.release()
        with self.assertRaises(LockError):
            lock.release()

        self.assertTrue(lock1.acquire())

        try:
            with lock:
                self.assertTrue(lock.owned())
                pass
        except LockError as err:
            print(err)
        self.assertTrue(lock.acquire())
        lock.release()
