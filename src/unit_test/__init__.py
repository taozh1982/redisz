# 测试用例存放路径
import unittest

from src import redisz

case_path = '.'


class RedisTestCase(unittest.TestCase):

    @classmethod
    def get_test_name(cls) -> str:
        pass

    @classmethod
    def setUpClass(cls):
        name = cls.get_test_name() + ' test'
        print(name.center(60, '-'))

    def setUp(self):
        self.rdz = redisz.Redisz('localhost')
        self.rdz.delete(self.rdz.get_names("test:*"))

    def tearDown(self) -> None:
        self.rdz.delete(self.rdz.get_names("test:*"))

    @classmethod
    def tearDownClass(cls) -> None:
        print('\n')


# 获取所有测试用例
def get_allcase():
    discover = unittest.defaultTestLoader.discover(case_path, pattern="*_test.py")
    suite = unittest.TestSuite()
    suite.addTest(discover)
    return suite


if __name__ == '__main__':
    # 运行测试用例
    runner = unittest.TextTestRunner()
    runner.run(get_allcase())
