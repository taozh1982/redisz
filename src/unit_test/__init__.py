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
        # self.rdz = redisz.Redisz('localhost')
        # self.rdz = redisz.Redisz('10.124.5.222:6388')
        self.rdz = redisz.Redisz(cluster=True, startup_nodes=[{'host': '10.124.5.222', 'port': 6379},
                                                              {'host': '10.124.5.190', 'port': 6379},
                                                              {'host': '10.124.5.191', 'port': 6379}])
        self.rdz.delete(self.rdz.get_names("*"))
        self.rdz.delete(self.rdz.get_names("redisz-lock:*"))
        # self.rdz.delete(['test:numbers2','test:count'])

    def tearDown(self) -> None:
        self.rdz.delete(self.rdz.get_names("*"))
        self.rdz.delete(self.rdz.get_names("redisz-lock:*"))

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
    print(runner.run(get_allcase()))
