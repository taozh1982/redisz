from contextlib import contextmanager
from functools import wraps

import redis

connection_pool = None


def init_redis(host, port=6379, decode_responses=True):
    """描述: 初始化redis, 包括host主机、port端口等"""
    global connection_pool
    connection_pool = redis.ConnectionPool(host=host, port=port, decode_responses=decode_responses)


def get_redis():
    """描述: 返回Redis对象, 用于redis操作"""
    return redis.Redis(connection_pool=connection_pool)


def with_redis(func):
    """
    描述:
        函数装饰器, 增加redis对象为函数的第一个参数以操作redis

    参数:
        func -要进行修饰的函数

    返回:
        wrapper -修饰好的函数

    示例:
        @with_redis
        def test(redis, name):
            return redis.get(name)

        print(test('test:name'))
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(get_redis(), *args, **kwargs)

    return wrapper


@contextmanager
def get_redis_pipeline(transaction=True):
    """
    描述:
        Redis pipeline对象上下文管理器, 通过pipeline可以事物/非事物【流水线】的方式对redis进行操作
        通过流水线可以减少客户端和服务器端的交互次数(一次提交)
        如果是事务性流水线，当多个客户端同时处理数据时，可以保证当前调用不会被其他客户端打扰

    参数:
        transaction:bool -是否是事务性流水线，如果只需要流水线，不需要事务，可以设置transaction=False

    返回:
        pipe:Pipeline -流水线对象

    示例:
        with get_redis_pipeline(False) as pipe
            pipe.set('test:name', 'Zhang Tao') #虽然多次操作，但是客户端只会提交一次
            pipe.hset('test:taozh', 'name', 'Zhang Tao')
            pipe.sadd('test:letters', 'a', 'b', 'c')

    """
    pipe = get_redis().pipeline(transaction)
    try:
        if transaction is True:
            pipe.multi()

        yield pipe
        pipe.execute()
    except Exception as e:
        raise e


def get_pubsub():
    """描述: 返回一个发布/订阅对象。 有了这个对象，就可以订阅频道并收听发布到的消息"""
    return get_redis().pubsub()


from ._rtype import *
from ._alltype import *
from ._pubsub import *
from ._ext import *

__version__ = '0.1'
