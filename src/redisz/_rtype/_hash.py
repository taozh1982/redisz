from redis.commands import list_or_args

from .. import get_redis

"""
散列可以将多个键值对存储到一个键里面, 相当于一个只存储字符串的redis, 所以很多操作跟redis全局操作类似
"""
__all__ = [
    'hash_set', 'hash_mset',  # 添加&修改
    'hash_del',  # 删除
    'hash_get', 'hash_mget', 'hash_getall', 'hash_exists', 'hash_len', 'hash_keys', 'hash_values',  # 查询&遍历
    'hash_incr', 'hash_decr', 'hash_incrfloat', 'hash_decrfloat',  # 自增&自减
    'hash_scan', 'hash_scan_iter'  # 迭代
]


def hash_set(name, key=None, value=None, mapping=None, nx=False):
    """
    描述:
        设置指定散列中的键值对, 默认情况, 如果指定散列不存在, 则创建并赋值, 否则修改已有散列
        可以通过mapping一次设置多个键值对(初始化)
        如果nx==True, 则只有当key(不是name)不存在时, set操作才执行, 而且nx操作只支持单个值的设置(key-value), 不支持mapping的设置方式

    参数:
        name:string -redis的键名
        key:string -要设置的key
        value:string|int|float -要设置的value
        mapping:dict -多个键值对组成的dict
        nx:bool - nx=True, 只有key在mapping中不存在时才设置

    返回:
        count:int -设置成功的键值数量

    示例:
        hash_set('test:taozh', 'name', 'Zhang Tao') # 创建散列 -> {'name': 'Zhang Tao'}
        hash_set('test:taozh', mapping={'age': 18, 'email': 'taozh@cisco.com'}) # 一次设置多个键值 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_set('test:taozh', 'email', 'zht@cisco.com', nx=True) # email已经存在, set操作无效 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_set('test:taozh', 'company', 'cisco', nx=True) # company不存在, set操作有效 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com', 'company': 'cisco'}
    """
    r = get_redis()
    if nx is True:
        # if key is not None:
        return r.hsetnx(name, key, value)

    return r.hset(name, key=key, value=value, mapping=mapping)


def hash_mset(name, mapping):
    """
    描述:
        在指定散列中批量设置键值对, 等价于hash_set(name,mapping={...}), 如果散列不存在则创建并赋值, 存在则修改

    参数:
        name:string -redis的键名
        mapping:dict -批量设置的键值对

    返回:
        result:bool -True

    示例:
        # test:taozh = {'name': 'Zhang Tao'}
        hash_mset('test:taozh', {'age': 18, 'email': 'taozh@cisco.com'}) # test:taozh -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_mset('test:zht', {'name': 'Zhang Tao', 'age': '18'}) # 不存在则创建 test:zht={'name': 'Zhang Tao', 'age': '18'}
    """
    return get_redis().hmset(name, mapping)


def hash_del(name, keys, *args):
    """
    描述:
        将指定散列中一个或多个指定key的键值对删除, 并返回删除成功的个数
        如果散列不存在, 返回0

    参数:
        name:string -redis的键名
        keys:list -要设置的多个key
        args -也可以通过位置参数传递要删除一个/多个的key

    返回:
        count:int -删除【成功】的个数, 

    示例:
        # test:kv={'k1': '1', 'k2': '2', 'k3': '3', 'k4': '4', 'k5': '5', 'k6': '6', 'k7': '7'}
        hash_del('test:kv', 'k1', 'k2') # 返回2, -> {'k3': '3', 'k4': '4', 'k5': '5', 'k6': '6', 'k7': '7'}
        hash_del('test:kv', ['k3', 'k4'])   # 返回2, -> {'k5': '5', 'k6': '6', 'k7': '7'}
        hash_del('test:kv1', ['k5','k6'], 'k-nx') # 返回2, 因为k-nx不存在, 只删除了k5+k6, -> {'k7': '7'}
    """
    keys = list_or_args(keys, args)
    return get_redis().hdel(name, *keys)


def hash_get(name, key):
    """
    描述:
        获取指定散列中指定key的键值, 如果散列不存在/key不存在, 返回None

    参数:
        name:string -redis的键名
        key:string -要获取的key

    返回:
        value:string -key对应的键值, 如果散列不存在/key不存在, 返回None

    示例:
        # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_get('test:taozh', 'email') # taozh@cisco.com
        hash_get('test:taozh', 'city') # None
    """
    return get_redis().hget(name, key)


def hash_mget(name, keys, *args):
    """
    描述:
        在指定散列中获取多个key的键值
        可以在keys中指定要获取的key列表, 也可以通过位置参数指定, 两者也可以混用
        返回结果为包含值的列表, 如果散列不存在/key不存在, 列表中的值为None

    参数:
        name:string -redis的键名
        keys:list -key列表
        args -通过位置参数传递的一个/多个key

    返回:
        values:list -返回的value列表, 如果散列不存在/key不存在, 列表中的值为None


    示例:
        #test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com', 'city': 'shanghai'}
        hash_mget('test:taozh', 'name') # ['Zhang Tao']
        hash_mget('test:taozh', 'name', 'age') # ['Zhang Tao', '18']
        hash_mget('test:taozh', ['name', 'age']) # ['Zhang Tao', '18']
        hash_mget('test:taozh', ['name', 'age'], 'email') # ['Zhang Tao', '18', 'taozh@cisco.com']
        hash_mget('test:taozh', 'key-nx') # [None]
    """
    return get_redis().hmget(name, keys, *args)


def hash_getall(name):
    """
    描述:
        获取指定散列的所有键值, 如果散列不存在则返回{}

    参数:
        name:string -redis的键名

    返回:
        map:dict -所有的键值dict


    示例:
        # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_getall('test:taozh') # {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_getall('test:hash-nx') # {}
    """
    return get_redis().hgetall(name)


def hash_exists(name, key):
    """
    描述:
        检查指定散列中是否存在指定的key
        如果散列中存在key对应的键值返回True, 如果不存在返回False, 如果散列不存在返回False

    参数:
        name:string -redis的键名
        key:string -指定的key

    返回:
        is_exist:bool -如果散列中存在key对应的键值返回True, 如果不存在返回False, 如果散列不存在返回False

    示例:
        # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_exists('test:taozh', 'name')   # True
        hash_exists('test:taozh', 'city')   # False, key不存在
        hash_exists('test:zht', 'name')     # False, 散列不存在
    """
    return get_redis().hexists(name, key)


def hash_len(name):
    """
    描述:
        获取指定散列中键值对的个数, 如果散列不存在, 则返回0

    参数:
        name:string -redis的键名

    返回:
        length:int - 键值个数

    示例:
        # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_len('test:taozh') # 3
        hash_len('test:zht') # 0
    """
    r = get_redis()
    return r.hlen(name)


def hash_keys(name):
    """
    描述:
        获取指定散列中所有的key的值列表, 如果散列不存在则返回[]

    参数:
        name:string -redis的键名

    返回:
        keys:list -散列中所有key的list, 如果散列不存在则返回[]

    示例:
        # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_keys('test:taozh') # ['name', 'age', 'email']
        hash_keys('test:hash-nx') # []
    """
    return get_redis().hkeys(name)


def hash_values(name):
    """
    描述:
        获取指定散列中所有value的list, 如果散列不存在, 则返回[]

    参数:
        name:string -redis的键名

    返回:
        values:list -散列中所有value的list, 如果散列不存在则返回[]

    示例:
        #test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_values('test:taozh') # ['Zhang Tao', '18', 'taozh@cisco.com']
        hash_values('test:hash-nx') # []

    """
    return get_redis().hvals(name)


def hash_incr(name, key, amount=1):
    """用法请参考str_incr"""
    return get_redis().hincrby(name, key, amount=amount)


def hash_decr(name, key, amount=1):
    """用法请参考str_decr"""
    return get_redis().hincrby(name, key, amount=-amount)


def hash_incrfloat(name, key, amount=1.0):
    """用法请参考str_incrfloat
    """
    return get_redis().hincrbyfloat(name, key, amount=amount)


def hash_decrfloat(name, key, amount=1.0):
    """
    用法请参考str_decrfloat
    """
    return get_redis().hincrbyfloat(name, key, amount=-amount)


def hash_scan(name, cursor=0, match=None, count=None):
    """
    描述:
        基于游标的迭代器, 以【分片】的方式【批量】获取数据, 对于数据量较大的数据非常有用, 可以避免取出全部数据把内存撑爆
        每次调用时, 返回一个更新的游标cursor和分片数据【字典】组成的元组
        match是匹配条件, 可以通过匹配条件对散列的key进行过滤
        但是请注意,【match是在检索以后应用的】, 如果每次检索出来的集合包含较少满足条件的数据, 在大多数迭代数据可能都是空

        count选项是每次分片的数据长度, 默认是10,
        请注意, 即便设置了count, 也【不能确保每次取出来的数据长度】, 真实的长度可能会【大于或等于】设置的值, 甚至会一次全部取出

    参数:
        name:string -redis的键名
        cursor:int -迭代器的游标
        match:string -pattern匹配条件, 有如下可选项Å
            h?llo -matches hello, hallo and hxllo
            h*llo -matches hllo and heeeello
            h[ae]llo -matches hello and hallo, but not hillo
            h[^e]llo -matches hallo, hbllo, ... but not hello
            h[a-b]llo -matches hallo and hbllo
        count:int -每次分片的数据长度, 默认是10

    返回:
        cursor:int - 更新的游标cursor
        data:dict -分片数据字典组成的元组

    示例:
        # 添加测试数据
        for i in range(10000):
            hash_set('test:xxx', i, i)

        cursor = 0
        count = 0
        while True:
            cursor, data = hash_scan('test:xxx', cursor=cursor,count=20) # data为包含分片数据的dict ->{'k188': 'v188', 'k938': 'v938',...}
            print(cursor, data)
            count+=1
            if cursor == 0:
                break
        print(count) # 迭代了大约490次左右
    """
    return get_redis().hscan(name, cursor=cursor, match=match, count=count)


def hash_scan_iter(name, match=None, count=None):
    """
    描述:
        以迭代器的方式分批去redis中批量获取散列数据, 每个迭代对象为由key和value组成的元组, 数据量较大的数据非常有用
        和hash_scan的主要区别是: hash_scan_iter【不需要记住游标】的位置, 迭代即可

    参数:
        name:string -redis的键名
        match:string -pattern匹配条件, 有如下可选项
            h?llo -matches hello, hallo and hxllo
            h*llo -matches hllo and heeeello
            h[ae]llo -matches hello and hallo, but not hillo
            h[^e]llo -matches hallo, hbllo, ... but not hello
            h[a-b]llo -matches hallo and hbllo
        count:int -每次分片的数据长度, 默认是10

    返回:
        iter -迭代器

    示例:
        # 添加测试数据
        for i in range(10000):
            hash_set('test:xxx', i, i)

        for item in hash_scan_iter('test:xxx'):
            print(item) # ('k368', 368.0)

    """
    return get_redis().hscan_iter(name, match=match, count=count)
