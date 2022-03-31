from .. import get_redis

"""
string类型可以存储字符串和数字, string是最基本的数据单位, 除了基本的增删改查操作, 还添加了强大的自增和自减功能
请注意string的length/range等操作, 都是以字节为单位进行的操作, 而不是字符, 尤其是字符串中包含汉字时(3个字节)更要注意
"""
__all__ = [
    'str_set', 'str_mset',  # 添加&批量添加
    'str_append', 'str_setrange', 'str_getset',  # 修改
    'str_get', 'str_mget', 'str_getrange', 'str_len',  # 查询
    'str_incr', 'str_decr', 'str_incrfloat', 'str_decrfloat'  # 自增&自减
]


def str_set(name, value, **kwargs):
    """
    描述:
        设置值, 默认情况, name对应的键值不存在则创建, 存在则替换,
        值的类型可以是: 字符串/整数/浮点数

    参数:
        name:string -redis中的键名
        value:str/int/float -要设置的值
        kwargs可选参数如下:
            ex:int -过期时间（秒
            px:int -过期时间（毫秒）
            nx:bool -如果设置为True, 则只有name不存在时, 当前set操作才执行
            xx:bool -如果设置为True, 则只有name存在时, 当前set操作才执行

    返回:
        result:bool - 如果设置成功返回True, 否则返回False

    示例:
        str_set('test:name', 'Zhang Tao')
        str_set('test:age', 18)
        str_set('test:email', 'taozh@cisco.com')
    """
    result = get_redis().set(name, value, **kwargs)
    if result is None:
        return False
    return result


def str_get(name):
    """
    描述:
        返回name对应的字符串类型键值, 如果键值不存在则返回None

    参数:
        name:string -redis中的键名

    返回:
        value:string -字符串键值, 如果不存在返回None

    示例:
        #test:name='focus-ui'
        str_get('test:name') # focus-ui
        str_get('test:not-exist') # None
    """
    return get_redis().get(name)


def str_mset(mapping):
    """
    描述:
        批量设置多个字符串类型的键值

    参数:
        mapping:dict -包含多个键值的字典

    返回:
        result:bool -设置成功返回True

    示例:
        #{'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
        hash_mget('test:taozh', 'name1')
        hash_mget('test:taozh', 'name', 'age')
        hash_mget('test:taozh', ['name', 'age'])
        hash_mget('test:taozh', ['name', 'age'], 'email')
    """
    return get_redis().mset(mapping)


def str_mget(names, *args):
    """
    描述:
        批量获取字符串类型键值list,

    参数:
        names:list -要获取的键名列表

    返回:
        values:list -获取到的键值列表, 如果只有一个name, 返回结果也是list

    示例:
        str_mget('test:name') # ['Zhang Tao']
        str_mget('test:name', 'test:age') # ['Zhang Tao', '18']
        str_mget(['test:name', 'test:age'], 'test:email') # ['Zhang Tao', '18', 'taozh@cisco.com']
        str_mget('test:name', 'test:not-exist') # ['Zhang Tao', None]
    """
    return get_redis().mget(names, *args)


def str_append(name, value):
    """
    描述:
        在name对应的字符串类型键值后面追加内容, name对应的字符串类型键值不存在则创建并赋值

    参数:
        name:string -redis的键名
        value:string/int/float -要追加的内容

    返回:
        length:int -添加成功的字符串【字节】长度(一个汉字三个字节)

    示例:
        str_append('test:email', '.cn') # test:email-> taozh@cisco.com.cn
    """
    if value is None:
        return str_len(name)
    return get_redis().append(name, value)


def str_getset(name, value):
    """
    描述:
        设置新值并获取原来的值, 如果name对应的键值不存在则创建, 并返回None
    
    参数:
        name:string -redis的键名
        value:string|int|float -要设置的新值

    返回:
        old_value:string -原来的值, 如果简直不存在, 则返回None
    
    示例:
        #test:age=18
        str_getset('test:age', 19) # 返回18, test:age -> 19
        str_getset('test:not-exist', 'new value') # 返回None, test:not-exist -> new value
    """
    return get_redis().getset(name, value)


def str_setrange(name, offset, value):
    """
    描述:
        修改字符串内容, 从指定字符串字节索引开始向后替换, 新值太长时, 则向后添加
        替换时【包含】offset索引处的字符
        请注意是【字节】非字符, 一个汉字3个字节

    参数:
        name:string -redis的键名
        offset:int -替换开始位置的索引
        value:string -要替换的字符

    返回:
        length:int -修改成功以后的【字节】长度

    示例:
        # test:email=taozh@cisco.com
        str_setrange('test:email', 6, '1982@gmail.com')  # 20, test:email -> taozh1982@cisco.com

        # test:study=好好学习
        str_setrange('test:study', 6, '工作')  # 12, test:study -> 好好工作, 一个汉字3个字节, 所以从6开始
    """
    return get_redis().setrange(name, offset, value)


def str_getrange(name, start, end):
    """
    描述:
        根据【字节】索引获取获取子串, 子串既包括start又包括end索引处的字节
        start和end可以为负数, 最后一个字符的索引是-1, 倒数第二个字符的索引是-2, 以此类推
        请注意是【字节】非字符, 一个汉字3个字节

    返回:
        result:string -子字符串

    参数:
        name:string -redis的键名
        start:int   -开始字节索引
        end:int     -结束字节索引

    示例:
        # test:email=taozh@cisco.com
        str_getrange('test:email', 0, 4) # taozh, 索引0-4的5个字节
        str_getrange('test:email', -3, -1) # com, 索引-2 - -1的2个字节
        # test:study=好好学习
        str_getrange('test:study', 0, 2) # 好, 索引0-2的3个字节, 一个汉字3个字节
    """
    return get_redis().getrange(name, start, end)


def str_len(name):
    """
    描述:
        返回name对应值的字节长度（一个汉字3个字节, 如果键值不存在, 返回0

    参数:
        name:str -redis的键名

    返回:
        length:int -键值的字节长度, 如果不存则, 则返回0

    示例:
        # test:email=taozh@cisco.com
        str_len('test:email') # 15
        # test:zh=好好学习
        str_len('test:zh') # 12, 3*4=12个字节
    """
    return get_redis().strlen(name)


def str_incr(name, amount=1):
    """
    描述:
        自增name对应的键值, 返回结果为自增以后的值
        当name不存在时, 则创建键值并赋值为amount
        amount必须是【整数】, 可以为负数, 负数表示自减
        如果name对应的键值不是整数(包括浮点数), 会引发异常

    参数:
        name:string -redis的键名
        amount:int -增加的数值

    返回
        value:int -自增以后的值

    示例:
        # test:age=18
        str_incr('test:age') # 19
        str_incr('test:age', 2) # 21
        str_incr('test:age', -1) # 20
        str_incr('test:not-exist') # 1, test:not-exist不存在, 创建test:not-exist, 并赋值为1

        str_incr('test:email') # test:email不是整数, 引发异常
        str_incr('test:float-1.1') # test:float-1.1不是整数, 引发异常
    """
    return get_redis().incrby(name, amount=amount)


def str_decr(name, amount=1):
    """
    描述:
        自减name对应的值, 返回结果为自减以后的值
        当name不存在时, 则创建键值并赋值为-amount(-1)
        amount必须是【整数】, 可以为负数, 负数表示自增
        如果name对应的键值不是整数(包括浮点数), 会引发异常

    参数:
        name:string -redis的键名
        amount:int -减去的数值

    返回
        value:int -自减以后的值

    示例:
        # test:count=10
        str_decr('test:count') # 9
        str_decr('test:count', 2) # 7
        str_decr('test:count', -1) # 8
        str_decr('test:not-exist') # -1, test:not-exist不存在, 创建test:not-exist, 并赋值-1

        str_decr('test:email') # test:email不是整数, 引发异常
        str_decr('test:float-1.1') # test:float-1.1不是整数, 引发异常
    """
    return get_redis().decrby(name, amount=amount)


def str_incrfloat(name, amount=1.0):
    """
    浮点类型的自增操作, 请参考str_incr
    """
    return get_redis().incrbyfloat(name, amount=amount)


def str_decrfloat(name, amount=1.0):
    """
     浮点类型的自减操作, 请参考str_decr
    """
    return get_redis().incrbyfloat(name, amount=-amount)
