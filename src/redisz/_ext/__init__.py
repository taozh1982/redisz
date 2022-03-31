from .. import get_type, str_set, list_push, zset_add, hash_set, set_add, str_get, list_get, hash_getall, set_members, zset_range, keys, list_iter, set_card, \
    set_ismember, zset_rank, zset_card

# --------------------------------- sys ---------------------------------
get_names = keys


def set_value(name, value, **kwargs):
    """
    描述:
        为name设置键值, 如果name对应的键值已经存在, 则会在原键值基础上进行操作(list/hast/set/zset), 如果要替换, 请先通过delete方法删除原键值
        方法通过value的类型决定键值的类型, 规则如下:
            -如果value类型是str/int/float, 将会以string类型存储
            -如果value类型是list, 将会以list类型存储(如果list已经存在, 则会将value中的值添加到原list中)
            -如果value类型是dict, 默认将会以hash类型存储, 设置关键字参数type='zset', 则将会以zset存储
            -如果value类型是set, 将会以set类型存储
            -其他类型, 将会引发异常
        此方法是一个通用方法, 如果想要更详细的控制设值操作, 请调用对应类型的函数进行处理

    返回:
        result:bool|integer
            -如果设置str, 返回True/False
            -如果设置list, 返回list的长度
            -如果设置hash/set/zset, 返回添加/修改的元素数量

    参数:
        name:string -redis的键名
        value:str|int|float|list|dict|set -要设置的值

    示例:
        set_value('test:str', 'a') # str
        set_value('test:str-number', 1.0) # number
        set_value('test:list', [1, 2, 3]) # list
        set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})
        set_value('test:set', {'x', 'y', 'z'})
        set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')
    """
    if value is None:
        return False
    type_ = type(value)
    if type_ is str or type_ is int or type_ is float:
        return str_set(name, value)
    if type_ is list:
        return list_push(name, value)
    if type_ is dict:
        if kwargs.get('type') == 'zset':
            return zset_add(name, value)
        else:
            return hash_set(name, mapping=value)
    if type_ is set:
        return set_add(name, value)
    raise TypeError('only list/dict/set/str/int/float are supported.')


def get_value(name):
    """
    描述:
        返回name对应的键值, 会根据name的类型分类返回

    参数:
        name:string -要获取的键值名

    返回:
        result -获取到的键值结果, 不同的键值类型, 返回的结果类型不一样

    示例:
        get_value('test:str')           # a
        get_value('test:str-number')    # 1.0
        get_value('test:list')          # ['1', '2', '3']
        get_value('test:hash')          # {'a': '1', 'b': '2', 'c': '3'}
        get_value('test:set')           # {'x', 'y', 'z'}
        get_value('test:zset')          # [('x', 1.0), ('y', 2.0), ('z', 3.0)]
    """
    type_ = get_type(name)
    if type_ == 'string':
        return str_get(name)
    if type_ == 'list':
        return list_get(name)
    if type_ == 'hash':
        return hash_getall(name)
    if type_ == 'set':
        return set_members(name)
    if type_ == 'zset':
        return zset_range(name, 0, -1, withscores=True)

    return None


# --------------------------------- list ---------------------------------
list_getall = list_get


def list_exists(name, value):
    """
    描述:
        检查指定列表中是否存在指定的值

    参数:
        name:string -redis的健名
        value:string|int|float -指定的元素

    返回:
        is_exists:bool -如果元素存在返回True, 否则返回False

    示例:
        # test:numbers=[1, 2, 3]
        list_exists('test:numbers', 1) # True
        list_exists('test:numbers', 10) # False
        list_exists('test:nx', 1) # False
    """
    value = str(value)
    for item in list_iter(name):  # 遍历列表
        if value == item:
            return True
    return False


# --------------------------------- set ---------------------------------
set_len = set_card
set_exists = set_ismember
set_getall = set_members

# --------------------------------- zset ---------------------------------
zset_index = zset_rank
zset_len = zset_card


def zset_getall(name, withscores=False):
    """
        描述:
            返回指定有序集合的所有元素

        参数:
            name -redis的键名
            withscores -默认只获取元素, 设置withscores=True, 会把分数也一起返回

        返回:
            values:list -有序集合所有元素的列表, 如果withscores=True则返回包含元素和分数元组的列表

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            zset_getall('test:zset') # ['a', 'b', 'c']
            zset_getall('test:zset', withscores=True) # 返回元素和分数, [('a', 10.0), ('b', 20.0), ('c', 30.0)]
        """
    return zset_range(name, 0, -1, withscores=withscores)


def zset_exists(name, value):
    """
    描述:
        检查指定排序集合中是否存在指定的元素

    参数:
        name:string -redis的健名
        value:string -指定的元素

    返回:
        is_exists:bool -如果元素存在返回True, 否则返回False

    示例:
        # test:numbers=[1, 2, 3]
        list_exists('test:numbers', 1) # True
        list_exists('test:numbers', 10) # False
        list_exists('test:nx', 1) # False
    """

    return zset_rank(name, value) is not None
