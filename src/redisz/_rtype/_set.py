from redis.commands import list_or_args

from .. import get_redis

"""
集合以【无序】的方式存储多个各不相同的元素, 除了基本增删改查操作外, 还增加了多个结合之间的差集/并集/交集计算功能
"""

__all__ = [
    'set_add',
    'set_pop', 'set_rem',
    'set_card', 'set_members',
    'set_ismember', 'set_scan', 'set_scan_iter',
    'set_diff', 'set_move',
    'set_inter',
    'set_union'
]


def set_members(name):
    """
    描述:
        获取指定集合中所有元素组成的set, 如果集合不存在返回一个空set对象
        因为集合是无序的, 所以每次取出的set元素顺序可能都是不一样的

    参数:
        name:string -redis的键名

    返回:
        members:set -所有集合元素组成的set

    示例:
        #test:letters = {'a', 'b', 'c', 'd', 'e'}
        set_members('test:letters') # {'a', 'b', 'c', 'd', 'e'}
        set_members('test:nx') # set()
    """
    return get_redis().smembers(name)


def set_add(name, values, *args):
    """
    描述:
        向指定集合中添加一个或多个元素, 如果集合不存在则新建并赋值, 返回结果为添加成功的元素个数
        可以通过列表或位置参数指定要添加的元素, 两者可以混用

    参数:
        name:string -redis的键名
        values:list -要添加的元素列表
        args -通过位置参数传递的一个/多个元素

    返回:
        success_count:int -添加成功的数量

    示例:
        set_add('test:letters', 'a', 'b', 'c') # 3, 创建集合并赋值, test:letters={'a', 'b', 'c'}
        set_add('test:letters', ['b', 'c', 'd']) # 1, 添加成功了'd', test:letters -> {'a', 'b', 'c', 'd'}
        set_add('test:letters', ['c', 'd'], 'e', 'f') # 2, 添加成功了'e'+'f', test:letters -> {'a', 'b', 'c', 'd', 'e', 'f'}
    """
    values = list_or_args(values, args)
    return get_redis().sadd(name, *values)


def set_rem(name, values, *args):
    """
    描述:
        从指定集合中删除指定的元素, 返回结果为删除成功的元素个数

    参数:
        name:string -redis的键名
        values:list -要删除的值list
        args -通过位置参数指定要删除的一个/多个值

    返回:
        success_count:int -删除成功的元素个数, 如果集合不存在, 返回0

    示例:
        # test:letters = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
        set_rem('test:letters', 'a', 'b') # 2, test:letters -> {c', 'd', 'e', 'f', 'g', 'h'}
        set_rem('test:letters', ['c', 'd']) # 2, test:letters-> {'e', 'f', 'g', 'h'}
        set_rem('test:letters', ['e', 'f'], 'g', 'x') # 3, test:letters-> {'h'}, x不存在, 所以结果为3
    """
    values = list_or_args(values, args)
    return get_redis().srem(name, *values)


def set_card(name):
    """
    描述
        获取指定集合的元素个数

    参数:
        name:string -redis的键名

    返回:
        count:int -元素个数, 如果集合不存在, 返回0

    示例:
        # test:letters = {'a', 'b', 'c', 'd', 'e'}
        set_card('test:letters') # 5
        set_card('test:nx') # 0
    """
    return get_redis().scard(name)


def set_ismember(name, value):
    """
    描述:
        检查value是否是指定集合的元素

    参数:
        name:string -redis的键名
        value:string|int|float -要检查的元素

    返回:
        is:bool -如果value是集合的成员, 返回True, 否则返回False, 集合不存在也返回False

    示例:
        # test:letters = {'a', 'b', 'c', 'd', 'e'}
        set_ismember('test:letters', 'a') # True
        set_ismember('test:letters', 'x') # False
        set_ismember('test:nx', 'a') # False
    """
    return get_redis().sismember(name, value)


def set_pop(name, count=None):
    """
    描述:
        从指定集合随机移除一个/多个元素, 并将其返回, 因为集合是无序的, 所以删除是随机的

    参数:
        name:string -redis的键名
        count:int -要移除的元素数, 取值范围是>=0的整数

    返回:
        item:string|list -移除的元素, 如果count>=0, 返回的是一个包含移除元素的列表

    示例:
        # test:letters = {'a', 'b', 'c', 'd', 'e'}
        set_pop('test:letters') # 返回'e', test:letters -> {'a', 'd', 'b', 'c'}
        set_pop('test:letters', 2) # 返回['a', 'b'], test:letters -> {'d', 'c'}
    """
    return get_redis().spop(name, count=count)


def set_scan(name, cursor=0, match=None, count=None):
    """
    描述:
        基于游标的迭代器, 以【分片】的方式【批量】获取数据, 对于数据量较大的数据非常有用, 可以避免取出全部数据把内存撑爆
        每次调用时, 返回一个更新的游标cursor和分片数据【列表】组成的元组
        match是匹配条件, 可以通过匹配条件对集合的值进行过滤
        但是请注意,【match是在检索以后应用的】, 如果每次检索出来的集合包含较少满足条件的数据, 在大多数迭代数据可能都是空

        count选项是每次分片的数据长度, 默认是10,
        请注意, 即便设置了count, 也【不能确保每次取出来的数据长度】, 真实的长度可能会【大于或等于】设置的值, 甚至会一次全部取出

    参数:
        name:string -redis的键名
        cursor:int -迭代器的游标
        match:string -pattern匹配条件, 有如下可选项
            h?llo -matches hello, hallo and hxllo
            h*llo -matches hllo and heeeello
            h[ae]llo -matches hello and hallo, but not hillo
            h[^e]llo -matches hallo, hbllo, ... but not hello
            h[a-b]llo -matches hallo and hbllo
        count:int -每次分片的数据长度, 默认是10

    返回:
        cursor:int -更新的游标cursor
        cursor_data:list -分片数据列表组成的元组

    示例:
        # 添加测试数据
        set_add('test:xxx', *range(10000))

        cursor = 0
        count = 0
        while True:
            cursor, data = set_scan('test:xxx', cursor=cursor, count=20) # data为包含元素的list -> ['1787', '219', '101',...]
            print(cursor, data)
            count += 1
            if cursor == 0:
                break
        print(count)  # 迭代了大约490次左右
    """
    return get_redis().sscan(name, cursor=cursor, match=match, count=count)


def set_scan_iter(name, match=None, count=None):
    """
    描述:
        以迭代器的方式, 以【分片】的方式【批量】获取数据, 对于数据量较大的数据非常有用, 可以避免取出全部数据把内存撑爆
        和set_scan的主要区别是: set_scan_iter【不需要记住游标】的位置, 迭代即可

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
        set_add('test:xxx', *range(10000))

        for i in set_scan_iter('test:xxx'):
            print(i) # 218
    """
    return get_redis().sscan_iter(name, match=match, count=count)


def set_move(src, dst, value):
    """
    描述:
        将指定元素从一个源集合中移动到目的集合
        请注意, 只要把元素从源集合移出, 返回结果就是True, 无论是否移入目标集合

    参数:
        src:string -源集合
        dst:string -目标集合
        value:string -要移动的元素

    返回:
        rem_success:bool -移动成功返回True, 否则返回False, 只要把元素从源集合移出, 返回结果就是True, 无论是否移入目标集合

    示例:
        # test:letters1={'a', 'b', 'c'}, test:letters2={'c', 'd', 'e'}
        set_move('test:letters1', 'test:letters2', 'a') # True, test:letters1={'b', 'c'}, test:letters2={'a', 'c', 'd', 'e'}
        set_move('test:letters1', 'test:letters2', 'c') # True, test:letters1={'b'}, test:letters2={'a', 'c', 'd', 'e'}
        set_move('test:letters1', 'test:letters2', 'f') # False, test:letters1={'b'}, test:letters2={'a', 'c', 'd', 'e'}
    """
    return get_redis().smove(src, dst, value)


def set_diff(names, *args, dst=None):
    """
    描述:
        差集, 返回只存在于【第一个】集合, 但不在其余集合中的元素集合, 即存在第一个集合中, 但不存在其他集合中的元素的集合
        可以将差集结果存储到一个新的dst集合中, 请注意, 如果dst对应的键值在redis已经存在(不论类型), 都会被替换

    参数:
        names:list -多个比较的集合列表
        args: -以位置参数方式传递的多个集合列表

    返回:
        result:set -差值集合, 如果设置dst, 返回差集中的元素数量

    示例:
        # test:letters1={'a', 'b', 'c'}, test:letters2={'b', 'm', 'n'}, test:letters3={'c', 'x', 'y'}
        set_diff('test:letters1', 'test:letters2') # {'c', 'a'}
        set_diff(['test:letters2', 'test:letters3']) # {'b', 'm', 'n'}
        set_diff(['test:letters1', 'test:letters2'], 'test:letters3') # {'a'}

        #test:diff=['a', 'x']
        set_diff(['test:letters1', 'test:letters2'], dst='test:diff') # 2, test:diff = {'a', 'c'}, 将diff结果存储到dst集合中, 无论原dst是什么类型
    """
    names = list_or_args(names, args)
    r = get_redis()
    if dst is not None:
        return r.sdiffstore(dst, names)
    return r.sdiff(names)


def set_inter(names, *args, dst=None):
    """
    描述:
        交集, 返回多个集合中元素的交集, 即同时存在于多个指定集合中的元素集合
        可以将交集结果存储到一个新的dst集合中, 请注意, 如果dst对应的键值在redis已经存在(不论类型), 都会被替换
        默认返回交集集合, 如果设置dst, 返回交集中的元素数量

    参数:
        names:list -进行交集运算的集合列表
        arge: -以位置参数方式传递的多个集合列表
        
    返回:
        result:set -交集集合, 如果设置dst, 返回交集中的元素数量

    示例: 
        # test:letters1={'a', 'b', 'c'}, test:letters2={'b', 'c', 'd'}, test:letters3={'c', 'd', 'e'}
        set_inter(['test:letters1', 'test:letters2']) # {'b', 'c'}
        set_inter(['test:letters2', 'test:letters3']) # {'c', 'd'}
        set_inter(['test:letters1', 'test:letters2'], 'test:letters3') # {'c'}

        #test:inter=['a', 'x']
        set_inter(['test:letters1', 'test:letters2'], dst='test:inter') # 2, test:inter = {'b', 'c'}
    """
    r = get_redis()
    if dst is not None:
        return r.sinterstore(dst, names, *args)
    return r.sinter(names, *args)


def set_union(names, *args, dst=None):
    """
    描述:
        并集, 获取多个集合中元素的并集
        可以将并结果存储到一个新的dst集合中, 请注意, 如果dst对应的键值在redis已经存在(不论类型), 都会被替换
        默认返回并集结合, 如果设置dst, 返回并集中的元素数量
        
    参数:
        names:list -进行并集运算的集合列表
        arge: -以位置参数方式传递的多个集合列表
        
    返回:
        result:set -并集集合, 如果设置dst, 返回并集中的元素数量

    示例: 
        #test:letters1={'a', 'b', 'c'}, test:letters2={'b', 'c', 'd'}, test:letters3={'c', 'd', 'e'}
        set_union(['test:letters1', 'test:letters2']) # {'a', 'b', 'c', 'd'}
        set_union(['test:letters2', 'test:letters3']) # {'b', 'c', 'd', 'e'}
        set_union(['test:letters1', 'test:letters2'], 'test:letters3') # {'a', 'b', 'c', 'd', 'e'}

        #test:union=['a', 'x']
        set_union(['test:letters1', 'test:letters2'],dst='test:union') # 4, test:union = {'a', 'b', 'c', 'd'}
    """
    r = get_redis()
    if dst is not None:
        return r.sunionstore(dst, names, *args)
    return r.sunion(names, *args)
