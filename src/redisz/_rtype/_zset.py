from redis.commands import list_or_args

from .. import get_redis

"""
有序集合在集合的基础上, 为每个元素添加了排序给你；元素的排序需要根据另外一个值来进行比较, 
对于有序集合, 每一个元素有两个值, 即：值和分数, 分数专门用来做排序
数据按分值由小到大排序
"""
__all__ = [
    'zset_add',  # 增加
    'zset_rem', 'zset_remrangebyrank', 'zset_remrangebyscore',  # 删除
    'zset_card', 'zset_count', 'zset_rank', 'zset_score',  # 查询
    'zset_range', 'zset_revrange','zset_rangebyscore','zset_revrangebyscore',  # 查询
    'zset_incr', 'zset_decr',  # 自增/自减
    'zset_scan', 'zset_scan_iter',  # 遍历
]


def zset_count(name, min, max):
    """
    描述:
        获取指定有序集合中【分数】在 [min,max] 之间的个数

    参数:
        name -redis的键名
        min:int/float -最小的分数值, >=min
        max:int/float -最大的分数值, <=max

    返回:
        count:int -有序集合中【分数】在 [min,max] 之间的个数

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_count('test:zset', 20, 30) # 2, 20=<score<=30
        zset_count('test:zset', 21, 30) # 1, 21=<score<=30
    """
    return get_redis().zcount(name, min, max)


def zset_rem(name, values, *args):
    """
    描述:
        删除指定有序集合中的一个/多个元素

    参数:
        name:string -redis的键名
        values:list -要删除元素的列表
        args -通过关键字参数传递的一个/多个要删除的元素

    返回:
        count:int -删除成功的个数

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_rem('test:zset', 'a', 'b','e') # 2 test:zset={'c': 30}
    """
    values = list_or_args(values, args)
    return get_redis().zrem(name, *values)


def zset_score(name, value):
    """
    描述:
        获取指定有序集合中value元素的分数

    参数:
        name:string -redis的键名
        value:string -有序集合中的元素

    返回:
        score:float -value元素的分数, 如果不存在则返回None

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_score('test:zset', 'a') # 10.0
        zset_score('test:zset', 'x') # None
    """
    return get_redis().zscore(name, value)


def zset_rank(name, value):
    """
    描述:
        获取指定的元素在有序集合中的索引(从0开始)

    参数:
        name:string -redis的键名
        value:str -指定的元素

    返回:
        index:int -value元素在有序集合中的索引, 如果有序集合不存在/元素不存在返回None

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_rank('test:zset', 'a') # 0
        zset_rank('test:zset', 'b') # 1
        zset_rank('test:zset', 'x') # None
    """
    return get_redis().zrank(name, value)


def zset_incr(name, amount, value):
    """
    描述:
        增加指定有序集合中value元素的【分数】, 如果元素在有序集合中不存在, 则创建并赋值

    参数:
        name:string -redis的键名
        amount:int|float -要增加的【分数】, 如果<0表示要减掉的分数
        value:str -要增加分数的元素

    返回:
        score:float -增加以后的分数

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_incr('test:zset', 1, 'a') # 11.0
        zset_incr('test:zset', 2.2, 'b') # 22.2
        zset_incr('test:zset', -2, 'c') # 28.0
        zset_incr('test:zset', 3, 'e') # 3.0
    """
    return get_redis().zincrby(name, amount, value)


def zset_decr(name, amount, value):
    """
    描述:
        减少指定有序集合中value元素的【分数】, 如果元素在有序集合中不存在, 则创建并赋值

    参数:
        name:string -redis的键名
        amount:int|float -要减少的【分数】, 如果>0表示要增加的分数
        value:str -要减少分数的元素

    返回:
        score:float -减少以后的分数

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_decr('test:zset', 1, 'a') # 9.0
        zset_decr('test:zset', 2.2, 'b') # 17。8
        zset_decr('test:zset', -2, 'c') # 32.0
        zset_decr('test:zset', 3, 'e') # -3.0
    """
    return get_redis().zincrby(name, -amount, value)


def zset_card(name):
    """
    描述:
        获取指定有序集合元素的数量

    参数:
        name:string -redis的键名

    返回:
        count:int -指定有序集合中元素的数量, 如果zset不存在则返回0

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_card('test:zset') # 3
        zset_card('test:not-exist-zset') # 0
    """
    return get_redis().zcard(name)


def zset_add(name, mapping, **kwargs):
    """
    描述:
        在指定有序集合中添加元素,
        默认情况, 如果zset不存在, 则创建并赋值
        如果mapping中指定的元素已存在, 则替换

    参数:
        name:string -redis的键名
        mapping:dict -要添加的元素和分数字典, 分数必须是【数字】
        kwargs可选参数如下:
            nx:bool - 如果设置为True, 则只有元素【不存在】时, 当前add操作才执行
            xx:bool - 如果设置为True, 则只有元素【存在】时, 当前add操作才执行
            ch:bool - 如果设置为True, 将返回【修改】的元素数

    返回:
        count:int - 添加成功的元素数, 默认如果是修改则不计数

    示例:
        zset_add('test:zset', {'a': 10, 'b': 20}) # 2, test:zset = {'a': 10, 'b': 20}
        zset_add('test:zset', {'b': 30, 'c': 40}) # 1, 替换b的值, test:zset = {'a': 10, 'b': 30, 'c': 40}
        zset_add('test:zset', {'c': 50, 'd': 60}, nx=True) # 1, nx=True不会替换d的值, test:zset = {'a': 10, 'b': 30, 'c': 40, 'd':60}
        zset_add('test:zset', {'d': 70, 'e': 80}, xx=True) # 0, nx=True不会替换d的值, test:zset = {'a': 10, 'b': 30, 'c': 40, 'd':70}

        zset_add('test:zset', {'x': 100, 'y': 200, 'z': 300})
        zset_add('test:zset', {'x': 110, 'y': 220, 'z': 300}, ch=True) # 2
    """
    return get_redis().zadd(name, mapping, **kwargs)


def zset_range(name, start, end, desc=False, withscores=False, score_cast_func=float, byscore=False):
    """
    描述:
        按照索引范围获取指定有序集合的元素
        默认情况, 返回结果按score【从小到大】排列, 设置desc=True以从大到小排列
        默认情况, start和end表示索引范围, 设置byscore=True, start和end表示分数范围, 
        无论start和end表示索引还是分数, 都会被包含在结果内

    参数:
        name -redis的键名
        start:int -有序集合索引起始位置, 如果byscore=True, 则表示起始分数, start=<
        end:int -有序集合索引结束位置, 如果byscore=True, 则表示结束分数, <=end
        desc:bool -默认按照分数从小到大排序, 设置desc=True, 则按分数【从大到小】排序
        withscores:bool -默认只获取元素, 设置withscores=True, 会把分数也一起返回
        score_cast_func:func -对分数进行数据转换的函数
        byscore:bool -默认按索引进行查询, 设置byscore=True, 按分数查询

    返回:
        values:list -有序集合在start-end范围内的元素列表, 如果withscores=True则返回包含元素和分数元组的列表, 参考示例

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_range('test:zset', 0, -1) # 从小到大, ['a', 'b', 'c']
        zset_range('test:zset', 0, -1, desc=True) # 从大到小, ['c', 'b', 'a']
        zset_range('test:zset', 0, -1, withscores=True) # 返回元素和分数, [('a', 10.0), ('b', 20.0), ('c', 30.0)]
        zset_range('test:zset', 0, 20, withscores=True, byscore=True) # start和end指定的是分数范围, [('a', 10.0), ('b', 20.0)]
    """

    return get_redis().zrange(name, start, end, desc=desc, withscores=withscores, score_cast_func=score_cast_func, byscore=byscore)


def zset_revrange(name, start, end, withscores=False, score_cast_func=float):
    """
    描述:
        按分数【从大到小】的方式从指定有序集合中获取元素
        start和end表示的是【索引】, 而不是分数
        是zset_range方法的一个简化版本, == zset_range(name, start, end, desc=True, withscores=False, score_cast_func=float)

    参数:
        name:string -redis的健名
        start:int -起始【索引】位置, 可以是负数 start=<
        end:int -结束【索引】位置, 可以是负数 <=end
        withscores:bool -默认只返回元素, 设置withscores=True, 会把分数也一起返回
        score_cast_func:func -对分数进行数据转换的函数

    返回:
        values:list -有序集合在start-end范围内的元素列表, 如果withscores=True则返回包含元素和分数元组的列表, 参考示例

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30}
        zset_revrange('test:zset', 0, 1) # ['c', 'b']
        zset_revrange('test:zset', 0, -1) # ['c', 'b', 'a']
        zset_revrange('test:zset', 0, -1, withscores=True) # [('c', 30.0), ('b', 20.0), ('a', 10.0)]
    """
    return get_redis().zrevrange(name, start, end, withscores=withscores, score_cast_func=score_cast_func)


def zset_rangebyscore(name, min, max, start=None, num=None, withscores=False, score_cast_func=float):
    """
    描述:
        按分数【从小到大】的方式从指定有序集合中获取元素
        min和max表示的是【分数】, 而不是索引
        start和num用来指定获取元素的开始和个数, 两者必须同时指定, 否则会引发异常
        在zset_range(name, start, end, desc=False, withscores=False, score_cast_func=float, byscore=True)的基础上增加了start/number两个参数

    参数:
        name:string -redis的健名
        min:int -最小【分数】min=<
        max:int -最大【分数】<=max
        start:int -开始【索引】, 请注意start索引处的元素不包含在结果内, 这是跟其他接口区别的地方
        num:int -要获取的元素【个数】
        withscores:bool -默认只返回元素, 设置withscores=True, 会把分数也一起返回
        score_cast_func:func -对分数进行数据转换的函数

    返回:
        values:list -有序集合在min-max范围内的从大到小排序的元素列表
                     如果有序集合不存在/没有符合条件的元素, 返回[]
                     如果withscores=True则返回包含元素和分数元组的列表, 请参考示例

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
        zset_rangebyscore('test:zset', 20, 50)  # ['b', 'c', 'd', 'e']
        zset_rangebyscore('test:zset', 20, 50, withscores=True)  # [('b', 20.0), ('c', 30.0), ('d', 40.0), ('e', 50.0)]
        zset_rangebyscore('test:zset', 20, 50, 1, 2, withscores=True)  # [('c', 30.0), ('d', 40.0)]
    """
    return get_redis().zrangebyscore(name, min, max, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func)


def zset_revrangebyscore(name, max, min, start=None, num=None, withscores=False, score_cast_func=float):
    """
    描述:
        按分数【从大到小】的方式从指定有序集合中获取元素
        min和max表示的是【分数】, 而不是索引
        start和num用来指定获取元素的开始和个数, 两者必须同时指定, 否则会引发异常
        在zset_range(name, start, end, desc=True, withscores=False, score_cast_func=float, byscore=True)的基础上增加了start/number两个参数
        和zset_rangebyscore的区别就是, 一个是从小到大, 一个是从大到小

    参数:
        name:string -redis的健名
        min:int -最小【分数】min=<
        max:int -最大【分数】<=max
        start:int -开始【索引】, 请注意start索引处的元素不包含在结果内, 这是跟其他接口区别的地方
        num:int -要获取的元素【个数】
        withscores:bool -默认只返回元素, 设置withscores=True, 会把分数也一起返回
        score_cast_func:func -对分数进行数据转换的函数

    返回:
        values:list -有序集合在min-max范围内从大到小排列的元素列表
                     如果有序集合不存在/没有符合条件的元素, 返回[]
                     如果withscores=True则返回包含元素和分数元组的列表, 请参考示例

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
        zset_revrangebyscore('test:zset', 50, 20))  # ['e', 'd', 'c', 'b']
        zset_revrangebyscore('test:zset', 50, 20, withscores=True))  # [('e', 50.0), ('d', 40.0), ('c', 30.0), ('b', 20.0)]
        zset_revrangebyscore('test:zset', 50, 20, 1, 2, withscores=True))  # [('d', 40.0), ('c', 30.0)]
    """
    return get_redis().zrevrangebyscore(name, max, min, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func)


def zset_scan(name, cursor=0, match=None, count=None, score_cast_func=float):
    """
    描述:
        基于游标的迭代器, 以【分片】的方式【批量】获取数据, 对于数据量较大的数据非常有用, 可以避免取出全部数据把内存撑爆
        每次调用时, 返回一个更新的游标cursor和分片数据【列表】组成的元组, 请注意, 数据列表中的数据是【无须】的
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
        cursor:int -更新的游标cursor
        data:tuple -分片数据列表组成的元组

    示例:
        # 添加测试数据
        maps = {}
        for i in range(10000):
            maps['k' + str(i)] = i
        zset_add('test:xxx', maps)

        cursor = 0
        count = 0
        while True:
            cursor, data = zset_scan('test:xxx', cursor=cursor, count=20)  # data为包含分片数据的列表 ->[('k3299', 3299.0), ('k6223', 6223.0),...]
            print(cursor, data)
            count += 1
            if cursor == 0:
                break
        print(count)  # 迭代了大约490次左右
    """
    return get_redis().zscan(name, cursor=cursor, match=match, count=count, score_cast_func=score_cast_func)


def zset_scan_iter(name, match=None, count=None, score_cast_func=float):
    """
        描述:
            以迭代器的方式, 以【分片】的方式【批量】获取数据, 对于数据量较大的数据非常有用, 可以避免取出全部数据把内存撑爆
            和zset_scan的主要区别是: zset_scan_iter【不需要记住游标】的位置, 迭代即可

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
            maps = {}
            for i in range(10000):
                maps['k' + str(i)] = i
            zset_add('test:xxx', maps)

            for i in set_scan_iter('test:xxx'):
                print(i) # ('k368', 368.0)
        """
    return get_redis().zscan_iter(name, match=match, count=count, score_cast_func=score_cast_func)


def zset_remrangebyrank(name, min, max):
    """
    描述:
        根据【索引】范围从有序集合删除元素
        min和max索引处的元素也会被删除

    参数:
        name:string -redis的健名
        min:int -最小索引 min<=, 可以为负数, -1表示最后一个元素
        max:int -最大索引 <=max, 可以为负数, -1表示最后一个元素

    返回:
        rem_count:int -成功删除的元素个数, 如果有序集合不存在或者索引超出返回, 返回0

    示例:
        # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
        zset_remrangebyrank('test:zset', 0, 3) # 4, test:zset -> [('e', 50.0), ('f', 60.0)]
        zset_remrangebyrank('test:zset', -3, -1) # 3, test:zset -> [('a', 10.0), ('b', 20.0), ('c', 30.0)]

        zset_remrangebyrank('test:nx', 0, 2) # 0
        zset_remrangebyrank('test:nx', 10, 12) # 0
    """
    return get_redis().zremrangebyrank(name, min, max)


def zset_remrangebyscore(name, min, max):
    """
     描述:
         根据【分数】范围从有序集合删除元素
         min和max分数对应的元素也会被删除

     参数:
         name:string -redis的健名
         min:int -最小分数 min<=
         max:int -最大分数 <=max

     返回:
         rem_count:int -成功删除的元素个数, 如果有序集合不存在或者索引超出返回, 返回0

     示例:
         # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
         zset_remrangebyscore('test:zset', 0, 40) # 4, test:zset -> [('e', 50.0), ('f', 60.0)]
         zset_remrangebyscore('test:nx', 30, 100) # 5, test:zset -> [('a', 10.0), ('b', 20.0)]

         zset_remrangebyscore('test:nx', 10, 30) # 0
         zset_remrangebyscore('test:nx', 10, 110) # 0
     """
    return get_redis().zremrangebyscore(name, min, max)
