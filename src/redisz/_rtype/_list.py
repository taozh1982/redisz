from redis.commands import list_or_args

from .. import get_redis

"""
列表可以【有序】的存储多个【字符串】, 请注意列表的索引从0开始
"""
__all__ = [
    'list_push', 'list_insert',  # 添加
    'list_set',  # 修改
    'list_pop', 'list_rem', 'list_trim',  # 删除
    'list_get', 'list_index', 'list_len', 'list_range', 'list_iter',  # 查询&遍历
    'list_bpop', 'list_rpoplpush',  # 阻塞&移动
]


def list_push(name, values, *args, left=False, xx=False):
    """
    描述:
        向指定列表中增加一个或多个值, 默认情况, 如果列表不存在则新建并赋值
        默认添加到列表的右边, 如果left参数为True, 则添加到列表的左边
        如果xx为True, 则只有指定列表存在时才会添加, 并且一次只能加一个值

    参数:
        name:string -redis的键名
        values:list -要添加的元素列表
        *args -也可以通过位置参数的方式添加多个元素
        left:bool -设置left=True, 会将元素添加到列表的左侧, 请注意添加的多个值在列表中的顺序跟传递参数的顺序【相反】(请参考示例)
        xx:bool -设置xx为True, 只会将【一个】元素添加到已有列表, 如果列表不存在, 则不会创建

    返回:
        length:int - 整个列表的长度, 如果xx=True, 但是列表不存在则返回0

    示例:
        list_push('test:numbers', 3, 4) # test:numbers -> ['3', '4']
        list_push('test:numbers', [2, 1], left=True) # test:numbers -> ['1', '2', '3', '4'], 请注意1和2的顺序
        list_push('test:numbers', [5, 6], 7) # test:numbers -> ['1', '2', '3', '4', '5', '6', '7']

        list_push('test:not-exist', 1, xx=True) # test:not-exist不存在, 因为xx为True, 所以不会新建list
    """
    values = list_or_args(values, args)
    r = get_redis()
    if left is True:
        if xx is True:
            return r.lpushx(name, *values)
        return r.lpush(name, *values)
    else:
        if xx is True:
            return r.rpushx(name, *values)
        return r.rpush(name, *values)


def list_insert(name, ref_value, value, before=False):
    """
    描述:
        在指定列表的指定参考值前或后插入一个新值, where参数指定插入的位置(before/after), 默认插入到指定值的后边
        如果指定列表不存在, 则不做处理

    参数:
        name:string -redis的键名
        ref_value:sting|int|float -参考值, 如果为None, 则会引发异常
        *args -也可以通过位置参数的方式添加多个元素
        left:bool -设置left=True, 会将元素添加到列表的左侧, 请注意添加的多个值在列表中的顺序跟传递参数的顺序【相反】(请参考示例)

    返回:
        length:int
            -如果插入成功, 返回整个列表的长度
            -如果列表不存在, 返回0
            -如果ref_value在列表中不存则, 返回-1

    示例:
        # test:numbers = ['1', '3', '5']
        list_insert('test:numbers', 1, 2)       # 把2插入到1后边, test:numbers -> ['1', '2', '3', '5']
        list_insert('test:numbers', 5, 4, before=True)     # 把4插入到5前, test:numbers -> ['1', '2', '3', '4', '5']
        list_insert('test:numbers', 10, 11)     # 返回-1, 不做处理
        list_insert('test:not-exist', 1, 2)     # 返回0, 不做处理
    """
    where = 'after'
    if before is True:
        where = 'before'
    return get_redis().linsert(name, where, ref_value, value)


def list_set(name, index, value):
    """
    描述:
        对指定列表中的指定索引位置重新赋值
        如果n列表不存在/index超出范围/value不是字符串, 都会引发异常

    参数:
        name:string -redis的键名
        index:int -重新赋值的索引, 支持负数(-1表示最后一个元素索引), 必须是列表范围内的索引, 否则会引发异常
        value:string|int|float -新值, 除string|int|float以外的类型, 都会引发异常

    返回:
        result:bool -如果赋值成功返回True

    示例:
        # test:numbers = ['1', 'b', 'c']
        list_set('test:numbers', 1, 2) # 把第一个元素('b')替换成2, test:numbers -> ['1', '2', 'c']
        list_set('test:numbers', -1, 3) # 把最后一个元素('c')替换成3, test:numbers -> ['1', '2', '3']
    """
    r = get_redis()
    return r.lset(name, index, value)


def list_pop(name, count=None, left=False):
    """
    描述:
        将指定列表的右侧第一个元素移除并返回, 可以通过left参数设置从左侧移除
        -如果列表不存在, 无论count是几个, 返回None
        -如果列表没有值, 无论count是几个, 返回None
        -如果count=0, 返回None
        -如果count>1时, 如果列表个数小于count, 返回由移除元素组成的列表, 即便只移除了一个元素, 返回的也是列表

    参数:
        name:string -redis的键名
        count:int -要移除的个数
        left:bool -默认从右侧移除元素, 如果left=True, 则从左侧移除元素

    返回:
        item:string|list - 移除的元素或元素组成的列表

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_pop('test:numbers')    # 移除最右侧的元素, 返回结果为6, test:numbers -> ['1', '2', '3', '4', '5'])
        list_pop('test:numbers',2)  # 移除最右侧的2个元素, 返回结果为['5', '4'], test:numbers -> ['1', '2', '3']
        list_pop('test:numbers',left=True)  # 返回结果为1, test:numbers -> ['2', '3']
        list_pop('test:numbers',3) # 返回结果为['2', '3'], test:numbers -> []

    """
    r = get_redis()
    if left is True:
        return r.lpop(name, count=count)
    else:
        return r.rpop(name, count=count)


def list_rem(name, value, count=1):
    """
    描述:
        在指定列表中删除指定的值, 默认删除第一个等于value的值, 返回值为删除的值个数
        count指定了删除个数和删除方向
        count > 0: 从左向右删除指定个数的等于value的值
        count < 0: 从右向左删除指定个数的等于value的值
        count = 0: 删除所有等于value的元素

    参数:
        name:string -redis的键名
        value:string|int|float -要删除的值
        count:int - 删除的个数和删除方向

    返回:
        count:int -删除的个数, 如果列表不存在或value在列表中不存在, 返回0

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6', '5', '4', '3', '2', '1']
        list_rem('test:numbers', 1)     # 从左向右删除第一个1 -> ['2', '3', '4', '5', '6', '5', '4', '3', '2', '1']
        list_rem('test:numbers', 2, -1)     # 从后向前删除第一个2 -> ['2', '3', '4', '5', '6', '5', '4', '3', '1']
        list_rem('test:numbers', 4, 0)      # 删除所有的 -> ['2', '3', '5', '6', '5', '3', '1']

        list_rem('test:numbers', 10)      # 值在列表中不存在, 返回0
        list_rem('test:numbers1', 10)      # 列表不存在, 返回0
    """
    return get_redis().lrem(name, count, value)


def list_trim(name, start, end):
    """
    描述:
        在指定列表中移除没有在start-end索引之间的值, start和end索引处的值不会被移除
        只保留start<=索引<=end的值, 如果start>end或者start<0, list所有的值都会被移除

    参数:
        name:string -redis的键名
        start:int -开始索引, start索引处的值不会被移除
        end:int -结束索引, end索引处的值不会被移除

    返回:
        result:bool -True

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_trim('test:numbers', 1, 3)  # 把索引在1-3以外的值移除, test:numbers -> ['2', '3', '4']
    """
    return get_redis().ltrim(name, start, end)


def list_get(name, nx_none=False):
    """
    描述:
        返回指定列表的所有元素
        如果指定列表不存在, 返回[]
        如果指定列表不存在, 但是nx_none=True, 返回None

    参数:
        name:string -redis的键名
        nx_none:bool -设置nx_none=True, 在列表不存在时, 返回None

    返回:
        result:list -列表中的元素组成的list

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_get('test:numbers') # ['1', '2', '3', '4', '5', '6']

        list_get('test:not-exist') # 列表不存在, 返回[]
        list_get('test:not-exist', nx_none=True) # 列表不存在, 返回None
    """
    r = get_redis()
    if nx_none is True and r.exists(name) == 0:
        return None
    return r.lrange(name, 0, -1)


def list_index(name, index):
    """
    描述:
        在指定列表中根据索引获取列表元素
        如果列表不存在/index超出了列表范围, 返回None

    参数:
        name:string -redis的键名
        index:int -索引, 支持负数, 最后一个元素的索引是-1

    返回:
        result:string -索引处的元素, 如果列表不存在/index超出了列表范围, 返回None

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_index('test:numbers', 1) # 索引为1的值为:2
        list_index('test:numbers', -1) # 索引为-1的值为:6
    """
    return get_redis().lindex(name, index)


def list_len(name):
    """
    描述:
        获取指定列表的长度
        如果列表不存在, 返回0

    参数:
        name:string -redis的键名

    返回:
        length:init -列表的长度, 如果列表不存在, 返回0

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_len('test:numbers') # 6
        list_len('test:not-exist') # 0
    """
    return get_redis().llen(name)


def list_range(name, start, end):
    """
    描述:
        返回指定列表在start和end范围内的数据list
        如果指定列表不存在, 返回[]
        返回list中包含start和end索引处的数据项
        如果start或end超出了列表的索引范围, 只会返回列表索引范围内的数据列表
        如果要返回所有数据项, 可以通过start=0 & end=-1进行获取

    参数:
        name:string -redis的键名
        start:int -开始索引
        end:int -结束索引

    返回:
        result:list -列表在start和end范围内数据组成的列表

    示例:
        # test:numbers = ['1', '2', '3', '4', '5', '6']
        list_range('test:numbers', 0, 2) # 包含第0个和第2个数据, ['1', '2', '3']
        list_range('test:numbers', 0, -1) # -1表示最后一个数据项的索引, ['1', '2', '3', '4', '5', '6']
        list_range('test:not-exist', 0, 100) # 只会返回范围内的数据, ['1', '2', '3', '4', '5', '6']
        list_range('test:not-exist', 0, -1) # []
    """
    return get_redis().lrange(name, start, end)


def list_iter(name):
    """
    描述:
        利用yield封装创建生成器, 对指定列表元素进行增量迭代, 数据量较大时比较有用, 避免将数据全部取出把内存撑爆

    参数:
    name:string -redis的键名

    示例:
        for item in list_iter('test:numbers'):  # 遍历列表
            print(item)
    """
    r = get_redis()
    count = r.llen(name)

    for index in range(count):
        yield r.lindex(name, index)


def list_bpop(names, timeout=0, left=False):
    """
    描述:
        从names对应的一个或多个列表中依次移除元素, 返回结果是一个包含name和移除数据的元组('test:numbers', '2')
        如果指定了多个列表, 则【依次】移除, 先移除第一个列表中的元素, 如果第一个列表的元素都被移除了, 再移除第二个列表中的元素, 依次类推
        默认按照从右向左的方向进行移除, 可以通过left参数指定从左向右的方向进行移除
        如果指定的列表不存在或都为空, 则会【阻塞】指定的时间(秒)直到数据存入, 如果time=0表示一直阻塞直到数据出现

    参数:
        name:string -redis的键名
        timeout:int -列表为空时, 阻塞的的时间, 单位是秒, 如果time=0表示一直阻塞直到任一列表中出现数据, 必须是>=0的整数, 否则time不起作用
        left:bool -left=True, 从左向右逐个移除元素

    返回:
        result:tuple -包含name和移除数据的元组, 如果阻塞指定时间以后没有数据, 返回None

    示例:
        # test:numbers1 = [1, 2], test:numbers2 = [3, 4],

        # 从右向左依次移除
        list_bpop(['test:numbers', 'test:numbers1']) # ('test:numbers', '2')
        list_bpop(['test:numbers', 'test:numbers1']) # ('test:numbers', '1')
        list_bpop(['test:numbers', 'test:numbers1']) # ('test:numbers', '4')
        list_bpop(['test:numbers', 'test:numbers1']) # ('test:numbers', '3')
        list_bpop(['test:numbers', 'test:numbers1'], 2) # 阻塞等待两秒, 如果数据没有出现, 则向下运行

        #从左向右依次移除
        list_bpop(['test:numbers', 'test:numbers1'], left=True) # ('test:numbers', '1')
        list_bpop(['test:numbers', 'test:numbers1'], left=True) # ('test:numbers', '1')
        list_bpop(['test:numbers', 'test:numbers1'], left=True) # ('test:numbers', '1')
        list_bpop(['test:numbers', 'test:numbers1'], left=True) # ('test:numbers', '1')
        list_bpop(['test:numbers', 'test:numbers1']) # 一直阻塞等待数据出现
    """
    r = get_redis()
    if left is True:
        return r.blpop(names, timeout=timeout)
    else:
        return r.brpop(names, timeout=timeout)


def list_rpoplpush(src, dst, timeout=None):
    """
    描述:
        从一个列表的右侧移除一个元素并将其添加到另一个列表的左侧, 并将值返回
        如果对应的列表中值不存在, 则会阻塞指定的时间(秒)直到数据存入, 如果time=0表示一直阻塞直到数据出现

    参数:
        src:string -源列表的键名
        dst:string -目的列表的键名
        timeout:int -源列表为空时, 阻塞的的时间, 单位是秒, None表示不阻塞, 0表示一直阻塞直到任一列表中出现数据, 

    返回:
        item:string - 移动的值, 如果阻塞指定时间以后没有数据, 返回None

    示例:
        #test:numbers1 = [1, 2], test:numbers2 = [3, 4],
        list_rpoplpush('test:numbers1','test:numbers2') # 返回2, test:numbers1 = ['1'], test:numbers2 = ['2', '3', '4']
        list_rpoplpush('test:numbers1','test:numbers2') # 返回1, test:numbers1 = [], test:numbers2 = ['1', '2', '3', '4']
        list_rpoplpush('test:numbers1', 'test:numbers2', 2) # 阻塞2s等待test:numbers1中的出现数据
        list_rpoplpush('test:numbers1', 'test:numbers2', 0) # 一直阻塞直到test:numbers1中的出现数据
    """
    r = get_redis()
    if timeout is not None:
        return r.brpoplpush(src, dst, timeout=timeout)
    return r.rpoplpush(src, dst)
