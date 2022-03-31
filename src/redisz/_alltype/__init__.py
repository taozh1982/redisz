from redis.commands import list_or_args

from .. import get_redis


def get_type(name):
    """
    描述:
        返回name对应的键值类型

    参数:
        name:string -要检测的键名

    返回:
        type:str -name键名对应的键值类型, 有如下类型string/list/hash/set/zset/none(不存在)

    示例:
        get_type('test:string')# string
        get_type('test:list')# list
        get_type('test:hash')# hash
        get_type('test:set')# set
        get_type('test:zset')# zset
        get_type('test:not-exist-list')# none
    """
    return get_redis().type(name)


def exists(names, *args, return_number=False):
    """
    描述:
        返回由names指定的一个或多个名字键值对是否存在, 如果都存在返回True, 有一个不存在则返回False

    参数:
        names -要检测的键名或键名list
        *args -通过位置参数传递的多个检测键名
        return_number:bool -return_number=True, 返回指定names中存在的数量

    返回:
        is_exist:bool/int -如果都存在返回True, 有一个不存在则返回False, 如果return_number=True, 则返回指定names中存在的数量

    示例:
        exists('test:name') # 单个检测
        exists('test:name', 'test:age') # 以关键字参数的方式检测多个
        exists(['test:name', 'test:age']) # 以列表的方式检测多个
        exists(['test:name', 'test:age'], return_number) # 返回存在的个数
    """
    names = list_or_args(names, args)
    result = get_redis().exists(*names)
    if return_number is True:
        return result

    count = len(names)
    if result == count:
        result = True
    else:
        result = False
    return result


def keys(pattern="*", **kwargs):
    """
    描述:
        获取redis中所有的键名称列表, 可以根据pattern进行过滤

    返回:
        names:list -所有键名的列表

    参数:
        pattern:str -过滤选项, 有如下可选项
            h?llo -matches hello, hallo and hxllo
            h*llo -matches hllo and heeeello
            h[ae]llo -matches hello and hallo, but not hillo
            h[^e]llo -matches hallo, hbllo, ... but not hello
            h[a-b]llo -matches hallo and hbllo

    示例:
        keys() # 返回所有键名列表
        keys('test:*') #  返回以test:开头的键名列表
    """
    return get_redis().keys(pattern=pattern, **kwargs)


def delete(names, *args):
    """
    描述:
        删除一个或多个name对应的键值, 并返回删除成功的数量

    参数:
        names:str|list -要删除的键值

    返回:
        count:int -删除成功的数量

    示例:
        delete('test:n1') # 单个删除
        delete('test:n1', 'test:n2') # 关键字参数多个删除
        delete(['test:n1', 'test:n2']) # 列表多个删除
        delete(['test:n1', 'test:n2'], 'test:n3') # 列表和关键字参数一起使用
    """
    names = list_or_args(names, args)
    if len(names) == 0:
        return 0
    return get_redis().delete(*names)


def rename(src, dst, nx=False):
    """
    描述:
        将src重命名为dst, 将dst_nx设为True可以确保在dst键值不存在时才进行重命名, 默认直接重命名

    参数:
        src:str -要重命名的键名, 如果不存在则会引发异常
        dst:str -新的键名
        nx:bool -nx设置True, 只有dst键名不存在时才会重命名

    返回:
        result:bool -如果操作成功返回True, 否则返回False

    示例:
        rename('old_name', 'new_name') #
        rename('old_name', 'new_name', nx=True) # 只有当new_name不存在时, 才会进行重命名操作
    """
    if nx is True:
        return get_redis().renamenx(src, dst)
    return get_redis().rename(src, dst)


def ttl(name):
    """
    描述:
        以秒为单位，返回指定键值的剩余存在时间(time to live)

    参数:
        name:string -redis的健名

    返回:
        time:int -指定键值的剩余存在时间(秒)
                    --如果指定键值不存在, 返回-2
                    --如果指定键值存在, 但没有过期时间, 返回-1

    示例:
        ttl('test:list') # 90, 剩余90秒
        ttl('test:set') # -1, test:set存在，但是没有过期时间
        ttl('test:not-exist') # -2, test:not-exist键值不存在
    """
    return get_redis().ttl(name)


def expire(name, time):
    """
    描述:
        为键值设置超时时间, 超时以后自动删除对应的键值对,
        请注意超时时间只能对整个键值进行设置, 不能对于键值中的子项进行设置

    参数:
        name:string -要设置超时的键名
        time:int -超时时间, 单位是秒

    返回:
        result:bool -如果设置成功返回True, 否则返回False(键值不存在)

    示例:
        expire('test:ex', 10) # 10秒以后移除test:ex键值
        expire('test:nx', 10) # 如果test:nx不存在，返回False
    """
    return get_redis().expire(name, time)


def expireat(name, when):
    """
    描述:
        为键值设置超时时间点, 超时以后自动删除对应的键值对,
        请注意超时时间只能对整个键值进行设置, 不能对于键值中的子项进行设置

    参数:
        name:string -要设置超时的键名
        when:int -超时时间点(unix timestamp)

    返回:
        result:bool -如果设置成功返回True, 否则返回False(键值不存在)

    示例:
        expireat('test:ex', 1648252800000) # 1648252800000=2022年3月26日0点
    """
    return get_redis().expireat(name, when)


def persist(name):
    """
    描述:
        移除指定键值的过期时间

    参数:
        name:string -redis的健名

    返回:
        result:bool -如果移除成功返回True, 如果失败返回False(键值不存在)

    示例:
        persist('test:list') # True
        persist('test:not-exist') # false
    """
    return get_redis().persist(name)


def sort(name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, groups=False):
    """
    描述:
        对列表、集合以及有序集合中的元素进行排序, 默认按数字从小到大排序, 类似于sql中的order by语句,
        可以实现如下功能
            -根据降序而不是默认的升序来排列元素
            -将元素看作是数字进行排序
            -将元素看作是二进制字符串进行排序
            -使用被排序元素之外的其他值作为权重来进行排序
            -可以从输入的列表、集合、有序集合以外的其他地方进行取值

    参数:
        name:string -redis的健名
        start:int -对已排序的数据进行分页过滤，和num结合使用
        num:int -对已排序的数据进行分页过滤，和start结合使用
        by: -使用其他外部键对项目进行加权和排序, 使用“*”指示项目值在键中的位置
        get: -从外部键返回项目，而不是排序数据本身, 使用“*”指示项目值在键中的位置
        desc:bool -设置desc=True, 按从大到小排序
        alpha:bool -按字符排序而不是按数字排序
        store:string -按排序结果存储到指定的键值
        groups:bool -如果groups=True,并且get函数返回至少两个元素，排序将返回一个元组列表，每个元组包含从参数中获取的值“get”。

    返回:
        sorted:list -排序成功的元素列表

    示例:
        # test:sort=[6, 88, 112, 18, 36]
        # test:sort-weight={'d-6': 1, 'd-88': 2, 'd-112': 3, 'd-18': 4, 'd-36': 5, }
        sort('test:sort') # ['6', '18', '36', '88', '112'], 默认按数字进行升序排列
        sort('test:sort', desc=True) # ['112', '88', '36', '18', '6'], 降序排列
        sort('test:sort', alpha=True) # ['112', '18', '36', '6', '88'], 按字母排序
        sort('test:sort', start=1, num=3) # ['18', '36', '88'], 截取从第一个开始的三个元素
        sort('test:sort', store='test:sort-1') # test:sort-1=['6', '18', '36', '88', '112']

        # test:obj-ids=[1, 2, 3]
        # test:obj-1={'name': 'a', 'weight': 3}
        # test:obj-2={'name': 'b', 'weight': 2}
        # test:obj-3={'name': 'c', 'weight': 1}
        sort('test:obj-ids', by='test:obj-*->weight') # ['3', '2', '1'], 根据id找到其对应的对象中的属性(weight)，然后通过对象属性进行排序
        sort('test:obj-ids', by='test:obj-*->weight', get='test:obj-*->name') # ['c', 'b', 'a'], 根据id找到对象的属性进行排序，然后返回对象的name属性
        sort('test:obj-ids', by='nosort', get='test:obj-*->name') # ['c', 'b', 'a'], 根据id找到对象的属性进行排序，然后返回对象的name属性
        sort('test:obj-ids', get='test:obj-*->name') # ['a', 'b', 'c'], 特殊应用, 不排序,只根据id返回对象的属性
    """
    return get_redis().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=alpha, store=store, groups=groups)


