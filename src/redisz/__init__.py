import math
import threading
import time
import uuid
import warnings
from contextlib import contextmanager

import redis
from redis.lock import Lock

from .utils import gen_lock_name, subscribe, get_list_args, bytes_to_str, create_cluster_nodes, create_sentinel_nodes


class Redisz:
    # ------------------------------------------ sys ------------------------------------------
    def __init__(self, url=None, **kwargs):
        """
        描述:
            初始化redis, url可以指定redis的账号,密码,地址,端口,数据库等信息, 也可以通过关键字参数指定其他连接属性.
            如果设置cluster=True，则初始化Cluster Redis对象，通过startup_nodes参数指定集群节点列表.

        参数:
            url:str -redis地址url,格式如下
                        -redis://[[username]:[password]]@localhost:6379/0
                        -rediss://[[username]:[password]]@localhost:6379/0
                        -unix://[[username]:[password]]@/path/to/socket.sock?db=0

        示例:
            # 单节点
            rdz = redisz.Redisz('redis://127.0.0.1', decode_responses=True)
            rdz = redisz.Redisz('redis://127.0.0.1:6379')
            rdz = redisz.Redisz('redis://127.0.0.1:6379/10')

            # 集群
            rdz = redisz.Redisz(cluster=True,
                            startup_nodes=[{'host': '10.20.30.40', 'port': 6379}, {'host': '10.20.30.40', 'port': 6379}, {'host': '10.20.30.40', 'port': 6379}])

            # 哨兵
            rdz = redisz.Redisz(sentinel=True,
                                sentinels=[{'host': '10.20.30.40', 'port': 26379}, {'host': '10.20.30.50', 'port': 26379}],
                                socket_connect_timeout=1)
        """

        if 'decode_responses' not in kwargs:
            kwargs['decode_responses'] = True
        if url and not url.lower().startswith(('redis://', 'rediss://', 'unix://')):
            url = 'redis://' + url

        ha_mode = None
        if kwargs.get('sentinel') is True:
            ha_mode = 'sentinel'
        elif kwargs.get('cluster') is True:
            ha_mode = 'cluster'
        self._ha_mode = ha_mode

        if ha_mode == 'sentinel':  # @2023-08-29: add
            kwargs.pop('sentinel', None)
            self.sentinel_service_name = kwargs.pop('sentinel_service_name', 'mymaster')
            sentinels = create_sentinel_nodes(kwargs.pop('sentinels', []))
            self.redis_sentinel = redis.Sentinel(sentinels=sentinels, **kwargs)
        elif ha_mode == 'cluster':
            kwargs['startup_nodes'] = create_cluster_nodes(kwargs.get('startup_nodes', []))
            self.redis_ins = redis.RedisCluster(url=url, **kwargs)
        else:
            self.redis_ins = redis.Redis(connection_pool=redis.ConnectionPool.from_url(url, **kwargs))

    def get_ha_mode(self):
        """
        描述:
            返回HA部署模式.

        版本:
            0.6 -添加

        返回:
            ha_type:str - HA类型, 哨兵模式:'sentinel', 集群模式:'cluster'

        示例:
            rdz = redisz.Redisz(sentinel=True, sentinels=[{'host': '10.20.30.40', 'port': 26379}, {'host': '10.20.30.50', 'port': 26379}])
            rdz.get_ha_mode()   # 'sentinel'
            rdz = redisz.Redisz(cluster=True, startup_nodes=[{'name': 'node1', 'host': '10.20.30.40', 'port': 6379}, {'name': 'node2', 'host': '10.20.30.50', 'port': 6379}]
            rdz.get_ha_mode()   # 'cluster'
        :return:
        """
        return self._ha_mode

    def get_redis(self):
        """
        描述:
            返回redis.Redis对象, 使用redis.Redis方法操作redis.

        返回:
            redis - redis.Redis对象

        示例:
            rdz = rdz.get_redis()
            rdz.set('test:name', 'Zhang Tao')
        """
        if self.get_ha_mode() == 'sentinel':  # @2023-08-29: add
            return self.redis_sentinel.master_for(self.sentinel_service_name)
        return self.redis_ins

    def close(self):
        """
        描述:
            关闭redis连接.

        示例:
            rdz.close()
        """
        if self.redis_ins:
            self.redis_ins.close()

    def get_redis_pipeline(self, transaction=True):
        """
        描述:
            返回Pipeline流水线对象, 通过Pipeline可以实现事务和减少交互次数

        参数:
            transaction -是否是事务性流水线, 如果只需要流水线, 不需要事务, 可以设置transaction=False

        返回:
            pipeline:Pipeline -流水线对象

        示例:
            pipe = rdz.get_redis_pipeline()
            pipe.set('test:name', 'Zhang Tao')
            pipe.hset('test:taozh', 'name', 'Zhang Tao')
            pipe.sadd('test:letters', 'a', 'b', 'c')
            pipe.execute()  # 虽然多次操作, 但是客户端只会提交一次

        """
        return self.get_redis().pipeline(transaction)

    @contextmanager
    def redis_pipeline(self, transaction=True):
        """
        描述:
            Redis pipeline对象上下文管理器, 通过pipeline可以事物/非事物【流水线】的方式对redis进行操作
            通过流水线可以减少客户端和服务器端的交互次数(一次提交)
            如果是事务性流水线, 当多个客户端同时处理数据时, 可以保证当前调用不会被其他客户端打扰

        参数:
            transaction:bool -是否是事务性流水线, 如果只需要流水线, 不需要事务, 可以设置transaction=False

        返回:
            pipe:Pipeline -Pipeline流水线对象

        示例:
            with rdz.redis_pipeline(False) as pipe:
                pipe.set('test:name', 'Zhang Tao') #虽然多次操作, 但是客户端只会提交一次
                pipe.hset('test:taozh', 'name', 'Zhang Tao')
                pipe.sadd('test:letters', 'a', 'b', 'c')

        """
        pipe = self.get_redis().pipeline(transaction)
        try:
            if transaction is True:
                pipe.multi()

            yield pipe
            pipe.execute()
        except Exception as e:
            raise e

    # ------------------------------------------ global ------------------------------------------
    def get_type(self, name):
        """
        描述:
            返回name对应的键值类型

        参数:
            name:string -要检测的键名

        返回:
            type:str -name键名对应的键值类型, 有如下类型string/list/hash/set/zset/none(不存在)

        示例:
            rdz.get_type('test:string')         # string
            rdz.get_type('test:list')           # list
            rdz.get_type('test:hash')           # hash
            rdz.get_type('test:set')            # set
            rdz.get_type('test:zset')           # zset
            rdz.get_type('test:not-exist')      # none
        """
        return self.get_redis().type(name)

    def exists(self, names, *args, return_number=False):
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
            rdz.exists('test:name')                 # 单个检测
            rdz.exists('test:name', 'test:age')     # 以关键字参数的方式检测多个
            rdz.exists(['test:name', 'test:age'])   # 以列表的方式检测多个
            rdz.exists(['test:name', 'test:age'], return_number) # 返回存在的个数
        """
        names = get_list_args(names, args)
        result = self.get_redis().exists(*names)
        if return_number is True:
            return result

        count = len(names)
        if result == count:
            result = True
        else:
            result = False
        return result

    def keys(self, pattern='*', **kwargs):
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
            rdz.keys()          # 返回所有键名列表
            rdz.keys('test:*')  #  返回以test:开头的键名列表
        """
        if self.get_ha_mode() == 'cluster':
            keys = []
            for k in self.get_redis().scan_iter(match=pattern, count=1000):
                keys.append(k)
            return keys
        return self.get_redis().keys(pattern=pattern, **kwargs)

    def delete(self, names, *args):
        """
        描述:
            删除一个或多个name对应的键值, 并返回删除成功的数量

        参数:
            names:str|list -要删除的键值

        返回:
            count:int -删除成功的数量

        示例:
            rdz.delete('test:n1')               # 单个删除
            rdz.delete('test:n1', 'test:n2')    # 关键字参数多个删除
            rdz.delete(['test:n1', 'test:n2'])  # 列表多个删除
            rdz.delete(['test:n1', 'test:n2'], 'test:n3') # 列表和关键字参数一起使用
        """
        names = get_list_args(names, args)
        if len(names) == 0:
            return 0
        return self.get_redis().delete(*names)

    def rename(self, src, dst, nx=False):
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
            rdz.rename('old_name', 'new_name')
            rdz.rename('old_name', 'new_name', nx=True) # 只有当new_name不存在时, 才会进行重命名操作
        """
        if nx is True:
            return self.get_redis().renamenx(src, dst)
        return self.get_redis().rename(src, dst)

    def ttl(self, name):
        """
        描述:
            以秒为单位, 返回指定键值的剩余存在时间(time to live)

        参数:
            name:string -redis的健名

        返回:
            time:int -指定键值的剩余存在时间(秒)
                      --如果指定键值不存在, 返回-2
                      --如果指定键值存在, 但没有过期时间, 返回-1

        示例:
            rdz.ttl('test:list')        # 90, 剩余90秒
            rdz.ttl('test:set')         # -1, test:set存在, 但是没有过期时间
            rdz.ttl('test:not-exist')   # -2, test:not-exist键值不存在
        """
        return self.get_redis().ttl(name)

    def expire(self, name, seconds):
        """
        描述:
            为键值设置超时时间, 超时以后自动删除对应的键值对
            请注意超时时间只能对整个键值进行设置, 不能对于键值中的子项进行设置

        参数:
            name:string -要设置超时的键名
            seconds:int -超时时间, 单位是秒

        返回:
            result:bool -如果设置成功返回True, 否则返回False(键值不存在)

        示例:
            rdz.expire('test:exist', 10)        # 10秒以后移除test:ex键值
            rdz.expire('test:not-exist', 10)    # not-exist不存在, 返回False
        """
        return self.get_redis().expire(name, seconds)

    def expireat(self, name, when):
        """
        描述:
            为键值设置超时时间点, 超时以后自动删除对应的键值对
            请注意:
            -如果设置的是当前时间之前的时间点, 则键值会被【立刻】删除
            -超时时间只能对整个键值进行设置, 不能对于键值中的子项进行设置

        参数:
            name:string -要设置超时的键名
            when:int -超时时间点(unix timestamp)

        返回:
            result:bool -如果设置成功返回True, 否则返回False(键值不存在)

        示例:
            rdz.expireat('test:ex', 1648252800) # 1648252800=2022年3月26日0点
        """
        return self.get_redis().expireat(name, when)

    def persist(self, name):
        """
        描述:
            移除指定键值的过期时间

        参数:
            name:string -redis的健名

        返回:
            result:bool -如果移除成功返回True, 如果失败返回False(键值不存在)

        示例:
            rdz.persist('test:list')        # True
            rdz.persist('test:not-exist')   # false
        """
        return self.get_redis().persist(name)

    def sort(self, name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, groups=False):
        """
        描述:
            对列表、集合或有序集合中的元素进行排序, 默认按数字从小到大排序, 类似于sql中的order by语句,
            可以实现如下功能
                -根据降序而不是默认的升序来排列元素
                -将元素看作是数字进行排序
                -将元素看作是二进制字符串进行排序
                -使用被排序元素之外的其他值作为权重来进行排序
                -可以从输入的列表、集合、有序集合以外的其他地方进行取值

        参数:
            name:string -redis的健名
            start:int -对【已排序】的数据进行分页过滤, 和num结合使用
            num:int -对【已排序】的数据进行分页过滤, 和start结合使用
            by: -使用其他外部键对项目进行加权和排序, 使用“*”指示项目值在键中的位置, 设置by=nosort禁止排序
            get: -从外部键返回项目, 而不是排序数据本身, 使用“*”指示项目值在键中的位置
            desc:bool -设置desc=True, 按从大到小排序
            alpha:bool -按字符排序而不是按数字排序, 如果要排序的值不是数字, 请设置alpha=True, 否则会引发异常
            store:string -按排序结果存储到指定的键值
            groups:bool -如果groups=True,并且get函数返回至少两个元素, 排序将返回一个元组列表, 每个元组包含从参数中获取的值“get”。

        返回:
            sorted:list|int -排序成功的元素列表, 如果设置了store, 则返回元素个数

        示例:
            # test:sort=[6, 88, 112, 18, 36]
            # test:sort-weight={'d-6': 1, 'd-88': 2, 'd-112': 3, 'd-18': 4, 'd-36': 5, }
            rdz.sort('test:sort')                       # ['6', '18', '36', '88', '112'], 默认按数字进行升序排列
            rdz.sort('test:sort', desc=True)            # ['112', '88', '36', '18', '6'], 降序排列
            rdz.sort('test:sort', alpha=True)           # ['112', '18', '36', '6', '88'], 按字母排序
            rdz.sort('test:sort', start=1, num=3)       # ['18', '36', '88'], 截取从第一个开始的三个元素
            rdz.sort('test:sort', store='test:sort-1')  # 返回5, test:sort-1=['6', '18', '36', '88', '112']

            # test:obj-ids=[1, 3, 2]
            # test:obj-1={'name': '1a', 'weight': 33}
            # test:obj-2={'name': '2b', 'weight': 22}
            # test:obj-3={'name': '3c', 'weight': 11}
            rdz.sort('test:obj-ids', by='test:obj-*->weight')   # ['3', '2', '1'], 根据id找到其对应的对象中的属性(weight), 然后通过对象属性进行排序
            rdz.sort('test:obj-ids', by='test:obj-*->weight', get='test:obj-*->name')   # ['3c', '2b', '1a'], 根据id找到对象的属性进行排序, 然后返回对象的name属性
            rdz.sort('test:obj-ids', get='test:obj-*->name')    # ['1a', '2b', '3c'], 对test:obj-ids进行排序以后([1,2,3]), 然后根据排序以后的id依次从对象中获取指定的属性并返回
            rdz.sort('test:obj-ids', by='nosort', get='test:obj-*->name')   # ['1a', '3c', '2b'], 特殊应用, 不排序,只根据id返回对象的属性并返回
        """
        return self.get_redis().sort(name, start=start, num=num, by=by, get=get, desc=desc, alpha=alpha, store=store, groups=groups)

    # ------------------------------------------ str ------------------------------------------
    def str_set(self, name, value, **kwargs):
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
            rdz.str_set('test:name', 'Zhang Tao')
            rdz.str_set('test:age', 18)
            rdz.str_set('test:email', 'taozh@cisco.com')
        """
        result = self.get_redis().set(name, value, **kwargs)
        if result is None:
            return False
        return result

    def str_get(self, name):
        """
        描述:
            返回name对应的字符串类型键值, 如果键值不存在则返回None

        参数:
            name:string -redis中的键名

        返回:
            value:string -字符串键值, 如果不存在返回None

        示例:
            #test:name='Zhang Tao'
            rdz.str_get('test:name') # Zhang Tao
            rdz.str_get('test:not-exist') # None
        """
        result = self.get_redis().get(name)
        return bytes_to_str(result)

    def str_mset(self, mapping):
        """
        描述:
            批量设置多个字符串类型的键值

        参数:
            mapping:dict -包含多个键值的字典

        返回:
            result:bool -设置成功返回True

        示例:
            rdz.str_mset({'test:name': 'Zhang Tao', 'test:age': '18', 'test:email': 'taozh@cisco.com'})

        """
        return self.get_redis().mset(mapping)

    def str_mget(self, names, *args):
        """
        描述:
            批量获取字符串类型键值list,

        参数:
            names:list -要获取的键名列表

        返回:
            values:list -获取到的键值列表, 如果只有一个name, 返回结果也是list

        示例:
            # test:name = 'Zhang Tao', test:ag= '18', test:email= 'taozh@cisco.com'}
            rdz.str_mget('test:name') # ['Zhang Tao']
            rdz.str_mget('test:name', 'test:age') # ['Zhang Tao', '18']
            rdz.str_mget(['test:name', 'test:age'], 'test:email') # ['Zhang Tao', '18', 'taozh@cisco.com']
            rdz.str_mget('test:name', 'test:not-exist') # ['Zhang Tao', None]
        """
        # result = self.get_redis().mget(names, *args)
        # [bytes_to_str(item) for item in self.get_redis().mget(names, *args)]
        return [bytes_to_str(item) for item in self.get_redis().mget(names, *args)]

    def str_append(self, name, value):
        """
        描述:
            在name对应的字符串类型键值后面追加内容, name对应的字符串类型键值不存在则创建并赋值

        参数:
            name:string -redis的键名
            value:string/int/float -要追加的内容

        返回:
            length:int -添加成功的字符串【字节】长度(一个汉字三个字节)

        示例:
            # 'test:email'='taozh@cisco.com' --15
            rdz.str_append('test:email', None) # 15
            rdz.str_append('test:email', '.cn') # 18, test:email-> taozh@cisco.com.cn
            rdz.str_append('test:not-exist', '.cn') # 3, test:not-exist-> .cn
        """
        if value is None:
            return self.str_len(name)
        return self.get_redis().append(name, value)

    def str_getset(self, name, value):
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
            rdz.str_getset('test:age', 19) # 返回18, test:age -> 19
            rdz.str_getset('test:not-exist', 'new value') # 返回None, test:not-exist -> new value
        """
        result = self.get_redis().getset(name, value)
        return bytes_to_str(result)

    def str_setrange(self, name, offset, value):
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
            rdz.str_setrange('test:email', 6, '1982@gmail.com')  # 20, test:email -> taozh1982@cisco.com

            # test:study=好好学习
            rdz.str_setrange('test:study', 6, '工作')  # 12, test:study -> 好好工作, 一个汉字3个字节, 所以从6开始
        """
        return self.get_redis().setrange(name, offset, value)

    def str_getrange(self, name, start, end):
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
            rdz.str_getrange('test:email', 0, 4) # taozh, 索引0-4的5个字节
            rdz.str_getrange('test:email', -3, -1) # com, 索引-2 - -1的2个字节
            # test:study=好好学习
            rdz.str_getrange('test:study', 0, 2) # 好, 索引0-2的3个字节, 一个汉字3个字节
        """
        result = self.get_redis().getrange(name, start, end)
        return bytes_to_str(result)

    def str_len(self, name):
        """
        描述:
            返回name对应值的字节长度（一个汉字3个字节, 如果键值不存在, 返回0

        参数:
            name:str -redis的键名

        返回:
            length:int -键值的字节长度, 如果不存则, 则返回0

        示例:
            # test:email=taozh@cisco.com
            rdz.str_len('test:email') # 15
            # test:zh=好好学习
            rdz.str_len('test:zh') # 12, 3*4=12个字节
            rdz.str_len('test:not-exist') # 0
        """
        return self.get_redis().strlen(name)

    def str_incr(self, name, amount=1):
        """
        描述:
            自增name对应的键值, 返回结果为自增以后的值
            当name不存在时, 则创建键值并赋值为amount
            amount必须是【整数】, 可以为负数, 负数表示自减
            如果name对应的键值不是整数(包括浮点数), 会引发异常

        参数:
            name:string -redis的键名
            amount:int -增加的数值

        返回:
            value:int -自增以后的值

        示例:
            # test:count=18
            rdz.str_incr('test:count')      # 19
            rdz.str_incr('test:count', 2)   # 21
            rdz.str_incr('test:count', -1)  # 20
            rdz.str_incr('test:not-exist')  # 1, test:not-exist不存在, 创建test:not-exist, 并赋值为1

            rdz.str_incr('test:email')      # test:email不是整数, 引发ResponseError异常
            rdz.str_incr('test:float-1.1')  # test:float-1.1不是整数, 引发ResponseError异常
        """
        return self.get_redis().incrby(name, amount=amount)

    def str_decr(self, name, amount=1):
        """
        描述:
            自减name对应的值, 返回结果为自减以后的值
            当name不存在时, 则创建键值并赋值为-amount(-1)
            amount必须是【整数】, 可以为负数, 负数表示自增
            如果name对应的键值不是整数(包括浮点数), 会引发异常

        参数:
            name:string -redis的键名
            amount:int -减去的数值

        返回:
            value:int -自减以后的值

        示例:
            # test:count=10
            rdz.str_decr('test:count')      # 9
            rdz.str_decr('test:count', 2)   # 7
            rdz.str_decr('test:count', -1)  # 8
            rdz.str_decr('test:not-exist')  # -1, test:not-exist不存在, 创建test:not-exist, 并赋值-1

            rdz.str_decr('test:email')      # test:email不是整数, 引发异常
            rdz.str_decr('test:float-1.1')  # test:float-1.1不是整数, 引发异常
        """
        return self.get_redis().decrby(name, amount=amount)

    def str_incrfloat(self, name, amount=1.0):
        """
        浮点类型的自增操作, 请参考str_incr
        """
        return self.get_redis().incrbyfloat(name, amount=amount)

    def str_decrfloat(self, name, amount=1.0):
        """
         浮点类型的自减操作, 请参考str_decr
        """
        return self.get_redis().incrbyfloat(name, amount=-amount)

    # ------------------------------------------ list ------------------------------------------
    def list_push(self, name, values, *args, left=False, xx=False):
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
            rdz.list_push('test:numbers', 3, 4)     # 2, test:numbers -> ['3', '4']
            rdz.list_push('test:numbers', [2, 1], left=True)    # 4, test:numbers -> ['1', '2', '3', '4'], 请注意1和2的顺序
            rdz.list_push('test:numbers', [5, 6], 7)    # 7, test:numbers -> ['1', '2', '3', '4', '5', '6', '7']
    
            rdz.list_push('test:not-exist', 1, xx=True)     # 0, test:not-exist不存在, 因为xx为True, 所以不会新建list
        """
        values = get_list_args(values, args)
        r = self.get_redis()
        if left is True:
            if xx is True:
                return r.lpushx(name, *values)
            return r.lpush(name, *values)
        else:
            if xx is True:
                return r.rpushx(name, *values)
            return r.rpush(name, *values)

    def list_insert(self, name, ref_value, value, before=False):
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
                -如果ref_value在列表中不存在, 返回-1
    
        示例:
            # test:numbers = ['1', '3', '5']
            rdz.list_insert('test:numbers', 1, 2)       # 把2插入到1后边, test:numbers -> ['1', '2', '3', '5']
            rdz.list_insert('test:numbers', 5, 4, before=True)     # 把4插入到5前, test:numbers -> ['1', '2', '3', '4', '5']
            rdz.list_insert('test:numbers', 10, 11)     # 返回-1, 不做处理
            rdz.list_insert('test:not-exist', 1, 2)     # 返回0, 不做处理
        """
        where = 'after'
        if before is True:
            where = 'before'
        return self.get_redis().linsert(name, where, ref_value, value)

    def list_set(self, name, index, value):
        """
        描述:
            对指定列表中的指定索引位置重新赋值
            如果列表不存在/index超出范围/value不是字符串, 都会引发异常
    
        参数:
            name:string -redis的键名
            index:int -重新赋值的索引, 支持负数(-1表示最后一个元素索引), 必须是列表范围内的索引, 否则会引发异常
            value:string|int|float -新值, 除string|int|float以外的类型, 都会引发异常
    
        返回:
            result:bool -如果赋值成功返回True
    
        示例:
            # test:numbers = ['1', 'b', 'c']
            rdz.list_set('test:numbers', 1, 2)  # 把第一个元素('b')替换成2, test:numbers -> ['1', '2', 'c']
            rdz.list_set('test:numbers', -1, 3) # 把最后一个元素('c')替换成3, test:numbers -> ['1', '2', '3']
        """
        return self.get_redis().lset(name, index, value)

    def list_pop(self, name, count=None, left=False):
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
            item:list - 移除的元素或元素组成的列表
    
        示例:
            # test:numbers = ['1', '2', '3', '4', '5', '6']
            rdz.list_pop('test:numbers', 0)     # 返回[]
            rdz.list_pop('test:numbers')        # 移除最右侧的元素, 返回结果为6, test:numbers -> ['1', '2', '3', '4', '5'])
            rdz.list_pop('test:numbers',2)      # 移除最右侧的2个元素, 返回结果为['5', '4'], test:numbers -> ['1', '2', '3']
            rdz.list_pop('test:numbers',left=True)  # 返回结果为1, test:numbers -> ['2', '3']
            rdz.list_pop('test:numbers',3)      # 返回结果为['3', '2'], test:numbers -> []
            rdz.list_pop('test:not-exist')      # 返回[]
        """
        r = self.get_redis()

        if left is True:
            result = r.lpop(name, count=count)
        else:
            result = r.rpop(name, count=count)
        if result is None:
            return []
        return result

    def list_rem(self, name, value, count=1):
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
            rdz.list_rem('test:numbers', 1)         # 1, 从左向右删除第一个1 -> ['2', '3', '4', '5', '6', '5', '4', '3', '2', '1']
            rdz.list_rem('test:numbers', 2, -1)     # 1, 从后向前删除第一个2 -> ['2', '3', '4', '5', '6', '5', '4', '3', '1']
            rdz.list_rem('test:numbers', 4, 0)      # 2, 删除所有的 -> ['2', '3', '5', '6', '5', '3', '1']
    
            rdz.list_rem('test:numbers', 10)        # 值在列表中不存在, 返回0
            rdz.list_rem('test:not-exist', 10)      # 列表不存在, 返回0
        """
        return self.get_redis().lrem(name, count, value)

    def list_trim(self, name, start, end):
        """
        描述:
            在指定列表中移除【没有】在start-end索引之间的值, start和end索引处的值不会被移除
            只保留start<=索引<=end的值, 如果start>end或者start<0, list所有的值都会被移除
    
        参数:
            name:string -redis的键名
            start:int -开始索引, start索引处的值不会被移除
            end:int -结束索引, end索引处的值不会被移除
    
        返回:
            result:bool -True
    
        示例:
            # test:numbers = ['1', '2', '3', '4', '5', '6']
            rdz.list_trim('test:numbers', 1, 3)     # 把索引在1-3以外的值移除, test:numbers -> ['2', '3', '4']
            rdz.list_trim('test:numbers', -1, 1)    # start<0, test:numbers -> []
            rdz.list_trim('test:numbers', 3, 1)     # start>end, test:numbers -> []
        """
        return self.get_redis().ltrim(name, start, end)

    def list_index(self, name, index):
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
            rdz.list_index('test:numbers', 1)       # 索引为1的值为:'2'
            rdz.list_index('test:numbers', -1)      # 索引为-1的值为:'6'
            rdz.list_index('test:numbers', 10)      # index超出了列表范围:None
            rdz.list_index('test:not-exist', 0)     # 列表不存:None
        """
        return self.get_redis().lindex(name, index)

    def list_len(self, name):
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
        return self.get_redis().llen(name)

    def list_range(self, name, start, end):
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
            rdz.list_range('test:numbers', 0, 2)        # 包含第0个和第2个数据, ['1', '2', '3']
            rdz.list_range('test:numbers', 0, -1)       # -1表示最后一个数据项的索引, ['1', '2', '3', '4', '5', '6']
            rdz.list_range('test:numbers', 0, 100)      # 只会返回范围内的数据, ['1', '2', '3', '4', '5', '6']
            rdz.list_range('test:not-exist', 0, -1)     # []
        """
        return self.get_redis().lrange(name, start, end)

    def list_iter(self, name):
        """
        描述:
            利用yield封装创建生成器, 对指定列表元素进行增量迭代, 数据量较大时比较有用, 避免将数据全部取出把内存撑爆
    
        参数:
        name:string -redis的键名
    
        示例:
            for item in rdz.list_iter('test:numbers'):  # 遍历列表
                print(item)
        """
        r = self.get_redis()
        count = r.llen(name)

        for index in range(count):
            yield r.lindex(name, index)

    def list_bpop(self, names, timeout=0, left=False):
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
            rdz.list_bpop(['test:numbers1', 'test:numbers2'])   # ('test:numbers1', '2')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'])   # ('test:numbers1', '1')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'])   # ('test:numbers2', '4')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'])   # ('test:numbers2', '3')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'], 2)    # 阻塞等待两秒, 如果数据没有出现, 则向下运行
    
            #从左向右依次移除
            rdz.list_bpop(['test:numbers1', 'test:numbers2'], left=True)    # ('test:numbers1', '1')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'], left=True)    # ('test:numbers1', '2')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'], left=True)    # ('test:numbers2', '3')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'], left=True)    # ('test:numbers2', '4')
            rdz.list_bpop(['test:numbers1', 'test:numbers2'])   # 一直阻塞等待数据出现
        """
        r = self.get_redis()
        if left is True:
            return r.blpop(names, timeout=timeout)
        else:
            return r.brpop(names, timeout=timeout)

    def list_rpoplpush(self, src, dst, timeout=None):
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
            rdz.list_rpoplpush('test:numbers1','test:numbers2')     # 返回2, test:numbers1 = ['1'], test:numbers2 = ['2', '3', '4']
            rdz.list_rpoplpush('test:numbers1','test:numbers2')     # 返回1, test:numbers1 = [], test:numbers2 = ['1', '2', '3', '4']
            rdz.list_rpoplpush('test:numbers1', 'test:numbers2', 2) # 阻塞2s等待test:numbers1中的出现数据
            rdz.list_rpoplpush('test:numbers1', 'test:numbers2', 0) # 一直阻塞直到test:numbers1中的出现数据
        """
        r = self.get_redis()
        if timeout is not None:
            return r.brpoplpush(src, dst, timeout=timeout)
        return r.rpoplpush(src, dst)

    # ------------------------------------------ hash ------------------------------------------
    def hash_set(self, name, key=None, value=None, mapping=None, nx=False):
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
            rdz.hash_set('test:taozh', 'name', 'Zhang Tao')     # 创建散列 -> {'name': 'Zhang Tao'}
            rdz.hash_set('test:taozh', mapping={'age': 18, 'email': 'taozh@cisco.com'})     # 一次设置多个键值 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_set('test:taozh', 'email', 'zht@cisco.com', nx=True)   # email已经存在, set操作无效 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_set('test:taozh', 'company', 'cisco', nx=True) # company不存在, set操作有效 -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com', 'company': 'cisco'}
        """
        r = self.get_redis()
        if nx is True:
            # if key is not None:
            return r.hsetnx(name, key, value)

        return r.hset(name, key=key, value=value, mapping=mapping)

    def hash_get(self, name, key):
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
            rdz.hash_get('test:taozh', 'email')     # taozh@cisco.com
            rdz.hash_get('test:taozh', 'city')      # None
        """
        return self.get_redis().hget(name, key)

    def hash_mset(self, name, mapping):
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
            rdz.hash_mset('test:taozh', {'age': 18, 'email': 'taozh@cisco.com'}) # test:taozh -> {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_mset('test:zht', {'name': 'Zhang Tao', 'age': '18'}) # 不存在则创建 test:zht={'name': 'Zhang Tao', 'age': '18'}
        """
        # return self.get_redis().hmset(name, mapping) # hmset deprecated
        return self.get_redis().hset(name, mapping=mapping)

    def hash_mget(self, name, keys, *args):
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
            rdz.hash_mget('test:taozh', 'name')         # ['Zhang Tao']
            rdz.hash_mget('test:taozh', 'name', 'age')  # ['Zhang Tao', '18']
            rdz.hash_mget('test:taozh', ['name', 'age']) # ['Zhang Tao', '18']
            rdz.hash_mget('test:taozh', ['name', 'age'], 'email') # ['Zhang Tao', '18', 'taozh@cisco.com']
            rdz.hash_mget('test:taozh', 'key-nx') # [None]
        """
        return self.get_redis().hmget(name, keys, *args)

    def hash_del(self, name, keys, *args):
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
            rdz.hash_del('test:kv', 'k1', 'k2')     # 返回2, -> {'k3': '3', 'k4': '4', 'k5': '5', 'k6': '6', 'k7': '7'}
            rdz.hash_del('test:kv', ['k3', 'k4'])   # 返回2, -> {'k5': '5', 'k6': '6', 'k7': '7'}
            rdz.hash_del('test:kv', ['k5','k-nx'])  # 返回1, 因为k-nx不存在, 只删除了k5, -> {'k6': '6', 'k7': '7'}
        """
        keys = get_list_args(keys, args)
        return self.get_redis().hdel(name, *keys)

    def hash_getall(self, name):
        """
        描述:
            获取指定散列的所有键值, 如果散列不存在则返回{}

        参数:
            name:string -redis的键名

        返回:
            map:dict -所有的键值dict


        示例:
            # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_getall('test:taozh')   # {'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_getall('test:hash-nx') # {}
        """
        return self.get_redis().hgetall(name)

    def hash_exists(self, name, key):
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
            rdz.hash_exists('test:taozh', 'name')       # True
            rdz.hash_exists('test:taozh', 'city')       # False, key不存在
            rdz.hash_exists('test:not-exist', 'name')   # False, 散列不存在
        """
        return self.get_redis().hexists(name, key)

    def hash_len(self, name):
        """
        描述:
            获取指定散列中键值对的个数, 如果散列不存在, 则返回0

        参数:
            name:string -redis的键名

        返回:
            length:int - 键值个数

        示例:
            # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_len('test:taozh')      # 3
            rdz.hash_len('test:not-exist')  # 0
        """
        return self.get_redis().hlen(name)

    def hash_keys(self, name):
        """
        描述:
            获取指定散列中所有的key的值列表, 如果散列不存在则返回[]

        参数:
            name:string -redis的键名

        返回:
            keys:list -散列中所有key的list, 如果散列不存在则返回[]

        示例:
            # test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_keys('test:taozh')     # ['name', 'age', 'email']
            rdz.hash_keys('test:not-exist') # []
        """
        return self.get_redis().hkeys(name)

    def hash_values(self, name):
        """
        描述:
            获取指定散列中所有value的list, 如果散列不存在, 则返回[]

        参数:
            name:string -redis的键名

        返回:
            values:list -散列中所有value的list, 如果散列不存在则返回[]

        示例:
            #test:taozh={'name': 'Zhang Tao', 'age': '18', 'email': 'taozh@cisco.com'}
            rdz.hash_values('test:taozh') # ['Zhang Tao', '18', 'taozh@cisco.com']
            rdz.hash_values('test:hash-nx') # []

        """
        return self.get_redis().hvals(name)

    def hash_incr(self, name, key, amount=1):
        """用法请参考str_incr"""
        return self.get_redis().hincrby(name, key, amount=amount)

    def hash_decr(self, name, key, amount=1):
        """用法请参考str_decr"""
        return self.get_redis().hincrby(name, key, amount=-amount)

    def hash_incrfloat(self, name, key, amount=1.0):
        """用法请参考str_incrfloat
        """
        return self.get_redis().hincrbyfloat(name, key, amount=amount)

    def hash_decrfloat(self, name, key, amount=1.0):
        """
        用法请参考str_decrfloat
        """
        return self.get_redis().hincrbyfloat(name, key, amount=-amount)

    def hash_scan(self, name, cursor=0, match=None, count=None):
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
                rdz.hash_set('test:xxx', i, i)

            cursor = 0
            count = 0
            while True:
                cursor, data = rdz.hash_scan('test:xxx', cursor=cursor,count=20) # data为包含分片数据的dict ->{'k188': 'v188', 'k938': 'v938',...}
                print(cursor, data)
                count+=1
                if cursor == 0:
                    break
            print(count) # 迭代了大约490次左右
        """
        return self.get_redis().hscan(name, cursor=cursor, match=match, count=count)

    def hash_scan_iter(self, name, match=None, count=None):
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
                rdz.hash_set('test:xxx', i, i)

            for item in rdz.hash_scan_iter('test:xxx'):
                print(item) # ('k368', 368.0)

        """
        return self.get_redis().hscan_iter(name, match=match, count=count)

    # ------------------------------------------ set ------------------------------------------
    def set_add(self, name, values, *args):
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
            rdz.set_add('test:letters', 'a', 'b', 'c')          # 3, 创建集合并赋值, test:letters={'a', 'b', 'c'}
            rdz.set_add('test:letters', ['b', 'c', 'd'])        # 1, 添加成功了'd', test:letters -> {'a', 'b', 'c', 'd'}
            rdz.set_add('test:letters', ['c', 'd'], 'e', 'f')   # 2, 添加成功了'e'+'f', test:letters -> {'a', 'b', 'c', 'd', 'e', 'f'}
        """
        values = get_list_args(values, args)
        return self.get_redis().sadd(name, *values)

    def set_rem(self, name, values, *args):
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
            rdz.set_rem('test:letters', 'a', 'b')       # 2, test:letters -> {c', 'd', 'e', 'f', 'g', 'h'}
            rdz.set_rem('test:letters', ['c', 'd'])     # 2, test:letters-> {'e', 'f', 'g', 'h'}
            rdz.set_rem('test:letters', ['e', 'f'], 'g', 'x') # 3, test:letters-> {'h'}, x不存在, 所以结果为3
        """
        values = get_list_args(values, args)
        return self.get_redis().srem(name, *values)

    def set_pop(self, name, count=None):
        """
        描述:
            从指定集合随机移除一个/多个元素, 并将其返回, 因为集合是无序的, 所以删除是【随机】的
            只要设置了count, 返回的肯定是列表, 根据情况, 可能返回的是空列表
            - 如果count=0, 返回一个空的列表[]
            - 如果count>0, 返回一个移除元素组成的列表, 如果集合不存在, 也会返回一个空的列表[]

        参数:
            name:string -redis的键名
            count:int -要移除的元素数, 取值范围是>=0的整数

        返回:
            item:string|list -移除的元素, 只要设置了count(>=0), 返回的都是包含移除元素的列表, count=0/1或集合不存在会返回[]

        示例:
            # test:letters = {'a', 'b', 'c', 'd', 'e'}
            rdz.set_pop('test:letters')     # 返回'e', test:letters -> {'a', 'd', 'b', 'c'}
            rdz.set_pop('test:letters', 2)  # 返回['a', 'b'], test:letters -> {'d', 'c'}
        """
        return self.get_redis().spop(name, count=count)

    def set_card(self, name):
        """
        描述
            获取指定集合的元素个数

        参数:
            name:string -redis的键名

        返回:
            count:int -元素个数, 如果集合不存在, 返回0

        示例:
            # test:letters = {'a', 'b', 'c', 'd', 'e'}
            rdz.set_card('test:letters')        # 5
            rdz.set_card('test:not-exist')      # 0
        """
        return self.get_redis().scard(name)

    def set_members(self, name):
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
            rdz.set_members('test:letters')     # {'a', 'b', 'c', 'd', 'e'}
            rdz.set_members('test:not-exist')   # set()
        """
        return self.get_redis().smembers(name)

    def set_ismember(self, name, value):
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
            rdz.set_ismember('test:letters', 'a')   # True
            rdz.set_ismember('test:letters', 'x')   # False
            rdz.set_ismember('test:not-exist', 'a') # False
        """
        return self.get_redis().sismember(name, value)

    def set_scan(self, name, cursor=0, match=None, count=None):
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
            rdz.set_add('test:xxx', *range(10000))

            cursor = 0
            count = 0
            while True:
                cursor, data = rdz.set_scan('test:xxx', cursor=cursor, count=20) # data为包含元素的list -> ['1787', '219', '101',...]
                print(cursor, data)
                count += 1
                if cursor == 0:
                    break
            print(count)  # 迭代了大约490次左右
        """
        return self.get_redis().sscan(name, cursor=cursor, match=match, count=count)

    def set_scan_iter(self, name, match=None, count=None):
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
            rdz.set_add('test:xxx', *range(10000))

            for i in rdz.set_scan_iter('test:xxx'):
                print(i) # 218
        """
        return self.get_redis().sscan_iter(name, match=match, count=count)

    def set_move(self, src, dst, value):
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
            rdz.set_move('test:letters1', 'test:letters2', 'a') # True, test:letters1={'b', 'c'}, test:letters2={'a', 'c', 'd', 'e'}
            rdz.set_move('test:letters1', 'test:letters2', 'c') # True, test:letters1={'b'}, test:letters2={'a', 'c', 'd', 'e'}
            rdz.set_move('test:letters1', 'test:letters2', 'f') # False, test:letters1={'b'}, test:letters2={'a', 'c', 'd', 'e'}
        """
        return self.get_redis().smove(src, dst, value)

    def set_diff(self, names, *args, dst=None):
        """
        描述:
            差集, 返回只存在于【第一个】集合, 但不在其余集合中的元素集合, 即存在第一个集合中, 但不存在其他集合中的元素的集合
            可以将差集结果存储到一个新的dst集合中, 请注意, 如果dst对应的键值在redis已经存在(不论类型), 都会被【替换】

        参数:
            names:list -多个比较的集合列表
            args: -以位置参数方式传递的多个集合列表

        返回:
            result:set -差值集合, 如果设置dst, 返回差集中的元素【数量】

        示例:
            # test:letters1={'a', 'b', 'c'}, test:letters2={'b', 'm', 'n'}, test:letters3={'c', 'x', 'y'}
            rdz.set_diff('test:letters1', 'test:letters2')      # {'c', 'a'}
            rdz.set_diff(['test:letters2', 'test:letters3'])    # {'b', 'm', 'n'}
            rdz.set_diff(['test:letters1', 'test:letters2', 'test:letters3']) # {'a'}

            rdz.set_diff('test:letters1', 'test:not-exist')     # {'a', 'b', 'c'}, 和不存在的set差集, 返回原集合中所有元素组成的集合
            rdz.set_diff('test:not-exist', 'test:letters1')     # set(), 不存在的集合和其他集合差集, 返回一空集合对象

            #test:diff=['a', 'x']
            rdz.set_diff(['test:letters1', 'test:letters2'], dst='test:diff') # 返回2, test:diff = {'a', 'c'}, 将diff结果存储到dst集合中, 无论原dst是什么类型
        """
        names = get_list_args(names, args)
        r = self.get_redis()
        if dst is not None:
            return r.sdiffstore(dst, names)
        return r.sdiff(names)

    def set_inter(self, names, *args, dst=None):
        """
        描述:
            交集, 返回多个集合中元素的交集, 即同时存在于多个指定集合中的元素集合
            可以将交集结果存储到一个新的dst集合中, 请注意, 如果dst对应的键值在redis已经存在(不论类型), 都会被替换
            默认返回交集集合, 如果设置dst, 返回交集中的元素数量

        参数:
            names:list -进行交集运算的集合列表
            arge: -以位置参数方式传递的多个集合列表

        返回:
            result:set -交集集合, 如果设置dst, 返回交集中的元素【数量】

        示例: 
            # test:letters1={'a', 'b', 'c'}, test:letters2={'b', 'c', 'd'}, test:letters3={'c', 'd', 'e'}
            rdz.set_inter(['test:letters1', 'test:letters2'])   # {'b', 'c'}
            rdz.set_inter(['test:letters2', 'test:letters3'])   # {'c', 'd'}
            rdz.set_inter(['test:letters1', 'test:letters2', 'test:letters3'])  # {'c'}

            rdz.set_inter('test:letters1', 'test:not-exist') # set(), 和不存在的集合交集, 返回一空集合对象

            #test:inter=['a', 'x']
            rdz.set_inter(['test:letters1', 'test:letters2'], dst='test:inter')     # 2, test:inter = {'b', 'c'}
        """
        r = self.get_redis()
        if dst is not None:
            return r.sinterstore(dst, names, *args)
        return r.sinter(names, *args)

    def set_union(self, names, *args, dst=None):
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
            rdz.set_union('test:letters1', 'test:letters2')   # {'a', 'b', 'c', 'd'}
            rdz.set_union(['test:letters2', 'test:letters3'])   # {'b', 'c', 'd', 'e'}
            rdz.set_union(['test:letters1', 'test:letters2', 'test:letters3'])  # {'a', 'b', 'c', 'd', 'e'}

            #test:union=['a', 'x']
            rdz.set_union(['test:letters1', 'test:letters2'],dst='test:union')  # 4, test:union = {'a', 'b', 'c', 'd'}
        """
        r = self.get_redis()
        if dst is not None:
            return r.sunionstore(dst, names, *args)
        return r.sunion(names, *args)

    # ------------------------------------------ zset ------------------------------------------
    def zset_add(self, name, mapping, **kwargs):
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
            rdz.zset_add('test:zset', {'a': 10, 'b': 20})   # 2, test:zset = {'a': 10, 'b': 20}
            rdz.zset_add('test:zset', {'b': 30, 'c': 40})   # 1, 替换b的值, test:zset = {'a': 10, 'b': 30, 'c': 40}
            rdz.zset_add('test:zset', {'c': 50, 'd': 60}, nx=True)  # 1, nx=True只添加不存在的值d, c已存在不做处理, test:zset = {'a': 10, 'b': 30, 'c': 40, 'd':60}
            rdz.zset_add('test:zset', {'d': 70, 'e': 80}, xx=True)  # 0, xx=True只替换已存在的值d, e不存在不做处理, test:zset = {'a': 10, 'b': 30, 'c': 40, 'd':70}

            rdz.zset_add('test:zset', {'x': 100, 'y': 200, 'z': 300})
            rdz.zset_add('test:zset', {'x': 110, 'y': 220, 'z': 300}, ch=True)  # 2, 只更新了x和y的值
        """
        return self.get_redis().zadd(name, mapping, **kwargs)

    def zset_rem(self, name, members, *args):
        """
        描述:
            删除指定有序集合中的一个/多个元素

        参数:
            name:string -redis的键名
            members:list -要删除元素的列表
            args -通过关键字参数传递的一个/多个要删除的元素

        返回:
            count:int -删除成功的个数, 如果有序集合不存在, 返回0

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30, 'x': 100}
            rdz.zset_rem('test:zset', 'a')  # 1, test:zset -> {'b': 20, 'c': 30, 'x': 100}
            rdz.zset_rem('test:zset', ['b', 'c','e'])   # 2, 只删除了b+c, test:zset->{'x': 100},
        """
        members = get_list_args(members, args)
        return self.get_redis().zrem(name, *members)

    def zset_card(self, name):
        """
        描述:
            获取指定有序集合元素的数量

        参数:
            name:string -redis的键名

        返回:
            count:int -指定有序集合中元素的数量, 如果zset不存在则返回0

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_card('test:zset')          # 3
            rdz.zset_card('test:not-exist')     # 0
        """
        return self.get_redis().zcard(name)

    def zset_count(self, name, min_score, max_score):
        """
        描述:
            获取指定有序集合中【分数】在 [min,max] 之间的个数
            如果有序集合不存在, 返回0

        参数:
            name:string -redis的键名
            min_score:int/float -最小的分数值, >=min
            max_score:int/float -最大的分数值, <=max

        返回:
            count:int -有序集合中【分数】在 [min,max] 之间的个数

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_count('test:zset', 20, 30)         # 2, 20=<score<=30
            rdz.zset_count('test:zset', 21, 30)         # 1, 21=<score<=30
            rdz.zset_count('test:not-exist', 1, 10)     # 0
        """
        return self.get_redis().zcount(name, min_score, max_score)

    def zset_score(self, name, member):
        """
        描述:
            获取指定有序集合中value元素的分数

        参数:
            name:string     -redis的键名
            member:string   -有序集合中的元素

        返回:
            score:float -value元素的分数, 如果有序集合不存在/元素不存在, 返回None

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_score('test:zset', 'a')        # 10.0
            rdz.zset_score('test:zset', 'x')        # None
            rdz.zset_score('test:not-exist', 'x')   # None
        """
        return self.get_redis().zscore(name, member)

    def zset_rank(self, name, member):
        """
        描述:
            获取指定的元素在有序集合中的索引(从0开始)

        参数:
            name:string -redis的键名
            member:str  -指定的元素

        返回:
            index:int -value元素在有序集合中的索引, 如果有序集合不存在/元素不存在返回None

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_rank('test:zset', 'a')         # 0
            rdz.zset_rank('test:zset', 'b')         # 1
            rdz.zset_rank('test:zset', 'x')         # None
            rdz.zset_rank('test:not-exist', 'x')    # None
        """
        return self.get_redis().zrank(name, member)

    def zset_incr(self, name, amount, member):
        """
        描述:
            增加指定有序集合中value元素的【分数】, 如果元素在有序集合中不存在, 则创建并赋值

        参数:
            name:string -redis的键名
            amount:int|float -要增加的【分数】, 如果<0表示要减掉的分数
            member:str -要增加分数的元素

        返回:
            score:float -增加以后的分数

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_incr('test:zset', 1, 'a')      # 11.0
            rdz.zset_incr('test:zset', 2.2, 'b')    # 22.2
            rdz.zset_incr('test:zset', -2, 'c')     # 28.0
            rdz.zset_incr('test:zset', 3, 'e')      # 3.0, test:zset -> [('e', 3), ('a', 11), ('b', 22.2), ('c', 28)]
        """
        return self.get_redis().zincrby(name, amount, member)

    def zset_decr(self, name, amount, member):
        """
        描述:
            减少指定有序集合中value元素的【分数】, 如果元素在有序集合中不存在, 则创建并赋值

        参数:
            name:string -redis的键名
            amount:int|float -要减少的【分数】, 如果>0表示要增加的分数
            member:str -要减少分数的元素

        返回:
            score:float -减少以后的分数

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_decr('test:zset', 1, 'a')      # 9.0
            rdz.zset_decr('test:zset', 2.2, 'b')    # 17.8
            rdz.zset_decr('test:zset', -2, 'c')     # 32.0
            rdz.zset_decr('test:zset', 3, 'e')      # -3.0, test:zset- > [('e', -3), ('a', 9), ('b', 17.8), ('c', 32)]
        """
        return self.get_redis().zincrby(name, -amount, member)

    def zset_range(self, name, start, end, desc=False, withscores=False, score_cast_func=float, byscore=False):
        """
        描述:
            按照索引范围获取指定有序集合的元素
            默认情况, 返回结果按score【从小到大】排列, 设置desc=True以从大到小排列
            默认情况, start和end表示【索引】范围, 支持负数, 最后一个元素的索引是-1, 设置byscore=True, start和end表示分数范围,
            无论start和end表示索引还是分数, 都会被【包含】在结果内(>= & <=)
            如果同时指定了desc=True和byscore=True, start的值要大于end的值, 否则会返回空

        参数:
            name -redis的键名
            start:int   -有序集合【索引】起始位置, 如果byscore=True, 则表示起始分数, start=<
            end:int     -有序集合【索引】结束位置, 如果byscore=True, 则表示结束分数, <=end
            desc:bool   -默认按照分数从小到大排序, 设置desc=True, 则按分数【从大到小】排序
            withscores:bool     -默认只获取元素, 设置withscores=True, 会把分数也一起返回
            score_cast_func:func    -对分数进行数据转换的函数, 默认是float, 函数只对返回的结果有影响, 【不影响排序】
            byscore:bool    -默认按索引进行查询, 设置byscore=True, 按分数查询, start和end作为分数范围使用

        返回:
            values:list -有序集合在start-end范围内的元素列表, 如果withscores=True则返回包含元素和分数元组的列表, 参考示例

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_range('test:zset', 0, -1)                      # 从小到大的所有元素, ['a', 'b', 'c']
            rdz.zset_range('test:zset', 0, 1)                       # 从小到大, 索引在0=<index<=1之间的所有元素, ['a', 'b']

            rdz.zset_range('test:zset', 0, -1, desc=True)           # 从大到小的所有元素, ['c', 'b', 'a']
            rdz.zset_range('test:zset', 0, 1, desc=True)            # 从大到小, 索引在0=<index<=1之间的所有元素, ['c', 'b']

            rdz.zset_range('test:zset', 0, -1, withscores=True)     # 返回元素和分数, [('a', 10.0), ('b', 20.0), ('c', 30.0)]

            rdz.zset_range('test:zset', 0, 20, withscores=True, byscore=True)   # start和end指定的是分数范围, [('a', 10.0), ('b', 20.0)]
            rdz.zset_range('test:zset', 20, 0, desc=True, withscores=True, byscore=True)   # 从大到小, 分数在20>=score>=0之间的元素, [('b', 20.0), ('a', 10)]
            rdz.zset_range('test:zset', 0, 20, desc=True, withscores=True, byscore=True)   # 返回[], 没有0>=score>=20的分数

            rdz.zset_range('test:zset', 0, -1, withscores=True, score_cast_func=int) # [('a', 10), ('b', 20), ('c', 30)]
            rdz.zset_range('test:zset', 0, -1, withscores=True, score_cast_func=lambda x: str(x) + '%') # [('a', '10%'), ('b', '20%'), ('c', '30%')]
        """

        return self.get_redis().zrange(name, start, end, desc=desc, withscores=withscores, score_cast_func=score_cast_func, byscore=byscore)

    def zset_revrange(self, name, start, end, withscores=False, score_cast_func=float):
        """
        描述:
            按索引范围, 以分数【从大到小】的顺序从指定有序集合中获取元素
            start和end表示的是【索引】范围, 而不是分数范围
            是zset_range方法的一个简化版本, == zset_range(name, start, end, withscores=False, score_cast_func=float, desc=True, byscore=True)

        参数:
            name:string -redis的健名
            start:int   -起始【索引】位置, 可以是负数 start=<
            end:int     -结束【索引】位置, 可以是负数 <=end
            withscores:bool         -默认只返回元素, 设置withscores=True, 会把分数也一起返回
            score_cast_func:func    -对分数进行数据转换的函数

        返回:
            values:list -有序集合在start-end范围内的元素列表, 如果withscores=True则返回包含元素和分数元组的列表, 参考示例

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_revrange('test:zset', 0, -1)   # ['c', 'b', 'a']
            rdz.zset_revrange('test:zset', 0, 1)    # ['c', 'b']

            rdz.zset_revrange('test:zset', 0, -1, withscores=True) # [('c', 30.0), ('b', 20.0), ('a', 10.0)]

            rdz.zset_revrange('test:zset', 0, -1, withscores=True, score_cast_func=lambda x: str(x) + '%'), [('c', '30%'), ('b', '20%'), ('a', '10%')]
        """
        return self.get_redis().zrevrange(name, start, end, withscores=withscores, score_cast_func=score_cast_func)

    def zset_rangebyscore(self, name, min_score, max_score, start=None, num=None, withscores=False, score_cast_func=float):
        """
        描述:
            按分数范围, 以分数【从小到大】的顺序从指定有序集合中获取元素
            min_score和max_score表示的是【分数】, 而不是索引
            start和num用来指定获取元素的开始和个数, 两者必须同时指定, 否则会引发异常
            在zset_range(name, start, end, desc=False, withscores=False, score_cast_func=float, byscore=True)的基础上增加了start/number两个参数

        参数:
            name:string     -redis的健名
            min_score:int   -最小【分数】min=<
            max_score:int   -最大【分数】<=max
            start:int       -开始【索引】<=start
            num:int         -要获取的元素【个数】
            withscores:bool -默认只返回元素, 设置withscores=True, 会把分数也一起返回
            score_cast_func:func -对分数进行数据转换的函数

        返回:
            values:list -有序集合在min-max范围内的从大到小排序的元素列表
                         如果有序集合不存在/没有符合条件的元素, 返回[]
                         如果withscores=True则返回包含元素和分数元组的列表, 请参考示例

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
            rdz.zset_rangebyscore('test:zset', 20, 50)      # ['b', 'c', 'd', 'e']
            rdz.zset_rangebyscore('test:zset', 20, 50, withscores=True)         # [('b', 20.0), ('c', 30.0), ('d', 40.0), ('e', 50.0)]

            rdz.zset_rangebyscore('test:zset', 20, 50, 0, 1, withscores=True)   # [('b', 20.0)]
            rdz.zset_rangebyscore('test:zset', 20, 50, 1, 2, withscores=True)   # [('c', 30.0), ('d', 40.0)]
            rdz.zset_rangebyscore('test:zset', 20, 50, 1, 10, withscores=True)  # [('c', 30.0), ('d', 40.0), ('e', 50.0)]
        """
        return self.get_redis().zrangebyscore(name, min_score, max_score, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func)

    def zset_revrangebyscore(self, name, max_score, min_score, start=None, num=None, withscores=False, score_cast_func=float):
        """
        描述:
            按分数范围, 以分数【从大到小】的顺序从指定有序集合中获取元素
            min和max表示的是【分数】, 而不是索引
            start和num用来指定获取元素的开始和个数, 两者必须同时指定, 否则会引发异常
            在zset_range(name, start, end, desc=True, withscores=False, score_cast_func=float, byscore=True)的基础上增加了start/number两个参数
            和zset_rangebyscore的区别是: zset_rangebyscore是从小到大, zset_revrangebyscore是从大到小

        参数:
            name:string     -redis的健名
            max_score:int   -最大【分数】<=max
            min_score:int   -最小【分数】min=<
            start:int       -开始【索引】
            num:int         -要获取的元素【个数】
            withscores:bool -默认只返回元素, 设置withscores=True, 会把分数也一起返回
            score_cast_func:func -对分数进行数据转换的函数

        返回:
            values:list -有序集合在min-max范围内从大到小排列的元素列表
                         如果有序集合不存在/没有符合条件的元素, 返回[]
                         如果withscores=True则返回包含元素和分数元组的列表, 请参考示例

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
            rdz.zset_revrangebyscore('test:zset', 50, 20))      # ['e', 'd', 'c', 'b']
            rdz.zset_revrangebyscore('test:zset', 50, 20, withscores=True))     # [('e', 50.0), ('d', 40.0), ('c', 30.0), ('b', 20.0)]

            rdz.zset_revrangebyscore('test:zset', 50, 20, 0, 1, withscores=True)    # [('e', 50.0)]
            rdz.zset_revrangebyscore('test:zset', 50, 20, 1, 2, withscores=True))   # [('d', 40.0), ('c', 30.0)]
            rdz.zset_revrangebyscore('test:zset', 50, 20, 1, 10, withscores=True)   # [('d', 40.0), ('c', 30.0), ('b', 20.0)]
        """
        return self.get_redis().zrevrangebyscore(name, max_score, min_score, start=start, num=num, withscores=withscores, score_cast_func=score_cast_func)

    def zset_remrangebyrank(self, name, start, end):
        """
        描述:
            从有序集合中删除指定【索引】的元素
            start和end索引处的元素也会被删除

        参数:
            name:string     -redis的健名
            start:int       -最小索引 min<=, 可以为负数, -1表示最后一个元素
            end:int         -最大索引 <=max, 可以为负数, -1表示最后一个元素

        返回:
            rem_count:int -成功删除的元素个数, 如果有序集合不存在或者索引超出返回, 返回0

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
            rdz.zset_remrangebyrank('test:zset', 0, 3)      # 4, test:zset -> [('e', 50.0), ('f', 60.0)]
            rdz.zset_remrangebyrank('test:zset', 10, 20)    # 0

            # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
            rdz.zset_remrangebyrank('test:zset', -3, -1)    # 3, test:zset -> [('a', 10.0), ('b', 20.0), ('c', 30.0)]

            rdz.zset_remrangebyrank('test:not-exist', 0, 2)     # 0
        """
        return self.get_redis().zremrangebyrank(name, start, end)

    def zset_remrangebyscore(self, name, min_score, max_score):
        """
         描述:
             根据【分数】范围从有序集合删除元素
             min_score和max_score分数对应的元素也会被删除

         参数:
             name:string   -redis的健名
             min_score:int -最小分数 min<=
             max_score:int -最大分数 <=max

         返回:
             rem_count:int -成功删除的元素个数, 如果有序集合不存在或者索引超出返回, 返回0

         示例:
             # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
             rdz.zset_remrangebyscore('test:zset', 0, 40)           # 4, test:zset -> [('e', 50.0), ('f', 60.0)]
             rdz.zset_remrangebyscore('test:zset', 100, 200)        # 0

             # test:zset={'a': 10, 'b': 20, 'c': 30, 'd': 40, 'e': 50, 'f': 60}
             rdz.zset_remrangebyscore('test:zset', 30, 50)          # 3, test:zset -> [('a', 10.0), ('b', 20.0), ('f', 60.0)]

             rdz.zset_remrangebyscore('test:not-exist', 0, 100)     # 0
         """
        return self.get_redis().zremrangebyscore(name, min_score, max_score)

    def zset_scan(self, name, cursor=0, match=None, count=None, score_cast_func=float):
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
            rdz.zset_add('test:xxx', maps)

            cursor = 0
            count = 0
            while True:
                cursor, data = rdz.zset_scan('test:xxx', cursor=cursor, count=20)  # data为包含分片数据的列表 ->[('k3299', 3299.0), ('k6223', 6223.0),...]
                print(cursor, data)
                count += 1
                if cursor == 0:
                    break
            print(count)  # 迭代了大约490次左右
        """
        return self.get_redis().zscan(name, cursor=cursor, match=match, count=count, score_cast_func=score_cast_func)

    def zset_scan_iter(self, name, match=None, count=None, score_cast_func=float):
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
                rdz.zset_add('test:xxx', maps)

                for i in rdz.set_scan_iter('test:xxx'):
                    print(i) # ('k368', 368.0)
            """
        return self.get_redis().zscan_iter(name, match=match, count=count, score_cast_func=score_cast_func)

    # ------------------------------------------ ext ------------------------------------------
    def get_names(self, pattern='*', **kwargs):
        return self.keys(pattern=pattern, **kwargs)

    def set_value(self, name, value, **kwargs):
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
            rdz.set_value('test:str', 'a')          # str
            rdz.set_value('test:str-number', 1.0)   # number
            rdz.set_value('test:list', [1, 2, 3])   # list
            rdz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})    # hash
            rdz.set_value('test:set', {'x', 'y', 'z'})  # set
            rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset') # zset
        """
        if value is None:
            return False
        type_ = type(value)
        if type_ is str or type_ is int or type_ is float:
            return self.str_set(name, value)
        if type_ is list:
            return self.list_push(name, value)
        if type_ is dict:
            if kwargs.get('type') == 'zset':
                return self.zset_add(name, value)
            else:
                return self.hash_set(name, mapping=value)
        if type_ is set:
            return self.set_add(name, value)
        raise TypeError('only list/dict/set/str/int/float are supported.')

    def get_value(self, name):
        """
        描述:
            返回name对应的键值, 会根据name的类型分类返回

        参数:
            name:string -要获取的键值名

        返回:
            result -获取到的键值结果, 不同的键值类型, 返回的结果类型不一样

        示例:
            rdz.get_value('test:str')           # a
            rdz.get_value('test:str-number')    # 1.0
            rdz.get_value('test:list')          # ['1', '2', '3']
            rdz.get_value('test:hash')          # {'a': '1', 'b': '2', 'c': '3'}
            rdz.get_value('test:set')           # {'x', 'y', 'z'}
            rdz.get_value('test:zset')          # [('x', 1.0), ('y', 2.0), ('z', 3.0)]
        """
        type_ = self.get_type(name)
        if type_ == 'string':
            return self.str_get(name)
        if type_ == 'list':
            return self.list_getall(name)
        if type_ == 'hash':
            return self.hash_getall(name)
        if type_ == 'set':
            return self.set_members(name)
        if type_ == 'zset':
            return self.zset_range(name, 0, -1, withscores=True)

        return None

    # ----------------- list -----------------
    def list_getall(self, name, nx_none=False):
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
            rdz.list_getall('test:numbers')     # ['1', '2', '3', '4', '5', '6']

            rdz.list_getall('test:not-exist')   # 列表不存在, 返回[]
            rdz.list_getall('test:not-exist', nx_none=True) # 列表不存在, 返回None
        """
        r = self.get_redis()
        if nx_none is True and r.exists(name) == 0:
            return None
        return r.lrange(name, 0, -1)

    def list_exists(self, name, value):
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
            rdz.list_exists('test:numbers', 1)      # True
            rdz.list_exists('test:numbers', 10)     # False
            rdz.list_exists('test:not-exist', 1)    # False
        """
        value = str(value)
        for item in self.list_iter(name):  # 遍历列表
            if value == item:
                return True
        return False

    # ----------------- set -----------------
    def set_len(self, name):
        """
        描述
            获取指定集合的元素个数

        参数:
            name:string -redis的键名

        返回:
            count:int -元素个数, 如果集合不存在, 返回0

        示例:
            # test:letters = {'a', 'b', 'c', 'd', 'e'}
            rdz.set_len('test:letters')     # 5
            rdz.set_len('test:not-exist')   # 0
        """
        return self.set_card(name)

    def set_exists(self, name, value):
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
            rdz.set_exists('test:letters', 'a')     # True
            rdz.set_exists('test:letters', 'x')     # False
            rdz.set_exists('test:not-exist', 'a')   # False
        """
        return self.set_ismember(name, value)

    def set_getall(self, name):
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
            rdz.set_getall('test:letters')      # {'a', 'b', 'c', 'd', 'e'}
            rdz.set_getall('test:not-exist')    # set()
        """
        return self.set_members(name)

    # ----------------- zset -----------------
    def zset_index(self, name, member):
        """
        描述:
            获取指定的元素在有序集合中的索引(从0开始)

        参数:
            name:string -redis的键名
            member:str  -指定的元素

        返回:
            index:int -value元素在有序集合中的索引, 如果有序集合不存在/元素不存在返回None

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_index('test:zset', 'a')         # 0
            rdz.zset_index('test:zset', 'b')         # 1
            rdz.zset_index('test:zset', 'x')         # None
            rdz.zset_index('test:not-exist', 'x')    # None
        """
        return self.zset_rank(name, member)

    def zset_len(self, name):
        """
        描述:
            获取指定有序集合元素的数量

        参数:
            name:string -redis的键名

        返回:
            count:int -指定有序集合中元素的数量, 如果zset不存在则返回0

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_len('test:zset')          # 3
            rdz.zset_len('test:not-exist')     # 0
        """
        return self.zset_card(name)

    def zset_getall(self, name, withscores=True):
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
            rdz.zset_getall('test:zset')                    # 返回元素和分数, [('a', 10.0), ('b', 20.0), ('c', 30.0)]
            rdz.zset_getall('test:zset', withscores=False)  # ['a', 'b', 'c']
        """
        return self.zset_range(name, 0, -1, withscores=withscores)

    def zset_exists(self, name, member):
        """
        描述:
            检查指定有序集合中是否存在指定的元素

        参数:
            name:string     -redis的健名
            member:string   -指定的元素

        返回:
            is_exists:bool -如果元素存在返回True, 否则返回False

        示例:
            # test:zset={'a': 10, 'b': 20, 'c': 30}
            rdz.zset_exists('test:zset', 'a')         # True
            rdz.zset_exists('test:zset', 'b')         # True
            rdz.zset_exists('test:zset', 'x')         # False
            rdz.zset_exists('test:not-exist', 'x')    # False
        """

        return self.zset_rank(name, member) is not None

    # ------------------------------------------ lock ------------------------------------------
    def lock(self, name, *, timeout=10, blocking=False, blocking_timeout=2, sleep=0.1, thread_local=True):
        """
        描述:
            Redis的事务是通过MULTI和EXEC命令实现的, 以确保一个客户端在不被其他客户端打断的情况下执行操作命令, 当一个事务执行完毕时, 才会处理其他客户端的命令
            Python客户端以流水线(pipeline)的方式实现的事务, 一次将所有命令都发送给Redis, 同时还要配合WATCH命令, 以确保执行EXEC之前, 操作的键值没有被修改
            这是一种"乐观锁", 不会阻止其他客户端对数据的修改, 而是WATCH到数据变化后, 会取消EXEC操作, 并以重试的方式再次检查操作条件是否满足, 再决定后续的操作
            请注意, WATCH必须和EXEC配合使用才有意义, 单纯的WATCH是不起作用的
            这种WATCH方式, 在有多个客户端操作相同数据时, 可能会造成大量的【重试】, 而且编码也比较麻烦
            锁提供了另外一种方式以解决分布式操作问题
            1.获取锁
                -如果成功返回True, 并且设置过期时间, 以避免客户端异常退出锁一直被占用问题
                -如果锁已经存在, 则等待blocking_timeout, 如果在等待的时间内没有获取到, 则返回False
            2.释放锁
                -如果锁的拥有者发生了变化则会抛出LockError异常
        参数:
            name:string         - 锁的名字
            timeout:int/float   - 锁的存在时间, 单位为秒, 如果超过了存在时间, 即便没有release, 锁也会被释放, 解决因客户端异常退出锁一直被占用问题
            sleep:float         - 每次while循环的sleep时间, 单位为秒, 默认是0.1, 一般不需要设置
            blocking:           - 指定当锁已被占用时, 是否等待, 如果不等待直接返回false, 如果等待, 则等待blocking_timeout
            blocking_timeout:   - 当锁被占用时, 等待的时间, 单位为秒, 如果在等待时间内没有获取到锁, 则返回false
            thread_local:       - 是否将生成的token存在在当前线程的local存储中, 一般不需要设置

        返回:
            lock -redis.lock.Lock对象
                   -也可以将lock对象作为一个上下文管理器, 配合with使用(推荐方式)
                   -可以通过lock.acquire/lock.release方法获取锁, 释放锁

        示例:
            # 1.配合with使用
            try:
                with rdz.lock('my_lock'):
                    rdz.set_value('foo', 'bar')
            except LockError as err:
                print(err)

            # 2.通过acquire/release方法获取锁/释放锁
            lock = rdz.lock('my_lock')
            if lock.acquire(blocking=True):
                rdz.set_value('foo', 'bar')
                lock.release()
        """

        return Lock(self.get_redis(), name=name, timeout=timeout, sleep=sleep, blocking=blocking, blocking_timeout=blocking_timeout, thread_local=thread_local)

    def acquire_lock(self, lock_name, lock_seconds=10, acquire_seconds=10):
        """
        描述:
            获取锁
            Redis的事务是通过MULTI和EXEC命令实现的, 以确保一个客户端在不被其他客户端打断的情况下执行操作命令, 当一个事务执行完毕时, 才会处理其他客户端的命令
            Python客户端以流水线(pipeline)的方式实现的事务, 一次将所有命令都发送给Redis, 同时还要配合WATCH命令, 以确保执行EXEC之前, 操作的键值没有被修改
            这是一种"乐观锁", 不会阻止其他客户端对数据的修改, 而是WATCH到数据变化后, 会取消EXEC操作, 并以重试的方式再次检查操作条件是否满足, 再决定后续的操作
            请注意, WATCH必须和EXEC配合使用才有意义, 单纯的WATCH是不起作用的
            这种WATCH方式, 在有多个客户端操作相同数据时, 可能会造成大量的重试, 而且编码也比较麻烦
            所以提供了acquire_lock/release_lock方法实现分布式锁
            1.获取锁
                -如果成功返回锁的标识符, 并且设置过期时间, 以避免客户端异常退出锁一直被占用问题
                -如果锁已经存在, 则等待acquire_timeout, 如果在等待的时间内没有获取到, 则返回False
                -如果锁已经存在, 但是没有设置过期时间, 则设置过期时间为lock_timeout, 以避免锁一直不可用的问题
            2.释放锁
                -通过标识符判断当前的锁是否已经发生变化, 如果没有变化, 则将锁删除, 如果有变化则返回False

        参数:
            lock_name:string -锁的名称, 可以有多个锁
            lock_seconds:int -锁的过期时间, 超时自动移除锁
            acquire_seconds:int -请求等待时间, 默认10秒, 如果acquire_seconds内没有获取到锁, 返回False

        返回:
            identifier:string|bool -如果获取成功, 返回锁对应的标识符, 如果获取失败, 返回False

        示例:
            def lock_test():
                locked = rdz.acquire_lock('a-lock')
                if locked is False:
                    return False

                redis_conn = rdz.get_redis()
                pipe = redis_conn.pipeline(True)
                try:
                    pipe.set('a', 1)
                    pipe.set('b', 2)
                    pipe.execute()
                finally:
                    redis_conn.release_lock('a-lock', locked)

        """
        warnings.warn('This method is deprecated. pls use lock method', DeprecationWarning, stacklevel=2)

        r = self.get_redis()
        identifier = str(uuid.uuid4())  # 释放锁时检查
        lock_name = gen_lock_name(lock_name)
        lock_seconds = int(math.ceil(lock_seconds))  # 整数
        end = time.time() + acquire_seconds

        while time.time() < end:
            if r.setnx(lock_name, identifier):  # 如果lockname不存在, 设置lockname&过期时间, 并返回identifier
                r.expire(lock_name, lock_seconds)
                return identifier
            elif r.ttl(lock_name) == -1:  # 如果lockname没有设置到期时间, 则设置超时时间, 避免一直lock
                r.expire(lock_name, lock_seconds)
            time.sleep(0.01)
        return False

    def release_lock(self, lockname, identifier):
        """
        描述:
            释放锁

        参数:
            lockname:string -要释放锁的名称
            identifier:string -要释放锁的标识符

        返回:
            result:bool -如果释放成功返回True, 否则返回False

        示例:
            # 请参考 acquire_lock
        """
        warnings.warn('This method is deprecated. pls use lock method', DeprecationWarning, stacklevel=2)

        pipe = self.get_redis().pipeline(True)
        lockname = gen_lock_name(lockname)
        while True:
            try:
                pipe.watch(lockname)  # 通过watch确保lockname没有被改变过
                if pipe.get(lockname) == identifier:  # 判断锁标识符是否发生变化
                    pipe.multi()
                    pipe.delete(lockname)
                    pipe.execute()  # execute中会调用unwatch
                    return True  # 释放成功
                pipe.unwatch()
                break
            except redis.exceptions.WatchError:
                pass

        return False  # 失去了锁

    # ------------------------------------------ pubsub ------------------------------------------
    def get_pubsub(self):
        """
        描述:
            返回一个发布/订阅对象, 可以订阅频道并收听发布到的消息

        返回L
            pubsub:PubSub - PubSub订阅发布对象

        示例:
            pubsub = rdz.get_pubsub()
            pubsub.subscribe('ws:channel')
        """
        return self.redis_ins.pubsub()

    def publish(self, channel, message, **kwargs):
        """
        描述:
            向指定的频道发送消息

        参数:
            channel:string -指定的频道/频道列表
            message:string -要发送的消息

        示例:
            rdz.publish('channel1','This is a message') # 向channel1发送消息
        """
        channel_type = type(channel)
        if channel_type is list or channel_type is tuple:
            for chl in channel:
                self.get_redis().publish(chl, message, **kwargs)
        else:
            return self.get_redis().publish(channel, message, **kwargs)

    def subscribe(self, channels, callback, thread=False, thread_kwargs=None):
        """
        描述:
            订阅一个或多个频道, 当有消息发布到指定频道时, callback函数将会被回掉以处理消息
            如果直接订阅频道, 代码会被block住, 不往下运行, 所以提供了thead参数, 用于以线程的方式处理订阅

        参数:
            channels:string|list -要订阅的一个或多个频道
            callback:func -回调函数, 如果callback返回False, 则退订
            thread:bool -如果thread=True, 将会启动一个线程处理订阅

        示例:
            def consumer(msg):  # 消息回调函数, msg是收到的消息对象
                data = msg.get('data')
                if type(data) == bytes:
                    data = data.decode('utf-8')
                if data == 'exit':
                    return False    # 返回False以退订
                print(msg)


            rdz.subscribe(['channel1', 'channel2'], consumer, thread=True) # 已线程的方式订阅'channel1', 'channel2'两个频道
            print('thread != True 则不会运行到此行代码')
        """
        if thread is True:
            thread_kwargs = thread_kwargs or {}
            threading.Thread(target=subscribe, args=(self, channels, callback), **thread_kwargs).start()
        else:
            subscribe(self, channels, callback)


__version__ = '0.6'
