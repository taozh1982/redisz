## 关于

`redisz`是基于`redis-py`封装的redis操作函数集合, 在原有函数的基础上, 进行了封装和扩展, 并且完善了代码注释和测试用例。

## 代码示例

```
from redisz import Redisz


rdz = Redisz('redis://10.20.30.40:6379')
# -cluster
# rdz = Redisz(cluster=True, startup_nodes=[{'host': '10.20.30.40', 'port': 6379},
#                                                   {'host': '10.20.30.40', 'port': 6379},
#                                                   {'host': '10.20.30.40', 'port': 6379}])
# -sentinel
# rdz = Redisz(sentinel=True, sentinels=[{'host': '10.20.30.40', 'port': 26379}, {'host': '10.20.30.50', 'port': 26379}])

# set
rdz.set_value('test:str', 'a')
rdz.set_value('test:str-number', 1.0)
rdz.set_value('test:list', [1, 2, 3])
rdz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})
rdz.set_value('test:set', {'x', 'y', 'z'})
rdz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')
rdz.list_push('test:{numbers}1', ['1', '2'])  # cluster hash tag
rdz.list_push('test:{numbers}2', ['3', '4'])  # cluster hash tag

# get
print('str:=', rdz.get_value('test:str'))
print('str:number=', rdz.get_value('test:str-number'))
print('str:list=', rdz.get_value('test:list'))
print('str:hash=', rdz.get_value('test:hash'))
print('str:set=', rdz.get_value('test:set'))
print('str:zset=', rdz.get_value('test:zset'))
print(rdz.get_names())

# lock
# -method1
with rdz.lock('my_lock'):
    rdz.set_value('foo', 'bar')
# -method2
lock = rdz.lock('my_lock')
if lock.acquire(blocking=True):
    rdz.set_value('foo', 'bar')
    lock.release()

rdz.close()
```

## 函数汇总

### 全局函数

| 操作 | 函数 |
| ------ | ------ |
|查询| get_type/ get_names / keys|
|包含| exists|
|删除| delete|
|重命名| rename|
|存在时间| ttl / expire / expireat / persist|
|排序| sort|
|键值操作| get_value / set_value|

### 数据函数

#### 添加/设置

| 类型 | 函数 |
| ------ | ------ |
|string| str_set / str_mset|
|list| list_push / list_insert|
|hash| hash_set / hash_mset|
|set| set_add|
|zset| zset_add|

#### 删除

| 类型 | 函数 |
| ------ | ------ |
|string| -|
|list| list_pop / list_rem / list_trim / list_bpop|
|hash| hash_del|
|set| set_pop / set_rem|
|zset| zset_rem / zset_remrangebyrank / zset_remrangebyscore|

#### 修改

| 类型 | 函数 |
| ------ | ------ |
|string| str_append / str_getset / str_setrange|
|list| list_set|
|hash| hash_set / hash_mset|
|set| -|
|zset| zset_add|

#### 查询/获取

| 类型 | 函数 |
| ------ | ------ |
|string| str_get / str_mget / str_getrange|
|list|  list_getall / list_range|
|hash| hash_get / hash_mget / hash_keys / hash_values/ hash_getall|
|set| set_members / set_getall|
|zset| zset_range / zset_revrange / zset_rangebyscore / zset_revrangebyscore / zset_getall|

#### 长度

| 类型 | 函数 |
| ------ | ------ |
|string| str_len|
|list| list_len|
|hash| hash_len|
|set| set_card / set_len|
|zset| zset_card / zset_count / zset_len|

#### 自增/自减

| 类型 | 函数 |
| ------ | ------ |
|string| str_incr / str_decr / str_incrfloat / str_decrfloat|
|list| -|
|hash| hash_incr / hash_decr / hash_incrfloat / hash_decrfloat|
|set| -|
|zset| zset_incr / zset_decr|

#### 索引

| 类型 | 函数 |
| ------ | ------ |
|string| -|
|list| list_index|
|hash| -|
|set| -|
|zset| zset_rank / zset_index|

#### 遍历&迭代

| 类型 | 函数 |
| ------ | ------ |
|string| -|
|list| list_iter|
|hash| hash_scan / hash_scan_iter|
|set| set_scan / set_scan_iter|
|zset| zset_scan / zset_scan_iter

#### 包含

| 类型 | 函数 |
| ------ | ------ |
|string| -|
|list| list_exists|
|hash| hash_exists|
|set| set_ismember / set_exists|
|zset| zset_exists|

### 消息订阅

| 操作 | 函数 |
| ------ | ------ |
|发布消息| publish|
|订阅频道| subscribe|

### 锁

| 操作 | 函数 |
| ------ | ------ |
|锁| lock|

## 注意事项

- 如果redis是 [Cluster](https://redis.io/docs/manual/scaling/) 部署模式，必须使用`hash tag`才能进行多键值操作(str_mset/str_mget/list_bpop/...)

## 版本

- **0.6** `2023/09/01`
    - [A] 添加对redis哨兵模式的支持
- **0.5** `2022/09/01`
    - [A] 添加对redis集群的支持
- **0.3.1** `2022/06/01`
    - [A] 添加`lock`方法以实现分布式锁相关操作(acquire/release)
    - [D] 废弃~~acquire_lock/release_lock~~方法，改由`lock`方法实现锁相关操作
- **0.3.0** `2022/05/12`
    - [C] 由函数模式改为了类&对象模式, 不同的类对象, 可以操作不同的redis
- **0.2.1** `2022/04/28`
    - [C] 将初始化方法`init_redis`的参数改为了url模式
- **0.2** `2022/04/27`
    - [A] 添加`acquire_lock/release_lock`分布式锁函数
- **0.1** `2022/04/01`
    - 发布