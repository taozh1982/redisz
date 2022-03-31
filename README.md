## 关于

redisz是基于redis-py封装的，用于redis操作的功能函数集合， 在原有函数的基础上，进行了封装和扩展， 并且完善了代码文档注释和测试用例。

## 代码示例

```
import redisz # 导入

redisz.init_redis('localhost') # 初始化
redisz.set_value('test:str', 'a') # 调用函数
redisz.set_value('test:str-number', 1.0)
redisz.set_value('test:list', [1, 2, 3])
redisz.set_value('test:hash', {'a': 1, 'b': 2, 'c': 3})
redisz.set_value('test:set', {'x', 'y', 'z'})
redisz.set_value('test:zset', {'x': 1, 'y': 2, 'z': 3}, type='zset')

print('str:=', redisz.get_value('test:str'))
print('str:number=', redisz.get_value('test:str-number'))
print('str:list=', redisz.get_value('test:list'))
print('str:hash=', redisz.get_value('test:hash'))
print('str:set=', redisz.get_value('test:set'))
print('str:zset=', redisz.get_value('test:zset'))

print(redisz.get_names())
```

## 函数汇总

### 全局函数

| <div style="min-width:100px">操作</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|查询| get_type / keys|
|包含| exists|
|删除| delete|
|重命名| rename|
|存在时间| ttl / expire / expireat / persist|
|排序| sort|
|键值操作| get_value / set_value|

### 数据函数

#### 添加/设置

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| str_set / str_mset|
|list| list_push / list_insert|
|hash| hash_set / hash_mset|
|set| set_add|
|zset| zset_add|

#### 删除

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| -|
|list| list_pop / list_rem / list_trim / list_bpop|
|hash| hash_del|
|set| set_pop / set_rem|
|zset| zset_rem / zset_remrangebyrank / zset_remrangebyscore|

#### 修改

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| str_append / str_getset / str_setrange|
|list| list_set|
|hash| hash_set / hash_mset|
|set| -|
|zset| zset_add|

#### 查询/获取

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| str_get / str_mget / str_getrange|
|list| list_get / list_range / list_getall|
|hash| hash_get / hash_mget / hash_keys / hash_values/ hash_getall|
|set| set_members / set_getall|
|zset| zset_range / zset_revrange / zset_rangebyscore / zset_revrangebyscore / zset_getall|

#### 长度

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| str_len|
|list| list_len|
|hash| hash_len|
|set| set_card / set_len|
|zset| zset_card / zset_count / zset_len|

#### 自增/自减

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| str_incr / str_decr / str_incrfloat / str_decrfloat|
|list| -|
|hash| hash_incr / hash_decr / hash_incrfloat / hash_decrfloat|
|set| -|
|zset| zset_incr / zset_decr|

#### 索引

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| -|
|list| list_index|
|hash| -|
|set| -|
|zset| zset_rank / zset_index|

#### 遍历&迭代

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| -|
|list| list_iter|
|hash| hash_scan / hash_scan_iter|
|set| set_scan / set_scan_iter|
|zset| zset_scan / zset_scan_iter

#### 包含

| <div style="min-width:100px">类型</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|string| -|
|list| list_exists|
|hash| hash_exists|
|set| set_ismember / set_exists|
|zset| zset_exists|

### 消息订阅

| <div style="min-width:100px">操作</div> | <div style="min-width:500px">函数</div> |
| ------ | ------ |
|发布消息| publish|
|订阅频道| subscribe|

## 版本

+ **0.1**
    + redisz正式发布