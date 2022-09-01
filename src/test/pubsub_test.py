import time

from src import redisz

# rdz = redisz.Redisz(cluster=True, startup_nodes=[{'host': '10.124.5.222', 'port': 6379},
#                                                  {'host': '10.124.5.190', 'port': 6379},
#                                                  {'host': '10.124.5.191', 'port': 6379}])
rdz = redisz.Redisz('localhost')


def consumer(msg):  # 消息回调函数, msg是收到的消息对象
    data = msg.get('data')
    if type(data) == bytes:
        data = data.decode('utf-8')
    if data == 'exit':
        return False  # 返回False以退订
    print(msg)


rdz.subscribe(['channel1', 'channel2'], consumer, True, {'daemon': True})  # 已线程的方式订阅'channel1', 'channel2'两个频道
print('thread != True 则不会运行到此行代码')

index = 0
while True:
    print('------publish message------')
    rdz.publish('channel1', 'This is a message-' + str(index))  # 向channel1发送消息
    index += 1
    time.sleep(1)
