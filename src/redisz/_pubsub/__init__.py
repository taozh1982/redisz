import threading

from .. import get_redis, get_pubsub


def publish(channel, message, **kwargs):
    """
    描述:
        向指定的频道发送消息

    参数:
        channel:string -指定的频道
        message:string -要发送的消息

    示例:
        publish('channel1','This is a message') # 向channel1发送消息
    """
    return get_redis().publish(channel, message, **kwargs)


def subscribe(channels, callback, thread=False):
    """
    描述:
        订阅一个或多个频道，当有消息发布到指定频道时，callback函数将会被回掉以处理消息
        如果直接订阅频道，代码会被block住，不往下运行，所以提供了thead参数，用于以线程的方式处理订阅

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


        subscribe(['channel1', 'channel2'], consumer, thread=True) # 已线程的方式订阅'channel1', 'channel2'两个频道
        print('thread != True 则不会运行到此行代码')
    """
    if thread is True:
        threading.Thread(target=_subscribe, args=(channels, callback)).start()
    else:
        _subscribe(channels, callback)


def _subscribe(channels, callback):
    pubsub = get_pubsub()
    pubsub.subscribe(channels)
    for msg in pubsub.listen():
        if callback(msg) is False:
            pubsub.unsubscribe(channels)
