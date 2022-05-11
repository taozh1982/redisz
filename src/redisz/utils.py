def get_list_args(keys, args):
    try:
        iter(keys)
        if isinstance(keys, (bytes, str)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args and len(args) > 0:
        if isinstance(args[0], (tuple, list)):
            keys.extend(*args)
        else:
            keys.extend(args)
    return keys


def gen_lock_name(lock_name):
    return 'redisz-lock:' + lock_name


def subscribe(rdz, channels, callback):
    pubsub = rdz.get_pubsub()
    pubsub.subscribe(channels)
    for msg in pubsub.listen():
        if callback(msg) is False:
            pubsub.unsubscribe(channels)
