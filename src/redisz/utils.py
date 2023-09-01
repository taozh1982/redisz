from redis.cluster import ClusterNode


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


def bytes_to_str(value):
    if type(value) is bytes:
        return value.decode('utf-8')
    return value


def create_cluster_nodes(startup_nodes):
    nodes = []
    for item in startup_nodes:
        item_type = type(item)
        if item_type is ClusterNode:
            nodes.append(item)
        else:
            node = {'host': None, 'port': 6379}
            if item_type is str:
                node['host'] = item
            elif item_type is dict:
                node.update(item)
            if node.get('host') is not None:
                nodes.append(ClusterNode(node.get('host'), node.get('port')))
    return nodes


def create_sentinel_nodes(sentinels):
    nodes = []
    for item in sentinels:
        item_type = type(item)
        if item_type is tuple:
            nodes.append(item)
        else:
            node = {'host': None, 'port': 26379}
            if item_type is str:
                node['host'] = item
            elif item_type is dict:
                node.update(item)
            if node.get('host') is not None:
                nodes.append((node.get('host'), node.get('port')))
    return nodes
