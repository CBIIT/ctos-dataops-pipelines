import redis


def redis_flush(redis_host, redis_password):
    r = redis.Redis(host=redis_host, password=redis_password)
    r.flushall(asynchronous=True)
