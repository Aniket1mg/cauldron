import asyncio
import aioredis
from asyncio import coroutine
from functools import wraps
import json
import hashlib
import inspect
allowed_types_for_caching = [str, int, list, tuple, float, dict, bool]

class RedisCache:
    _pool = None
    _host = None
    _port = None
    _minsize = None
    _maxsize = None
    _lock = asyncio.Semaphore(1)
    _utf8 = 'utf-8'

    @classmethod
    @coroutine
    async def get_pool(cls):
        if not cls._pool:
            with (await cls._lock):
                if not cls._pool:
                    cls._pool = await aioredis.create_pool((cls._host, cls._port), minsize=cls._minsize,
                                                                maxsize=cls._maxsize)
        return (await cls._pool)

    @classmethod
    def connect_v2(cls, host, port, minsize=5, maxsize=10, loop=None):
        """
        Setup a connection pool params
        :param host: Redis host
        :param port: Redis port
        :param loop: Event loop
        """
        cls._host = host
        cls._port = port
        cls._minsize = minsize
        cls._maxsize = maxsize

    @classmethod
    @coroutine
    async def connect(cls, host, port, minsize=5, maxsize=10, loop=asyncio.get_event_loop()):
        """
        Setup a connection pool
        :param host: Redis host
        :param port: Redis port
        :param loop: Event loop
        """
        cls._pool = await aioredis.create_pool((host, port), minsize=minsize, maxsize=maxsize, loop=loop)

    @classmethod
    @coroutine
    async def set_key(cls, key, value, namespace=None, expire=0):
        """
        Set a key in a cache.
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            await redis.set(key, value, expire=expire)

    @classmethod
    @coroutine
    async def increment_value(cls, key, namespace=None):
        # Set a redis key and increment the value by one
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            await redis.incr(key)

    @classmethod
    @coroutine
    async def increment_by_value(cls, key, value:int, namespace=None):
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            await redis.incrby(key, value)

    @classmethod
    @coroutine
    async def set_key_if_not_exists(cls, key, value, namespace=None, expire=0):
        """
        Set a redis key and return True if the key does not exists else return False
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (await cls.get_pool()) as redis:
            if namespace:
                key = cls._get_key(namespace, key)
            return (await redis.set(key, value, expire=expire, exist='SET_IF_NOT_EXIST'))

    @classmethod
    @coroutine
    async def get_key(cls, key, namespace=None):
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            return (await redis.get(key, encoding=cls._utf8))

    @classmethod
    @coroutine
    async def hmget(cls, fields, namespace=''):
        with (await cls.get_pool()) as redis:
            return (await redis.hmget(namespace, *fields))

    @classmethod
    @coroutine
    async def hmset(cls, field, value, namespace=''):
        with (await cls.get_pool()) as redis:
            await redis.hmset(namespace, field, value)

    @classmethod
    @coroutine
    async def delete(cls, key, namespace=None):
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            await redis.delete(key)

    @classmethod
    async def hdel(cls, key, namespace):
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                await redis.hdel(namespace, key)

    @classmethod
    async def hgetall(cls, namespace):
        with (await cls.get_pool()) as redis:
            if namespace is not None:
                return (await redis.hgetall(namespace, encoding=cls._utf8))

    @classmethod
    @coroutine
    async def clear_namespace(cls, namespace) -> int:
        pattern = namespace + '*'
        return (await cls._delete_by_pattern(pattern))

    @classmethod
    @coroutine
    async def _delete_by_pattern(cls, pattern: str) -> int:
        if not pattern:
            return 0
        with (await cls.get_pool()) as redis:
            _keys = await redis.keys(pattern)
            if _keys:
                await redis.delete(*_keys)
        return len(_keys)

    @classmethod
    @coroutine
    async def delete_by_prefix(cls, prefix, namespace=None):
        prefix_with_namespace = cls._get_key(namespace, prefix) if namespace else prefix
        pattern = '{}*'.format(prefix_with_namespace)
        return (await cls._delete_by_pattern(pattern))

    @classmethod
    @coroutine
    async def exit(cls):
        if cls._pool:
            await cls._pool.clear()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ':' + key

    @classmethod
    def asyncio_redis_decorator(cls, name_space=''):
        def wrapped(func):
            @wraps(func)
            async def redis_check(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if isinstance(arg, dict):
                            new_args.append(json.dumps(arg, sort_keys=True))
                        else:
                            new_args.append(arg)
                    _args = str(new_args)
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(cls._utf8)).hexdigest()
                result = await RedisCache.hmget([digest_key], name_space)
                if result and len(result) > 0 and result[0]:
                    return json.loads(result[0].decode(cls._utf8))
                else:
                    result = await func(*args, **kwargs)
                    await RedisCache.hmset(digest_key, json.dumps(result), name_space)
                    return result
            return redis_check
        return wrapped

    @classmethod
    def redis_cache_decorator(cls, name_space='', expire_time=0):
        def wrapped(func):
            @wraps(func)
            async def apply_cache(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if isinstance(arg, dict):
                            new_args.append(json.dumps(arg, sort_keys=True))
                        else:
                            new_args.append(arg)
                    _args = str(new_args)
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(cls._utf8)).hexdigest()
                result = await RedisCache.get_key(digest_key, name_space)
                if result:
                    return json.loads(result)
                result = await func(*args, **kwargs)
                await RedisCache.set_key(digest_key, json.dumps(result), name_space, expire_time)
                return result
            return apply_cache
        return wrapped

    @classmethod
    @coroutine
    async def run_lua(cls, script: str, keys: list, args: list = None, namespace=None):
        args = args or []
        with (await cls.get_pool()) as redis:
            if script:
                if namespace:
                    keys = [cls._get_key(namespace, key) for key in keys]
                return (await redis.eval(script=script, keys=keys, args=args))
            return None

    @classmethod
    @coroutine
    async def scan(cls, pattern_str: str, scan_top_records=10000):
        """
        Function to get all keys using scan in redis matching to pattern_str
        :param pattern_str: keys pattern
        :return: list of all redis keys available in top scan_top_records (default 10000) records
        """
        if pattern_str:
            with (await cls.get_pool()) as redis:
                return (await redis.scan(cursor=0, match=pattern_str, count=scan_top_records))
        return []

    @classmethod
    @coroutine
    async def keys(cls, pattern_str:str):
        """
        Function to get all keys in redis matching to pattern_str
        :param pattern_str: keys pattern
        :return: list of redis keys
        """
        if pattern_str:
            with (await cls.get_pool()) as redis:
                return (await redis.keys(pattern_str))
        return []

    @classmethod
    def redis_cache_decorator_v2(cls, name_space='', expire_time=0):
        def wrapped(func):
            @wraps(func)
            async def apply_cache(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if type(arg) in allowed_types_for_caching:
                            if isinstance(arg, dict):
                                new_args.append(json.dumps(arg, sort_keys=True))
                            else:
                                new_args.append(arg)
                    _args = str(new_args)
                _kwargs = {}
                for key in kwargs:
                    if type(kwargs[key]) in allowed_types_for_caching:
                        _kwargs[key]= kwargs[key]

                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': _kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(cls._utf8)).hexdigest()
                result = await RedisCache.get_key(digest_key, name_space)
                if result:
                    return json.loads(result)
                if inspect.isgeneratorfunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                await RedisCache.set_key(digest_key, json.dumps(result), name_space, expire_time)
                return result

            return apply_cache

        return wrapped

    @classmethod
    @coroutine
    async def lpush(cls, namespace, key, value, *values):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.lpush(key, value, *values))

    @classmethod
    @coroutine
    async def llen(cls, namespace, key):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.llen(key))

    @classmethod
    @coroutine
    async def lrange(cls, namespace, key, start, stop):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.lrange(key, start, stop))


class RedisCacheV2:
    _utf8 = 'utf-8'

    def __init__(self, host, port, minsize=5, maxsize=10):
        self._host = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self._lock = asyncio.Semaphore(1)
        self._pool = None

    @coroutine
    async def get_pool(self):
        if not self._pool:
            with (await self._lock):
                if not self._pool:
                    self._pool = await aioredis.create_pool((self._host, self._port), minsize=self._minsize,
                                                                 maxsize=self._maxsize)
        return (await self._pool)

    @coroutine
    async def set_key(self, key, value, namespace=None, expire=0):
        """
        Set a key in a cache.
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (await self.get_pool()) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            await redis.set(key, value, expire=expire)

    @coroutine
    async def increment_value(self, key, namespace=None):
        # Set a redis key and increment the value by one
        with (await self.get_pool()) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            await redis.incr(key)

    @coroutine
    async def increment_by_value(self, key, value: int, namespace=None):
        with (await self.get_pool()) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            await redis.incrby(key, value)

    @coroutine
    async def set_key_if_not_exists(self, key, value, namespace=None, expire=0):
        """
        Set a redis key and return True if the key does not exists else return False
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (await self.get_pool()) as redis:
            if namespace:
                key = self._get_key(namespace, key)
            return (await redis.set(key, value, expire=expire, exist='SET_IF_NOT_EXIST'))

    @coroutine
    async def get_key(self, key, namespace=None):
        with (await self.get_pool()) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            return (await redis.get(key, encoding=self._utf8))

    @coroutine
    async def hmget(self, fields, namespace=''):
        with (await self.get_pool()) as redis:
            return (await redis.hmget(namespace, *fields))

    @coroutine
    async def hmset(self, field, value, namespace=''):
        with (await self.get_pool()) as redis:
            await redis.hmset(namespace, field, value)

    @coroutine
    async def delete(self, key, namespace=None):
        with (await self.get_pool()) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            await redis.delete(key)

    async def hdel(self, key, namespace):
        with (await self.get_pool()) as redis:
            if namespace is not None:
                await redis.hdel(namespace, key)

    async def hgetall(self, namespace):
        with (await self.get_pool()) as redis:
            if namespace is not None:
                return (await redis.hgetall(namespace, encoding=self._utf8))

    @coroutine
    async def clear_namespace(self, namespace) -> int:
        pattern = namespace + '*'
        return (await self._delete_by_pattern(pattern))

    @coroutine
    async def _delete_by_pattern(self, pattern: str) -> int:
        if not pattern:
            return 0
        with (await self.get_pool()) as redis:
            _keys = await redis.keys(pattern)
            if _keys:
                await redis.delete(*_keys)
        return len(_keys)

    @coroutine
    async def delete_by_prefix(self, prefix, namespace=None):
        prefix_with_namespace = self._get_key(namespace, prefix) if namespace else prefix
        pattern = '{}*'.format(prefix_with_namespace)
        return (await self._delete_by_pattern(pattern))

    @coroutine
    async def exit(self):
        if self._pool:
            await self._pool.clear()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ':' + key

    def asyncio_redis_decorator(self, name_space=''):
        def wrapped(func):
            @wraps(func)
            async def redis_check(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if isinstance(arg, dict):
                            new_args.append(json.dumps(arg, sort_keys=True))
                        else:
                            new_args.append(arg)
                    _args = str(new_args)
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(self._utf8)).hexdigest()
                result = await self.hmget([digest_key], name_space)
                if result and len(result) > 0 and result[0]:
                    return json.loads(result[0].decode(self._utf8))
                else:
                    result = await func(*args, **kwargs)
                    await self.hmset(digest_key, json.dumps(result), name_space)
                    return result

            return redis_check

        return wrapped

    def redis_cache_decorator(self, name_space='', expire_time=0):
        def wrapped(func):
            @wraps(func)
            async def apply_cache(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if isinstance(arg, dict):
                            new_args.append(json.dumps(arg, sort_keys=True))
                        else:
                            new_args.append(arg)
                    _args = str(new_args)
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(self._utf8)).hexdigest()
                result = await self.get_key(digest_key, name_space)
                if result:
                    return json.loads(result)
                result = await func(*args, **kwargs)
                await self.set_key(digest_key, json.dumps(result), name_space, expire_time)
                return result

            return apply_cache

        return wrapped

    @coroutine
    async def run_lua(self, script: str, keys: list, args: list = None, namespace=None):
        args = args or []
        with (await self.get_pool()) as redis:
            if script:
                if namespace:
                    keys = [self._get_key(namespace, key) for key in keys]
                return (await redis.eval(script=script, keys=keys, args=args))
            return None

    @classmethod
    @coroutine
    async def scan(cls, pattern_str: str, scan_top_records=10000):
        """
        Function to get all keys using scan in redis matching to pattern_str
        :param pattern_str: keys pattern
        :return: list of all redis keys available in top scan_top_records (default 10000) records
        """
        if pattern_str:
            with (await cls.get_pool()) as redis:
                return (await redis.scan(cursor=0, match=pattern_str, count=scan_top_records))
        return []


    @coroutine
    async def keys(self, pattern_str: str):
        """
        Function to get all keys in redis matching to pattern_str
        :param pattern_str: keys pattern
        :return: list of redis keys
        """
        if pattern_str:
            with (await self.get_pool()) as redis:
                return (await redis.keys(pattern_str))
        return []

    def redis_cache_decorator_v2(self, name_space='', expire_time=0):
        def wrapped(func):
            @wraps(func)
            async def apply_cache(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    new_args = []
                    for arg in args[1:]:
                        if type(arg) in allowed_types_for_caching:
                            if isinstance(arg, dict):
                                new_args.append(json.dumps(arg, sort_keys=True))
                            else:
                                new_args.append(arg)
                    _args = str(new_args)
                _kwargs = {}
                for key in kwargs:
                    if type(kwargs[key]) in allowed_types_for_caching:
                        _kwargs[key] = kwargs[key]

                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': _kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(self._utf8)).hexdigest()
                result = await self.get_key(digest_key, name_space)
                if result:
                    return json.loads(result)
                result = await func(*args, **kwargs)
                await self.set_key(digest_key, json.dumps(result), name_space, expire_time)
                return result

            return apply_cache

        return wrapped

    @classmethod
    @coroutine
    async def lpush(cls, namespace, key, value, *values):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.lpush(key, value, *values))

    @classmethod
    @coroutine
    async def llen(cls, namespace, key):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.llen(key))

    @classmethod
    @coroutine
    async def lrange(cls, namespace, key, start, stop):
        key = cls._get_key(namespace, key)
        with (await cls.get_pool()) as redis:
            return (await redis.lrange(key, start, stop))
