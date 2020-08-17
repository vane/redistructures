#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis

class Connection:
    REDIS = None
    HOST = "localhost"
    PORT = 6379
    DB = 0
    @classmethod
    def get_connection(cls):
        if not Connection.REDIS:
            Connection.REDIS = redis.StrictRedis(host=Connection.HOST,
                                                 port=Connection.PORT,
                                                 db=Connection.DB)
        return Connection.REDIS

class Struct:
    @staticmethod
    def set_iterator(key="set"):
        """Return redis set iterator based on provided key"""
        return SetIterator(connection=Connection.get_connection(), key=key)

    @staticmethod
    def set(key="set"):
        """Return redis set based on provided key"""
        return Set(connection=Connection.get_connection(), key=key)

    @staticmethod
    def dictionary(key="dict"):
        """Return redis key / value structure"""
        return Dict(connection=Connection.get_connection(), key=key)

    @staticmethod
    def queue(key="queue"):
        """Return redis queue based on list lpush/brpop"""
        return Queue(connection=Connection.get_connection(), key=key)

    @staticmethod
    def counter(key="counter"):
        return Counter(connection=Connection.get_connection(), key=key)


class Queue:
    def __init__(self, connection, key="queue"):
        self._key = key
        self._conn = connection

    @property
    def key(self):
        return self._key

    def get(self, timeout=0):
        """Blocking atomic get from redis queue"""
        return self._conn.brpop(self._key, timeout=timeout)

    def add(self, value):
        """Add to redis queue"""
        return self._conn.lpush(self._key, value)


class Dict:
    """Dictionary on top of redis"""
    def __init__(self, connection, key="dict"):
        self._conn = connection
        self.key = key

    def exists(self, key):
        if self._conn.exists(f"{self.key}:{key}"):
            return True

    def __setitem__(self, key, value):
        self._conn.set(f"{self.key}:{key}", value)
        return value

    def __getitem__(self, key):
        return self._conn.get(f"{self.key}:{key}")

    def __contains__(self, key):
        return self._conn.get(f"{self.key}:{key}")

    def keys(self, wildcard="*"):
        return self._conn.scan_iter(f"{self.key}:{wildcard}")

    def values(self, wildcard="*"):
        iter = self._conn.scan_iter(f"{self.key}:{wildcard}")
        for key in iter:
            yield self._conn.get(key)

    def items(self, wildcard="*"):
        iter = self._conn.scan_iter(f"{self.key}:{wildcard}")
        for key in iter:
            yield key, self._conn.get(key)

    def set(self, key, value):
        self._conn.set(f"{self.key}:{key}", value)
        return value

    def get(self, key):
        return self._conn.get(f"{self.key}:{key}")

    def getcheck(self, key):
        if self.exists(key):
            return self.get(f"{self.key}:{key}")
        return False


class SetIterator:
    """Set iterator on top of redis"""
    def __init__(self, connection, key="set"):
        self._conn = connection
        self._key = key
        self._iter = self._conn.sscan_iter(self._key)

    def __next__(self):
        """Iterator next value"""
        return next(self._iter)

    def next(self):
        """Iterator next value"""
        return next(self._iter)

    def __iter__(self):
        return self


class Set:
    """Set on top of redis"""
    def __init__(self, connection, key="set"):
        self._conn = connection
        self._key = key
        self._iter = None

    @property
    def key(self):
        return self._key

    def add(self, value):
        """Add value to set"""
        self._conn.sadd(self._key, value)

    def remove(self, value):
        """Remove value from set"""
        self._conn.srem(self._key, value)

    def pyset(self):
        return self._conn.smembers(self._key)

    def __len__(self):
        """Set length"""
        return self._conn.scard(self._key)

    def __contains__(self, value):
        """Check if value is in set"""
        return self._conn.sismember(self._key, value)

    def __iter__(self):
        """Return set iterator @see SetIterator"""
        return SetIterator(connection=self._conn, key=self._key)

    def __add__(self, set2):
        """Add two set together based on provided set key"""
        return self._conn.sunion(self._key, set2.key)

    def __sub__(self, set2):
        """Substracts two sets  based on provided set key"""
        return self._conn.sdiff(self._key, set2.key)

    def __repr__(self):
        return "{}".format(self.pyset())


class Counter:
    def __init__(self, connection, key="counter"):
        self._conn = connection
        self._key = key
        if self._conn.exists(key):
            self._count = int(self._conn.get(key))
        else:
            self._count = int(self._conn.set(key, 0))

    @property
    def key(self):
        return self._key

    def value(self, padding=0):
        return ("{0:0"+str(padding)+"d}").format(self._count)

    def __repr__(self):
        return str(self._count)

    def incr(self, padding=0):
        self._count = self._conn.incr(self._key)
        return ("{0:0"+str(padding)+"d}").format(self._count)

    def decr(self, padding=0):
        self._count = self._conn.decr(self._key)
        return ("{0:0"+str(padding)+"d}").format(self._count)