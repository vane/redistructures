#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""datastructures on top of redis"""
import redis

class Connection:
    """Connection to redis server"""
    REDIS = None
    HOST = "localhost"
    PORT = 6379
    DB = 0

    @staticmethod
    def init_connection(host='localhost', port=6379, db=0): #pylint: disable=C0103
        """Initialize redis connection details"""
        Connection.HOST = host
        Connection.PORT = port
        Connection.DB = db

    @classmethod
    def get_connection(cls):
        """Return redis connection if not initialized connects to redis"""
        if not Connection.REDIS:
            Connection.REDIS = redis.StrictRedis(host=Connection.HOST,
                                                 port=Connection.PORT,
                                                 db=Connection.DB)
        return Connection.REDIS

class Struct:
    """Factory to return structures on top of redis"""
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
        """Return redis counter"""
        return Counter(connection=Connection.get_connection(), key=key)

    @staticmethod
    def list_iterator(key="list"):
        """Return list iterator based on provided key"""
        return ListIterator(connection=Connection.get_connection(), key=key)

    @staticmethod
    def list(key="list"):
        """Return redis list based on provided key"""
        return List(connection=Connection.get_connection(), key=key)


class Queue:
    """Queue implementation"""
    def __init__(self, connection, key="queue"):
        self._key = key
        self._conn = connection

    @property
    def key(self):
        """Get queue key"""
        return self._key

    def get(self, timeout=0):
        """Blocking atomic get from redis queue"""
        return self._conn.brpop(self._key, timeout=timeout)

    def add(self, value):
        """Add to redis queue"""
        return self._conn.lpush(self._key, value)

    def __len__(self):
        """Get queue size"""
        return self._conn.llen(self._key)


class Dict:
    """Dictionary implementation"""

    def __init__(self, connection, key="dict"):
        """Constructor"""
        self._conn = connection
        self.key = key

    def exists(self, key):
        """Check if key exists"""
        if self._conn.exists(f"{self.key}:{key}"):
            return True
        return False

    def __setitem__(self, key, value):
        """Set dictionary key,value"""
        self._conn.set(f"{self.key}:{key}", value)
        return value

    def __getitem__(self, key):
        """Get dictionary value based on key"""
        return self._conn.get(f"{self.key}:{key}")

    def __contains__(self, key):
        """Check if dictionary has key"""
        return self._conn.get(f"{self.key}:{key}")

    def __delitem__(self, key):
        """Delete dictionary key"""
        return self._conn.delete(f"{self.key}:{key}")

    def keys(self, wildcard="*"):
        """Get dictionary keys"""
        return self._conn.scan_iter(f"{self.key}:{wildcard}")

    def values(self, wildcard="*"):
        """Get dictionary values"""
        iter = self._conn.scan_iter(f"{self.key}:{wildcard}")
        for key in iter:
            yield self._conn.get(key)

    def items(self, wildcard="*"):
        """Get dictionary key,value pairs"""
        iter = self._conn.scan_iter(f"{self.key}:{wildcard}")
        for key in iter:
            yield key, self._conn.get(key)

    def set(self, key, value):
        """Set dictionary key,value"""
        self._conn.set(f"{self.key}:{key}", value)
        return value

    def get(self, key):
        """Get dictionary value based on key"""
        return self._conn.get(f"{self.key}:{key}")

    def getcheck(self, key):
        """Get dictionary key and returns False if key not exists"""
        if self.exists(key):
            return self.get(key)
        return False


class SetIterator:
    """Set iterator implementation"""
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
        """Iterator instance"""
        return self


class Set:
    """Set implementation"""
    def __init__(self, connection, key="set"):
        self._conn = connection
        self._key = key

    @property
    def key(self):
        """Get set key"""
        return self._key

    def add(self, value):
        """Add value to set"""
        self._conn.sadd(self._key, value)

    def remove(self, value):
        """Remove value from set"""
        self._conn.srem(self._key, value)

    def pyset(self):
        """Return python set"""
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
        """Show set as string"""
        return "{}".format(self.pyset())


class Counter:
    """Counter implementation"""
    def __init__(self, connection, key="counter"):
        """Constructor"""
        self._conn = connection
        self._key = key
        if not self._conn.exists(key):
            self._conn.set(key, 0)
        self._count = int(self._conn.get(key))

    @property
    def key(self):
        """Get counter key"""
        return self._key

    def value(self, padding=0):
        """Get counter value with optional padding"""
        return ("{0:0"+str(padding)+"d}").format(self._count)

    def __repr__(self):
        """Get counter string value"""
        return str(self._count)

    def __del__(self):
        """Remove counter"""
        self._conn.delete(self.key)

    def incr(self, padding=0):
        """Increment counter with optional padding"""
        self._count = self._conn.incr(self._key)
        return ("{0:0"+str(padding)+"d}").format(self._count)

    def decr(self, padding=0):
        """Decrement counter with optional padding"""
        self._count = self._conn.decr(self._key)
        return ("{0:0"+str(padding)+"d}").format(self._count)

class ListIterator:
    """List iterator implementation"""
    def __init__(self, connection, key):
        """Constructor"""
        self._key = key
        self._conn = connection
        self.index = 0
        self.end = self._conn.llen(self._key)
        self.current = None

    def __next__(self):
        """Iterator next value"""
        val = self._conn.lindex(self._key, self.index)
        self.index += 1
        if val:
            return val
        raise StopIteration()

    def next(self):
        """Iterator next value"""
        val = self._conn.lindex(self._key, self.index)
        self.index += 1
        if val:
            return val
        raise StopIteration()

    def __iter__(self):
        """Iterator instance"""
        return self

class List:
    """List implementation"""
    def __init__(self, connection, key='list'):
        """Constructor"""
        self._conn = connection
        self._key = key

    def append(self, value):
        """Append value on the end of list"""
        return self._conn.rpush(self._key, value)

    def insert(self, index, value):
        """Append value to list based on index"""
        return self._conn.lset(self._key, index, value)

    def pop(self, index):
        """Remove value from list"""
        return self._conn.lrem(self._key, index, self[index])

    def __getitem__(self, index):
        """Get value from list based on index"""
        return self._conn.lindex(self._key, index)

    def __setitem__(self, index, value):
        """Set value in list based on index"""
        return self._conn.lset(self._key, index, value)

    def __contains__(self, item):
        """Check if value is in list"""
        return self._conn.execute_command('LPOS', self._key, item)

    def __iter__(self):
        """Return set iterator @see SetIterator"""
        return ListIterator(connection=self._conn, key=self._key)

    def __len__(self):
        """List size"""
        return self._conn.llen(self._key)
