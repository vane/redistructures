#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""redistructure tests"""
import uuid
import unittest
import redistructures
# C0103 - invalid-name
# pylint: disable=C0103

class BaseTest(unittest.TestCase):
    """Base unittest class"""
    def setUp(self):
        """Set up queue name"""
        self.name = str(uuid.uuid4())
        self.name2 = str(uuid.uuid4())

    def byte_to_str(self, value):
        """Convert byte to string"""
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        return value

    def str_to_byte(self, value):
        """Convert string to byte"""
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return value

class RedistructureQueueTest(BaseTest):
    """Test for redistructure.py Queue"""
    def tearDown(self):
        """Empty queue"""
        q = redistructures.Struct.queue(key=self.name)
        k = q.get(timeout=1)
        while k:
            k = q.get(timeout=1)

    def testAdd(self):
        """Test add to queue"""
        q = redistructures.Struct.queue(key=self.name)
        q.add('x')
        assert len(q) == 1

    def testGet(self):
        """Test get queue item"""
        q = redistructures.Struct.queue(key=self.name)
        value = 'x'
        q.add(value)
        el = q.get(timeout=1)
        assert self.byte_to_str(el[1]) == value


class RedistructureDictTest(BaseTest):
    """Test for redistructure.py Dict"""
    def tearDown(self):
        """Empty dict"""
        d = redistructures.Struct.dictionary(key=self.name)
        for key in d.keys():
            del d[key]

    def createKey(self, key, value):
        """Test create dict key"""
        d = redistructures.Struct.dictionary(key=self.name)
        d[key] = value
        return d

    def testSetItem(self):
        """Test set dict key"""
        d = self.createKey('foo', 'bar')
        assert self.byte_to_str(d['foo']) == 'bar'

    def testExists(self):
        """Test dict key exists"""
        d = self.createKey('foo', 'bar')
        assert d.exists('foo')

    def testContains(self):
        """Test dict key contains"""
        d = self.createKey('foo', 'bar')
        assert 'foo' in d

    def testDelete(self):
        """Test delete dict key"""
        d = self.createKey('foo', 'bar')
        del d['foo']
        assert d.get('foo') is None

    def testKeys(self):
        """Test return dict keys list"""
        d = self.createKey('foo', 'bar')
        d.set('bar', 'foo')
        k = list(d.keys())
        foo_key = self.str_to_byte(f'{d.key}:foo')
        bar_key = self.str_to_byte(f'{d.key}:bar')
        assert foo_key in k
        assert bar_key in k

    def testValues(self):
        """Test return dict values list"""
        d = self.createKey('foo', 'bar')
        d.set('bar', 'foo')
        v = list(d.values())
        assert b'foo' in v
        assert b'bar' in v

    def testItems(self):
        """Test return dict key,value pairs"""
        d = self.createKey('foo', 'bar')
        d.set('bar', 'foo')
        foo_key = self.str_to_byte(f'{d.key}:foo')
        foo_kv = (foo_key, b'bar')
        bar_key = self.str_to_byte(f'{d.key}:bar')
        bar_kv = (bar_key, b'foo')
        kv = list(d.items())
        assert foo_kv in kv
        assert bar_kv in kv

    def testGetCheck(self):
        """Test dict getcheck method"""
        d = self.createKey('foo', 'bar')
        assert d.getcheck('bar') is False
        assert d.getcheck('foo') == b'bar'

class RedistructureSetTest(BaseTest):
    """Test for redistructure.py Set"""
    def tearDown(self):
        """Empty set"""
        s1 = redistructures.Struct.set(key=self.name)
        ps1 = s1.pyset()
        s2 = redistructures.Struct.set(key=self.name2)
        ps2 = s2.pyset()
        for e in ps1:
            s1.remove(e)
        for e in ps2:
            s2.remove(e)

    def testAdd(self):
        """Test set add"""
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        assert b'foo' in s

    def testRemove(self):
        """Test set remove"""
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        s.remove('foo')
        assert b'foo' not in s
        assert b'bar' in s

    def testPyset(self):
        """Test set return python set"""
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        ps = s.pyset()
        assert ps == {b'foo', b'bar'}

    def testIter(self):
        """Test set iterator"""
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        for e in s:
            assert e in (b'foo', b'bar')

    def testRemSet(self):
        """Test substraction of two sets"""
        s1 = redistructures.Struct.set(key=self.name)
        s1.add('foo')
        s1.add('bar')
        s2 = redistructures.Struct.set(key=self.name2)
        s2.add('bar')
        assert s1 - s2 == {b'foo'}

    def testAddSet(self):
        """Test adding one set to other"""
        s1 = redistructures.Struct.set(key=self.name)
        s1.add('foo')
        s1.add('bar')
        s2 = redistructures.Struct.set(key=self.name2)
        s2.add('bar')
        s2.add('far')
        assert s1 + s2 == {b'foo', b'bar', b'far'}

class RedistructureListTest(BaseTest):
    """Test for redistructure.py List"""
    def tearDown(self):
        """Empty list"""
        l = redistructures.Struct.list(key=self.name)
        while len(l) > 0:
            l.pop(0)

    def testAppend(self):
        """Test list append"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        assert l[0] == b'foo'

    def testInsert(self):
        """Test list insert with index"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l.insert(1, 'bar')
        assert l[1] == b'bar'

    def testPop(self):
        """Test list pop"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l.append('bar')
        l.pop(1)
        assert l[1] == b'bar'

    def testGetItem(self):
        """Test list get item by index"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        assert l[0] == b'foo'

    def testSetItem(self):
        """Test list set item by index"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l[1] = 'bar'
        assert l[1] == b'bar'

    def testContains(self):
        """Test list contains"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert 'bar' not in l
        l.append('bar')
        assert 'bar' in l

    def testLen(self):
        """Test list size"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert len(l) == 2

    def testIter(self):
        """Test list iterator"""
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert list(l) == [b'foo', b'foo']

class RedistructureCounterTest(BaseTest):
    """Test for redistructure.py Counter"""

    def tearDown(self):
        """Remove counter"""
        c = redistructures.Struct.counter(key=self.name)
        del c

    def testValue(self):
        """Test counter value"""
        c = redistructures.Struct.counter(key=self.name)
        assert c.value() == '0'

    def testIncr(self):
        """Test counter increment"""
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr() == '1'

    def testDecr(self):
        """Test counter decrement"""
        c = redistructures.Struct.counter(key=self.name)
        assert c.decr() == '-1'

    def testIncrPadding(self):
        """Test counter increment with padding"""
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr(5) == '00001'

    def testDecrPadding(self):
        """Test counter decrement with padding"""
        c = redistructures.Struct.counter(key=self.name)
        c.incr()
        c.incr()
        assert c.decr(5) == '00001'

    def testRemove(self):
        """Test counter remove"""
        c = redistructures.Struct.counter(key=self.name)
        c.incr()
        c.incr()
        del c
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr() == '1'
