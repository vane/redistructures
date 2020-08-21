#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid
import unittest
import redistructures

class BaseTest(unittest.TestCase):
    def setUp(self):
        """Set up queue name"""
        self.name = str(uuid.uuid4())
        self.name2 = str(uuid.uuid4())

    def byte_to_str(self, value):
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        return value

    def str_to_byte(self, value):
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return value

class RedistructureQueueTest(BaseTest):

    def tearDown(self):
        """Empty queue"""
        q = redistructures.Struct.queue(key=self.name)
        k = q.get(timeout=1)
        while k:
            k = q.get(timeout=1)

    def testAdd(self):
        q = redistructures.Struct.queue(key=self.name)
        q.add('x')
        assert len(q) == 1

    def testGet(self):
        q = redistructures.Struct.queue(key=self.name)
        value = 'x'
        q.add(value)
        el = q.get(timeout=1)
        assert self.byte_to_str(el[1]) == value


class RedistructureDictTest(BaseTest):

    def tearDown(self):
        """Empty queue"""
        d = redistructures.Struct.dictionary(key=self.name)
        for key in d.keys():
            del d[key]

    def createKey(self, key, value):
        d = redistructures.Struct.dictionary(key=self.name)
        d[key] = value
        return d

    def testSetItem(self):
        d = self.createKey('foo', 'bar')
        assert self.byte_to_str(d['foo']) == 'bar'

    def testExists(self):
        d = self.createKey('foo', 'bar')
        assert d.exists('foo')

    def testContains(self):
        d = self.createKey('foo', 'bar')
        assert ('foo' in d) == True

    def testDelete(self):
        d = self.createKey('foo', 'bar')
        del d['foo']
        assert d.get('foo') == None

    def testKeys(self):
        d = self.createKey('foo', 'bar')
        d.set('bar', 'foo')
        k = list(d.keys())
        foo_key = self.str_to_byte(f'{d.key}:foo')
        bar_key = self.str_to_byte(f'{d.key}:bar')
        assert foo_key in k
        assert bar_key in k

    def testValues(self):
        d = self.createKey('foo', 'bar')
        d.set('bar', 'foo')
        v = list(d.values())
        assert b'foo' in v
        assert b'bar' in v

    def testItems(self):
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
        d = self.createKey('foo', 'bar')
        assert d.getcheck('bar') == False
        assert d.getcheck('foo') == b'bar'

class RedistructureSetTest(BaseTest):

    def tearDown(self):
        s1 = redistructures.Struct.set(key=self.name)
        ps1 = s1.pyset()
        s2 = redistructures.Struct.set(key=self.name2)
        ps2 = s2.pyset()
        for e in ps1:
            s1.remove(e)
        for e in ps2:
            s2.remove(e)

    def testAdd(self):
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        assert b'foo' in s

    def testRemove(self):
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        s.remove('foo')
        assert b'foo' not in s
        assert b'bar' in s

    def testPyset(self):
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        ps = s.pyset()
        assert ps == {b'foo', b'bar'}

    def testIter(self):
        s = redistructures.Struct.set(key=self.name)
        s.add('foo')
        s.add('bar')
        for e in s:
            assert e == b'foo' or e == b'bar'

    def testRemSet(self):
        s1 = redistructures.Struct.set(key=self.name)
        s1.add('foo')
        s1.add('bar')
        s2 = redistructures.Struct.set(key=self.name2)
        s2.add('bar')
        assert s1 - s2 == {b'foo'}

    def testAddSet(self):
        s1 = redistructures.Struct.set(key=self.name)
        s1.add('foo')
        s1.add('bar')
        s2 = redistructures.Struct.set(key=self.name2)
        s2.add('bar')
        s2.add('far')
        assert s1 + s2 == {b'foo', b'bar', b'far'}

class RedistructureListTest(BaseTest):

    def tearDown(self):
        l = redistructures.Struct.list(key=self.name)
        while len(l) > 0:
            l.pop(0)

    def testAppend(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        assert l[0] == b'foo'

    def testInsert(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l.insert(1, 'bar')
        assert l[1] == b'bar'

    def testPop(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l.append('bar')
        l.pop(1)
        assert l[1] == b'bar'

    def testGetItem(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        assert l[0] == b'foo'

    def testSetItem(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        l[1] = 'bar'
        assert l[1] == b'bar'

    def testContains(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert 'bar' not in l
        l.append('bar')
        assert 'bar' in l

    def testLen(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert len(l) == 2

    def testIter(self):
        l = redistructures.Struct.list(key=self.name)
        l.append('foo')
        l.append('foo')
        assert list(l) == [b'foo', b'foo']

class RedistructureCounterTest(BaseTest):

    def tearDown(self):
        c = redistructures.Struct.counter(key=self.name)
        del c

    def testValue(self):
        c = redistructures.Struct.counter(key=self.name)
        assert c.value() == '0'

    def testIncr(self):
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr() == '1'

    def testDecr(self):
        c = redistructures.Struct.counter(key=self.name)
        assert c.decr() == '-1'

    def testIncrPadding(self):
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr(5) == '00001'

    def testDecrPadding(self):
        c = redistructures.Struct.counter(key=self.name)
        c.incr()
        c.incr()
        assert c.decr(5) == '00001'

    def testRemove(self):
        c = redistructures.Struct.counter(key=self.name)
        c.incr()
        c.incr()
        del c
        c = redistructures.Struct.counter(key=self.name)
        assert c.incr() == '1'
