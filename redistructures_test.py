#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid
import unittest
import redistructures

class BaseTest(unittest.TestCase):
    def byte_to_str(self, value):
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        return value

    def str_to_byte(self, value):
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return value

class RedistructureQueueTest(BaseTest):
    def setUp(self):
        """Set up queue name"""
        self.name = str(uuid.uuid4())

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
    def setUp(self):
        """Set up key name"""
        self.name = str(uuid.uuid4())

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
