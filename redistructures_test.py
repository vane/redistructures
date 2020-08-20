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

    def testSetItem(self):
        key = 'foo'
        value = 'bar'
        d = redistructures.Struct.dictionary(key=self.name)
        d[key] = value
        assert self.byte_to_str(d[key]) == value
