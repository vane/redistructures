#!/usr/bin/env python
# -*- coding: utf-8 -*-
from redistructures import Struct

def test_dict():
    print('-'*10, 'Dictionary', '-'*10)
    d = Struct.dictionary()
    d['x'] = 'y'
    d['w'] = 'v'
    print(d['x'])
    print('x' in d)
    print('q' in d)
    for k in d.keys():
        print(k)
    for v in d.values():
        print(v)
    for k, v in d.items():
        print(k, v)
    print('-'*50)

def test_set():
    print('-'*10, 'Set', '-'*10)
    s1 = Struct.set('s1')
    s2 = Struct.set('s2')
    s1.add(1)
    s1.add(2)
    s1.add(3)
    s2.add(3)
    s2.add(4)
    s2.add(5)
    print(s1.pyset())
    print(s2.pyset())
    print(s1 - s2, s1 + s2, len(s1), len(s2))
    for v in s2:
        print(v)
    print('-'*50)

def test_counter():
    print('-'*10, 'Counter', '-'*10)
    c = Struct.counter()
    print(c.incr(padding=10))
    print(c.incr(padding=10))
    print(c.decr(padding=10))
    print(c.value())
    print('-'*50)

def test_queue():
    print('-'*10, 'Queue', '-'*10)
    q = Struct.queue()
    q.add('x')
    q.add('y')
    q.add('z')
    k = q.get()
    while k:
        print(k)
        k = q.get(timeout=1)
    print('-'*50)

if __name__ == '__main__':
    test_dict()
    test_set()
    test_counter()
    test_queue()
