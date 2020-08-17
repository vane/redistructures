redistructures ?
====

### Description
Dictionary, List, Set, Queue, Counter implemented in python with redis data store under the hood

### Examples
All examples in `example/structures.py`  

Dictionary example :  
```python
from redistructures import Struct
d = Struct.dictionary('hey')
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
```
output  
```
b'y'
True
False
b'dict:x'
b'dict:w'
b'y'
b'v'
b'dict:x' b'y'
b'dict:w' b'v'
```
After next run 
```
d = Struct.dictionary('hey')
for k, v in d.items():
    print(k, v)
```
output
```  
b'dict:x' b'y'
b'dict:w' b'v'
```
