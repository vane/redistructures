redistructures ?
====

### Description
Dictionary, Set, Queue, Counter implemented in python with redis under the hood

### Examples
```python
from redistructures import Struct
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
```
