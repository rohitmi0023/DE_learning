----------------------Map And FlatMap
```py
map(callable function, iterable)
map(lambda x: x**2, numbers)
```

Flatmap ka bhi calling ka same method hei. 
```py
from itertools import chain
flatted = list(chain.from_iterable(map(lambda x: x.split(), words)))
```

Key differences:

| **Feature**   | **map**                    | **flatMap**                             |
| --------- | ---------------------- | ----------------------------------- |
| output    | 1 output per input     | 0-N outputs per input               |
| Structure | preserves nesting      | Flattens nested result              |
| UseCase   | Simple transformations | Splitting strings, exploding arrays |
| Python    | Built-in map()         | itertools.chain or list comp        |
| PySpark   | rdd.map()              | rdd.flatMap()                       |

Real World UseCases:
1. Tokenizing Text- flatMap
2. Extracting Nested Data- flatMap
3. Simple Math- map




Make a note of something, [[Pandas Notes]], or try [the Importer](https://help.obsidian.md/Plugins/Importer)!
AWS- Quantiphi@123456

