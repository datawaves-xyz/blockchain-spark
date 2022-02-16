# blockchain-spark: Spark extension utils for Web3

## How to use

```python
from spark3.transfomer import Transformer

t = Transformer()
df = spark.sql('select * from ethereum.traces limit 100')
df = t.parse_traces_input(df=df, abi='xxxx')
df.show()
```

```python
from spark3.transfomer import Transformer

t = Transformer()
df = spark.sql('select * from ethereum.logs limit 100')
df = t.parse_logs_data(df=df, abi='xxxx')
df.show()
```