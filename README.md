# blockchain-spark: Blockchain Data Analysis using Spark

[![Auto test](https://github.com/tellery/blockchain-spark/actions/workflows/auto-test.yml/badge.svg?branch=master&event=push)](https://github.com/tellery/blockchain-spark/actions/workflows/auto-test.yml)

## Using this Repo

### Building

We use Maven for building Java UDFs used by Python library.

To compile, run tests, and build jars:

```shell
cd java
mvn package
```

To run Python tests:

```shell
export SPARK_HOME=<location of local Spark installation>
python3 setup.py nosetests
```

### Running

TBD

## Usage

### Contract API

Spark3 context initialization

```Python
sc = pyspark.SparkContext()
spark = pyspark.SQLContext(sc)
spark3 = Spark3(spark.sparkSession)
```

Get the functions and events of a certain contract

```python
contract = spark3.contract(address=..., abi=...)
function = contract.get_function_by_name("function_name")
event = contract.get_event_by_name("event_name")
```

Display the first 100 lines of function call DataFrame

```python
contract.get_function_by_name('atomicMatch_').filter('dt = "2022-01-01"').show()
```

Aggregate on the event DataFrame

```python
df = contract.get_event_by_name('OrdersMatched')
df.select("event_parameter.inputs.*", "dt").createOrReplaceTempView("opensea_order_matched")
spark.sql("""
select dt,count(1),sum(price) from
opensea_order_matched
where dt > "2022-01-01"
group by dt
order by dt
""").show()
```

### Transformer

If your data tables have a different schema, just use the `Transformer` object to perform data transformation

```python

t = spark3.transformer()
df = spark.sql('select * from ethereum.traces limit 100')
function_df = t.parse_trace_to_function(df=df, abi=..., schema=..., name=...)
function_df.show()
```
